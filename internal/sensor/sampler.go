package sensor

// The sampler is why Device.Read may block. A bus transaction takes
// milliseconds when it goes well and never returns when a slave holds
// the line low, and the goroutines that answer questions — a protocol
// engine mid-reception, a console session — can afford neither. So one
// goroutine owns each device, reads it on a cadence, and leaves the
// answer where anyone may pick it up.

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

// readBound is how long one sample may take before the sampler stops
// waiting for it. Generous for any part — a conversion is tens of
// milliseconds, not seconds — and short enough that a daemon asked to
// stop does not visibly hang. It bounds a driver that waits on its
// context; a driver blocked in an uninterruptible syscall is beyond
// any caller's reach, which is why Device.Read makes that the
// driver's own responsibility.
const readBound = 5 * time.Second

// DefaultInterval is the cadence a sensor that names none is read at.
// Slow enough to cost nothing on a bus shared with anything else,
// brisk enough that a telemetry answer is not archaeology.
const DefaultInterval = 30 * time.Second

// openBackoff is how long a sampler waits before trying a part that
// would not open again, doubling to openBackoffMax. A part is often
// absent for a reason that goes away — a permission the unit gains, a
// board plugged in after boot — and a daemon that only tries at start
// makes the operator restart it to be believed.
const (
	openBackoff    = time.Second
	openBackoffMax = time.Minute
)

// readFailuresBeforeReopen is how many samples in a row may fail
// before the part is closed and opened again. One failure is usually
// the bus being busy and says nothing; a run of them is a descriptor
// that has outlived its device — a board unplugged, an adapter reset
// — which no amount of reading will mend. Reopening on the first
// would thrash a bus that is merely contended.
const readFailuresBeforeReopen = 3

// Sampler owns one device and reads it on its own goroutine. Every
// other goroutine sees only the last answer, through Latest.
type Sampler struct {
	open  func() (Device, error)
	every time.Duration
	log   *zap.Logger

	// mu guards the cache alone, and is never held across a Read —
	// that is the whole point: a reader waits for a slice copy, never
	// for the bus. Plain, not RW: one writer a cadence apart and
	// readers that copy three values contend over nothing.
	mu     sync.Mutex
	last   []Reading
	opened bool
	// cause is why the part is not answering, kept because a journal
	// on an embedded host rotates and a status line is what the
	// operator actually reads.
	cause string
}

// NewSampler prepares a sampler. It does not open anything: opening is
// a bus transaction, and the caller is holding a lock. A cadence of
// zero takes the default.
func NewSampler(open func() (Device, error), every time.Duration, log *zap.Logger) *Sampler {
	if every <= 0 {
		every = DefaultInterval
	}
	return &Sampler{open: open, every: every, log: log}
}

// Opened reports whether the part is answering. False is a part that
// has not opened yet or would not open, which a console must say
// rather than showing an empty reading list as a quiet part.
func (s *Sampler) Opened() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.opened
}

// Cause is why the part is not answering, empty while it is. It is
// the last refusal, not a history: what an operator needs is the
// reason it is failing now.
func (s *Sampler) Cause() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cause
}

// Run keeps the part read for as long as the context lasts: it opens,
// retrying while the part refuses, reads until the context ends or the
// part stops answering, and opens again. A descriptor that has
// outlived its device is not something reading harder fixes.
func (s *Sampler) Run(ctx context.Context) {
	for {
		dev := s.opening(ctx)
		if dev == nil {
			return // the context ended before the part ever answered
		}
		s.reading(ctx, dev)
		s.closing(dev)
		if ctx.Err() != nil {
			return
		}
	}
}

// reading samples until the context ends or the part has refused
// readFailuresBeforeReopen times running. It samples once before
// waiting, so a relay asked for telemetry in the first seconds of its
// life has something true to say.
func (s *Sampler) reading(ctx context.Context, dev Device) {
	t := time.NewTicker(s.every)
	defer t.Stop()
	failures := 0
	for {
		err := s.sample(ctx, dev)
		if err == nil {
			failures = 0
		} else if failures++; failures >= readFailuresBeforeReopen {
			s.mu.Lock()
			s.cause = err.Error()
			s.mu.Unlock()
			if s.log != nil && ctx.Err() == nil {
				s.log.Error("sensor stopped answering — opening it again",
					zap.Error(err), zap.Int("failures", failures))
			}
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// closing gives the device back and says the part is not answering.
func (s *Sampler) closing(dev Device) {
	s.mu.Lock()
	s.opened = false
	s.mu.Unlock()
	if err := dev.Close(); err != nil && s.log != nil {
		s.log.Warn("sensor close", zap.Error(err))
	}
}

// opening returns a device, or nil when the context ended first. It
// keeps trying because the reasons a part refuses are usually
// temporary, and it says so once per attempt so the journal shows
// what is being waited for.
func (s *Sampler) opening(ctx context.Context) Device {
	wait := openBackoff
	for {
		dev, err := s.open()
		if err == nil {
			s.mu.Lock()
			s.opened, s.cause = true, ""
			s.mu.Unlock()
			return dev
		}
		s.mu.Lock()
		said := s.cause == err.Error()
		s.cause = err.Error()
		s.mu.Unlock()
		// Said once, and again only when the reason changes: an
		// absent part would otherwise write an error a minute for as
		// long as it is absent, into a journal that may be in RAM.
		if s.log != nil && !said {
			s.log.Error("sensor not open", zap.Error(err), zap.Duration("retry_in", wait))
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(wait):
		}
		if wait *= 2; wait > openBackoffMax {
			wait = openBackoffMax
		}
	}
}

// sample takes one reading set. A failure keeps what was there: the
// readings carry the moment they were taken, so a consumer can see
// the answer going stale, which is more use than an empty one.
func (s *Sampler) sample(ctx context.Context, dev Device) error {
	read, cancel := context.WithTimeout(ctx, readBound)
	defer cancel()
	got, err := dev.Read(read)
	if err != nil {
		if s.log != nil && ctx.Err() == nil {
			s.log.Warn("sensor read", zap.Error(err))
		}
		return err
	}
	if len(got) == 0 {
		// A part with nothing to say yet is warming up, not failing.
		return nil
	}
	// Copied, not kept: a driver is free to refill its own buffer,
	// and Latest would otherwise hand out memory the bus is writing.
	kept := make([]Reading, len(got))
	copy(kept, got)
	s.mu.Lock()
	s.last = kept
	s.mu.Unlock()
	return nil
}

// Latest copies the last readings. Safe from any goroutine, and it
// never waits on the bus — an empty result means nothing has been read
// yet, not that the part answered nothing.
func (s *Sampler) Latest() []Reading {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.last) == 0 {
		return nil
	}
	out := make([]Reading, len(s.last))
	copy(out, s.last)
	return out
}
