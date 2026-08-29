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

// Sampler owns one device and reads it on its own goroutine. Every
// other goroutine sees only the last answer, through Latest.
type Sampler struct {
	dev   Device
	every time.Duration
	log   *zap.Logger

	// mu guards the cache alone, and is never held across a Read —
	// that is the whole point: a reader waits for a slice copy, never
	// for the bus. Plain, not RW: one writer a cadence apart and
	// readers that copy three values contend over nothing.
	mu   sync.Mutex
	last []Reading
}

// NewSampler prepares a sampler. A cadence of zero takes the default.
func NewSampler(dev Device, every time.Duration, log *zap.Logger) *Sampler {
	if every <= 0 {
		every = DefaultInterval
	}
	return &Sampler{dev: dev, every: every, log: log}
}

// Run reads until the context ends, then closes the device. It samples
// once before waiting, so a relay that asks for telemetry in the first
// seconds of its life has something true to say.
func (s *Sampler) Run(ctx context.Context) {
	defer func() {
		if err := s.dev.Close(); err != nil && s.log != nil {
			s.log.Warn("sensor close", zap.Error(err))
		}
	}()
	t := time.NewTicker(s.every)
	defer t.Stop()
	for {
		s.sample(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// sample takes one reading set. A failure keeps what was there: the
// readings carry the moment they were taken, so a consumer can see
// the answer going stale, which is more use than an empty one.
func (s *Sampler) sample(ctx context.Context) {
	read, cancel := context.WithTimeout(ctx, readBound)
	defer cancel()
	got, err := s.dev.Read(read)
	if err != nil {
		if s.log != nil && ctx.Err() == nil {
			s.log.Warn("sensor read", zap.Error(err))
		}
		return
	}
	if len(got) == 0 {
		return
	}
	// Copied, not kept: a driver is free to refill its own buffer,
	// and Latest would otherwise hand out memory the bus is writing.
	kept := make([]Reading, len(got))
	copy(kept, got)
	s.mu.Lock()
	s.last = kept
	s.mu.Unlock()
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
