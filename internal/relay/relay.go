// Package relay runs one protocol instance against its radio: open,
// validate the engine's waveform against the board's envelope,
// configure, listen, and start over with backoff when anything fails.
// A relay that cannot run is a visible error state, never a dead
// daemon and never a silent success.
package relay

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/protocol"
	"meshrunner.dev/lotor/internal/radio"
)

// States a relay reports.
const (
	StateStarting = "starting"
	StateRunning  = "running"
	StateError    = "error"
)

const (
	backoffFirst = 5 * time.Second
	backoffCap   = time.Minute
)

// Relay supervises one protocol engine on one radio.
type Relay struct {
	Name string

	driver   radio.Driver
	radioCfg map[string]any
	engine   protocol.Engine

	bus *bus.Bus
	log *zap.Logger
	// status holds state and cause as one value: a reader never sees a
	// fresh state paired with a stale cause, or the reverse.
	status atomic.Value
	// noise holds the last radio.NoiseFloor the session's monitor saw;
	// it outlives the session so the last reading stays consultable.
	noise atomic.Value
	// noiseHistory gates publishing the floor to the bus — the write
	// path to the journal. Off, the measurement still runs and the
	// latest value stays readable here, in RAM only.
	noiseHistory bool
	// stillborn marks a relay whose configuration failed: it exists to
	// be visible in the error state, never to retry — a config error
	// does not heal by waiting.
	stillborn bool
}

// lifecycle is the state/cause pair status stores atomically.
type lifecycle struct {
	state string
	cause string
}

// New assembles a relay from its resolved parts. noiseHistory decides
// whether the floor reaches the bus (and so the journal); nil takes
// the build's default.
func New(name string, drv radio.Driver, radioCfg map[string]any,
	eng protocol.Engine, b *bus.Bus, log *zap.Logger, noiseHistory *bool,
) *Relay {
	r := &Relay{
		Name:         name,
		driver:       drv,
		radioCfg:     radioCfg,
		engine:       eng,
		bus:          b,
		log:          log.With(zap.String("relay", name)),
		noiseHistory: NoiseHistoryDefault,
	}
	if noiseHistory != nil {
		r.noiseHistory = *noiseHistory
	}
	r.status.Store(lifecycle{state: StateStarting})
	return r
}

// Stillborn builds a relay pinned in the error state, so a broken
// configuration is a visible casualty instead of a dead daemon.
func Stillborn(name string, cause error, b *bus.Bus, log *zap.Logger) *Relay {
	r := &Relay{
		Name:      name,
		bus:       b,
		log:       log.With(zap.String("relay", name)),
		stillborn: true,
	}
	r.status.Store(lifecycle{state: StateError, cause: cause.Error()})
	return r
}

// State reports the current lifecycle state.
func (r *Relay) State() string {
	l, _ := r.status.Load().(lifecycle)
	return l.state
}

// Err reports the cause of the current error state, empty otherwise.
func (r *Relay) Err() string {
	l, _ := r.status.Load().(lifecycle)
	return l.cause
}

func (r *Relay) setState(state string, err error) {
	l := lifecycle{state: state}
	if err != nil {
		l.cause = err.Error()
	}
	r.status.Store(l)
	r.bus.Publish(bus.RelayState{
		Relay: r.Name, At: time.Now(), State: state, Err: l.cause,
	})
}

// Run supervises until the context ends: every failure is logged,
// published, and retried with capped exponential backoff.
func (r *Relay) Run(ctx context.Context) {
	if r.stillborn {
		r.bus.Publish(bus.RelayState{
			Relay: r.Name, At: time.Now(), State: StateError, Err: r.Err(),
		})
		r.log.Error("relay unrunnable, configuration failed", zap.String("cause", r.Err()))
		<-ctx.Done()
		return
	}
	backoff := backoffFirst
	for {
		reached, err := r.session(ctx)
		if ctx.Err() != nil {
			// Shutdown — but a real fault that raced it still deserves
			// its error state, or the journal would read a failed
			// session as a clean exit of a healthy relay.
			if err != nil && !errors.Is(err, context.Canceled) &&
				!errors.Is(err, context.DeadlineExceeded) {
				r.setState(StateError, err)
				r.log.Error("relay failed during shutdown", zap.Error(err))
			}
			return
		}
		if reached {
			// A session that ran resets the ladder: the next fault is
			// a fresh incident, not the continuation of the last one.
			backoff = backoffFirst
		}
		r.setState(StateError, err)
		r.log.Error("relay down, will retry",
			zap.Error(err), zap.Duration("backoff", backoff))
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, backoffCap)
	}
}

// session runs one open→configure→listen cycle to completion, and
// reports whether it reached the running state.
func (r *Relay) session(ctx context.Context) (reached bool, err error) {
	r.setState(StateStarting, nil)

	dev, err := r.driver.Open(r.radioCfg, r.log)
	if err != nil {
		return false, err
	}
	defer func() {
		if cerr := dev.Close(); cerr != nil {
			r.log.Warn("radio close", zap.Error(cerr))
		}
	}()

	w := r.engine.Waveform()
	if err := dev.Envelope().Allows(w); err != nil {
		return false, err
	}
	if err := dev.Configure(w); err != nil {
		return false, err
	}
	if err := dev.StartReceive(); err != nil {
		return false, err
	}

	r.setState(StateRunning, nil)
	r.log.Info("relay running",
		zap.Uint32("frequency_hz", w.FrequencyHz),
		zap.Int("sf", w.SpreadingFactor),
		zap.Int("bandwidth_hz", w.BandwidthHz),
	)
	nctx, stopNoise := context.WithCancel(ctx)
	defer stopNoise()
	go r.watchNoise(nctx, dev)
	return true, r.engine.Run(ctx, dev)
}

const (
	// noisePollEvery paces reading the device's floor — a state read,
	// never a hardware touch.
	noisePollEvery = 2 * time.Second
	// noisePublishDelta is the change worth telling the bus about.
	noisePublishDelta = 1.0 // dBm
	// noisePublishEvery bounds the silence between publications, so a
	// perfectly stable floor still leaves regular points for the
	// journal's consolidation to work with.
	noisePublishEvery = 5 * time.Minute
)

// watchNoise mirrors the device's noise floor for the session: the
// latest reading is kept on the relay for anyone to consult, and the
// bus hears about meaningful changes plus a slow heartbeat — not every
// measurement, or a stable floor would grind the journal.
func (r *Relay) watchNoise(ctx context.Context, dev radio.Device) {
	tick := time.NewTicker(noisePollEvery)
	defer tick.Stop()
	var published radio.NoiseFloor
	var publishedAt time.Time
	var starved uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			// Starvation is checked before the floor: a channel too
			// busy to ever converge is exactly the case to report.
			if cur := dev.NoiseStarved(); cur > starved {
				if r.noiseHistory {
					r.bus.Publish(bus.NoiseStarved{
						Relay: r.Name, At: time.Now(), Aborted: cur - starved,
					})
				}
				starved = cur
			}
			nf, ok := dev.NoiseFloor()
			if !ok {
				continue
			}
			r.noise.Store(nf)
			if !r.noiseHistory {
				continue // measurement stays; the disk hears nothing
			}
			// A moving spread is news too: a site turning impulsive
			// under a stable median is exactly what the archive is for.
			moved := abs(nf.DBm-published.DBm) >= noisePublishDelta ||
				abs(nf.SpreadDB-published.SpreadDB) >= noisePublishDelta
			if publishedAt.IsZero() || moved ||
				time.Since(publishedAt) >= noisePublishEvery {
				r.bus.Publish(bus.NoiseFloor{
					Relay: r.Name, At: nf.At, DBm: nf.DBm, SpreadDB: nf.SpreadDB,
				})
				published, publishedAt = nf, time.Now()
			}
		}
	}
}

// NoiseFloor reports the channel's last measured ambient level; ok is
// false until the radio's first measurement converges.
func (r *Relay) NoiseFloor() (radio.NoiseFloor, bool) {
	nf, ok := r.noise.Load().(radio.NoiseFloor)
	return nf, ok
}

func abs(f float64) float64 {
	if f < 0 {
		return -f
	}
	return f
}
