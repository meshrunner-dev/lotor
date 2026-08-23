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

// New assembles a relay from its resolved parts.
func New(name string, drv radio.Driver, radioCfg map[string]any,
	eng protocol.Engine, b *bus.Bus, log *zap.Logger,
) *Relay {
	r := &Relay{
		Name:     name,
		driver:   drv,
		radioCfg: radioCfg,
		engine:   eng,
		bus:      b,
		log:      log.With(zap.String("relay", name)),
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
	return true, r.engine.Run(ctx, dev)
}
