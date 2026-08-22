// Package relay runs one protocol instance against its radio: open,
// validate the engine's waveform against the board's envelope,
// configure, listen, and start over with backoff when anything fails.
// A relay that cannot run is a visible error state, never a dead
// daemon and never a silent success.
package relay

import (
	"context"
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

	bus   *bus.Bus
	log   *zap.Logger
	state atomic.Value
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
	r.state.Store(StateStarting)
	return r
}

// State reports the current lifecycle state.
func (r *Relay) State() string {
	s, _ := r.state.Load().(string)
	return s
}

func (r *Relay) setState(state string, err error) {
	r.state.Store(state)
	ev := bus.RelayState{Relay: r.Name, State: state}
	if err != nil {
		ev.Err = err.Error()
	}
	r.bus.Publish(ev)
}

// Run supervises until the context ends: every failure is logged,
// published, and retried with capped exponential backoff.
func (r *Relay) Run(ctx context.Context) {
	backoff := backoffFirst
	for {
		err := r.session(ctx)
		if ctx.Err() != nil {
			return
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

// session runs one open→configure→listen cycle to completion.
func (r *Relay) session(ctx context.Context) error {
	r.setState(StateStarting, nil)

	dev, err := r.driver.Open(r.radioCfg, r.log)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := dev.Close(); cerr != nil {
			r.log.Warn("radio close", zap.Error(cerr))
		}
	}()

	w := r.engine.Waveform()
	if err := dev.Envelope().Allows(w); err != nil {
		return err
	}
	if err := dev.Configure(w); err != nil {
		return err
	}
	if err := dev.StartReceive(); err != nil {
		return err
	}

	r.setState(StateRunning, nil)
	r.log.Info("relay running",
		zap.Uint32("frequency_hz", w.FrequencyHz),
		zap.Int("sf", w.SpreadingFactor),
		zap.Int("bandwidth_hz", w.BandwidthHz),
	)
	return r.engine.Run(ctx, dev)
}
