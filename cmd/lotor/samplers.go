package main

// The sensor half of the manager's lifecycle. A sampler's life is the
// daemon's, never a relay's: the bus belongs to the machine, so a
// relay bounce — which happens on every change to it — must leave the
// part alone rather than reopen the device and lose a warm cache.

import (
	"context"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/sensor"
)

// managedSampler is one running sampler and what it takes to stop it.
type managedSampler struct {
	cancel context.CancelFunc
	done   chan struct{}
	smp    *sensor.Sampler
}

// opened reports whether this part is answering, and why not when it
// is not — the truth a status line owes an operator, rather than a
// pointer at a journal that rotates.
func (h *managedSampler) opened() (bool, string) {
	return h.smp.Opened(), h.smp.Cause()
}

// startSampler opens one part and gives it its goroutine. A part that
// will not open is logged and skipped: a missing sensor is no reason
// for a relay to stop carrying traffic. The caller holds mu.
func (m *manager) startSampler(ctx context.Context, name string) {
	sn, ok := m.file.Sensors[name]
	if !ok {
		return
	}
	log := m.log.With(zap.String("sensor", name))
	drv, err := sensor.Lookup(sn.Driver)
	if err != nil {
		log.Error("sensor not started", zap.Error(err))
		return
	}
	if drv.Open == nil {
		log.Error("sensor not started", zap.String("driver", sn.Driver),
			zap.String("why", "this driver opens no device"))
		return
	}
	// nil: a sensor has no preset catalogue, so what the operator
	// wrote is the whole configuration.
	cfg, _, err := sn.Layered.Resolve(nil)
	if err != nil {
		log.Error("sensor not started", zap.Error(err))
		return
	}
	every := sn.SampleInterval
	if every <= 0 {
		every = sensor.DefaultInterval
	}
	sctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	// Opening is a bus transaction and mu is held here, so the sampler
	// does it on its own goroutine — and keeps trying, since the
	// reasons a part refuses are usually temporary.
	smp := sensor.NewSampler(func() (sensor.Device, error) {
		return drv.Open(cfg, log)
	}, every, log)
	m.samplers[name] = &managedSampler{cancel: cancel, done: done, smp: smp}
	m.viewMu.Lock()
	m.sensorViews[name] = smp
	m.viewMu.Unlock()
	m.wg.Go(func() {
		defer close(done)
		smp.Run(sctx)
	})
	log.Info("sensor up", zap.String("driver", sn.Driver), zap.Duration("every", every))
}

// stopWait bounds how long stopSampler waits for a goroutine to
// notice. It is held under mu, so it may not be unbounded: a part
// blocked in a syscall would otherwise freeze every console command
// and every mutation behind the lifecycle lock.
const stopWait = 3 * time.Second

// stopSampler takes one down. It waits briefly — the usual case is a
// goroutine asleep on its ticker, which cancel wakes at once — and
// gives up rather than hold mu on a bus that is not answering. The
// goroutine closes its own device whenever it comes back. The caller
// holds mu.
// It reports whether the goroutine actually finished, so a caller
// that means to reopen the part knows whether it may.
func (m *manager) stopSampler(name string) bool {
	h, ok := m.samplers[name]
	if !ok {
		return true
	}
	h.cancel()
	delete(m.samplers, name)
	m.viewMu.Lock()
	delete(m.sensorViews, name)
	m.viewMu.Unlock()
	log := m.log.With(zap.String("sensor", name))
	select {
	case <-h.done:
		log.Info("sensor stopped")
		return true
	case <-time.After(stopWait):
		log.Warn("sensor still stopping — it will close its device when the bus lets go")
		return false
	}
}

// bounceSampler reopens one part under the daemon's own context: a
// mutation's order outlives the session that gave it.
//
// It reopens even when the old goroutine has not let go, which can put
// two samplers on one chip for as long as the bus holds the first.
// That is deliberate: their register writes may interleave and cost a
// wrong reading or two, where refusing to reopen would lose the part
// until somebody changed its configuration again. A transient wrong
// value is the smaller harm, and it heals itself when the old one
// exits.
func (m *manager) bounceSampler(name string) {
	if !m.stopSampler(name) {
		m.log.Warn("sensor reopened while the old one still holds the bus",
			zap.String("sensor", name))
	}
	m.startSampler(m.ctx, name)
}
