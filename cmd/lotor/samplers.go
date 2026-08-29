package main

// The sensor half of the manager's lifecycle. A sampler's life is the
// daemon's, never a relay's: the bus belongs to the machine, so a
// relay bounce — which happens on every change to it — must leave the
// part alone rather than reopen the device and lose a warm cache.

import (
	"context"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/sensor"
)

// managedSampler is one running sampler and what it takes to stop it.
type managedSampler struct {
	cancel context.CancelFunc
	done   chan struct{}
	smp    *sensor.Sampler
}

// startSampler opens one part and gives it its goroutine. A part that
// will not open is logged and skipped: a missing sensor is no reason
// for a relay to stop carrying traffic. The caller holds mu.
func (m *manager) startSampler(ctx context.Context, name string) {
	sn, ok := m.file.Sensors[name]
	if !ok {
		return
	}
	log := m.log.Named("sensor").With(zap.String("sensor", name))
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
	dev, err := drv.Open(cfg, log)
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
	smp := sensor.NewSampler(dev, every, log)
	m.samplers[name] = &managedSampler{cancel: cancel, done: done, smp: smp}
	m.wg.Go(func() {
		defer close(done)
		smp.Run(sctx)
	})
	log.Info("sensor up", zap.String("driver", sn.Driver), zap.Duration("every", every))
}

// stopSampler takes one down and waits it out. The caller holds mu.
func (m *manager) stopSampler(name string) {
	h, ok := m.samplers[name]
	if !ok {
		return
	}
	h.cancel()
	<-h.done
	delete(m.samplers, name)
	m.log.Named("sensor").Info("sensor stopped", zap.String("sensor", name))
}

// bounceSampler reopens one part under the daemon's own context: a
// mutation's order outlives the session that gave it.
func (m *manager) bounceSampler(name string) {
	m.stopSampler(name)
	m.startSampler(m.ctx, name)
}
