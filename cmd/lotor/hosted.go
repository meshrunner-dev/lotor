package main

// A station and an application are hosted the same way: looked up in
// their registry, resolved against their presets, checked, built,
// attached to a radio they can live without, rebound, stopped, traced,
// and judged by the duty preflight. What tells them apart is small,
// and it is named here once — the kind word, the radio role, where the
// file keeps them, which registry answers for one, what their
// provenance head says — so the manager writes hosting once and reads
// the difference from a table, instead of keeping two copies of every
// path in step by hand. What genuinely differs — the Spec a builder is
// given, the Info a failure reports — stays in each kind's start.

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/application"
	"meshrunner.dev/lotor/internal/confdb"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/hosted"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/station"
)

// hostedKind is one hosted family as the manager tells it apart.
type hostedKind struct {
	// kind is the store's word for the family, which is also the noun
	// the log and the consumer keys use.
	kind string
	role radio.ConsumerRole
	// names lists the file's entries of this kind in stable order;
	// host projects one to its hosted half.
	names func(f *config.File) []string
	host  func(f *config.File, name string) config.Host
	// lookup answers for one entry from the kind's registry, projected
	// to what hosting needs of a builder.
	lookup func(f *config.File, name string) (hostedBuilder, error)
	// structural is the kind's own provenance head with the shared rows.
	structural func(f *config.File, name string) []config.Trace
}

// hostedBuilder is the half of a station or application builder the
// hosting path reads: presets to resolve against, the check, the
// radio demand. Build stays with the kind, because its Spec does.
type hostedBuilder struct {
	presets map[string]map[string]any
	check   func(map[string]any) error
	asks    func(map[string]any) (hosted.RadioDemand, error)
}

func hostedStation(b station.Builder) hostedBuilder {
	return hostedBuilder{presets: b.Presets, check: b.Check, asks: b.Asks}
}

func hostedApplication(b application.Builder) hostedBuilder {
	return hostedBuilder{presets: b.Presets, check: b.Check, asks: b.Asks}
}

var stationHosting = hostedKind{
	kind: confdb.KindStation, role: radio.RoleStation,
	names: func(f *config.File) []string { return sortedObjectNames(f.Stations) },
	host: func(f *config.File, name string) config.Host {
		sc := f.Stations[name]
		return sc.Host()
	},
	lookup: func(f *config.File, name string) (hostedBuilder, error) {
		b, err := station.Lookup(f.Stations[name].Protocol)
		return hostedStation(b), err
	},
	structural: func(f *config.File, name string) []config.Trace { return stationStructural(f.Stations[name]) },
}

var applicationHosting = hostedKind{
	kind: confdb.KindApplication, role: radio.RoleApplication,
	names: func(f *config.File) []string { return sortedObjectNames(f.Applications) },
	host: func(f *config.File, name string) config.Host {
		ac := f.Applications[name]
		return ac.Host()
	},
	lookup: func(f *config.File, name string) (hostedBuilder, error) {
		ac := f.Applications[name]
		b, err := application.Lookup(ac.Protocol, ac.Type)
		return hostedApplication(b), err
	},
	structural: func(f *config.File, name string) []config.Trace {
		return applicationStructural(f.Applications[name])
	},
}

// managedHost is the half of a running station or application the
// manager drives alike: the cancel, the join, the radio binding.
type managedHost struct {
	cancel  context.CancelFunc
	done    chan struct{}
	binding *radio.Binding
}

// hostPolicy is the origination gate a hosted identity is built with,
// read from its declared tx: block; an absent block is a dry gate with
// CAD on.
func hostPolicy(h config.Host) hosted.TXPolicy {
	policy := hosted.TXPolicy{Mode: h.TXMode()}
	if h.TX == nil {
		return policy
	}
	policy.LBTThresholdDB = h.TX.LBTThresholdDB
	policy.LBTExhausted = h.TX.LBTExhausted
	policy.CAD = h.TX.CAD == nil || *h.TX.CAD
	policy.QueueDepth = h.TX.QueueDepth
	return policy
}

// resolveHost is what assembly asks before a builder is given
// anything: the layers resolved against its presets, and the kind's
// own check of the result. The caller holds mu.
func (m *manager) resolveHost(k hostedKind, name string, b hostedBuilder) (map[string]any, []config.Trace, error) {
	cfg, traces, err := k.host(m.file, name).Layered.Resolve(b.presets)
	if err != nil {
		return nil, nil, err
	}
	if err := b.check(cfg); err != nil {
		return nil, nil, err
	}
	return cfg, traces, nil
}

// runHost records a built service's provenance and runs it on the
// manager's group; the caller has already placed its entry in the
// kind's map and attached its radio. The caller holds mu.
func (m *manager) runHost(ctx context.Context, k hostedKind, name string, h *managedHost,
	run func(context.Context) error, traces []config.Trace,
) {
	hctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	h.cancel, h.done = cancel, done
	m.viewMu.Lock()
	m.traces[k.kind+" "+name] = withStructural(traces, k.structural(m.file, name))
	m.viewMu.Unlock()
	m.wg.Go(func() {
		defer close(done)
		if err := run(hctx); err != nil && hctx.Err() == nil {
			m.log.Error(k.kind+" stopped", zap.String(k.kind, name), zap.Error(err))
		}
	})
}

// attachHostRadio supplies or withdraws only the RF capability; the
// service is never bounced by it. The caller holds mu.
func (m *manager) attachHostRadio(k hostedKind, name string, h *managedHost, svc any,
	b hostedBuilder, cfg map[string]any,
) {
	attacher, ok := svc.(hosted.RadioAttacher)
	if !ok {
		return
	}
	host := k.host(m.file, name)
	demand := func() (hosted.RadioDemand, error) {
		d, err := b.asks(cfg)
		if err != nil {
			return hosted.RadioDemand{}, err
		}
		if requester, ok := svc.(hosted.RadioRequester); ok {
			d = requester.RadioDemand()
		}
		return d, nil
	}
	h.binding = m.attachConsumerRadio(k.kind, name, host.Radio, host.TXMode(), k.role, attacher, demand)
}

// rebindHost applies a radio= mutation without stopping the service: a
// configuration the registry now refuses leaves the service running
// with its RF door told why. The caller holds mu.
func (m *manager) rebindHost(k hostedKind, name string, h *managedHost, svc any) {
	if h.binding != nil {
		h.binding.Unbind()
		h.binding = nil
	}
	m.releaseAirtimeConsumer(k.kind + ":" + name)
	host := k.host(m.file, name)
	b, err := k.lookup(m.file, name)
	var cfg map[string]any
	if err == nil {
		cfg, _, err = host.Layered.Resolve(b.presets)
	}
	if err != nil {
		if a, ok := svc.(hosted.RadioAttacher); ok {
			a.AttachRadio(host.Radio, nil, nil, err.Error())
		}
		return
	}
	m.attachHostRadio(k, name, h, svc, b, cfg)
}

// stopHost ends a running service, joins it, releases its radio and
// its duty. The caller holds mu and removes the entry from its map.
func (m *manager) stopHost(k hostedKind, name string, h *managedHost) {
	if h.cancel != nil {
		h.cancel()
		<-h.done
	}
	if h.binding != nil {
		h.binding.Unbind()
	}
	m.releaseAirtimeConsumer(k.kind + ":" + name)
	m.log.Info(k.kind+" stopped", zap.String(k.kind, name))
}

// syntheticHostTraces gives a configured entry that has no assembly to
// trace — failed, or not started — the provenance its layers resolve
// to, so print and export keep showing it. The caller holds mu.
func (m *manager) syntheticHostTraces(k hostedKind, out map[string][]config.Trace) {
	for _, name := range k.names(m.file) {
		if _, live := out[k.kind+" "+name]; live {
			continue
		}
		rows := k.structural(m.file, name)
		if b, err := k.lookup(m.file, name); err == nil {
			if _, traces, rerr := k.host(m.file, name).Layered.Resolve(b.presets); rerr == nil {
				rows = withStructural(traces, rows)
			}
		}
		out[k.kind+" "+name] = rows
	}
}

// checkHostAlone judges one entry without a radio: its registry
// answers for it, its scopes are legal, its layers resolve, and the
// kind's own check accepts the result.
func checkHostAlone(k hostedKind, f *config.File, name string) error {
	b, err := k.lookup(f, name)
	if err != nil {
		return err
	}
	host := k.host(f, name)
	if err := checkScopes(host.Layered, b.presets, b.check); err != nil {
		return err
	}
	cfg, _, err := host.Layered.Resolve(b.presets)
	if err != nil {
		return err
	}
	return b.check(cfg)
}

// configuredHostDuties is the preflight's view of every entry of one
// kind that follows the radio: each judged alone and against the
// radio, and its duty budget named for the shared ledger.
func configuredHostDuties(k hostedKind, next *config.File, radioName string, driver radio.Driver,
	radioCfg map[string]any, envelope radio.Envelope,
) ([]configuredDuty, error) {
	var duties []configuredDuty
	for _, name := range k.names(next) {
		if k.host(next, name).Radio != radioName {
			continue
		}
		budget, enabled, err := checkHostAttachment(k, next, name, driver, radioCfg, envelope)
		if err != nil {
			return nil, fmt.Errorf("%s %q: %w", k.kind, name, err)
		}
		if enabled {
			duties = append(duties, configuredDuty{consumer: k.kind + ":" + name, budget: budget})
		}
	}
	return duties, nil
}

func checkHostAttachment(k hostedKind, next *config.File, name string, driver radio.Driver,
	radioCfg map[string]any, envelope radio.Envelope,
) (budget time.Duration, enabled bool, err error) {
	if err := checkHostAlone(k, next, name); err != nil {
		return 0, false, err
	}
	b, err := k.lookup(next, name)
	if err != nil {
		return 0, false, err
	}
	host := k.host(next, name)
	cfg, _, err := host.Layered.Resolve(b.presets)
	if err != nil {
		return 0, false, err
	}
	if b.asks == nil {
		return 0, false, fmt.Errorf("%s %q cannot describe a radio demand", k.kind, name)
	}
	demand, err := b.asks(cfg)
	if err != nil {
		return 0, false, err
	}
	return configuredConsumerDuty(host.TXMode(), demand, driver, radioCfg, envelope)
}
