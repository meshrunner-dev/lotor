package main

// The manager owns what a running daemon may change about itself: the
// configuration store, the live relays, and the path between the two.
// Every administration channel mutates through here — parse, validate,
// persist, bounce the owning relay — so the rules live once, whoever
// asks. One line is one transaction: a retune touching three
// attributes restarts the relay once.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/cli"
	"meshrunner.dev/lotor/internal/confdb"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/logging"
	"meshrunner.dev/lotor/internal/mqtt"
	"meshrunner.dev/lotor/internal/protocol"
	enginemc "meshrunner.dev/lotor/internal/protocol/meshcore"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/relay"
	"meshrunner.dev/lotor/internal/schema"
	"meshrunner.dev/lotor/internal/sensor"
	"meshrunner.dev/lotor/internal/sentinel"

	"meshrunner.dev/pkg/meshcore"
)

// The structural attribute names, written once. The schema declares
// them for the console; these are the write path's spellings.
const (
	attrProtocol     = "protocol"
	attrIdentity     = "identity"
	attrRadio        = "radio"
	attrDriver       = "driver"
	attrSampleEvery  = "sample_interval"
	attrProfile      = "profile"
	attrNoiseHistory = "noise_history"
	attrTXMode       = "tx.mode"
	attrTXThreshold  = "tx.lbt_threshold_db"
	attrTXExhausted  = "tx.lbt_exhausted"
	attrTXQueueDepth = "tx.queue_depth"
	attrTXCAD        = "tx.cad"
	attrSocket       = "socket"
	attrName         = "name"

	// sourceConfig is what a singleton's print shows as provenance —
	// no layering, just the store.
	sourceConfig = "config"
)

type manager struct {
	store *confdb.Store
	bus   *bus.Bus
	log   *zap.Logger
	sen   *sentinel.Sentinel
	kinds []schema.Kind

	// The log level's live knob and where the boot flag left it — the
	// fallback when the stored override is cleared.
	logKnob   zap.AtomicLevel
	bootLevel zapcore.Level

	// air carries the work ordered over the air to a goroutine that is
	// not the engine's: a relay bounce joins the engine goroutine, so
	// the engine may never wait on the manager itself.
	air chan airOrder

	// The two-lock discipline, written once and load-bearing.
	//
	// mu is the lifecycle lock: the configuration file, the running
	// and observer tables, the store transaction, and the right to
	// bounce — which waits for goroutines to die. Nothing that runs
	// inside a joined goroutine (the engine loop, an observer's Run)
	// may ever acquire it: the goroutine would be waiting for a lock
	// whose holder is waiting for the goroutine.
	//
	// viewMu is the view lock, a leaf: it guards the live views alone
	// (infos, radios, traces), is held only across map reads and
	// writes — never across a join, a dial, or anything that blocks —
	// and mu may be held when taking it, never the reverse. The
	// closures the engine and the observers call (health, rounds,
	// over-the-air gets) read under viewMu only, which is what makes
	// them safe while a mutation holds mu and waits.
	mu        sync.Mutex
	viewMu    sync.RWMutex
	ctx       context.Context //nolint:containedctx // the daemon's lifetime, set once at Start
	file      *config.File
	wg        sync.WaitGroup
	running   map[string]*managedRelay
	observers map[string]*managedObserver
	// obsCause remembers why each configured observer is not running —
	// the truth the status line owes the operator, kept beside the
	// observer table under the same lock.
	obsCause map[string]string
	samplers map[string]*managedSampler
	infos    map[string]cli.RelayInfo
	radios   map[string]cli.RadioInfo
	traces   map[string][]config.Trace
	// sensorViews is every running sampler, under viewMu with the rest
	// of the view: a telemetry answer is composed on the engine's
	// goroutine, which may never take mu.
	sensorViews map[string]*sensor.Sampler
	// cfgs holds each relay's resolved engine configuration, under
	// viewMu with the rest of the view: the over-the-air deep check
	// clones one and asks the protocol, exactly what assembly asks.
	cfgs map[string]map[string]any
}

type managedRelay struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func newManager(store *confdb.Store, f *config.File, b *bus.Bus,
	sen *sentinel.Sentinel, kinds []schema.Kind, log *zap.Logger,
) *manager {
	return &manager{
		store: store, file: f, bus: b, sen: sen, kinds: kinds, log: log,
		running:     map[string]*managedRelay{},
		observers:   map[string]*managedObserver{},
		obsCause:    map[string]string{},
		samplers:    map[string]*managedSampler{},
		sensorViews: map[string]*sensor.Sampler{},
		infos:       map[string]cli.RelayInfo{},
		radios:      map[string]cli.RadioInfo{},
		traces:      map[string][]config.Trace{},
	}
}

// Start assembles and launches every configured relay, then the
// observers that watch them.
func (m *manager) Start(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ctx = ctx
	m.air = make(chan airOrder, 16)
	m.wg.Go(func() { m.serveAir(ctx) })
	for name := range m.file.Relays {
		m.startRelay(ctx, name)
	}
	for name := range m.file.MQTT {
		m.startObserver(ctx, name)
	}
	for name := range m.file.Sensors {
		m.startSampler(ctx, name)
	}
}

// airOrder is one thing ordered from the air, waiting for a goroutine
// that is not the engine's — a bounce joins the engine, and an advert
// waits on the engine's own loop, so neither may run in it. advert set
// makes it an announcement; otherwise it is a mutation.
type airOrder struct {
	relay     string
	principal string
	set       map[string]string
	unset     []string
	advert    bool
	flood     bool
	// discover keys a neighbourhood scan — it waits on the engine's
	// own loop, so like the advert it may not run in it.
	discover bool
	// grant carries a permission change: the byte for pubKey, zero
	// role meaning removal.
	grant  bool
	pubKey []byte
	perms  byte
}

// serveAir applies over-the-air mutations off the engine's goroutine,
// one at a time. The reply to the admin has already been enqueued by
// the time an order lands here, so it radiates before the bounce this
// triggers tears the engine down — and the admin's session, persisted,
// is there when the successor comes up.
func (m *manager) serveAir(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case o := <-m.air:
			m.serveAirOrder(ctx, o)
		}
	}
}

// airReplyGrace is how long an over-the-air mutation waits so its
// admin's reply radiates before the bounce clears the queue.
const airReplyGrace = 2 * time.Second

// serveAirDiscover keys a neighbourhood scan for an over-the-air
// order. The answers join the shared table as they land; draining
// the channel only keeps the engine from holding a reader nobody
// reads.
func (m *manager) serveAirDiscover(relay string) {
	m.viewMu.RLock()
	info, ok := m.infos[relay]
	m.viewMu.RUnlock()
	if !ok || info.Discover == nil {
		return
	}
	answers, _, err := info.Discover()
	if err != nil {
		m.log.Warn("over-the-air discover refused",
			zap.String("relay", relay), zap.Error(err))
		return
	}
	go func() {
		for range answers {
		}
	}()
}

// serveAirOrder carries out one order off the engine's goroutine.
func (m *manager) serveAirOrder(ctx context.Context, o airOrder) {
	if o.grant {
		// Reading the door takes viewMu; calling it waits on the
		// engine's ack loop, safe here on the manager's own goroutine.
		m.viewMu.RLock()
		info, ok := m.infos[o.relay]
		m.viewMu.RUnlock()
		if ok && info.Grant != nil {
			if err := info.Grant(o.pubKey, o.perms); err != nil {
				m.log.Warn("over-the-air permission change refused",
					zap.String("relay", o.relay), zap.String("principal", o.principal),
					zap.Error(err))
			}
		}
		return
	}
	if o.discover {
		m.serveAirDiscover(o.relay)
		return
	}
	if o.advert {
		// The reply is already queued; the announcement can go at
		// once. This is the manager's own goroutine — waiting on the
		// engine's ack loop is safe here, joining nothing.
		m.viewMu.RLock()
		info, ok := m.infos[o.relay]
		m.viewMu.RUnlock()
		if ok && info.TriggerAdvert != nil {
			if err := info.TriggerAdvert(o.flood); err != nil {
				m.log.Warn("over-the-air advert refused",
					zap.String("relay", o.relay), zap.Error(err))
			}
		}
		return
	}
	// A short grace lets the admin's reply leave the queue before the
	// bounce drains it: the answer was optimistic, the change is what
	// a follow-up get confirms.
	select {
	case <-ctx.Done():
		return
	case <-time.After(airReplyGrace):
	}
	if _, err := m.Mutate(ctx, "relay", o.relay, o.set, o.unset, o.principal); err != nil {
		m.log.Warn("over-the-air mutation refused",
			zap.String("relay", o.relay), zap.String("principal", o.principal),
			zap.Error(err))
	}
}

// orderAir queues one over-the-air order and reports what to tell the
// admin now — optimistically, because the work happens after the
// reply leaves. A full channel means the daemon is already swamped
// with air orders, which is the one time refusing is the honest word.
func (m *manager) orderAir(o airOrder, ok string) string {
	select {
	case m.air <- o:
		return ok
	default:
		return "ERR: busy"
	}
}

// relayCfgCopy clones one relay's resolved engine configuration for
// a what-if check — safe from any goroutine, the engine's included.
func (m *manager) relayCfgCopy(relay string) map[string]any {
	m.viewMu.RLock()
	defer m.viewMu.RUnlock()
	cfg, ok := m.cfgs[relay]
	if !ok {
		return nil
	}
	out := make(map[string]any, len(cfg))
	maps.Copy(out, cfg)
	return out
}

// relayEnvelope reads the envelope of the radio a relay owns, from
// the live view — safe from any goroutine, the engine's included.
func (m *manager) relayEnvelope(relay string) (radio.Envelope, bool) {
	m.viewMu.RLock()
	defer m.viewMu.RUnlock()
	info, ok := m.infos[relay]
	if !ok {
		return radio.Envelope{}, false
	}
	rd, ok := m.radios[info.Radio]
	return rd.Envelope, ok
}

// relayValue reads one effective attribute from the live view — safe
// from any goroutine, the engine's included. A value nobody set reads
// as the empty one it is: absence and emptiness are the same answer
// to "what is this set to", and the callers that must tell them apart
// have the traces themselves.
func (m *manager) relayValue(relay, attr string) string {
	m.viewMu.RLock()
	defer m.viewMu.RUnlock()
	for _, t := range m.traces["relay "+relay] {
		if t.Key == attr {
			return fmt.Sprintf("%v", t.Value)
		}
	}
	return ""
}

// Wait blocks until every relay has stopped — the shutdown's phase
// one, before the journal drains.
func (m *manager) Wait() { m.wg.Wait() }

// Close releases the store, after Wait.
func (m *manager) Close() { _ = m.store.Close() }

// startRelay assembles one relay from the current configuration and
// launches it. A broken one is a visible casualty, never a dead
// daemon: it exists, in the error state, with its cause. The caller
// holds mu.
func (m *manager) startRelay(ctx context.Context, name string) {
	rc := m.file.Relays[name]
	var r *relay.Relay
	asm, err := assemble(ctx, name, rc, m.file.Radios[rc.Radio], m.bus, m.log, m.sen,
		m.sessionStore(name), m.regionStore(name), m.otaCommands(name),
		sensorFeed{supply: m.supplyVoltage, sensors: m.sensorTelemetry})
	if err != nil {
		m.log.Error("relay configuration failed",
			zap.String("relay", name), zap.Error(err))
		r = relay.Stillborn(name, err, m.bus, m.log)
		m.viewMu.Lock()
		// A predecessor's face must go with it: its resolved config,
		// its provenance, its claim on the radio. A view that survived
		// the failure would show this error beside a configuration
		// that no longer runs anywhere.
		delete(m.cfgs, name)
		delete(m.traces, "relay "+name)
		for radioName, info := range m.radios {
			if info.Relay == name {
				delete(m.radios, radioName)
				delete(m.traces, "radio "+radioName)
			}
		}
		m.infos[name] = cli.RelayInfo{
			Name: name, Protocol: rc.Protocol, Radio: rc.Radio,
			State: r.State, Err: r.Err,
			// The configured intent survives the failure: an operator
			// reading "tx dry" next to an error would think the relay
			// was meant to stay silent.
			TXMode: rc.TXMode(),
		}
		m.viewMu.Unlock()
	} else {
		r = asm.relay
		m.viewMu.Lock()
		if m.cfgs == nil {
			m.cfgs = map[string]map[string]any{}
		}
		m.cfgs[name] = asm.relayCfg
		m.infos[name] = asm.info
		m.radios[rc.Radio] = asm.radio
		m.traces["radio "+rc.Radio] = withStructural(asm.radioTraces,
			radioStructural(m.file.Radios[rc.Radio]))
		m.traces["relay "+name] = withStructural(asm.relayTraces, relayStructural(rc))
		m.viewMu.Unlock()
	}
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	m.running[name] = &managedRelay{cancel: cancel, done: done}
	m.wg.Go(func() {
		defer close(done)
		r.Run(rctx)
	})
}

// sessionStore hands a relay a persistence door onto the acl table,
// keyed to its name: the successor of a bounced relay reads back the
// very sessions its predecessor kept.
func (m *manager) sessionStore(relay string) enginemc.SessionStore {
	return &aclStore{store: m.store, relay: relay}
}

// regionStore hands a relay a persistence door onto the region
// tables, keyed to its name, like sessionStore does for the acl.
func (m *manager) regionStore(relay string) enginemc.RegionStore {
	return &regionStoreAdapter{store: m.store, relay: relay}
}

// regionStoreAdapter ties the engine's region map to confdb. Like the
// aclStore it touches the database alone, never the manager's mutex.
type regionStoreAdapter struct {
	store *confdb.Store
	relay string
}

func (a *regionStoreAdapter) LoadRegions() (*enginemc.PersistedRegions, error) {
	ctx, cancel := aclStoreCtx()
	defer cancel()
	rows, meta, ok, err := a.store.LoadRegions(ctx, a.relay)
	if err != nil || !ok {
		return nil, err
	}
	pr := &enginemc.PersistedRegions{
		NextID: meta.NextID, HomeID: meta.HomeID,
		DefaultID: meta.DefaultID, WildcardFlags: meta.WildcardFlags,
	}
	for _, r := range rows {
		pr.Entries = append(pr.Entries, meshcore.Region{
			ID: r.ID, Parent: r.Parent,
			Flags: meshcore.RegionFlags(r.Flags), Name: r.Name,
		})
	}
	return pr, nil
}

func (a *regionStoreAdapter) SaveRegions(pr enginemc.PersistedRegions) error {
	ctx, cancel := aclStoreCtx()
	defer cancel()
	rows := make([]confdb.RegionRow, 0, len(pr.Entries))
	for _, r := range pr.Entries {
		rows = append(rows, confdb.RegionRow{
			ID: r.ID, Parent: r.Parent, Flags: uint8(r.Flags), Name: r.Name,
		})
	}
	return a.store.ReplaceRegions(ctx, a.relay, rows, confdb.RegionsMeta{
		NextID: pr.NextID, HomeID: pr.HomeID,
		DefaultID: pr.DefaultID, WildcardFlags: pr.WildcardFlags,
	})
}

// aclStore ties the meshcore session table to confdb. It touches the
// database alone, never the manager's mutex — a save fired from the
// engine goroutine mid-bounce must not wait on the lock the bounce
// holds.
type aclStore struct {
	store *confdb.Store
	relay string
}

// aclStoreWait bounds one session-table operation. The engine's own
// goroutine waits on these — a durable replay guard is the price of
// serving the request that moved it — so an unbounded wait would let
// disk contention stop reception itself. Past the bound the operation
// is an error the caller must act on, which is the honest report: the
// store did not take it. Generous next to SQLite's five-second busy
// timeout, short next to a radio going deaf.
const aclStoreWait = 10 * time.Second

// bounded is one store operation's context.
func aclStoreCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), aclStoreWait)
}

func (a *aclStore) LoadSessions() ([]enginemc.PersistedSession, error) {
	ctx, cancel := aclStoreCtx()
	defer cancel()
	rows, err := a.store.LoadACL(ctx, a.relay)
	if err != nil {
		return nil, err
	}
	out := make([]enginemc.PersistedSession, 0, len(rows))
	for _, r := range rows {
		p := enginemc.PersistedSession{
			Perms: r.Perms, Granted: r.Granted, LastTimestamp: r.LastTimestamp,
			HasOut: r.HasOut, OutPath: r.OutPath, OutPathLen: r.OutPathLen,
			Learned: r.Learned, LastActive: r.LastActive,
		}
		copy(p.PubKey[:], r.PubKey)
		out = append(out, p)
	}
	return out, nil
}

func (a *aclStore) SaveSession(p enginemc.PersistedSession) error {
	ctx, cancel := aclStoreCtx()
	defer cancel()
	return a.store.SaveACL(ctx, a.relay, aclRowOf(p))
}

func (a *aclStore) ForgetSession(pubKey [meshcore.PubKeySize]byte) error {
	ctx, cancel := aclStoreCtx()
	defer cancel()
	return a.store.ForgetACL(ctx, a.relay, pubKey[:])
}

func (a *aclStore) ReplaceSession(add enginemc.PersistedSession,
	drop [meshcore.PubKeySize]byte,
) error {
	ctx, cancel := aclStoreCtx()
	defer cancel()
	return a.store.SwapACL(ctx, a.relay, aclRowOf(add), drop[:])
}

// aclRowOf is one session in the shape the store keeps it.
func aclRowOf(p enginemc.PersistedSession) confdb.ACLRow {
	return confdb.ACLRow{
		PubKey: p.PubKey[:], Perms: p.Perms, Granted: p.Granted,
		LastTimestamp: p.LastTimestamp,
		HasOut:        p.HasOut, OutPath: p.OutPath, OutPathLen: p.OutPathLen,
		Learned: p.Learned, LastActive: p.LastActive,
	}
}

// stopRelay stops one relay and waits for it to let its radio go —
// the successor needs the hardware. The caller holds mu.
func (m *manager) stopRelay(name string) {
	h, ok := m.running[name]
	if !ok {
		return
	}
	h.cancel()
	<-h.done
	delete(m.running, name)
	m.log.Info("relay stopped", zap.String("relay", name))
}

// The live views the console reads. Each returns a copy: sessions
// iterate while relays rebuild.

func (m *manager) RelayInfos() []cli.RelayInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.viewMu.RLock()
	defer m.viewMu.RUnlock()
	out := make([]cli.RelayInfo, 0, len(m.infos))
	for _, i := range m.infos {
		out = append(out, i)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (m *manager) RadioInfos() []cli.RadioInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.viewMu.RLock()
	defer m.viewMu.RUnlock()
	out := make([]cli.RadioInfo, 0, len(m.radios))
	for _, i := range m.radios {
		out = append(out, i)
	}
	// The configured-but-unclaimed radios: real objects, no envelope
	// to show yet.
	for name, rd := range m.file.Radios {
		if _, live := m.radios[name]; !live {
			out = append(out, cli.RadioInfo{Name: name, Driver: rd.Driver})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// SensorInfos lists the configured parts and what each last measured.
// Latest is a copy behind the sampler's own lock, so this never waits
// on a bus.
func (m *manager) SensorInfos() []cli.SensorInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]cli.SensorInfo, 0, len(m.file.Sensors))
	for name, sn := range m.file.Sensors {
		info := cli.SensorInfo{
			Name: name, Driver: sn.Driver, SampleInterval: sn.SampleInterval,
			Running: false, Cause: "", Readings: nil,
		}
		if h, live := m.samplers[name]; live {
			info.Running, info.Cause = h.opened()
			info.Readings = h.smp.Latest()
		}
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (m *manager) Traces() map[string][]config.Trace {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.viewMu.RLock()
	out := make(map[string][]config.Trace, len(m.traces)+2)
	maps.Copy(out, m.traces)
	m.viewMu.RUnlock()
	// A radio nobody claims yet, or a relay whose assembly failed,
	// has no assembly to trace — but it is configuration all the
	// same: resolve the layers without hardware, so the persisted
	// form keeps showing in print and replaying in export even while
	// the hardware is missing.
	m.syntheticRadioTraces(out)
	m.syntheticRelayTraces(out)
	addSensorTraces(out, m.file.Sensors)
	// The singletons have no layering, so their "provenance" is the
	// store itself — synthesised here so print works the same way
	// everywhere.
	if sen := m.file.Sentinel; sen != nil {
		rows := []config.Trace{
			{Key: "journal", Value: sen.Journal, Source: sourceConfig},
			{Key: "retention", Value: sen.Retention.String(), Source: sourceConfig},
		}
		if sen.MaxFrames != 0 {
			rows = append(rows, config.Trace{Key: "max_frames", Value: sen.MaxFrames, Source: sourceConfig})
		}
		out[confdb.KindSentinel] = rows
	}
	out[confdb.KindSystem] = m.systemTraces()
	if c := m.file.CLI; c != nil {
		rows := []config.Trace{{Key: attrListen, Value: c.Listen, Source: sourceConfig}}
		if c.Socket != nil {
			rows = append(rows, config.Trace{Key: "socket", Value: *c.Socket, Source: sourceConfig})
		}
		out[confdb.KindCLI] = rows
	}
	if w := m.file.Web; w != nil {
		out[confdb.KindWeb] = []config.Trace{
			{Key: attrListen, Value: w.Listen, Source: sourceConfig},
		}
	}
	m.mqttTraces(out)
	if u := m.file.Update; u != nil {
		rows := []config.Trace{}
		if u.Channel != "" {
			rows = append(rows, config.Trace{Key: "channel", Value: u.Channel, Source: sourceConfig})
		}
		if u.URL != "" {
			rows = append(rows, config.Trace{Key: "url", Value: u.URL, Source: sourceConfig})
		}
		if u.Token != "" {
			rows = append(rows, config.Trace{Key: attrToken, Value: u.Token, Source: sourceConfig})
		}
		if len(rows) > 0 {
			out[confdb.KindUpdate] = rows
		}
	}
	return out
}

// syntheticRadioTraces resolves the layers of every radio no live
// assembly speaks for. The caller holds mu.
func (m *manager) syntheticRadioTraces(out map[string][]config.Trace) {
	for name, rd := range m.file.Radios {
		if _, live := out["radio "+name]; live {
			continue
		}
		rows := radioStructural(rd)
		if drv, err := radio.Lookup(rd.Driver); err == nil {
			if _, traces, rerr := rd.Layered.Resolve(drv.Presets); rerr == nil {
				rows = withStructural(traces, rows)
			}
		}
		out["radio "+name] = rows
	}
}

// syntheticRelayTraces does the same for relays. The caller holds mu.
func (m *manager) syntheticRelayTraces(out map[string][]config.Trace) {
	for name, rc := range m.file.Relays {
		if _, live := out["relay "+name]; live {
			continue
		}
		rows := relayStructural(rc)
		if b, err := protocol.Lookup(rc.Protocol); err == nil {
			if _, traces, rerr := rc.Layered.Resolve(b.Presets); rerr == nil {
				rows = withStructural(traces, rows)
			}
		}
		out["relay "+name] = rows
	}
}

// systemTraces shows the system block as it is felt: the name however
// it was decided, and the log level in effect, flag or override. The
// caller holds mu.
func (m *manager) systemTraces() []config.Trace {
	rows := []config.Trace{
		{Key: attrName, Value: m.systemName(), Source: m.nameSource()},
	}
	levelSource := "flag"
	if m.file.System != nil && m.file.System.LogLevel != "" {
		levelSource = sourceConfig
	}
	if m.logKnob != (zap.AtomicLevel{}) {
		rows = append(rows, config.Trace{
			Key: attrLogLevel, Value: logging.LevelName(m.logKnob.Level()), Source: levelSource,
		})
	}
	return rows
}

// historyCap bounds one windowed read, the way frames bounds its own;
// the listing confesses when the window held more.
const historyCap = 1000

// History reads the revision journal for the console, newest first:
// who changed what, when — values as the store recorded them, which
// means secrets arrive already masked. around= centres the window on
// one revision's moment, a span each side.
func (m *manager) History(ctx context.Context, q cli.HistoryQuery) ([]cli.HistoryEntry, int, error) {
	since, until := q.Since, q.Until
	if q.AroundID != 0 {
		anchor, err := m.store.RevisionByID(ctx, q.AroundID)
		if err != nil {
			return nil, 0, err
		}
		span := q.Span
		if span == 0 {
			span = time.Minute
		}
		since, until = anchor.At.Add(-span), anchor.At.Add(span)
	}
	limit := q.Count
	switch {
	case limit > historyCap:
		return nil, 0, fmt.Errorf("last= wants 1..%d", historyCap)
	case limit > 0:
	case !since.IsZero() || !until.IsZero():
		limit = historyCap
	default:
		limit = 50
	}
	revs, total, err := m.store.RevisionsIn(ctx, since, until, limit)
	if err != nil {
		return nil, 0, err
	}
	out := make([]cli.HistoryEntry, 0, len(revs))
	for _, r := range revs {
		e := cli.HistoryEntry{
			ID: r.ID, At: r.At, Principal: r.Principal,
			Kind: r.Kind, Name: r.Name, Op: r.Op,
		}
		changes, err := r.Changes()
		if err != nil {
			// A row the journal cannot decode still names itself.
			e.Changes = []cli.AttrDelta{{Attr: "change", New: "(unreadable: " + err.Error() + ")"}}
		} else {
			attrs := make([]string, 0, len(changes))
			for attr := range changes {
				attrs = append(attrs, attr)
			}
			sort.Strings(attrs)
			for _, attr := range attrs {
				c := changes[attr]
				e.Changes = append(e.Changes, cli.AttrDelta{
					Attr: attr, Old: deltaValue(c.Old), New: deltaValue(c.New),
				})
			}
		}
		out = append(out, e)
	}
	return out, total, nil
}

// deltaValue renders one side of a recorded change; nil is absence,
// not the word "nil".
func deltaValue(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

// SystemName is what this installation calls itself — the console's
// prompt, and whatever a browser puts in its title bar, read the same
// answer from here. Safe from any goroutine.
func (m *manager) SystemName() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.systemName()
}

// systemName resolves the configured name or falls back to the
// machine's. The caller holds mu.
func (m *manager) systemName() string {
	if m.file.System != nil && m.file.System.Name != "" {
		return m.file.System.Name
	}
	return machineName()
}

// nameSource says where the shown name came from, so print does not
// pass a fallback off as a choice. The caller holds mu.
func (m *manager) nameSource() string {
	if m.file.System != nil && m.file.System.Name != "" {
		return sourceConfig
	}
	return "hostname"
}

// machineName is the host's own name, read once: it does not change
// under a running daemon, and a lookup per prompt would be a syscall
// per keystroke. A host that will not say gets the product's name,
// because a prompt with a hole in it helps nobody.
var machineName = sync.OnceValue(func() string {
	name, err := os.Hostname()
	if err != nil || name == "" {
		return "lotor"
	}
	// The short form: an operator knows which domain they are in, and
	// a prompt is not the place to restate it.
	short, _, _ := strings.Cut(name, ".")
	return short
})

// The structural rows: what an object IS, rendered beside what its
// layers resolved, so print and export see one coherent surface.

func relayStructural(rc config.Relay) []config.Trace {
	rows := []config.Trace{
		{Key: attrProtocol, Value: rc.Protocol, Source: sourceConfig},
		{Key: attrRadio, Value: rc.Radio, Source: sourceConfig},
		{Key: attrProfile, Value: profileName(rc.Layered), Source: sourceConfig},
	}
	if rc.NoiseHistory != nil {
		rows = append(rows, config.Trace{Key: attrNoiseHistory, Value: *rc.NoiseHistory, Source: sourceConfig})
	}
	if rc.TX != nil {
		rows = append(rows,
			config.Trace{Key: attrTXMode, Value: rc.TX.Mode, Source: sourceConfig},
			config.Trace{Key: attrTXExhausted, Value: rc.TX.LBTExhausted, Source: sourceConfig},
			config.Trace{Key: attrTXQueueDepth, Value: rc.TX.QueueDepth, Source: sourceConfig},
			config.Trace{Key: attrTXCAD, Value: rc.TX.CAD == nil || *rc.TX.CAD, Source: sourceConfig},
		)
		if rc.TX.LBTThresholdDB != 0 {
			rows = append(rows, config.Trace{Key: attrTXThreshold, Value: rc.TX.LBTThresholdDB, Source: sourceConfig})
		}
	}
	return rows
}

// addSensorTraces resolves every sensor's layers without hardware. A
// sensor is never claimed, so it is always in this shape — there is no
// assembled counterpart to prefer, as there is for a live radio.
func addSensorTraces(out map[string][]config.Trace, sensors map[string]config.Sensor) {
	for name, sn := range sensors {
		rows := sensorStructural(sn)
		if _, err := sensor.Lookup(sn.Driver); err == nil {
			if _, traces, rerr := sn.Layered.Resolve(nil); rerr == nil {
				rows = withStructural(traces, rows)
			}
		}
		out["sensor "+name] = rows
	}
}

func sensorStructural(sn config.Sensor) []config.Trace {
	return []config.Trace{
		{Key: attrDriver, Value: sn.Driver, Source: sourceConfig},
		{Key: attrSampleEvery, Value: sn.SampleInterval.String(), Source: sourceConfig},
	}
}

func radioStructural(rd config.Radio) []config.Trace {
	return []config.Trace{
		{Key: attrDriver, Value: rd.Driver, Source: sourceConfig},
		{Key: attrProfile, Value: profileName(rd.Layered), Source: sourceConfig},
	}
}

func profileName(l config.Layered) string {
	if l.Profile == "" {
		return config.CustomProfile
	}
	return l.Profile
}

// withStructural merges the structural rows into a resolved trace
// set, sorted like the rest.
func withStructural(traces, structural []config.Trace) []config.Trace {
	out := make([]config.Trace, 0, len(traces)+len(structural))
	out = append(out, traces...)
	out = append(out, structural...)
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

// Mutate applies one line of changes: parse against the schema,
// apply to a copy, validate the whole, persist with its revision,
// swap, bounce the owning relay. Nothing is written unless everything
// upstream of the hardware agreed.
func (m *manager) Mutate(ctx context.Context, kind, name string,
	set map[string]string, unset []string, principal string,
) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// identity=new mints in place, the way Create does: the operator
	// asks for a key rather than pasting one, and the seed never
	// leaves the daemon. Replacing a live identity is a real act —
	// the mesh learns a new node and forgets the paths to the old —
	// so the minted public key is returned for the console to show.
	minted := ""
	if kind == confdb.KindRelay && set[attrIdentity] == "new" {
		seed, pub, err := mintIdentity()
		if err != nil {
			return "", err
		}
		set = maps.Clone(set)
		set[attrIdentity], minted = seed, pub
	}
	typed, err := m.parseChanges(kind, name, set, unset)
	if err != nil {
		return "", err
	}
	msg, err := m.applyTyped(ctx, kind, name, typed, unset, principal, opOf(set, unset))
	if err != nil {
		return "", err
	}
	if minted != "" {
		msg += fmt.Sprintf(" (new identity, pubkey %s)", minted)
	}
	return msg, nil
}

// maskSecrets replaces every secret value in a revision with the mask,
// asking the schema which attributes those are — the same Secret flag
// print and export honour. Naming them here would be a fourth list to
// keep in step, and lists drift.
func (m *manager) maskSecrets(f *config.File, kind, name string, change map[string]confdb.Change) {
	k, choice, err := m.kindAndChoiceIn(f, kind, name)
	if err != nil {
		return // the caller's own error path reports it
	}
	attrs := k.AttrsFor(choice)
	known := make(map[string]bool, len(attrs))
	mask := func(name string) {
		c, ok := change[name]
		if !ok {
			return
		}
		if c.Old != nil {
			c.Old = maskedChange
		}
		if c.New != nil {
			c.New = maskedChange
		}
		change[name] = c
	}
	for _, a := range attrs {
		known[a.Name] = true
		if a.Secret {
			mask(a.Name)
		}
	}
	// An orphan — a key only a past shape of the schema could name —
	// cannot say whether it was secret; the mask is the safe default.
	for name := range change {
		if !known[name] {
			mask(name)
		}
	}
}

// undoValues turns a revision's recorded changes into the set and
// unset an undo applies: the old value becomes the new one, and an
// attribute that had none is unset.
//
// A secret is masked precisely so it cannot be read back, which
// leaves nothing to restore. Replaying the mask would write the word
// "<secret>" into a live credential and report success; refusing says
// what to do instead.
func undoValues(revID int64, changes map[string]confdb.Change) (map[string]any, []string, error) {
	typed := map[string]any{}
	var unset []string
	for attr, c := range changes {
		if c.Old == nil {
			unset = append(unset, attr)
			continue
		}
		if c.Old == maskedChange {
			return nil, nil, fmt.Errorf(
				"revision %d changed %s, a secret the history does not record — "+
					"undo cannot restore it; set it by hand", revID, attr)
		}
		typed[attr] = c.Old
	}
	return typed, unset, nil
}

// Undo inverts the newest recorded mutation — the old values become
// the new ones, recorded like any other change so an undo can itself
// be undone.
func (m *manager) Undo(ctx context.Context, principal string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	rev, err := m.store.LastMutation(ctx)
	if err != nil {
		return "", err
	}
	changes, err := rev.Changes()
	if err != nil {
		return "", err
	}
	typed, unset, err := undoValues(rev.ID, changes)
	if err != nil {
		return "", err
	}
	msg, err := m.applyTyped(ctx, rev.Kind, rev.Name, typed, unset, principal, "undo")
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("undid revision %d (%s by %s) — %s",
		rev.ID, rev.Op, rev.Principal, msg), nil
}

// opOf names what a line did, for its revision.
func opOf(set map[string]string, unset []string) string {
	if len(set) == 0 && len(unset) > 0 {
		return "unset"
	}
	return "set"
}

// applyTyped is the shared tail of Mutate and Undo: typed values in,
// validated configuration out, persisted and running. The caller
// holds mu.
func (m *manager) applyTyped(ctx context.Context, kind, name string,
	typed map[string]any, unset []string, principal, op string,
) (string, error) {
	next, err := cloneFile(m.file)
	if err != nil {
		return "", err
	}
	change, relayName, err := applyChanges(next, kind, name, typed, unset)
	if err != nil {
		return "", err
	}
	m.maskSecrets(next, kind, name, change)
	if err := next.Validate(false); err != nil {
		return "", err
	}
	if err := deepCheck(next, kind, name, relayName); err != nil {
		return "", err
	}

	section, err := objectSection(next, kind, name)
	if err != nil {
		return "", err
	}
	if err := m.store.Replace(ctx, kind, name, section, principal, op, change); err != nil {
		return "", err
	}
	m.file = next

	if relayName == "" {
		switch kind {
		case confdb.KindSystem:
			// Both attributes are read live.
			m.applyLogLevel()
			return fmt.Sprintf("applied — this system is now %s, logging at %s",
				cli.TerminalSafe(m.systemName()), m.liveLevelName()), nil
		case confdb.KindSentinel, confdb.KindCLI, confdb.KindWeb:
			return "applied — takes effect when the daemon restarts", nil
		case confdb.KindUpdate:
			// check and install read the store live; nothing bounces.
			return "applied — the next check reads it", nil
		case confdb.KindSensor:
			// The part is reopened; no relay is disturbed by it.
			m.bounceSampler(name)
			return "applied — sensor " + name, nil
		case confdb.KindMQTT:
			m.bounceObserver(name)
			if next.MQTT[name].Disabled {
				return "applied — observer " + name + " disabled", nil
			}
			return "applied — observer " + name + " reconnecting", nil
		}
		return "applied — no running relay uses this yet", nil
	}
	m.stopRelay(relayName)
	// The successor lives as long as the daemon, not as long as the
	// session that ordered the change — hence the stored context, not
	// the request's.
	m.startRelay(m.ctx, relayName) //nolint:contextcheck // deliberate: daemon lifetime
	// The observers watching it captured its face — name, key,
	// waveform — and must follow the successor.
	m.bounceObserversOf(relayName)
	return fmt.Sprintf("applied — relay %s restarting", relayName), nil
}

// deepCheck runs the checks the assembly would, minus the hardware:
// resolution, strict decode, every override scope.
func deepCheck(next *config.File, kind, name, relayName string) error {
	switch {
	case relayName != "":
		// The whole pre-hardware preparation, not just resolution: a
		// transmit gate the assembly would refuse — no identity, no
		// node name, no duty ceiling — must be refused here, while the
		// running relay is still untouched and nothing has persisted.
		rc := next.Relays[relayName]
		if err := preflight(relayName, rc, next.Radios[rc.Radio]); err != nil {
			return err
		}
	case kind == confdb.KindRadio:
		return checkRadioAlone(next.Radios[name])
	case kind == confdb.KindSensor:
		return checkSensorAlone(next.Sensors[name])
	case kind == confdb.KindMQTT:
		_, err := resolveMQTTParams(next.MQTT[name])
		return err
	}
	return nil
}

// Create brings one object into existence: the structural minimum in
// its fields, everything else parsed like any set. A relay starts the
// moment it exists; a radio waits to be claimed. identity=new mints a
// fresh node identity in the daemon — the seed goes into the store and
// never over the wire, only the public key comes back.
func (m *manager) Create(ctx context.Context, kind, name string,
	attrs map[string]string, principal string,
) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// The same grammar the file and the import answer to: one rule,
	// so what an operator may create is exactly what a configuration
	// may carry and an export may restore.
	if err := config.ValidInstanceName(name); err != nil {
		return "", err
	}
	next, err := cloneFile(m.file)
	if err != nil {
		return "", err
	}
	var minted string
	change := map[string]confdb.Change{}
	switch kind {
	case confdb.KindRelay:
		if _, dup := next.Relays[name]; dup {
			return "", fmt.Errorf("relay %q already exists", name)
		}
		minted, err = m.createRelay(next, name, attrs, change)
	case confdb.KindRadio:
		if _, dup := next.Radios[name]; dup {
			return "", fmt.Errorf("radio %q already exists", name)
		}
		err = m.createRadio(next, name, attrs, change)
	case confdb.KindSensor:
		if _, dup := next.Sensors[name]; dup {
			return "", fmt.Errorf("sensor %q already exists", name)
		}
		err = m.createSensor(next, name, attrs, change)
	case confdb.KindMQTT:
		if _, dup := next.MQTT[name]; dup {
			return "", fmt.Errorf("observer %q already exists", name)
		}
		err = m.createMQTT(next, name, attrs, change)
	default:
		return "", fmt.Errorf("%q takes no new instances", kind)
	}
	if err != nil {
		return "", err
	}
	msg, err := m.commitCreate(ctx, next, kind, name, principal, change)
	if err != nil {
		return "", err
	}
	if minted != "" {
		msg += fmt.Sprintf(" (new identity, pubkey %s)", minted)
	}
	return msg, nil
}

// commitCreate is Create's tail: validate, persist, start.
func (m *manager) commitCreate(ctx context.Context, next *config.File,
	kind, name, principal string, change map[string]confdb.Change,
) (string, error) {
	if err := next.Validate(false); err != nil {
		return "", err
	}
	switch kind {
	case confdb.KindRelay:
		rc := next.Relays[name]
		if err := preflight(name, rc, next.Radios[rc.Radio]); err != nil {
			return "", err
		}
	case confdb.KindRadio:
		if err := checkRadioAlone(next.Radios[name]); err != nil {
			return "", err
		}
	case confdb.KindSensor:
		if err := checkSensorAlone(next.Sensors[name]); err != nil {
			return "", err
		}
	case confdb.KindMQTT:
		if _, err := resolveMQTTParams(next.MQTT[name]); err != nil {
			return "", err
		}
	}
	m.maskSecrets(next, kind, name, change)
	section, err := objectSection(next, kind, name)
	if err != nil {
		return "", err
	}
	if err := m.store.Replace(ctx, kind, name, section, principal, "add", change); err != nil {
		return "", err
	}
	m.file = next
	switch kind {
	case confdb.KindRelay:
		m.startRelay(m.ctx, name) //nolint:contextcheck // deliberate: daemon lifetime
		// The new relay changes what "the only relay" resolves to: the
		// observers reading that phrase must follow, now and not at the
		// next restart.
		m.reconcileObservers()
		return fmt.Sprintf("added — relay %s starting", name), nil
	case confdb.KindMQTT:
		m.startObserver(m.ctx, name) //nolint:contextcheck // deliberate: daemon lifetime
		return fmt.Sprintf("added — observer %s connecting", name), nil
	case confdb.KindSensor:
		m.startSampler(m.ctx, name) //nolint:contextcheck // deliberate: daemon lifetime
		return "added — sensor " + name, nil
	}
	return fmt.Sprintf("added — %s %s", kind, name), nil
}

// createRelay fills a new relay from its creation line. protocol and
// radio are the identity of the thing; they are required here and
// immutable after.
func (m *manager) createRelay(next *config.File, name string,
	attrs map[string]string, change map[string]confdb.Change,
) (minted string, err error) {
	rc := config.Relay{
		Protocol: attrs[attrProtocol],
		Radio:    attrs[attrRadio],
		Layered:  config.Layered{Profile: attrs[attrProfile]},
	}
	if rc.Protocol == "" || rc.Radio == "" {
		return "", errors.New("a new relay needs protocol= and radio=")
	}
	for _, a := range []string{attrProtocol, attrRadio, attrProfile} {
		if v, ok := attrs[a]; ok {
			change[a] = confdb.Change{New: v}
		}
	}
	rest := make(map[string]string, len(attrs))
	for k, v := range attrs {
		if k == attrProtocol || k == attrRadio || k == attrProfile {
			continue
		}
		rest[k] = v
	}
	if rest[attrIdentity] == "new" {
		seed, pub, err := mintIdentity()
		if err != nil {
			return "", err
		}
		rest[attrIdentity], minted = seed, pub
	}
	next.Relays[name] = rc
	typed, err := m.parseAgainst(confdb.KindRelay, rc.Protocol, rest, nil)
	if err != nil {
		return "", err
	}
	rc = next.Relays[name]
	for attr, v := range typed {
		if _, err := setRelayAttr(&rc, attr, v); err != nil {
			return "", err
		}
		change[attr] = confdb.Change{New: v}
	}
	next.Relays[name] = rc
	return minted, nil
}

// mqttTraces resolves each observer's layers into provenance rows,
// exactly as a radio's are shown: profile rows, override rows, and
// the structural profile knob beside them.
func (m *manager) mqttTraces(out map[string][]config.Trace) {
	for name, mq := range m.file.MQTT {
		rows := []config.Trace{
			{Key: attrProfile, Value: profileName(mq.Layered), Source: sourceConfig},
		}
		if mq.Disabled {
			rows = append(rows, config.Trace{Key: attrDisabled, Value: true, Source: sourceConfig})
		}
		if _, traces, err := mq.Layered.Resolve(mqtt.Presets()); err == nil {
			rows = withStructural(traces, rows)
		}
		out[confdb.KindMQTT+" "+name] = rows
	}
}

// createMQTT fills a new observer from its creation line: profile is
// the structural knob, everything else lands in its override scope —
// and the resolved whole must already name a dialable broker.
func (m *manager) createMQTT(next *config.File, name string,
	attrs map[string]string, change map[string]confdb.Change,
) error {
	mq := config.MQTT{Layered: config.Layered{Profile: attrs[attrProfile]}}
	if v, ok := attrs[attrProfile]; ok {
		change[attrProfile] = confdb.Change{New: v}
	}
	rest := make(map[string]string, len(attrs))
	for k, v := range attrs {
		if k != attrProfile {
			rest[k] = v
		}
	}
	typed, err := m.parseAgainst(confdb.KindMQTT, "", rest, nil)
	if err != nil {
		return err
	}
	for attr, v := range typed {
		if attr == attrDisabled {
			b, err := asBool(attr, v)
			if err != nil {
				return err
			}
			mq.Disabled = b
			change[attr] = confdb.Change{New: fmt.Sprintf("%v", v)}
			continue
		}
		v, err := mqttOverrideValue(attr, v)
		if err != nil {
			return err
		}
		setOverride(&mq.Layered, attr, v)
		change[attr] = confdb.Change{New: fmt.Sprintf("%v", v)}
	}
	if next.MQTT == nil {
		next.MQTT = map[string]config.MQTT{}
	}
	next.MQTT[name] = mq
	return nil
}

// orderedAttrs walks one mutation's keys deterministically, the
// structural profile FIRST: it decides which scope every override on
// the same line lands in, and a Go map's iteration order must not —
// "set profile=custom node_name=x" wrote node_name into the OLD
// profile's scope whenever the map happened to yield it first.
func orderedAttrs(typed map[string]any) []string {
	keys := make([]string, 0, len(typed))
	for k := range typed {
		if k != attrProfile {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	if _, held := typed[attrProfile]; held {
		keys = append([]string{attrProfile}, keys...)
	}
	return keys
}

// applyMQTTChanges edits one observer's layers: the profile knob on
// its field, everything else into the live profile's override scope,
// the same discipline a relay's waveform follows.
func applyMQTTChanges(next *config.File, name string,
	typed map[string]any, unset []string,
) (map[string]confdb.Change, error) {
	mq, ok := next.MQTT[name]
	if !ok {
		return nil, fmt.Errorf("no observer %q", name)
	}
	change := map[string]confdb.Change{}
	for _, attr := range orderedAttrs(typed) {
		old, stored, err := setMQTTAttr(&mq, attr, typed[attr])
		if err != nil {
			return nil, err
		}
		change[attr] = confdb.Change{Old: old, New: stored}
	}
	for _, attr := range unset {
		if attr == attrProfile {
			return nil, errors.New("profile cannot be unset — set it to what it should be")
		}
		if attr == attrDisabled {
			change[attr] = confdb.Change{Old: mq.Disabled}
			mq.Disabled = false
			continue
		}
		old, err := unsetOverride(&mq.Layered, attr)
		if err != nil {
			return nil, err
		}
		change[attr] = confdb.Change{Old: old}
	}
	next.MQTT[name] = mq
	return change, nil
}

// setMQTTAttr writes one observer attribute where it belongs — the
// structural knobs on their fields, everything else into the live
// profile's override scope — and reports what it held and what was
// actually stored, normalization included.
func setMQTTAttr(mq *config.MQTT, attr string, v any) (old, stored any, err error) {
	switch attr {
	case attrProfile:
		old = mq.Layered.Profile
		text, err := asString(attr, v)
		if err != nil {
			return nil, nil, err
		}
		mq.Layered.Profile = text
		return old, text, nil
	case attrDisabled:
		old = mq.Disabled
		b, err := asBool(attr, v)
		if err != nil {
			return nil, nil, err
		}
		mq.Disabled = b
		return old, b, nil
	default:
		v, err := mqttOverrideValue(attr, v)
		if err != nil {
			return nil, nil, err
		}
		return setOverride(&mq.Layered, attr, v), v, nil
	}
}

// attrDisabled is the parking flag: the object stays, nothing runs.
const attrDisabled = "disabled"

// attrIATA is the region code, normalized at the door like the rest
// of the ecosystem stores it: validated and uppercased before it
// lands in an override, so print and the wire say the same thing.
const attrIATA = "iata"

// mqttOverrideValue is the one door an observer override value passes
// through, whichever line wrote it: iata comes out normalized, owner
// proven a key, everything else verbatim.
func mqttOverrideValue(attr string, v any) (any, error) {
	switch attr {
	case attrIATA:
		text, err := asString(attr, v)
		if err != nil {
			return nil, err
		}
		return mqtt.NormalizeIATA(text)
	case "owner":
		text, err := asString(attr, v)
		if err != nil {
			return nil, err
		}
		return text, mqtt.ValidOwner(text)
	}
	return v, nil
}

// hasBrokerScheme admits the transports the client dials.
func hasBrokerScheme(url string) bool {
	for _, scheme := range []string{"tcp://", "ssl://", "ws://", "wss://"} {
		if strings.HasPrefix(url, scheme) {
			return true
		}
	}
	return false
}

// maskedChange stands in a revision for a value too secret to record.
const maskedChange = "<secret>"

// attrToken is the update block's secret attribute.
const attrToken = "token"

// createSensor brings one part into existence: the driver it speaks
// to, and whatever that driver needs to find it on its bus.
func (m *manager) createSensor(next *config.File, name string,
	attrs map[string]string, change map[string]confdb.Change,
) error {
	sn := config.Sensor{
		Driver:         attrs[attrDriver],
		SampleInterval: 0,
		// No profile: every value lands in the one scope setOverride
		// gives a layering with none, which is what "custom" means.
		Layered: config.Layered{Profile: "", Overrides: nil},
	}
	if sn.Driver == "" {
		return errors.New("a new sensor needs driver=")
	}
	if v, ok := attrs[attrDriver]; ok {
		change[attrDriver] = confdb.Change{New: v}
	}
	rest := make(map[string]string, len(attrs))
	for k, v := range attrs {
		if k == attrDriver {
			continue
		}
		rest[k] = v
	}
	if next.Sensors == nil {
		next.Sensors = map[string]config.Sensor{}
	}
	next.Sensors[name] = sn
	typed, err := m.parseAgainst(confdb.KindSensor, sn.Driver, rest, nil)
	if err != nil {
		return err
	}
	sn = next.Sensors[name]
	for attr, v := range typed {
		if _, err := setSensorAttr(&sn, attr, v); err != nil {
			return err
		}
		change[attr] = confdb.Change{New: v}
	}
	next.Sensors[name] = sn
	return nil
}

func (m *manager) createRadio(next *config.File, name string,
	attrs map[string]string, change map[string]confdb.Change,
) error {
	rd := config.Radio{
		Driver:  attrs[attrDriver],
		Layered: config.Layered{Profile: attrs[attrProfile]},
	}
	if rd.Driver == "" {
		return errors.New("a new radio needs driver=")
	}
	for _, a := range []string{attrDriver, attrProfile} {
		if v, ok := attrs[a]; ok {
			change[a] = confdb.Change{New: v}
		}
	}
	rest := make(map[string]string, len(attrs))
	for k, v := range attrs {
		if k == attrDriver || k == attrProfile {
			continue
		}
		rest[k] = v
	}
	next.Radios[name] = rd
	typed, err := m.parseAgainst(confdb.KindRadio, rd.Driver, rest, nil)
	if err != nil {
		return err
	}
	rd = next.Radios[name]
	for attr, v := range typed {
		if _, err := setRadioAttr(&rd, attr, v); err != nil {
			return err
		}
		change[attr] = confdb.Change{New: v}
	}
	next.Radios[name] = rd
	return nil
}

// Remove takes one object out of existence. A relay stops first; a
// radio somebody claims refuses; the sentinel and the CLI blocks come
// off at the next daemon start.
func (m *manager) Remove(ctx context.Context, kind, name, principal string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	next, err := cloneFile(m.file)
	if err != nil {
		return "", err
	}
	// An observer explicitly pointing at this relay is a claim,
	// refused exactly like a relay's claim on a radio: silently
	// orphaning the reference would leave a connection publishing for
	// a relay the tree no longer holds. Implicit observers claim
	// nothing — "the only relay" is a phrase, not a reference — and
	// the reconciliation below re-reads it.
	if kind == confdb.KindRelay {
		for obsName, mq := range m.file.MQTT {
			if p, err := resolveMQTTParams(mq); err == nil && p.Relay == name {
				return "", fmt.Errorf("observer %q claims this relay — remove it first", obsName)
			}
		}
	}
	msg, err := removeFromFile(next, kind, name)
	if err != nil {
		return "", err
	}
	if err := next.Validate(false); err != nil {
		return "", err
	}
	if err := m.store.Remove(ctx, kind, name, principal); err != nil {
		return "", err
	}
	m.file = next
	if kind == confdb.KindSensor {
		m.stopSampler(name)
	}
	if kind == confdb.KindRelay {
		m.stopRelay(name)
		m.viewMu.Lock()
		delete(m.infos, name)
		delete(m.cfgs, name)
		delete(m.traces, "relay "+name)
		for radioName, info := range m.radios {
			if info.Relay == name {
				delete(m.radios, radioName)
				delete(m.traces, "radio "+radioName)
			}
		}
		m.viewMu.Unlock()
		m.reconcileObservers()
	}
	if kind == confdb.KindMQTT {
		m.stopObserver(name)
		delete(m.obsCause, name)
	}
	return msg, nil
}

// removeFromFile deletes one object from the copy, naming what that
// takes with it.
func removeFromFile(next *config.File, kind, name string) (string, error) {
	switch kind {
	case confdb.KindRelay:
		if _, ok := next.Relays[name]; !ok {
			return "", fmt.Errorf("no relay %q", name)
		}
		delete(next.Relays, name)
		return fmt.Sprintf("removed — relay %s stopped", name), nil
	case confdb.KindRadio:
		if _, ok := next.Radios[name]; !ok {
			return "", fmt.Errorf("no radio %q", name)
		}
		for rn, rl := range next.Relays {
			if rl.Radio == name {
				return "", fmt.Errorf("relay %q claims this radio — remove it first", rn)
			}
		}
		delete(next.Radios, name)
		return "removed — radio " + name, nil
	case confdb.KindSensor:
		if _, ok := next.Sensors[name]; !ok {
			return "", fmt.Errorf("no sensor %q", name)
		}
		// Nothing claims a sensor, so nothing has to let go of it
		// first: the relays that read it simply stop finding it.
		delete(next.Sensors, name)
		return "removed — sensor " + name, nil
	case confdb.KindSentinel:
		next.Sentinel = nil
		return removedAtRestart, nil
	case confdb.KindCLI:
		next.CLI = nil
		return removedAtRestart, nil
	case confdb.KindWeb:
		next.Web = nil
		return removedAtRestart, nil
	case confdb.KindMQTT:
		if _, ok := next.MQTT[name]; !ok {
			return "", fmt.Errorf("no observer %q", name)
		}
		delete(next.MQTT, name)
		return "removed — observer " + name + " disconnected", nil
	}
	return "", fmt.Errorf("%q cannot be removed", kind)
}

// mintIdentity draws a fresh node identity: the seed for the store,
// the public key for the operator.
func mintIdentity() (seed, pubkey string, err error) {
	raw := make([]byte, meshcore.SeedSize)
	if _, err := rand.Read(raw); err != nil {
		return "", "", err
	}
	id, err := meshcore.LocalIdentityFromSeed(raw)
	if err != nil {
		return "", "", err
	}
	return hex.EncodeToString(raw), hex.EncodeToString(id.PubKey[:]), nil
}

// parseAgainst is parseChanges against an explicit choice — the
// creation path, where the object is not in the file yet.
func (m *manager) parseAgainst(kind, choice string,
	set map[string]string, unset []string,
) (map[string]any, error) {
	var k *schema.Kind
	for i := range m.kinds {
		if m.kinds[i].Name == kind {
			k = &m.kinds[i]
			break
		}
	}
	if k == nil {
		return nil, fmt.Errorf("no such context %q", kind)
	}
	attrs := k.AttrsFor(choice)
	typed := make(map[string]any, len(set))
	for attr, text := range set {
		a, ok := schema.Find(attrs, attr)
		if !ok {
			return nil, fmt.Errorf("no attribute %q here — \"?\" lists them", attr)
		}
		v, err := schema.Parse(a, text)
		if err != nil {
			return nil, err
		}
		typed[attr] = v
	}
	for _, attr := range unset {
		if _, ok := schema.Find(attrs, attr); !ok {
			return nil, fmt.Errorf("no attribute %q here — \"?\" lists them", attr)
		}
	}
	return typed, nil
}

// parseChanges types every value against the schema the instance's
// choice resolves — an unknown attribute or a value of the wrong
// shape is refused before anything else happens. The one exception is
// an unset naming an orphan: a key a past shape stored that the
// schema no longer speaks. Refusing those stranded them in the store
// with no door but sqlite — clearing is the one verb an orphan still
// answers to.
func (m *manager) parseChanges(kind, name string,
	set map[string]string, unset []string,
) (map[string]any, error) {
	_, choice, err := m.kindAndChoice(kind, name)
	if err != nil {
		return nil, err
	}
	var orphans, known []string
	for _, attr := range unset {
		if m.orphanOverride(kind, name, attr) {
			orphans = append(orphans, attr)
			continue
		}
		known = append(known, attr)
	}
	typed, err := m.parseAgainst(kind, choice, set, known)
	if err != nil {
		return nil, err
	}
	// The orphans need no validation beyond existing: the apply path
	// deletes override keys by name and asks the schema nothing.
	_ = orphans
	return typed, nil
}

// orphanOverride reports whether an attribute, unknown to the current
// schema, still sits in the object's live override scope — stored by
// a past shape of the software, removable and nothing else.
func (m *manager) orphanOverride(kind, name, attr string) bool {
	var l *config.Layered
	switch kind {
	case confdb.KindRelay:
		if rc, ok := m.file.Relays[name]; ok {
			l = &rc.Layered
		}
	case confdb.KindRadio:
		if rd, ok := m.file.Radios[name]; ok {
			l = &rd.Layered
		}
	case confdb.KindSensor:
		if sn, ok := m.file.Sensors[name]; ok {
			l = &sn.Layered
		}
	case confdb.KindMQTT:
		if mq, ok := m.file.MQTT[name]; ok {
			l = &mq.Layered
		}
	}
	if l == nil {
		return false
	}
	scope := l.Profile
	if scope == "" {
		scope = config.CustomProfile
	}
	if _, held := l.Overrides[scope][attr]; !held {
		return false
	}
	// Known attributes go through the schema door; only what the
	// schema no longer names is an orphan.
	k, choice, err := m.kindAndChoice(kind, name)
	if err != nil {
		return false
	}
	_, known := schema.Find(k.AttrsFor(choice), attr)
	return !known
}

func (m *manager) kindAndChoice(kind, name string) (*schema.Kind, string, error) {
	return m.kindAndChoiceIn(m.file, kind, name)
}

// kindAndChoiceIn resolves against a given configuration rather than
// the running one: an object being created lives only in the file
// about to be committed.
func (m *manager) kindAndChoiceIn(f *config.File, kind, name string) (*schema.Kind, string, error) {
	var k *schema.Kind
	for i := range m.kinds {
		if m.kinds[i].Name == kind {
			k = &m.kinds[i]
			break
		}
	}
	if k == nil {
		return nil, "", fmt.Errorf("no such context %q", kind)
	}
	if k.Singleton {
		return k, "", nil
	}
	switch kind {
	case confdb.KindRelay:
		rc, ok := f.Relays[name]
		if !ok {
			return nil, "", fmt.Errorf("no relay %q", name)
		}
		return k, rc.Protocol, nil
	case confdb.KindRadio:
		rd, ok := f.Radios[name]
		if !ok {
			return nil, "", fmt.Errorf("no radio %q", name)
		}
		return k, rd.Driver, nil
	case confdb.KindSensor:
		sn, ok := f.Sensors[name]
		if !ok {
			return nil, "", fmt.Errorf("no sensor %q", name)
		}
		return k, sn.Driver, nil
	case confdb.KindMQTT:
		if _, ok := f.MQTT[name]; !ok {
			return nil, "", fmt.Errorf("no observer %q", name)
		}
		return k, "", nil
	}
	return nil, "", fmt.Errorf("%q is not configurable from here yet", kind)
}

// applyChanges edits the copy: structural attributes onto their
// fields, contributed ones into the override scope of the current
// profile — exactly where a hand-written file would have put them.
// It returns what changed, old value by old value, and which running
// relay must bounce to feel it.
func applyChanges(next *config.File, kind, name string,
	typed map[string]any, unset []string,
) (map[string]confdb.Change, string, error) {
	switch kind {
	case confdb.KindSentinel:
		change, err := applySentinelChanges(next, typed, unset)
		return change, "", err
	case confdb.KindCLI:
		change, err := applyCLIChanges(next, typed, unset)
		return change, "", err
	case confdb.KindWeb:
		change, err := applyWebChanges(next, typed, unset)
		return change, "", err
	case confdb.KindSystem:
		change, err := applySystemChanges(next, typed, unset)
		return change, "", err
	case confdb.KindUpdate:
		change, err := applyUpdateChanges(next, typed, unset)
		return change, "", err
	case confdb.KindMQTT:
		change, err := applyMQTTChanges(next, name, typed, unset)
		return change, "", err
	case confdb.KindRelay:
		change, err := applyRelayChanges(next, name, typed, unset)
		return change, name, err
	case confdb.KindSensor:
		// No relay restarts for a sensor: its sampler is the daemon's,
		// and the relays that read its cache never held it.
		change, err := applySensorChanges(next, name, typed, unset)
		return change, "", err
	case confdb.KindRadio:
		change, err := applyRadioChanges(next, name, typed, unset)
		if err != nil {
			return nil, "", err
		}
		// The owner bounces; a radio nobody claimed applies when one does.
		for rn, rl := range next.Relays {
			if rl.Radio == name {
				return change, rn, nil
			}
		}
		return change, "", nil
	}
	return nil, "", fmt.Errorf("%q is not configurable from here yet", kind)
}

func applyRelayChanges(next *config.File, name string,
	typed map[string]any, unset []string,
) (map[string]confdb.Change, error) {
	change := map[string]confdb.Change{}
	rc := next.Relays[name]
	for _, attr := range orderedAttrs(typed) {
		old, err := setRelayAttr(&rc, attr, typed[attr])
		if err != nil {
			return nil, err
		}
		change[attr] = confdb.Change{Old: old, New: typed[attr]}
	}
	for _, attr := range unset {
		old, err := unsetRelayAttr(&rc, attr)
		if err != nil {
			return nil, err
		}
		change[attr] = confdb.Change{Old: old}
	}
	next.Relays[name] = rc
	return change, nil
}

func applySensorChanges(next *config.File, name string,
	typed map[string]any, unset []string,
) (map[string]confdb.Change, error) {
	change := map[string]confdb.Change{}
	sn := next.Sensors[name]
	for _, attr := range orderedAttrs(typed) {
		old, err := setSensorAttr(&sn, attr, typed[attr])
		if err != nil {
			return nil, err
		}
		change[attr] = confdb.Change{Old: old, New: typed[attr]}
	}
	for _, attr := range unset {
		old, err := unsetOverride(&sn.Layered, attr)
		if err != nil {
			return nil, err
		}
		change[attr] = confdb.Change{Old: old}
	}
	next.Sensors[name] = sn
	return change, nil
}

func applyRadioChanges(next *config.File, name string,
	typed map[string]any, unset []string,
) (map[string]confdb.Change, error) {
	change := map[string]confdb.Change{}
	rd := next.Radios[name]
	for _, attr := range orderedAttrs(typed) {
		old, err := setRadioAttr(&rd, attr, typed[attr])
		if err != nil {
			return nil, err
		}
		change[attr] = confdb.Change{Old: old, New: typed[attr]}
	}
	for _, attr := range unset {
		old, err := unsetOverride(&rd.Layered, attr)
		if err != nil {
			return nil, err
		}
		change[attr] = confdb.Change{Old: old}
	}
	next.Radios[name] = rd
	return change, nil
}

// setRelayAttr writes one relay attribute and reports what it held.
func setRelayAttr(rc *config.Relay, attr string, v any) (old any, err error) {
	switch attr {
	case attrProtocol, attrRadio:
		return nil, fmt.Errorf("%s says what the relay IS — remove it and add it anew", attr)
	case attrProfile:
		old = rc.Layered.Profile
		rc.Layered.Profile, err = asString(attr, v)
		return old, err
	case attrNoiseHistory:
		if rc.NoiseHistory != nil {
			old = *rc.NoiseHistory
		}
		b, err := asBool(attr, v)
		if err != nil {
			return nil, err
		}
		rc.NoiseHistory = &b
		return old, nil
	case attrTXMode, attrTXThreshold, attrTXExhausted, attrTXQueueDepth, attrTXCAD:
		return setTXAttr(rc, attr, v)
	default:
		return setOverride(&rc.Layered, attr, v), nil
	}
}

// unsetRelayAttr removes one relay attribute where removal means
// something: a contributed override, or the opt-outs.
func unsetRelayAttr(rc *config.Relay, attr string) (old any, err error) {
	switch attr {
	case attrProtocol, attrRadio, attrProfile,
		attrTXMode, attrTXThreshold, attrTXExhausted, attrTXQueueDepth:
		return nil, fmt.Errorf("%s cannot be unset — set it to what it should be", attr)
	case attrTXCAD:
		if rc.TX != nil && rc.TX.CAD != nil {
			old = *rc.TX.CAD
			rc.TX.CAD = nil
		}
		return old, nil
	case attrNoiseHistory:
		if rc.NoiseHistory != nil {
			old = *rc.NoiseHistory
		}
		rc.NoiseHistory = nil
		return old, nil
	default:
		return unsetOverride(&rc.Layered, attr)
	}
}

// setSensorAttr writes one sensor attribute and reports what it held.
// sample_interval is a field rather than an override: it belongs to
// the sampler, which no preset describes.
func setSensorAttr(sn *config.Sensor, attr string, v any) (old any, err error) {
	switch attr {
	case attrDriver:
		return nil, errors.New("driver says what the sensor IS — remove it and add it anew")
	case attrSampleEvery:
		old = sn.SampleInterval.String()
		d, derr := asDuration(attr, v)
		if derr != nil {
			return nil, derr
		}
		sn.SampleInterval = d
		return old, nil
	default:
		return setOverride(&sn.Layered, attr, v), nil
	}
}

func setRadioAttr(rd *config.Radio, attr string, v any) (old any, err error) {
	switch attr {
	case attrDriver:
		return nil, errors.New("driver says what the radio IS — remove it and add it anew")
	case attrProfile:
		old = rd.Layered.Profile
		rd.Layered.Profile, err = asString(attr, v)
		return old, err
	default:
		return setOverride(&rd.Layered, attr, v), nil
	}
}

// setTXAttr writes into the transmit block, creating it the first
// time — the dotted names are the flat console's reach into it.
func setTXAttr(rc *config.Relay, attr string, v any) (old any, err error) {
	if rc.TX == nil {
		rc.TX = &config.TX{}
	}
	switch attr {
	case attrTXMode:
		old = rc.TX.Mode
		rc.TX.Mode, err = asString(attr, v)
	case attrTXThreshold:
		old = rc.TX.LBTThresholdDB
		rc.TX.LBTThresholdDB, err = asFloat(attr, v)
	case attrTXExhausted:
		old = rc.TX.LBTExhausted
		rc.TX.LBTExhausted, err = asString(attr, v)
	case attrTXQueueDepth:
		old = rc.TX.QueueDepth
		rc.TX.QueueDepth, err = asInt(attr, v)
	case attrTXCAD:
		if rc.TX.CAD != nil {
			old = *rc.TX.CAD
		}
		var b bool
		if b, err = asBool(attr, v); err == nil {
			rc.TX.CAD = &b
		}
	}
	return old, err
}

// setOverride writes a contributed attribute into the live profile's
// override scope, and reports what that scope held before.
func setOverride(l *config.Layered, attr string, v any) (old any) {
	scope := l.Profile
	if scope == "" {
		scope = config.CustomProfile
	}
	if l.Overrides == nil {
		l.Overrides = map[string]map[string]any{}
	}
	if l.Overrides[scope] == nil {
		l.Overrides[scope] = map[string]any{}
	}
	old = l.Overrides[scope][attr]
	l.Overrides[scope][attr] = v
	return old
}

// unsetOverride removes a contributed attribute from the live
// profile's scope: the preset's value shows through again.
func unsetOverride(l *config.Layered, attr string) (old any, err error) {
	scope := l.Profile
	if scope == "" {
		scope = config.CustomProfile
	}
	ov := l.Overrides[scope]
	v, ok := ov[attr]
	if !ok {
		return nil, fmt.Errorf("%s is not set here — nothing to unset", attr)
	}
	delete(ov, attr)
	return v, nil
}

// applySentinelChanges edits the journal block, creating it on first
// touch — the block's absence is "no observation", and setting a
// journal path is how observation begins.
func applySentinelChanges(next *config.File,
	typed map[string]any, unset []string,
) (map[string]confdb.Change, error) {
	change := map[string]confdb.Change{}
	sen := config.Sentinel{}
	if next.Sentinel != nil {
		sen = *next.Sentinel
	}
	for attr, v := range typed {
		var err error
		switch attr {
		case "journal":
			change[attr] = confdb.Change{Old: orNil(sen.Journal), New: v}
			sen.Journal, err = asString(attr, v)
		case "retention":
			change[attr] = confdb.Change{Old: orNil(sen.Retention.String()), New: v}
			sen.Retention, err = asDuration(attr, v)
		case "max_frames":
			change[attr] = confdb.Change{Old: orNilInt(sen.MaxFrames), New: v}
			sen.MaxFrames, err = asInt(attr, v)
		default:
			err = fmt.Errorf("no attribute %q here", attr)
		}
		if err != nil {
			return nil, err
		}
	}
	if len(unset) > 0 {
		return nil, errors.New("the sentinel's attributes cannot be unset — remove the whole block, or set them")
	}
	next.Sentinel = &sen
	return change, nil
}

// applyCLIChanges edits the operator-listener block.
func applyCLIChanges(next *config.File,
	typed map[string]any, unset []string,
) (map[string]confdb.Change, error) {
	change := map[string]confdb.Change{}
	c := config.CLI{}
	if next.CLI != nil {
		c = *next.CLI
	}
	for attr, v := range typed {
		var err error
		switch attr {
		case attrListen:
			change[attr] = confdb.Change{Old: orNil(c.Listen), New: v}
			c.Listen, err = asString(attr, v)
		case attrSocket:
			old := any(nil)
			if c.Socket != nil {
				old = *c.Socket
			}
			change[attr] = confdb.Change{Old: old, New: v}
			var sock string
			if sock, err = asString(attr, v); err == nil {
				c.Socket = &sock
			}
		default:
			err = fmt.Errorf("no attribute %q here", attr)
		}
		if err != nil {
			return nil, err
		}
	}
	for _, attr := range unset {
		if attr != attrSocket {
			return nil, fmt.Errorf("%s cannot be unset — set it to what it should be", attr)
		}
		old := any(nil)
		if c.Socket != nil {
			old = *c.Socket
		}
		change[attr] = confdb.Change{Old: old}
		c.Socket = nil // back to the default path
	}
	next.CLI = &c
	return change, nil
}

// applyWebChanges edits the web UI block, creating it on the first
// set — a singleton is born from a set, never from a create.
func applyWebChanges(next *config.File,
	typed map[string]any, unset []string,
) (map[string]confdb.Change, error) {
	change := map[string]confdb.Change{}
	w := config.Web{}
	if next.Web != nil {
		w = *next.Web
	}
	for attr, v := range typed {
		if attr != attrListen {
			return nil, fmt.Errorf("no attribute %q here", attr)
		}
		change[attr] = confdb.Change{Old: orNil(w.Listen), New: v}
		var err error
		if w.Listen, err = asString(attr, v); err != nil {
			return nil, err
		}
	}
	if len(unset) > 0 {
		return nil, fmt.Errorf("%s cannot be unset — set it to what it should be", unset[0])
	}
	next.Web = &w
	return change, nil
}

// applySystemChanges edits what this installation calls itself. The
// name is felt at once — the prompt reads it live — so unsetting it is
// meaningful too: back to the machine's hostname.
func applySystemChanges(next *config.File,
	typed map[string]any, unset []string,
) (map[string]confdb.Change, error) {
	change := map[string]confdb.Change{}
	sys := config.System{}
	if next.System != nil {
		sys = *next.System
	}
	for attr, v := range typed {
		text, err := asString(attr, v)
		if err != nil {
			return nil, err
		}
		switch attr {
		case attrName:
			change[attr] = confdb.Change{Old: orNil(sys.Name), New: v}
			sys.Name = text
		case attrLogLevel:
			change[attr] = confdb.Change{Old: orNil(sys.LogLevel), New: v}
			sys.LogLevel = text
		default:
			return nil, fmt.Errorf("no attribute %q here", attr)
		}
	}
	for _, attr := range unset {
		switch attr {
		case attrName:
			change[attr] = confdb.Change{Old: orNil(sys.Name)}
			sys.Name = ""
		case attrLogLevel:
			change[attr] = confdb.Change{Old: orNil(sys.LogLevel)}
			sys.LogLevel = ""
		default:
			return nil, fmt.Errorf("no attribute %q here", attr)
		}
	}
	next.System = &sys
	return change, nil
}

// attrLogLevel is the system knob an investigation turns live.
const attrLogLevel = "log_level"

// attrListen is the address attribute the listener singletons share.
const attrListen = "listen"

// removedAtRestart is what removing a restart-scoped singleton says.
const removedAtRestart = "removed — takes effect when the daemon restarts"

// adoptLevelKnob hands the manager the logger's live level and where
// the boot flag left it, then lets the stored override speak.
func (m *manager) adoptLevelKnob(knob zap.AtomicLevel) {
	m.logKnob, m.bootLevel = knob, knob.Level()
	m.applyLogLevel()
}

// liveLevelName names the level the live knob points at, or the boot
// default when none was adopted.
func (m *manager) liveLevelName() string {
	if m.logKnob == (zap.AtomicLevel{}) {
		return logging.LevelName(m.bootLevel)
	}
	return logging.LevelName(m.logKnob.Level())
}

// applyLogLevel points the live knob where the configuration says —
// the stored override when one is set, the boot flag otherwise.
func (m *manager) applyLogLevel() {
	if m.logKnob == (zap.AtomicLevel{}) {
		return // no live knob adopted — nothing to point yet
	}
	lvl := m.bootLevel
	if m.file.System != nil && m.file.System.LogLevel != "" {
		parsed, err := logging.ParseLevel(m.file.System.LogLevel)
		if err != nil {
			// The enum guards the door; a stored value only fails here
			// if the vocabulary shrank across versions.
			m.log.Warn("stored log level unreadable — boot flag holds", zap.Error(err))
		} else {
			lvl = parsed
		}
	}
	m.logKnob.SetLevel(lvl)
}

// applyUpdateChanges edits the update block. Every attribute is read
// live by the console's check and install, so nothing bounces and
// nothing waits for a restart.
func applyUpdateChanges(next *config.File,
	typed map[string]any, unset []string,
) (map[string]confdb.Change, error) {
	change := map[string]confdb.Change{}
	u := config.Update{}
	if next.Update != nil {
		u = *next.Update
	}
	field := func(attr string) (*string, error) {
		switch attr {
		case "channel":
			return &u.Channel, nil
		case "url":
			return &u.URL, nil
		case attrToken:
			return &u.Token, nil
		}
		return nil, fmt.Errorf("no attribute %q here", attr)
	}
	for attr, v := range typed {
		f, err := field(attr)
		if err != nil {
			return nil, err
		}
		text, err := asString(attr, v)
		if err != nil {
			return nil, err
		}
		change[attr] = confdb.Change{Old: orNil(*f), New: text}
		*f = text
	}
	for _, attr := range unset {
		f, err := field(attr)
		if err != nil {
			return nil, err
		}
		change[attr] = confdb.Change{Old: orNil(*f)}
		*f = ""
	}
	next.Update = &u
	return change, nil
}

func orNil(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func orNilInt(n int) any {
	if n == 0 {
		return nil
	}
	return n
}

// objectSection extracts the stored shape of one object.
func objectSection(f *config.File, kind, name string) (any, error) {
	switch kind {
	case confdb.KindRelay:
		return f.Relays[name], nil
	case confdb.KindRadio:
		return f.Radios[name], nil
	case confdb.KindSensor:
		return f.Sensors[name], nil
	case confdb.KindSentinel:
		if f.Sentinel == nil {
			return nil, errors.New("no sentinel block")
		}
		return *f.Sentinel, nil
	case confdb.KindCLI:
		if f.CLI == nil {
			return nil, errors.New("no cli block")
		}
		return *f.CLI, nil
	case confdb.KindWeb:
		if f.Web == nil {
			return nil, errors.New("no web block")
		}
		return *f.Web, nil
	case confdb.KindSystem:
		if f.System == nil {
			return nil, errors.New("no system block")
		}
		return *f.System, nil
	case confdb.KindUpdate:
		if f.Update == nil {
			return nil, errors.New("no update block")
		}
		return *f.Update, nil
	case confdb.KindMQTT:
		mq, ok := f.MQTT[name]
		if !ok {
			return nil, fmt.Errorf("no observer %q", name)
		}
		return mq, nil
	}
	return nil, fmt.Errorf("%q is not configurable from here yet", kind)
}

// checkRadioAlone validates an unowned radio: resolution and the
// driver's strict read, without a relay to anchor the deep checks.
func checkRadioAlone(rd config.Radio) error {
	drv, err := radio.Lookup(rd.Driver)
	if err != nil {
		return err
	}
	check := func(cfg map[string]any) error { _, e := drv.Inspect(cfg); return e }
	if err := checkScopes(rd.Layered, drv.Presets, check); err != nil {
		return err
	}
	// The selected profile may have no override scope, so the loop
	// above never sees it. Resolve it explicitly: an unowned radio is
	// still not allowed to persist a profile this binary does not know.
	cfg, _, err := rd.Layered.Resolve(drv.Presets)
	if err != nil {
		return err
	}
	return check(cfg)
}

// checkSensorAlone validates a sensor that no relay has to consult:
// the driver's own dry run, over every override scope.
func checkSensorAlone(sn config.Sensor) error {
	drv, err := sensor.Lookup(sn.Driver)
	if err != nil {
		return err
	}
	if drv.Inspect == nil {
		return nil
	}
	// nil: a sensor has no preset catalogue, so every scope resolves
	// from nothing but what the operator wrote.
	if err := checkScopes(sn.Layered, nil, drv.Inspect); err != nil {
		return err
	}
	// The selected scope may hold no overrides at all, and then the
	// loop above never saw it: a part declared with nothing but its
	// driver would reach the store unexamined.
	cfg, _, err := sn.Layered.Resolve(nil)
	if err != nil {
		return err
	}
	return drv.Inspect(cfg)
}

// cloneFile deep-copies the configuration through its own dialect.
func cloneFile(f *config.File) (*config.File, error) {
	raw, err := yaml.Marshal(f)
	if err != nil {
		return nil, err
	}
	var out config.File
	if err := yaml.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	if out.Radios == nil {
		out.Radios = map[string]config.Radio{}
	}
	if out.Relays == nil {
		out.Relays = map[string]config.Relay{}
	}
	if out.Sensors == nil {
		out.Sensors = map[string]config.Sensor{}
	}
	return &out, nil
}

// The coercions undo needs: a revision's old values come back from
// JSON, where every number is a float64 and every list []any.

func asString(attr string, v any) (string, error) {
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("%s: %v is not text", attr, v)
	}
	return s, nil
}

func asBool(attr string, v any) (bool, error) {
	b, ok := v.(bool)
	if !ok {
		return false, fmt.Errorf("%s: %v is not true or false", attr, v)
	}
	return b, nil
}

func asFloat(attr string, v any) (float64, error) {
	switch n := v.(type) {
	case float64:
		return n, nil
	case int:
		return float64(n), nil
	}
	return 0, fmt.Errorf("%s: %v is not a number", attr, v)
}

func asDuration(attr string, v any) (time.Duration, error) {
	switch d := v.(type) {
	case string:
		out, err := time.ParseDuration(d)
		if err != nil {
			return 0, fmt.Errorf("%s: %q is not a duration", attr, d)
		}
		return out, nil
	case float64:
		return time.Duration(int64(d)), nil
	}
	return 0, fmt.Errorf("%s: %v is not a duration", attr, v)
}

func asInt(attr string, v any) (int, error) {
	switch n := v.(type) {
	case int:
		return n, nil
	case float64:
		if n != float64(int(n)) {
			return 0, fmt.Errorf("%s: %v is not a whole number", attr, v)
		}
		return int(n), nil
	}
	return 0, fmt.Errorf("%s: %v is not a whole number", attr, v)
}

// Layers reads one instance's persisted layering whole — the selected
// profile and every override scope, inactive ones included — for the
// export that must not lose what a switch would come back to.
func (m *manager) Layers(kind, name string) (string, map[string]map[string]any, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var l config.Layered
	switch kind {
	case confdb.KindRelay:
		rc, ok := m.file.Relays[name]
		if !ok {
			return "", nil, false
		}
		l = rc.Layered
	case confdb.KindRadio:
		rd, ok := m.file.Radios[name]
		if !ok {
			return "", nil, false
		}
		l = rd.Layered
	case confdb.KindSensor:
		sn, ok := m.file.Sensors[name]
		if !ok {
			return "", nil, false
		}
		l = sn.Layered
	case confdb.KindMQTT:
		mq, ok := m.file.MQTT[name]
		if !ok {
			return "", nil, false
		}
		l = mq.Layered
	default:
		return "", nil, false
	}
	out := make(map[string]map[string]any, len(l.Overrides))
	for scope, kv := range l.Overrides {
		copied := make(map[string]any, len(kv))
		maps.Copy(copied, kv)
		out[scope] = copied
	}
	return l.Profile, out, true
}
