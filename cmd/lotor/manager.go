package main

// The manager owns what a running daemon may change about itself: the
// configuration store, the live relays, and the path between the two.
// Every administration channel mutates through here — parse, validate,
// persist, bounce the owning relay — so the rules live once, whoever
// asks. One line is one transaction: a retune touching three
// attributes restarts the relay once.

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"sort"
	"sync"

	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/cli"
	"meshrunner.dev/lotor/internal/confdb"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/relay"
	"meshrunner.dev/lotor/internal/schema"
	"meshrunner.dev/lotor/internal/sentinel"
)

// The structural attribute names, written once. The schema declares
// them for the console; these are the write path's spellings.
const (
	attrProtocol     = "protocol"
	attrRadio        = "radio"
	attrDriver       = "driver"
	attrProfile      = "profile"
	attrNoiseHistory = "noise_history"
	attrTXMode       = "tx.mode"
	attrTXThreshold  = "tx.lbt_threshold_db"
	attrTXExhausted  = "tx.lbt_exhausted"
	attrTXQueueDepth = "tx.queue_depth"
)

type manager struct {
	store *confdb.Store
	bus   *bus.Bus
	log   *zap.Logger
	sen   *sentinel.Sentinel
	kinds []schema.Kind

	mu      sync.Mutex
	ctx     context.Context //nolint:containedctx // the daemon's lifetime, set once at Start
	file    *config.File
	wg      sync.WaitGroup
	running map[string]*managedRelay
	infos   map[string]cli.RelayInfo
	radios  map[string]cli.RadioInfo
	traces  map[string][]config.Trace
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
		running: map[string]*managedRelay{},
		infos:   map[string]cli.RelayInfo{},
		radios:  map[string]cli.RadioInfo{},
		traces:  map[string][]config.Trace{},
	}
}

// Start assembles and launches every configured relay.
func (m *manager) Start(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ctx = ctx
	for name := range m.file.Relays {
		m.startRelay(ctx, name)
	}
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
	asm, err := assemble(ctx, name, rc, m.file.Radios[rc.Radio], m.bus, m.log, m.sen)
	if err != nil {
		m.log.Error("relay configuration failed",
			zap.String("relay", name), zap.Error(err))
		r = relay.Stillborn(name, err, m.bus, m.log)
		m.infos[name] = cli.RelayInfo{
			Name: name, Protocol: rc.Protocol, Radio: rc.Radio,
			State: r.State, Err: r.Err,
			// The configured intent survives the failure: an operator
			// reading "tx dry" next to an error would think the relay
			// was meant to stay silent.
			TXMode: rc.TXMode(),
		}
	} else {
		r = asm.relay
		m.infos[name] = asm.info
		m.radios[rc.Radio] = asm.radio
		m.traces["radio "+rc.Radio] = asm.radioTraces
		m.traces["relay "+name] = asm.relayTraces
	}
	rctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	m.running[name] = &managedRelay{cancel: cancel, done: done}
	m.wg.Go(func() {
		defer close(done)
		r.Run(rctx)
	})
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
}

// The live views the console reads. Each returns a copy: sessions
// iterate while relays rebuild.

func (m *manager) RelayInfos() []cli.RelayInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
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
	out := make([]cli.RadioInfo, 0, len(m.radios))
	for _, i := range m.radios {
		out = append(out, i)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (m *manager) Traces() map[string][]config.Trace {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string][]config.Trace, len(m.traces))
	maps.Copy(out, m.traces)
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

	typed, err := m.parseChanges(kind, name, set, unset)
	if err != nil {
		return "", err
	}
	return m.applyTyped(ctx, kind, name, typed, unset, principal, opOf(set, unset))
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
	typed := map[string]any{}
	var unset []string
	for attr, c := range changes {
		if c.Old == nil {
			unset = append(unset, attr)
			continue
		}
		typed[attr] = c.Old
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
	if err := next.Validate(false); err != nil {
		return "", err
	}
	// The deep checks the assembly would run, minus the hardware:
	// resolution, strict decode, every override scope.
	if relayName != "" {
		rc := next.Relays[relayName]
		if _, err := resolveConfigs(rc, next.Radios[rc.Radio]); err != nil {
			return "", err
		}
	} else if kind == confdb.KindRadio {
		if err := checkRadioAlone(next.Radios[name]); err != nil {
			return "", err
		}
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
		return "applied — no running relay uses this yet", nil
	}
	m.stopRelay(relayName)
	// The successor lives as long as the daemon, not as long as the
	// session that ordered the change — hence the stored context, not
	// the request's.
	m.startRelay(m.ctx, relayName) //nolint:contextcheck // deliberate: daemon lifetime
	return fmt.Sprintf("applied — relay %s restarting", relayName), nil
}

// parseChanges types every value against the schema the instance's
// choice resolves — an unknown attribute or a value of the wrong
// shape is refused before anything else happens.
func (m *manager) parseChanges(kind, name string,
	set map[string]string, unset []string,
) (map[string]any, error) {
	k, choice, err := m.kindAndChoice(kind, name)
	if err != nil {
		return nil, err
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

func (m *manager) kindAndChoice(kind, name string) (*schema.Kind, string, error) {
	var k *schema.Kind
	for i := range m.kinds {
		if m.kinds[i].Name == kind {
			k = &m.kinds[i]
			break
		}
	}
	if k == nil || k.Singleton {
		return nil, "", fmt.Errorf("%q is not configurable from here yet", kind)
	}
	switch kind {
	case confdb.KindRelay:
		rc, ok := m.file.Relays[name]
		if !ok {
			return nil, "", fmt.Errorf("no relay %q", name)
		}
		return k, rc.Protocol, nil
	case confdb.KindRadio:
		rd, ok := m.file.Radios[name]
		if !ok {
			return nil, "", fmt.Errorf("no radio %q", name)
		}
		return k, rd.Driver, nil
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
	case confdb.KindRelay:
		change, err := applyRelayChanges(next, name, typed, unset)
		return change, name, err
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
	for attr, v := range typed {
		old, err := setRelayAttr(&rc, attr, v)
		if err != nil {
			return nil, err
		}
		change[attr] = confdb.Change{Old: old, New: v}
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

func applyRadioChanges(next *config.File, name string,
	typed map[string]any, unset []string,
) (map[string]confdb.Change, error) {
	change := map[string]confdb.Change{}
	rd := next.Radios[name]
	for attr, v := range typed {
		old, err := setRadioAttr(&rd, attr, v)
		if err != nil {
			return nil, err
		}
		change[attr] = confdb.Change{Old: old, New: v}
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
	case attrTXMode, attrTXThreshold, attrTXExhausted, attrTXQueueDepth:
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

// objectSection extracts the stored shape of one object.
func objectSection(f *config.File, kind, name string) (any, error) {
	switch kind {
	case confdb.KindRelay:
		return f.Relays[name], nil
	case confdb.KindRadio:
		return f.Radios[name], nil
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
	return checkScopes(rd.Layered, drv.Presets,
		func(cfg map[string]any) error { _, e := drv.Inspect(cfg); return e })
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
