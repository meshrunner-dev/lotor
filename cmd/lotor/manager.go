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
	"strconv"
	"strings"
	"sync"
	"time"

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

	"meshrunner.dev/pkg/meshcore"
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

	mu        sync.Mutex
	ctx       context.Context //nolint:containedctx // the daemon's lifetime, set once at Start
	file      *config.File
	wg        sync.WaitGroup
	running   map[string]*managedRelay
	observers map[string]*managedObserver
	infos     map[string]cli.RelayInfo
	radios    map[string]cli.RadioInfo
	traces    map[string][]config.Trace
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
		running:   map[string]*managedRelay{},
		observers: map[string]*managedObserver{},
		infos:     map[string]cli.RelayInfo{},
		radios:    map[string]cli.RadioInfo{},
		traces:    map[string][]config.Trace{},
	}
}

// Start assembles and launches every configured relay, then the
// observers that watch them.
func (m *manager) Start(ctx context.Context) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.ctx = ctx
	for name := range m.file.Relays {
		m.startRelay(ctx, name)
	}
	for name := range m.file.MQTT {
		m.startObserver(ctx, name)
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
		m.traces["radio "+rc.Radio] = withStructural(asm.radioTraces,
			radioStructural(m.file.Radios[rc.Radio]))
		m.traces["relay "+name] = withStructural(asm.relayTraces, relayStructural(rc))
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

func (m *manager) Traces() map[string][]config.Trace {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string][]config.Trace, len(m.traces)+2)
	maps.Copy(out, m.traces)
	// A radio nobody claims yet has no assembly to trace, but it is
	// configuration all the same: resolve its layers without hardware
	// so print and export see it like any other.
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
	// The system name is always shown, hostname included, because a
	// default is still the answer to "what is this machine called".
	out[confdb.KindSystem] = []config.Trace{
		{Key: attrName, Value: m.systemName(), Source: m.nameSource()},
	}
	if c := m.file.CLI; c != nil {
		rows := []config.Trace{{Key: "listen", Value: c.Listen, Source: sourceConfig}}
		if c.Socket != nil {
			rows = append(rows, config.Trace{Key: "socket", Value: *c.Socket, Source: sourceConfig})
		}
		out[confdb.KindCLI] = rows
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
		)
		if rc.TX.LBTThresholdDB != 0 {
			rows = append(rows, config.Trace{Key: attrTXThreshold, Value: rc.TX.LBTThresholdDB, Source: sourceConfig})
		}
	}
	return rows
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
		switch kind {
		case confdb.KindSystem:
			// The one attribute anything reads live.
			return "applied — this system is now " + m.systemName(), nil
		case confdb.KindSentinel, confdb.KindCLI:
			return "applied — takes effect when the daemon restarts", nil
		case confdb.KindUpdate:
			// check and install read the store live; nothing bounces.
			return "applied — the next check reads it", nil
		case confdb.KindMQTT:
			m.bounceObserver(name)
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

	if name == "" || strings.ContainsAny(name, " /\"") {
		return "", fmt.Errorf("%q is not a usable name", name)
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
		if _, err := resolveConfigs(rc, next.Radios[rc.Radio]); err != nil {
			return "", err
		}
	case confdb.KindRadio:
		if err := checkRadioAlone(next.Radios[name]); err != nil {
			return "", err
		}
	}
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
		return fmt.Sprintf("added — relay %s starting", name), nil
	case confdb.KindMQTT:
		m.startObserver(m.ctx, name) //nolint:contextcheck // deliberate: daemon lifetime
		return fmt.Sprintf("added — observer %s connecting", name), nil
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
	if rest["identity"] == "new" {
		seed, pub, err := mintIdentity()
		if err != nil {
			return "", err
		}
		rest["identity"], minted = seed, pub
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
		if attr == "identity" && minted != "" {
			// The seed is a secret from birth: the revision records
			// that an identity exists, never what it is.
			change[attr] = confdb.Change{New: maskedChange}
			continue
		}
		change[attr] = confdb.Change{New: v}
	}
	next.Relays[name] = rc
	return minted, nil
}

// mqttTraces synthesises the observers' provenance rows: no layering,
// so the store itself is the source of everything set.
func (m *manager) mqttTraces(out map[string][]config.Trace) {
	for name, mq := range m.file.MQTT {
		rows := []config.Trace{{Key: attrMQTTURL, Value: mq.URL, Source: sourceConfig}}
		add := func(key, value string) {
			if value != "" {
				rows = append(rows, config.Trace{Key: key, Value: value, Source: sourceConfig})
			}
		}
		add(attrMQTTUsername, mq.Username)
		add(attrMQTTPassword, mq.Password)
		add(attrMQTTIATA, mq.IATA)
		add(attrToken, mq.Token)
		add(attrMQTTTopic, mq.Topic)
		add(attrMQTTRelay, mq.Relay)
		if mq.Status != nil {
			add(attrMQTTStatus, strconv.FormatBool(*mq.Status))
		}
		if mq.Packets != nil {
			add(attrMQTTPackets, strconv.FormatBool(*mq.Packets))
		}
		if mq.Raw {
			add(attrMQTTRaw, "true")
		}
		if mq.RX != nil {
			add(attrMQTTRX, strconv.FormatBool(*mq.RX))
		}
		add(attrMQTTTX, mq.TX)
		add(attrMQTTTypes, strings.Join(mq.Types, ","))
		if mq.StatusInterval != 0 {
			add(attrMQTTInterval, mq.StatusInterval.String())
		}
		out[confdb.KindMQTT+" "+name] = rows
	}
}

// createMQTT fills a new observer from its creation line: every
// attribute goes through the same typed door a set would use, and the
// broker url is the one thing it cannot exist without.
func (m *manager) createMQTT(next *config.File, name string,
	attrs map[string]string, change map[string]confdb.Change,
) error {
	set := make(map[string]string, len(attrs))
	maps.Copy(set, attrs)
	typed, err := m.parseAgainst(confdb.KindMQTT, "", set, nil)
	if err != nil {
		return err
	}
	mq := config.MQTT{}
	for attr, v := range typed {
		if err := setMQTTField(&mq, attr, v); err != nil {
			return err
		}
		newVal := any(fmt.Sprintf("%v", v))
		if attr == attrMQTTPassword {
			newVal = maskedChange
		}
		change[attr] = confdb.Change{New: newVal}
	}
	if mq.URL == "" {
		return errors.New("a new observer needs url=")
	}
	if next.MQTT == nil {
		next.MQTT = map[string]config.MQTT{}
	}
	next.MQTT[name] = mq
	return nil
}

// applyMQTTChanges edits one observer's stored shape.
func applyMQTTChanges(next *config.File, name string,
	typed map[string]any, unset []string,
) (map[string]confdb.Change, error) {
	mq, ok := next.MQTT[name]
	if !ok {
		return nil, fmt.Errorf("no observer %q", name)
	}
	change := map[string]confdb.Change{}
	record := func(attr string, old any, hasNew bool, newText string) {
		if attr == attrMQTTPassword {
			old = maskedChange
			if hasNew {
				newText = maskedChange
			}
		}
		c := confdb.Change{Old: old}
		if hasNew {
			c.New = newText
		}
		change[attr] = c
	}
	for attr, v := range typed {
		old := mqttFieldValue(&mq, attr)
		if err := setMQTTField(&mq, attr, v); err != nil {
			return nil, err
		}
		record(attr, old, true, fmt.Sprintf("%v", v))
	}
	for _, attr := range unset {
		old := mqttFieldValue(&mq, attr)
		if err := clearMQTTField(&mq, attr); err != nil {
			return nil, err
		}
		record(attr, old, false, "")
	}
	next.MQTT[name] = mq
	return change, nil
}

// The observer attributes, named once for the setter, the reader and
// the traces. token reuses attrToken: same word, same meaning.
const (
	attrMQTTURL      = "url"
	attrMQTTUsername = "username"
	attrMQTTPassword = "password"
	attrMQTTIATA     = "iata"
	attrMQTTTopic    = "topic"
	attrMQTTRelay    = "relay"
	attrMQTTTX       = "tx"
	attrMQTTTypes    = "types"
	attrMQTTInterval = "status_interval"
	attrMQTTStatus   = "status"
	attrMQTTPackets  = "packets"
	attrMQTTRaw      = "raw"
	attrMQTTRX       = "rx"
)

// setMQTTField writes one typed attribute onto the stored shape; it
// is the single door creation and mutation share.
func setMQTTField(mq *config.MQTT, attr string, v any) error {
	if field := mqttStringField(mq, attr); field != nil {
		text, err := asString(attr, v)
		if err != nil {
			return err
		}
		if attr == attrMQTTURL && !hasBrokerScheme(text) {
			return fmt.Errorf("url %q — want tcp://, ssl://, ws:// or wss://", text)
		}
		*field = text
		return nil
	}
	switch attr {
	case attrMQTTStatus, attrMQTTPackets, attrMQTTRX, attrMQTTRaw:
		return setMQTTBool(mq, attr, v)
	case attrMQTTTypes:
		words, ok := v.([]string)
		if !ok {
			return fmt.Errorf("%s wants a list of payload type names", attr)
		}
		mq.Types = words
	case attrMQTTInterval:
		text, err := asString(attr, v)
		if err != nil {
			return err
		}
		d, err := time.ParseDuration(text)
		if err != nil {
			return fmt.Errorf("%s: %w", attr, err)
		}
		mq.StatusInterval = d
	default:
		return fmt.Errorf("no attribute %q here", attr)
	}
	return nil
}

// mqttStringField maps the plain string attributes onto their fields.
func mqttStringField(mq *config.MQTT, attr string) *string {
	switch attr {
	case attrMQTTURL:
		return &mq.URL
	case attrMQTTUsername:
		return &mq.Username
	case attrMQTTPassword:
		return &mq.Password
	case attrMQTTIATA:
		return &mq.IATA
	case attrToken:
		return &mq.Token
	case attrMQTTTopic:
		return &mq.Topic
	case attrMQTTRelay:
		return &mq.Relay
	case attrMQTTTX:
		return &mq.TX
	}
	return nil
}

// setMQTTBool writes one of the switches.
func setMQTTBool(mq *config.MQTT, attr string, v any) error {
	b, ok := v.(bool)
	if !ok {
		return fmt.Errorf("%s wants true or false", attr)
	}
	switch attr {
	case attrMQTTStatus:
		mq.Status = &b
	case attrMQTTPackets:
		mq.Packets = &b
	case attrMQTTRX:
		mq.RX = &b
	case attrMQTTRaw:
		mq.Raw = b
	}
	return nil
}

// clearMQTTField resets one attribute to its default.
func clearMQTTField(mq *config.MQTT, attr string) error {
	switch attr {
	case attrMQTTURL:
		return errors.New("an observer cannot lose its url — remove it instead")
	case attrMQTTUsername:
		mq.Username = ""
	case attrMQTTPassword:
		mq.Password = ""
	case attrMQTTIATA:
		mq.IATA = ""
	case attrToken:
		mq.Token = ""
	case attrMQTTTopic:
		mq.Topic = ""
	case attrMQTTRelay:
		mq.Relay = ""
	case attrMQTTTX:
		mq.TX = ""
	case attrMQTTStatus:
		mq.Status = nil
	case attrMQTTPackets:
		mq.Packets = nil
	case attrMQTTRX:
		mq.RX = nil
	case attrMQTTRaw:
		mq.Raw = false
	case attrMQTTTypes:
		mq.Types = nil
	case attrMQTTInterval:
		mq.StatusInterval = 0
	default:
		return fmt.Errorf("no attribute %q here", attr)
	}
	return nil
}

// mqttFieldValue reads one attribute back, for a revision's old side.
func mqttFieldValue(mq *config.MQTT, attr string) any {
	switch attr {
	case attrMQTTURL:
		return orNil(mq.URL)
	case attrMQTTUsername:
		return orNil(mq.Username)
	case attrMQTTPassword:
		return orNil(mq.Password)
	case attrMQTTIATA:
		return orNil(mq.IATA)
	case attrToken:
		return orNil(mq.Token)
	case attrMQTTTopic:
		return orNil(mq.Topic)
	case attrMQTTRelay:
		return orNil(mq.Relay)
	case attrMQTTTX:
		return orNil(mq.TX)
	case attrMQTTStatus:
		return boolPtrValue(mq.Status)
	case attrMQTTPackets:
		return boolPtrValue(mq.Packets)
	case attrMQTTRX:
		return boolPtrValue(mq.RX)
	case attrMQTTRaw:
		return mq.Raw
	case attrMQTTTypes:
		if len(mq.Types) == 0 {
			return nil
		}
		return strings.Join(mq.Types, ",")
	case attrMQTTInterval:
		if mq.StatusInterval == 0 {
			return nil
		}
		return mq.StatusInterval.String()
	}
	return nil
}

func boolPtrValue(b *bool) any {
	if b == nil {
		return nil
	}
	return *b
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
	if kind == confdb.KindRelay {
		m.stopRelay(name)
		delete(m.infos, name)
		delete(m.traces, "relay "+name)
		for radioName, info := range m.radios {
			if info.Relay == name {
				delete(m.radios, radioName)
				delete(m.traces, "radio "+radioName)
			}
		}
	}
	if kind == confdb.KindMQTT {
		m.stopObserver(name)
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
	case confdb.KindSentinel:
		next.Sentinel = nil
		return "removed — takes effect when the daemon restarts", nil
	case confdb.KindCLI:
		next.CLI = nil
		return "removed — takes effect when the daemon restarts", nil
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
// shape is refused before anything else happens.
func (m *manager) parseChanges(kind, name string,
	set map[string]string, unset []string,
) (map[string]any, error) {
	_, choice, err := m.kindAndChoice(kind, name)
	if err != nil {
		return nil, err
	}
	return m.parseAgainst(kind, choice, set, unset)
}

func (m *manager) kindAndChoice(kind, name string) (*schema.Kind, string, error) {
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
	case confdb.KindMQTT:
		if _, ok := m.file.MQTT[name]; !ok {
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
		case "listen":
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
		if attr != attrName {
			return nil, fmt.Errorf("no attribute %q here", attr)
		}
		change[attr] = confdb.Change{Old: orNil(sys.Name), New: v}
		name, err := asString(attr, v)
		if err != nil {
			return nil, err
		}
		sys.Name = name
	}
	for _, attr := range unset {
		if attr != attrName {
			return nil, fmt.Errorf("no attribute %q here", attr)
		}
		change[attr] = confdb.Change{Old: orNil(sys.Name)}
		sys.Name = ""
	}
	next.System = &sys
	return change, nil
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
		old := orNil(*f)
		newVal := any(text)
		if attr == attrToken {
			// The revision records that it changed, never what to.
			old, newVal = maskedChange, maskedChange
		}
		change[attr] = confdb.Change{Old: old, New: newVal}
		*f = text
	}
	for _, attr := range unset {
		f, err := field(attr)
		if err != nil {
			return nil, err
		}
		old := orNil(*f)
		if attr == attrToken {
			old = maskedChange
		}
		change[attr] = confdb.Change{Old: old}
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
