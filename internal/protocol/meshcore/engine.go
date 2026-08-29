// Package meshcore is the MeshCore protocol engine. In this stage it
// is deliberately receive-only: it hears, parses, deduplicates and
// judges frames — logging what it *would* relay — without owning a
// transmit path at all. The dry run is how the judgement earns trust
// on a live mesh before it is allowed to key a transmitter.
package meshcore

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/protocol"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/txn"
)

func init() {
	protocol.Register("meshcore", protocol.Builder{
		Build: build, Check: check, Asks: asks, Presets: presets, Schema: Schema(),
	})
}

// params is the relay-side configuration: the waveform choice plus
// the engine's own knobs.
type params struct {
	radio.Waveform `yaml:",inline"`

	// TxPowerDBm is validated against the radio's envelope at load
	// even though the engine cannot transmit yet; "auto" (the default)
	// resolves to the radio's cap when transmit exists.
	TxPowerDBm txPower `yaml:"tx_power_dbm"`

	// DedupTTL bounds how long a packet hash suppresses its copies.
	DedupTTL time.Duration `yaml:"dedup_ttl"`
	// DedupEntries bounds the seen table's size.
	DedupEntries int `yaml:"dedup_entries"`

	// FloodMaxHops and FloodMaxAdvertHops bound how far a flood is
	// carried onward: a packet already holding this many path hashes
	// is not re-flooded. Zero takes the reference repeater's defaults
	// (64, and 8 for adverts).
	FloodMaxHops int `yaml:"flood_max_hops"`
	// FloodMaxUnscopedHops bounds plain floods alone — the reference's
	// third knob, and the way to throttle traffic belonging to no
	// scope without touching the traffic that does.
	FloodMaxUnscopedHops int `yaml:"flood_max_unscoped_hops"`
	FloodMaxAdvertHops   int `yaml:"flood_max_advert_hops"`

	// DutyCyclePct is the band's regulatory ceiling on airtime, in
	// percent per sliding hour; zero leaves emission unbudgeted. Band
	// presets carry the lawful figure.
	DutyCyclePct float64 `yaml:"duty_cycle_pct"`

	// AdvertFloodInterval paces the routable self-announcement a
	// repeater owes the mesh's directories; applied only when the
	// transmit pipeline runs. The reference takes 3..168 hours and
	// ships at 47 — deliberately not a round day, so the announcement
	// drifts across the hours instead of always striking the same
	// one. Unset follows it; negative disables.
	AdvertFloodInterval time.Duration `yaml:"advert_flood_interval"`
	// AdvertLocalInterval paces the zero-hop announcement — the signed
	// packet that carries the node's name, and what makes a repeater
	// discoverable to whoever merely listens. The reference takes
	// 60..240 minutes; unset picks 2h, negative disables. Whatever the
	// choice, one boot announcement goes out shortly after the
	// pipeline comes up, as the reference's does.
	AdvertLocalInterval time.Duration `yaml:"advert_local_interval"`
	// NodeLat and NodeLon place the node on companion maps, riding the
	// advert's appdata as the reference's do. Both zero — the default —
	// announces no position.
	NodeLat float64 `yaml:"node_lat"`
	NodeLon float64 `yaml:"node_lon"`
	// DefaultScope is the transport scope this relay speaks in: what
	// it stamps on the adverts it originates and on a reply whose
	// question named no scope. Empty speaks unscoped, which is what a
	// relay does on a mesh that has no scopes. See the Vocabulary —
	// this is not a radio band.
	DefaultScope string `yaml:"default_scope"`
	// AcceptScopes lists the scopes whose floods this relay carries.
	// A scoped flood matching none of them is somebody else's
	// business, exactly as the reference treats one whose code it
	// cannot match.
	AcceptScopes scopeList `yaml:"accept_scopes"`
	// AcceptUnscoped decides whether plain floods are relayed at all —
	// the reference's wildcard and its deny-flood bit. Unset relays
	// them, which is what every mesh without scopes needs.
	AcceptUnscoped *bool `yaml:"accept_unscoped"`

	// GuestAccess decides whether a stranger may open a read-only
	// session at all — status, telemetry, the neighbourhood, nothing
	// that changes anything. Blocked by default: a repeater owes the
	// mesh its relaying, not its confidences. Setting a password is
	// enough to mean "password"; opening the door to anyone needs the
	// word said out loud.
	GuestAccess string `yaml:"guest_access"`
	// SessionLimit bounds how many answers one logged-in guest may
	// make this relay emit per minute; zero takes the default.
	SessionLimit int `yaml:"session_limit"`
	// GuestPassword is the credential when GuestAccess asks for one.
	GuestPassword string `yaml:"guest_password"`
	// AdminPassword grants the admin role over the air — the whole
	// switch: empty keeps over-the-air administration off.
	AdminPassword string `yaml:"admin_password"`
	// OwnerInfo rides the anonymous owner reply after the name — the
	// reference's free-text field for "who runs this node"; optional.
	OwnerInfo string `yaml:"owner_info"`
	// NodeName is the name adverts carry — what the mesh's directories
	// and every companion screen will call this node. There is no
	// default: a config slug is an implementation detail, not a name,
	// and announcing needs a deliberate one.
	NodeName string `yaml:"node_name"`

	// Identity is this relay's node key material, inline and in hex:
	// a 32-byte seed, a 64-byte expanded private key (the reference
	// CLI's prv.key form, for migrating an existing node), or the
	// 96-byte key pair. `lotor identity new` mints a fresh one.
	// Without one the relay hears everything but is addressed by
	// nothing: direct judgement stays honest and incomplete.
	Identity string `yaml:"identity"`
}

// txPower is either "auto" or an explicit dBm figure.
type txPower struct {
	explicit bool
	dbm      int8
}

func (t *txPower) UnmarshalYAML(node *yaml.Node) error {
	if node.Value == "auto" || node.Value == "" {
		*t = txPower{}
		return nil
	}
	// Read the scalar's text, not its tag: a value that crossed the
	// console is a string whatever it spells, and "0" set by an
	// operator is the same figure 0 imported from a file.
	dbm, err := strconv.ParseInt(node.Value, 10, 8)
	if err != nil {
		return fmt.Errorf(`tx_power_dbm wants "auto" or a dBm figure, not %q`, node.Value)
	}
	*t = txPower{explicit: true, dbm: int8(dbm)}
	return nil
}

type engine struct {
	relay string
	p     params
	id    *meshcore.LocalIdentity // nil when no identity is configured
	bus   *bus.Bus
	log   *zap.Logger
	seen  *seenTable

	// The transmit pipeline, armed at assembly when the gate is not
	// dry; zero values otherwise, and Run never consults them.
	policy          protocol.TXPolicy
	queue           *txQueue
	duty            *dutyLedger
	nextFloodAdvert time.Time
	nextLocalAdvert time.Time
	// lastAskedAdvert is when an operator last ordered one, which
	// paces those orders and nothing else.
	lastAskedAdvert time.Time
	limits          limits
	scopes          *scopeTable
	discoverySince  time.Time
	clockWarned     bool
	acl             *acl
	neighbours      *neighbourTable
	stats           Stats
	started         time.Time
	// floor reads the radio's measured noise floor for the status
	// reply; wired at Run, nil until then.
	floor func() (radio.NoiseFloor, bool)

	// advertAsk carries operator-triggered announcements into the
	// pipeline's goroutine; wakeRx interrupts a blocked Receive so the
	// order is served now, not at the next scheduled duty.
	advertAsk chan *advertOrder
	// scopeAsk carries an operator's question about a neighbour's
	// scopes; pendingScope is the one in flight, engine-goroutine only.
	scopeAsk     chan *scopeQuery
	pendingScope *scopeQuery
	// sweepAsk carries an operator's neighbourhood scan; pendingSweep
	// is the window currently open, engine-goroutine only.
	sweepAsk     chan *sweep
	pendingSweep *sweep
	// telemetry extends the telemetry answer with sensor readings,
	// under the permission mask the request carried — nil while no
	// sensors exist, which is what the base readings already cover.
	telemetry TelemetrySensors
	// commands runs one administration line for a logged-in admin and
	// returns what to answer; nil leaves over-the-air administration
	// unserved, which is what a relay with no mutation door behind it
	// must do.
	commands func(line string, admin []byte) string
	// sweepUntil mirrors the open window's end for readers outside
	// the loop — zero when no scan listens.
	sweepUntil atomic.Int64
	// aclAsk carries a grant or revoke, aclListAsk a request for the
	// whole access list, both served on the pipeline's own turn.
	aclAsk     chan *aclOrder
	aclListAsk chan *aclListOrder
	// sessionsAsk carries the console's request for a snapshot of the
	// client table. It asks for no emission, so unlike the others it
	// is served whatever the gate's mode.
	sessionsAsk chan *sessionsOrder
	wakeMu      sync.Mutex
	wakeRx      context.CancelFunc
	// busySince starts a continuous busy spell and is cleared by the
	// first clear channel — Dispatcher::cad_busy_start's clock, not
	// the age of any one frame.
	busySince time.Time
}

// paramsFrom is the strict decode both build and the config checker
// share.
func paramsFrom(cfg map[string]any) (params, error) {
	p, err := config.Decode[params](cfg)
	if err != nil {
		return p, fmt.Errorf("meshcore params: %w", err)
	}
	if p.FrequencyHz == 0 {
		return p, errors.New("meshcore params: frequency_hz is required")
	}
	// The reference's operator ranges: outside them, its CLI refuses
	// the setting — so does the config, or a site would run a cadence
	// no reference node would.
	if err := normalizeGuest(&p); err != nil {
		return p, err
	}
	if err := normalizeScopes(&p); err != nil {
		return p, err
	}
	// The reference's own field sizes (char[32] / char[120]); past
	// them an owner answer no longer fits a packet and the node goes
	// quiet on a question it should serve.
	if len(p.NodeName) > 31 {
		return p, fmt.Errorf("meshcore params: node_name is %d bytes — the mesh carries at most 31", len(p.NodeName))
	}
	if len(p.OwnerInfo) > 119 {
		return p, fmt.Errorf("meshcore params: owner_info is %d bytes — the mesh carries at most 119", len(p.OwnerInfo))
	}
	if v := p.AdvertLocalInterval; v > 0 && (v < time.Hour || v > 4*time.Hour) {
		return p, fmt.Errorf(
			"meshcore params: advert_local_interval %s — the reference accepts 60..240 minutes; negative disables", v)
	}
	if v := p.AdvertFloodInterval; v > 0 && (v < 3*time.Hour || v > 168*time.Hour) {
		return p, fmt.Errorf(
			"meshcore params: advert_flood_interval %s — the reference accepts 3..168 hours; negative disables", v)
	}
	return p, nil
}

// acceptUnscoped resolves the wildcard default: a relay carries plain
// floods unless told otherwise.
func (p params) acceptUnscoped() bool {
	return p.AcceptUnscoped == nil || *p.AcceptUnscoped
}

// How a stranger may open a session.
const (
	guestBlocked  = "blocked"  // nobody may; the default
	guestPassword = "password" // whoever knows guest_password may
	guestOpen     = "open"     // anyone may, with no credential at all
)

// normalizeGuest resolves the access mode and refuses the two ways of
// asking for contradictory things. A password on its own means the
// obvious; an open door has to be named, because it is the one choice
// nobody makes by accident.
func normalizeGuest(p *params) error {
	switch p.GuestAccess {
	case "":
		p.GuestAccess = guestBlocked
		if p.GuestPassword != "" {
			p.GuestAccess = guestPassword
		}
	case guestBlocked:
		if p.GuestPassword != "" {
			return errors.New(
				"meshcore params: guest_access is blocked but guest_password is set — " +
					"say password to use it, or drop it")
		}
	case guestPassword:
		if p.GuestPassword == "" {
			return errors.New("meshcore params: guest_access password needs guest_password")
		}
	case guestOpen:
		if p.GuestPassword != "" {
			return errors.New(
				"meshcore params: guest_access is open but guest_password is set — " +
					"open asks for no credential")
		}
	default:
		return fmt.Errorf(
			"meshcore params: guest_access %q — want blocked, password or open", p.GuestAccess)
	}
	if p.AdminPassword != "" && p.AdminPassword == p.GuestPassword {
		return errors.New(
			"meshcore params: admin_password equals guest_password — " +
				"one word cannot grant two roles")
	}
	return nil
}

// check is the console's dry run: everything build validates, minus
// the building. It must refuse exactly what build refuses — a value
// the console accepts and the engine then rejects is a relay that
// reports "applied" and dies at its next start.
func check(cfg map[string]any) error {
	_, _, err := resolve(cfg)
	return err
}

// asks reports what a configuration would demand of the radio. It
// reads the same resolution check and build do, so the three cannot
// disagree about what a configuration means.
func asks(cfg map[string]any) (radio.Waveform, int8, bool, error) {
	p, _, err := resolve(cfg)
	if err != nil {
		return radio.Waveform{}, 0, false, err
	}
	return p.Waveform, p.TxPowerDBm.dbm, p.TxPowerDBm.explicit, nil
}

// resolve reads the configuration into the parameters and the identity
// the engine runs on. It is the single validation path: check discards
// what it returns, build keeps it, and neither can drift from the
// other by validating something the other does not.
func resolve(cfg map[string]any) (params, *meshcore.LocalIdentity, error) {
	p, err := paramsFrom(cfg)
	if err != nil {
		return p, nil, err
	}
	if p.SessionLimit < 0 {
		return p, nil, fmt.Errorf(
			"meshcore params: session_limit %d — a budget below zero answers nothing at all",
			p.SessionLimit)
	}
	if p.Identity == "" {
		return p, nil, nil
	}
	id, err := identityFromConfig(p.Identity)
	if err != nil {
		return p, nil, err
	}
	return p, id, nil
}

func build(relayName string, cfg map[string]any, b *bus.Bus, log *zap.Logger) (protocol.Engine, error) {
	p, id, err := resolve(cfg)
	if err != nil {
		return nil, err
	}
	if id != nil {
		log.Info("node identity",
			zap.String("pubkey", hex.EncodeToString(id.PubKey[:])[:keyPrefixLen]))
	}
	return newEngine(relayName, p, id, b, log), nil
}

// newEngine assembles the state every engine has whatever its gate:
// what it needs to hear, judge and remember. The transmit pipeline is
// added later by Arm, and only then — but nothing here may wait for
// that, because judging happens in every mode. Both the daemon and
// the tests build through here, so a field added to the engine cannot
// be missed by one of them.
// withDefaults fills the values a config may leave unsaid. It runs in
// newEngine rather than beside the config parse so that every engine
// gets them, however it was built — a default applied on one path
// only is a default the other path discovers as a zero.
func (p params) withDefaults() params {
	// Zero values give the reference's dedup: a fixed 160-entry ring,
	// no time bound. dedup_ttl adds an operator time bound on top.
	if p.DedupEntries == 0 {
		p.DedupEntries = referenceCapacity
	}
	if p.AdvertFloodInterval == 0 {
		p.AdvertFloodInterval = 47 * time.Hour
	}
	if p.AdvertLocalInterval == 0 {
		p.AdvertLocalInterval = 2 * time.Hour
	}
	if p.SessionLimit == 0 {
		p.SessionLimit = sessionLimitMax
	}
	return p
}

func newEngine(relayName string, p params, id *meshcore.LocalIdentity,
	b *bus.Bus, log *zap.Logger,
) *engine {
	p = p.withDefaults()
	return &engine{
		relay:       relayName,
		p:           p,
		id:          id,
		bus:         b,
		log:         log,
		seen:        newSeenTable(p.DedupTTL, p.DedupEntries),
		neighbours:  newNeighbourTable(),
		acl:         newACL(nil),
		sessionsAsk: make(chan *sessionsOrder, 1),
		aclAsk:      make(chan *aclOrder, 1),
		aclListAsk:  make(chan *aclListOrder, 1),
		limits:      newLimits(),
		scopes:      newScopeTable(p),
	}
}

func (e *engine) Waveform() radio.Waveform { return e.p.Waveform }

// NodeName is what this relay calls itself on the air.
func (e *engine) NodeName() string { return e.p.NodeName }

// TrafficStats copies the lifetime tally out, for observers.
func (e *engine) TrafficStats() StatsSnapshot { return e.stats.Snapshot() }

// TelemetrySensors extends a telemetry answer with sensor readings.
// permMask is the request's gate, already resolved: the inverse of
// the wire's reserved byte, forced to zero for a guest. Which sensor
// a mask admits is the implementation's own judgement, mirroring the
// reference's SensorManager::querySensors.
type TelemetrySensors func(permMask byte, enc *meshcore.LPPEncoder) error

// AttachTelemetry gives the engine its sensor readings. Called once,
// before Run.
func (e *engine) AttachTelemetry(read TelemetrySensors) {
	e.telemetry = read
}

// AttachCommands gives the engine the door administration runs
// through. Called once, before Run.
func (e *engine) AttachCommands(run func(line string, admin []byte) string) {
	e.commands = run
}

// AttachSessions gives the engine somewhere to persist its session
// table and loads what is already there, the shared secret recomputed
// per session. Called once, before Run: a nil identity keeps the
// table in memory, since a secret it cannot recompute is a session it
// cannot restore.
func (e *engine) AttachSessions(store SessionStore) {
	if e.id == nil {
		return
	}
	e.acl.store = store
	e.acl.load(func(pubKey []byte) ([]byte, error) {
		return e.id.SharedSecret(pubKey)
	}, func() rateLimiter {
		return rateLimiter{max: e.p.SessionLimit, window: sessionLimitWindow}
	})
	if n := len(e.acl.by); n > 0 {
		e.log.Info("sessions restored", zap.Int("count", n))
	}
}

// IdentitySign signs a message under the node identity — the device
// authentication some observer brokers demand. Nil without one.
func (e *engine) IdentitySign(message []byte) []byte {
	if e.id == nil {
		return nil
	}
	return e.id.Sign(message)
}

// DefaultScope is the transport scope this relay speaks under.
func (e *engine) DefaultScope() string { return e.p.DefaultScope }

// RemoveNeighbours drops the neighbours a key prefix names — all of
// them for the empty prefix, the wire's own purge — and reports how
// many went.
func (e *engine) RemoveNeighbours(prefix []byte) int {
	return e.neighbours.removeMatching(prefix)
}

// ScanWindow reports when the scan currently listening closes; ok is
// false when none does.
func (e *engine) ScanWindow() (time.Time, bool) {
	ns := e.sweepUntil.Load()
	if ns == 0 || time.Now().UnixNano() > ns {
		return time.Time{}, false
	}
	return time.Unix(0, ns), true
}

// TxPower reports the configured transmit power choice; explicit is
// false for "auto", which resolves against the radio's cap.
func (e *engine) TxPower() (dbm int8, explicit bool) {
	return e.p.TxPowerDBm.dbm, e.p.TxPowerDBm.explicit
}

// Identity reports this relay's public key, empty when none is
// configured.
func (e *engine) Identity() string {
	if e.id == nil {
		return ""
	}
	return hex.EncodeToString(e.id.PubKey[:])
}

func (e *engine) Run(ctx context.Context, dev radio.Device) error {
	e.floor = dev.NoiseFloor
	if e.txEnabled() {
		// A previous session's queue holds frames the mesh has moved on
		// from: the backoff alone outlived their usefulness.
		e.dropQueued("session-restart")
		e.scheduleAdverts(time.Now())
		e.log.Info("transmit pipeline up",
			zap.String("mode", e.policy.Mode),
			zap.Int8("power_dbm", e.policy.PowerDBm),
			zap.Int("queue_depth", e.policy.QueueDepth))
	} else {
		e.log.Info("dry run: judging frames, transmitting nothing")
	}
	for {
		e.drainSessionsAsk(time.Now())
		e.drainACLAsk()
		// Reception blocks until the pipeline next needs the radio —
		// the queue's earliest schedule or an advert clock. Nothing
		// pending means no deadline at all.
		rctx, cancel := e.receiveWindow(ctx)
		frame, err := dev.Receive(rctx)
		cancel()
		switch {
		case err == nil:
			e.judge(dev, frame)
		case errors.Is(err, radio.ErrCorrupt):
			e.stats.countCorrupt()
			e.log.Debug("corrupt reception", zap.Error(err))
			e.bus.Publish(bus.FrameCorrupt{
				Relay: e.relay, At: time.Now(), Err: err.Error(),
			})
		case (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) &&
			ctx.Err() == nil:
			// The receive window closed — by its deadline, or by an
			// operator order waking the receiver; while the parent
			// lives, nobody else holds that cancel. The pipeline's
			// turn — which a dry gate has none of: the only order that
			// reaches it is the snapshot, served at the loop's top.
			if !e.txEnabled() {
				continue
			}
			if err := e.txPhase(ctx, dev); err != nil {
				return err
			}
		default:
			// Returned as-is: a context error means shutdown, anything
			// else is the driver's fault — replacing it with ctx.Err()
			// when the two race would mask the fault's cause.
			return err
		}
	}
}

// receiveWindow bounds one Receive call by the pipeline's next duty.
// The window is always cancellable when the pipeline runs, and its
// cancel is registered so an operator order can close it early; an
// order that landed while no window was open is caught by the pending
// check here, on the way into the next one.
func (e *engine) receiveWindow(ctx context.Context) (context.Context, context.CancelFunc) {
	// Held across the choice and the store both: an order landing
	// between them would otherwise fire a cancel this window has not
	// published yet, and wait for a frame that may never come. A dry
	// gate registers the wake too — the snapshot order asks for no
	// emission, and it must not have to wait for a frame to land.
	e.wakeMu.Lock()
	defer e.wakeMu.Unlock()
	var rctx context.Context
	var cancel context.CancelFunc
	switch wait, ok := e.txWait(time.Now()); {
	case len(e.sessionsAsk) > 0 || len(e.aclAsk) > 0 || len(e.aclListAsk) > 0:
		rctx, cancel = context.WithDeadline(ctx, time.Now())
	case !e.txEnabled():
		rctx, cancel = context.WithCancel(ctx)
	case len(e.advertAsk) > 0 || len(e.scopeAsk) > 0 || len(e.sweepAsk) > 0:
		rctx, cancel = context.WithDeadline(ctx, time.Now())
	case ok:
		rctx, cancel = context.WithDeadline(ctx, time.Now().Add(wait))
	default:
		rctx, cancel = context.WithCancel(ctx)
	}
	e.wakeRx = cancel
	return rctx, cancel
}

// wakeReceiver closes the current receive window so a pending order
// is served now rather than at the next scheduled duty.
func (e *engine) wakeReceiver() {
	e.wakeMu.Lock()
	if e.wakeRx != nil {
		e.wakeRx()
	}
	e.wakeMu.Unlock()
}

// passingBy reports whether a direct packet is in transit between
// other nodes: hops remain, and the head of the path is not ours to
// serve. Traces and control packets keep their own judgement — their
// targets do not ride the path head.
func (e *engine) passingBy(pkt *meshcore.Packet) (string, bool) {
	if !pkt.IsRouteDirect() || pkt.PathHashCount() == 0 {
		return "", false
	}
	switch pkt.PayloadType() {
	case meshcore.PayloadTypeTrace, meshcore.PayloadTypeControl:
		return "", false
	default:
	}
	if e.id != nil && e.id.HashMatches(pkt.Path[:min(pkt.PathHashSize(), len(pkt.Path))]) {
		return "", false // ours to relay: witnessed and judged like anything else
	}
	return verdictNotAddressed, true
}

// Neighbours reports the nodes heard with no relay in between, newest
// heard first — any goroutine.
func (e *engine) Neighbours() []Neighbour {
	if e.neighbours == nil {
		return nil
	}
	return e.neighbours.snapshot()
}

// observe feeds the neighbour table: the repeaters we hear with no
// relay in between — a zero-hop advert, or their answer to a scan.
// The SNR recorded is ours: how well WE hear THEM.
func (e *engine) observe(rx *reception) {
	pkt := rx.pkt
	if e.neighbours == nil || pkt.PathHashCount() != 0 {
		return
	}
	switch pkt.PayloadType() {
	case meshcore.PayloadTypeAdvert:
		if !rx.advertOK || rx.selfAdvert || rx.advert == nil {
			return
		}
		// Transport codes of {0, 0} mean "send to nowhere": a contact
		// somebody re-shared from their companion, not a node
		// announcing itself. Recorded as a neighbour it would claim a
		// direct link that does not exist, at the sharer's own SNR.
		if pkt.HasTransportCodes() && pkt.TransportCodes == [2]uint16{0, 0} {
			return
		}
		if rx.advert.Data != nil && rx.advert.Data.Type == meshcore.AdvTypeRepeater {
			e.neighbours.put(rx.advert.Identity.PubKey, rx.advert.Data.Name,
				rx.frame.SNR, rx.frame.At)
		}
	// A discovery answer is not evidence for us: the reference learns
	// from one only when it matches a scan it sent itself, and this
	// node scans for nobody. Taking them on trust would let any
	// stranger write into the neighbourhood we report.
	default: // other zero-hop traffic names nobody reliably
	}
}

// heard logs and publishes one reception, returning the transaction
// that names it from here on.
func (e *engine) heard(frame radio.Frame) (txn.ID, *zap.Logger) {
	id := txn.New()
	log := e.log.With(zap.String("txn", id.Short()))
	log.Info("frame heard",
		zap.Int("bytes", len(frame.Payload)),
		zap.Float64("rssi_dbm", frame.RSSI),
		zap.Float64("snr_db", frame.SNR),
		zap.Float64("signal_rssi_dbm", frame.SignalRSSI),
		zap.Float64("freq_err_hz", frame.FreqErrHz),
		zap.Duration("airtime", frame.Airtime),
	)
	e.bus.Publish(bus.FrameHeard{
		Relay: e.relay, Txn: id, At: frame.At,
		Bytes: len(frame.Payload), RSSI: frame.RSSI, SNR: frame.SNR,
		SignalRSSI: frame.SignalRSSI, FreqErrHz: frame.FreqErrHz,
		Airtime: frame.Airtime,
		Raw:     append([]byte(nil), frame.Payload...),
	})
	return id, log
}

func (e *engine) judge(dev radio.Device, frame radio.Frame) {
	e.stats.countFrame()
	id, log := e.heard(frame)
	pkt, err := meshcore.ParsePacket(frame.Payload)
	if err != nil {
		log.Info("frame judged", zap.String("verdict", verdictMalformed), zap.Error(err))
		e.bus.Publish(bus.FrameJudged{Relay: e.relay, Txn: id, Verdict: verdictMalformed})
		return
	}
	// PathLen is the hop count the path descriptor declares, not its
	// byte length: hashes are 1-4 bytes wide.
	hops := pkt.PathHashCount()
	log = log.With(
		zap.Stringer("type", pkt.PayloadType()),
		zap.Stringer("route", pkt.Route()),
		zap.Int("hops", hops),
	)

	judged := bus.FrameJudged{
		Relay: e.relay, Txn: id,
		Type:    pkt.PayloadType().String(),
		Route:   pkt.Route().String(),
		PathLen: hops,
	}
	rx := &reception{pkt: pkt, frame: frame, id: id}
	judged.Scope = e.scopeName(rx)
	log = log.With(describe(rx, &judged, e.id)...)
	// A direct packet still carrying hops belongs to the nodes its
	// path names. When none of them is us, it passes by without
	// touching the duplicate table — the reference releases it
	// unmarked — because the same bytes come back with the path
	// consumed when we are the destination, and a table that
	// witnessed the transit copy would call that delivery a
	// duplicate. Seen off the air: a companion's routed login died
	// exactly that way, judged in transit and deduplicated on
	// arrival.
	if verdict, passing := e.passingBy(pkt); passing {
		e.stats.countHeard(pkt, frame.RSSI, frame.SNR, frame.Airtime, false)
		log.Info("frame judged", zap.String("verdict", verdict))
		judged.Verdict = verdict
		e.bus.Publish(judged)
		return
	}
	if first, dup := e.seen.witness(pkt.Hash(), id, frame.At); dup {
		e.stats.countHeard(pkt, frame.RSSI, frame.SNR, frame.Airtime, true)
		log.Info("frame judged",
			zap.String("verdict", verdictDuplicate),
			zap.String("duplicate_of", first.Short()),
		)
		judged.Verdict, judged.DuplicateOf = verdictDuplicate, first.Short()
		e.bus.Publish(judged)
		return
	}

	e.stats.countHeard(pkt, frame.RSSI, frame.SNR, frame.Airtime, false)
	// Past the duplicate gate: a replayed advert is not fresh evidence
	// that its sender is still there.
	e.observe(rx)

	verdict, why := e.verdict(rx)
	judged.Verdict = verdict
	if why != "" && judged.Detail == "" {
		judged.Detail = why
	}
	log.Info("frame judged", zap.String("verdict", verdict), zap.String("why", why))
	e.bus.Publish(judged)

	if e.txEnabled() {
		e.relayFor(dev, rx, verdict)
	}
}
