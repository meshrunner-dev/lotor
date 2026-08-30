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
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/correlation"
	"meshrunner.dev/lotor/internal/logging"
	"meshrunner.dev/lotor/internal/protocol"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/version"
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

	// TxDelayFactor and DirectTxDelayFactor scale the retransmission
	// jitter — the random delay in [0, 5×airtime×factor) that
	// desynchronises repeaters relaying the same frame — flood and
	// routed traffic each under their own. The reference accepts 0..2
	// and ships at 0.5 and 0.3; unset follows it, and the pointers are
	// what let an explicit 0 — relay with no jitter at all — be a
	// choice rather than an absence.
	TxDelayFactor       *float64 `yaml:"tx_delay_factor"`
	DirectTxDelayFactor *float64 `yaml:"direct_tx_delay_factor"`
	// RxDelayBase staggers flood handling by reception score: a heard
	// flood is held (base^(0.85−score)−1)×airtime before it is judged,
	// so the repeater that heard it best relays first and everyone
	// else hears that relay as the duplicate it now is. The reference
	// accepts 0..20 and ships at 0 — off — which unset follows.
	RxDelayBase float64 `yaml:"rx_delay_base"`

	// PathHashMode picks the per-hop hash width this node's own
	// floods declare: mode+1 bytes, the reference's 0..2. Every
	// relayer appends its hash at the width the originator declared,
	// so the choice buys the whole path its collision room at one
	// byte per hop per step. Unset takes mode 1 — two-byte hashes, a
	// deliberate step past the reference's one-byte default: one byte
	// collides once per 256 hops, and the loop gate then reads
	// relays this node never made.
	PathHashMode *int `yaml:"path_hash_mode"`
	// LoopDetect arms the flood orbit gate: how many times this
	// node's hash may already ride a path before the relay is
	// refused, thresholds per hash width since a narrow hash makes
	// apparent visits out of collisions. off, minimal, moderate or
	// strict — the reference's ladder, shipped off there. Unset takes
	// minimal, deliberately: the dedup guards while it remembers, and
	// minimal keeps the orbit armour after it forgets without
	// strict's false refusals on long one-byte paths.
	LoopDetect string `yaml:"loop_detect"`

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
	// firmware is what the OTA surfaces answer as this node's build —
	// handed down at assembly so every surface repeats the ONE value
	// the daemon read at boot, with the engine's own read as the
	// fallback for a bare construction.
	firmware string
	id       *meshcore.LocalIdentity // nil when no identity is configured
	bus      *bus.Bus
	log      *zap.Logger
	seen     *seenTable

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
	regions         *regionTable
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
	// held are the floods waiting out their score delay — see
	// rxdelay.go; engine-goroutine only, and empty while rx_delay_base
	// stays at its zero default.
	held []heldRx
	// telemetry extends the telemetry answer with sensor readings,
	// under the permission mask the request carried — nil while no
	// sensors exist, which is what the base readings already cover.
	telemetry TelemetrySensors
	// supply reports this node's own voltage, the reading the
	// reference takes from its board; nil leaves the base zero that
	// keeps a companion's battery field parsable.
	supply SupplyVoltage
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
	// regionAsk carries one region command line, regionSnapAsk the
	// outside readers' snapshot; regionStaging is the armed modal
	// load, engine-goroutine only, mirrored into regionLoadState for
	// the dispatcher's cheap pre-check.
	regionAsk       chan *regionOrder
	regionSnapAsk   chan *regionSnapOrder
	regionStaging   *regionLoad
	regionLoadState atomic.Value
	// regionStore persists the region map; nil keeps it in memory.
	regionStore RegionStore
	// sessionsAsk carries the console's request for a snapshot of the
	// client table. It asks for no emission, so unlike the others it
	// is served whatever the gate's mode.
	sessionsAsk chan *sessionsOrder
	wakeMu      sync.Mutex
	wakeRx      context.CancelFunc
	wakeReason  string
	// busySince starts a continuous busy spell and is cleared by the
	// first clear channel — Dispatcher::cad_busy_start's clock, not
	// the age of any one frame.
	busySince time.Time
}

// paramsFrom is the strict decode both build and the config checker
// share.
func paramsFrom(cfg map[string]any) (params, error) {
	// The old scope attributes live in the region table now, mutated
	// by the `region` command; a config still carrying them deserves
	// the pointer, not a bare unknown-key refusal.
	for _, gone := range []string{"default_scope", "accept_scopes", "accept_unscoped"} {
		if _, held := cfg[gone]; held {
			var p params
			return p, fmt.Errorf(
				"meshcore params: %s moved into the region table — administer it with the `region` command", gone)
		}
	}
	p, err := config.Decode[params](cfg)
	if err != nil {
		return p, fmt.Errorf("meshcore params: %w", err)
	}
	if p.FrequencyHz == 0 {
		return p, errors.New("meshcore params: frequency_hz is required")
	}
	if !validDutyCyclePct(p.DutyCyclePct) {
		return p, fmt.Errorf("meshcore params: duty_cycle_pct %g — want a finite percentage in 0..100", p.DutyCyclePct)
	}
	// The reference's operator ranges: outside them, its CLI refuses
	// the setting — so does the config, or a site would run a cadence
	// no reference node would.
	if err := normalizeGuest(&p); err != nil {
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
	return p, checkRelayingBounds(p)
}

// checkRelayingBounds holds the relaying knobs to the reference's own
// operator ranges — outside them its CLI refuses the setting, and so
// does this config.
func checkRelayingBounds(p params) error {
	if v := p.TxDelayFactor; v != nil && !inRange(*v, 0, 2) {
		return fmt.Errorf("meshcore params: tx_delay_factor %g — the reference accepts 0..2", *v)
	}
	if v := p.DirectTxDelayFactor; v != nil && !inRange(*v, 0, 2) {
		return fmt.Errorf("meshcore params: direct_tx_delay_factor %g — the reference accepts 0..2", *v)
	}
	if v := p.RxDelayBase; !inRange(v, 0, 20) {
		return fmt.Errorf("meshcore params: rx_delay_base %g — the reference accepts 0..20, 0 holding nothing", v)
	}
	if v := p.PathHashMode; v != nil && (*v < 0 || *v > 2) {
		return fmt.Errorf("meshcore params: path_hash_mode %d — the reference accepts 0, 1 or 2", *v)
	}
	switch p.LoopDetect {
	case "", loopOff, loopMinimal, loopModerate, loopStrict:
		return nil
	default:
		return fmt.Errorf(
			"meshcore params: loop_detect %q — off, minimal, moderate or strict", p.LoopDetect)
	}
}

// inRange is a bounds check a NaN cannot slip through: both
// comparisons are asserted rather than their complements denied.
func inRange(v, lo, hi float64) bool {
	return v >= lo && v <= hi
}

// validDutyCyclePct keeps a configuration value inside the only range
// the sliding one-hour ledger can honestly enforce. Zero is meaningful
// before the transmit gate is armed; Arm requires a strictly positive
// value whenever it opens that gate.
func validDutyCyclePct(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0) && v >= 0 && v <= 100
}

// txDelayFactor and directTxDelayFactor resolve the retransmission
// jitter factors, unset taking the reference repeater's defaults.
func (p params) txDelayFactor() float64 {
	if p.TxDelayFactor == nil {
		return defaultTxDelayFactor
	}
	return *p.TxDelayFactor
}

func (p params) directTxDelayFactor() float64 {
	if p.DirectTxDelayFactor == nil {
		return defaultDirectTxDelayFactor
	}
	return *p.DirectTxDelayFactor
}

// pathHashWidth is the hash width this node's own floods declare, in
// bytes — the origination half of the width story; the relay half
// mirrors whatever arrives.
func (p params) pathHashWidth() int {
	if p.PathHashMode == nil {
		return defaultPathHashWidth
	}
	return *p.PathHashMode + 1
}

// loopDetect resolves the orbit gate's mode, unset taking minimal.
func (p params) loopDetect() string {
	if p.LoopDetect == "" {
		return loopMinimal
	}
	return p.LoopDetect
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
		relay:         relayName,
		p:             p,
		firmware:      version.Current().Version,
		id:            id,
		bus:           b,
		log:           log,
		seen:          newSeenTable(p.DedupTTL, p.DedupEntries),
		neighbours:    newNeighbourTable(),
		acl:           newACL(nil),
		sessionsAsk:   make(chan *sessionsOrder, 1),
		aclAsk:        make(chan *aclOrder, 1),
		aclListAsk:    make(chan *aclListOrder, 1),
		regionAsk:     make(chan *regionOrder, 1),
		regionSnapAsk: make(chan *regionSnapOrder, 1),
		limits:        newLimits(),
		regions:       newRegionTable(meshcore.NewRegionMap()),
	}
}

func (e *engine) Waveform() radio.Waveform { return e.p.Waveform }

// AttachBuild hands the engine the build identity the daemon read at
// boot, so the air answers with the same value every other surface
// speaks.
func (e *engine) AttachBuild(firmware string) { e.firmware = firmware }

// NodeName is what this relay calls itself on the air.
func (e *engine) NodeName() string { return e.p.NodeName }

// TrafficStats copies the lifetime tally out, for observers.
func (e *engine) TrafficStats() StatsSnapshot { return e.stats.Snapshot() }

// TelemetrySensors extends a telemetry answer with sensor readings.
// permMask is the request's gate, already resolved: the inverse of
// the wire's reserved byte, forced to zero for a guest. Which sensor
// a mask admits is the implementation's own judgement, mirroring the
// reference's SensorManager::querySensors.
//
// The encoder is bounded to the route the answer will travel and
// refuses a reading that would not fit with meshcore.ErrLPPFull.
// Treat that as the end of the list — return it or nil, both read the
// same — and order readings most-important first, since the tail is
// what a long list loses.
type TelemetrySensors func(permMask byte, enc *meshcore.LPPEncoder) error

// The telemetry permission bits, as the reference defines them in
// SensorManager.h. They ride the request's first reserved byte,
// inverted; the engine resolves that into the mask a hook receives.
const (
	// TelemPermBase covers the readings every asker gets, the supply
	// among them.
	TelemPermBase byte = 0x01
	// TelemPermLocation covers where the node is.
	TelemPermLocation byte = 0x02
	// TelemPermEnvironment covers what its attached parts measure.
	TelemPermEnvironment byte = 0x04
)

// SupplyVoltage reports what this node is running on, in volts. It is
// the daemon's answer to the reference's board.getBattMilliVolts():
// the engine cannot know what measures a supply, only that the base
// telemetry owes one. False leaves the base zero.
type SupplyVoltage func() (float64, bool)

// AttachSupply gives the engine the node's own voltage. Called once,
// before Run.
func (e *engine) AttachSupply(read SupplyVoltage) {
	e.supply = read
}

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

// AttachSessions gives the engine somewhere to persist non-guest
// access entries and loads what is already there, the shared secret
// recomputed per entry. Guest sessions remain in memory only. Called
// once, before Run: a nil identity keeps the table in memory, since a
// secret it cannot recompute is an entry it cannot restore.
// A store that cannot be read refuses the attachment, and with it the
// relay: the table holds every admin's replay guard, and coming up
// without it would rewind those clocks to zero — a recent capture of
// a login and its command replays straight through.
func (e *engine) AttachSessions(store SessionStore) error {
	if e.id == nil {
		return nil
	}
	e.acl.store = store
	if err := e.acl.load(func(pubKey []byte) ([]byte, error) {
		return e.id.SharedSecret(pubKey)
	}, func() rateLimiter {
		return rateLimiter{max: e.p.SessionLimit, window: sessionLimitWindow}
	}); err != nil {
		return fmt.Errorf("access list: the store holds this node's replay guards and could not be read: %w", err)
	}
	if n := len(e.acl.by); n > 0 {
		e.log.Info("access entries restored", zap.Int("count", n))
	}
	return nil
}

// IdentitySign signs a message under the node identity — the device
// authentication some observer brokers demand. Nil without one.
func (e *engine) IdentitySign(message []byte) []byte {
	if e.id == nil {
		return nil
	}
	return e.id.Sign(message)
}

// DefaultScope is the region this relay speaks under — the bare name
// of the default designation, empty when it speaks unscoped. Any
// goroutine: it reads through the snapshot order.
func (e *engine) DefaultScope() string {
	snap, err := e.Regions()
	if err != nil {
		return ""
	}
	return snap.Default
}

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
		// The CAD line is announced rather than assumed: leaving it on
		// is this daemon's own posture, one step politer than the
		// firmware, and a divergence nobody can read in the log is a
		// divergence nobody can measure.
		e.log.Info("transmit pipeline up",
			zap.String("mode", e.policy.Mode),
			zap.Int8("power_dbm", e.policy.PowerDBm),
			zap.Int("queue_depth", e.policy.QueueDepth),
			zap.Bool("cad", e.policy.CAD))
	} else {
		e.log.Info("dry run: judging frames, transmitting nothing")
	}
	for {
		e.drainSessionsAsk(time.Now())
		e.drainACLAsk()
		e.drainRegionAsk()
		e.drainRegionSnapAsk()
		e.drainHeld(dev, time.Now())
		// Reception blocks until the pipeline next needs the radio —
		// the queue's earliest schedule or an advert clock. Nothing
		// pending means no deadline at all.
		rctx, cancel := e.receiveWindow(ctx)
		frame, err := dev.Receive(rctx)
		wakeReason := e.finishReceiveWindow()
		cancel()
		switch {
		case err == nil:
			e.judge(dev, frame)
		case errors.Is(err, radio.ErrCorrupt):
			e.stats.countCorrupt()
			id := frame.Correlation
			if id.IsZero() {
				id = correlation.New()
			}
			at := frame.At
			if at.IsZero() {
				at = time.Now()
			}
			log := e.log.With(zap.String("corr", id.Short()))
			log.Debug("corrupt reception", zap.Error(err))
			e.bus.Publish(bus.FrameCorrupt{
				Relay: e.relay, Correlation: id, At: at, Err: err.Error(),
			})
		case (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) &&
			ctx.Err() == nil:
			// The receive window closed — by its deadline, or by an
			// operator order waking the receiver; while the parent
			// lives, nobody else holds that cancel. The pipeline's
			// turn — which a dry gate has none of: the only order that
			// reaches it is the snapshot, served at the loop's top.
			if !e.txEnabled() {
				logging.Trace(e.log, "rx window yielded", zap.String("reason", wakeReason))
				continue
			}
			logging.Trace(e.log, "rx window yielded", zap.String("reason", wakeReason))
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
	now := time.Now()
	var reason string
	switch {
	case len(e.sessionsAsk) > 0 || len(e.aclAsk) > 0 || len(e.aclListAsk) > 0 ||
		len(e.regionAsk) > 0 || len(e.regionSnapAsk) > 0:
		reason = "operator-order"
		rctx, cancel = context.WithDeadline(ctx, now)
	case !e.txEnabled():
		// A dry gate schedules no emission, but a held flood is still
		// owed its judgement: a hold that only released on the next
		// reception would stretch with the silence around it.
		if wait, held := e.heldWait(now); held {
			reason = "held-due"
			rctx, cancel = context.WithDeadline(ctx, now.Add(wait))
		} else {
			reason = "external-wake"
			rctx, cancel = context.WithCancel(ctx)
		}
	case len(e.advertAsk) > 0 || len(e.scopeAsk) > 0 || len(e.sweepAsk) > 0:
		reason = "operator-order"
		rctx, cancel = context.WithDeadline(ctx, now)
	default:
		if wait, scheduledReason, scheduled := e.txWake(now); scheduled {
			reason = scheduledReason
			rctx, cancel = context.WithDeadline(ctx, now.Add(wait))
		} else {
			reason = "external-wake"
			rctx, cancel = context.WithCancel(ctx)
		}
	}
	e.wakeRx = cancel
	e.wakeReason = reason
	return rctx, cancel
}

// wakeReceiver closes the current receive window so a pending order
// is served now rather than at the next scheduled duty.
func (e *engine) wakeReceiver(reason string) {
	e.wakeMu.Lock()
	if e.wakeRx != nil {
		e.wakeReason = reason
		e.wakeRx()
	}
	e.wakeMu.Unlock()
}

// finishReceiveWindow retires the registered cancel and returns the reason
// chosen when the window was armed or supplied by whoever woke it.
func (e *engine) finishReceiveWindow() string {
	e.wakeMu.Lock()
	defer e.wakeMu.Unlock()
	reason := e.wakeReason
	e.wakeRx = nil
	e.wakeReason = ""
	return reason
}

// passingBy reports whether a direct packet is in transit between
// other nodes: hops remain, and the head of the path is not ours to
// serve. Traces and control packets keep their own judgement — their
// targets do not ride the path head.
func (e *engine) passingBy(pkt *meshcore.Packet) (string, bool) {
	if !pkt.IsRouteDirect() || pkt.PathHashCount() == 0 {
		return "", false
	}
	// A trace's target does not ride the path head, and the high-bit
	// control subset is answered rather than routed. Every other
	// control packet is ordinary directed traffic.
	if pkt.PayloadType() == meshcore.PayloadTypeTrace || highBitControl(pkt) {
		return "", false
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

// heard logs and publishes one reception, returning the correlation
// that names it from here on.
func (e *engine) heard(frame radio.Frame) (correlation.ID, *zap.Logger) {
	id := frame.Correlation
	if id.IsZero() {
		id = correlation.New()
	}
	log := e.log.With(zap.String("corr", id.Short()))
	log.Debug("frame heard", zap.Int("bytes", len(frame.Payload)))
	logging.Trace(log, "rx frame measurements",
		zap.Float64("rssi_dbm", frame.RSSI),
		zap.Float64("snr_db", frame.SNR),
		zap.Float64("signal_rssi_dbm", frame.SignalRSSI),
		zap.Float64("freq_err_hz", frame.FreqErrHz),
		zap.Duration("airtime", frame.Airtime),
	)
	e.bus.Publish(bus.FrameHeard{
		Relay: e.relay, Correlation: id, At: frame.At,
		Bytes: len(frame.Payload), RSSI: frame.RSSI, SNR: frame.SNR,
		SignalRSSI: frame.SignalRSSI, FreqErrHz: frame.FreqErrHz,
		Airtime: frame.Airtime,
		Raw:     append([]byte(nil), frame.Payload...),
	})
	return id, log
}

func packetHashHex(pkt *meshcore.Packet) string {
	h := pkt.Hash()
	return hex.EncodeToString(h[:])
}

// judgedEvent seeds a verdict event with the reception it is about:
// the journal archives on this one event, so it must carry everything
// FrameHeard measured.
func (e *engine) judgedEvent(id correlation.ID, frame radio.Frame) bus.FrameJudged {
	return bus.FrameJudged{
		Relay: e.relay, Correlation: id, At: frame.At,
		Bytes: len(frame.Payload), RSSI: frame.RSSI, SNR: frame.SNR,
		SignalRSSI: frame.SignalRSSI, FreqErrHz: frame.FreqErrHz,
		Airtime: frame.Airtime,
	}
}

func (e *engine) judge(dev radio.Device, frame radio.Frame) {
	e.stats.countFrame()
	id, log := e.heard(frame)
	pkt, err := meshcore.ParsePacket(frame.Payload)
	if err != nil {
		log.Debug("frame judged", zap.String("verdict", verdictMalformed), zap.Error(err))
		j := e.judgedEvent(id, frame)
		j.Verdict = verdictMalformed
		e.bus.Publish(j)
		return
	}
	// The version gate stands here, where the reference's dispatcher
	// puts it: ahead of the score hold, the duplicate table, the
	// signature check and the neighbourhood. A frame this engine says
	// it cannot read must have no effect at all, and an advert
	// wearing a version we reject was still naming neighbours.
	if unsupportedVersion(pkt) {
		if log.Core().Enabled(zap.DebugLevel) {
			log.Debug("frame judged", zap.String("verdict", verdictBadVersion),
				zap.String("packet_hash", packetHashHex(pkt)),
				zap.Stringer("type", pkt.PayloadType()), zap.Stringer("route", pkt.Route()),
				zap.Int("hops", pkt.PathHashCount()))
		}
		j := e.judgedEvent(id, frame)
		j.Verdict = verdictBadVersion
		j.Type, j.Route = pkt.PayloadType().String(), pkt.Route().String()
		j.PathLen = pkt.PathHashCount()
		e.bus.Publish(j)
		return
	}
	// The score hold, the reference dispatcher's gesture: a flood we
	// heard weakly waits before it is judged at all — dedup included,
	// so a better-placed repeater's relay arriving meanwhile turns our
	// copy into the duplicate it should be.
	if pkt.IsRouteFlood() {
		if d, score := e.rxDelayAndScore(frame); d > 0 {
			now := time.Now()
			if log.Core().Enabled(zap.DebugLevel) {
				log.Debug("flood judgement deferred",
					zap.String("packet_hash", packetHashHex(pkt)),
					zap.Float64("score", score), zap.Duration("hold", d),
					zap.Time("due", now.Add(d)), zap.Float64("snr_db", frame.SNR),
					zap.Int("sf", e.p.SpreadingFactor), zap.Duration("airtime", frame.Airtime))
			}
			e.held = append(e.held, heldRx{
				pkt: pkt, frame: frame, id: id, heldAt: now, due: now.Add(d),
			})
			return
		}
	}
	e.process(dev, pkt, frame, id)
}

// process is judgement past the hold: dedup, verdict, and whatever
// the verdict schedules. Everything before it already happened at
// reception — the audit heard the frame when the air carried it, not
// when the hold released it.
func (e *engine) process(dev radio.Device, pkt *meshcore.Packet, frame radio.Frame, id correlation.ID) {
	// PathLen is the hop count the path descriptor declares, not its
	// byte length: hashes are 1-4 bytes wide.
	hops := pkt.PathHashCount()
	log := e.log.With(zap.String("corr", id.Short()))
	if log.Core().Enabled(zap.DebugLevel) {
		log = log.With(
			zap.String("packet_hash", packetHashHex(pkt)),
			zap.Stringer("type", pkt.PayloadType()),
			zap.Stringer("route", pkt.Route()),
			zap.Int("hops", hops),
		)
	}

	judged := e.judgedEvent(id, frame)
	judged.Type = pkt.PayloadType().String()
	judged.Route = pkt.Route().String()
	judged.PathLen = hops
	rx := &reception{pkt: pkt, frame: frame, id: id}
	judged.Scope = e.regionName(rx)
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
		log.Debug("frame judged", zap.String("verdict", verdict))
		judged.Verdict = verdict
		e.bus.Publish(judged)
		return
	}
	if first, dup := e.seen.witness(pkt.Hash(), id, frame.At); dup {
		e.stats.countHeard(pkt, frame.RSSI, frame.SNR, frame.Airtime, true)
		log.Debug("frame judged",
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
	log.Debug("frame judged", zap.String("verdict", verdict), zap.String("why", why))
	e.bus.Publish(judged)

	if e.txEnabled() {
		e.relayFor(dev, rx, verdict)
	}
}
