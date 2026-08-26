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
	protocol.Register("meshcore", protocol.Builder{Build: build, Check: check, Presets: presets})
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
	FloodMaxHops       int `yaml:"flood_max_hops"`
	FloodMaxAdvertHops int `yaml:"flood_max_advert_hops"`

	// DutyCyclePct is the band's regulatory ceiling on airtime, in
	// percent per sliding hour; zero leaves emission unbudgeted. Band
	// presets carry the lawful figure.
	DutyCyclePct float64 `yaml:"duty_cycle_pct"`

	// AdvertFloodInterval paces the routable self-announcement a
	// repeater owes the mesh's directories; applied only when the
	// transmit pipeline runs, 48h when unset.
	AdvertFloodInterval time.Duration `yaml:"advert_flood_interval"`
	// AdvertLocalInterval paces the zero-hop announcement — the form
	// band rules allow first; zero (the default) disables it.
	AdvertLocalInterval time.Duration `yaml:"advert_local_interval"`
	// NodeName is the name adverts carry; the relay's name by default.
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
	var dbm int8
	if err := node.Decode(&dbm); err != nil {
		return fmt.Errorf(`tx_power_dbm wants "auto" or a dBm figure: %w`, err)
	}
	*t = txPower{explicit: true, dbm: dbm}
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
	return p, nil
}

func check(cfg map[string]any) error {
	_, err := paramsFrom(cfg)
	return err
}

func build(relayName string, cfg map[string]any, b *bus.Bus, log *zap.Logger) (protocol.Engine, error) {
	p, err := paramsFrom(cfg)
	if err != nil {
		return nil, err
	}
	// Zero values give the reference's dedup: a fixed 160-entry ring,
	// no time bound. dedup_ttl adds an operator time bound on top.
	if p.DedupEntries == 0 {
		p.DedupEntries = referenceCapacity
	}
	if p.AdvertFloodInterval == 0 {
		p.AdvertFloodInterval = 48 * time.Hour
	}
	if p.NodeName == "" {
		p.NodeName = relayName
	}
	var id *meshcore.LocalIdentity
	if p.Identity != "" {
		if id, err = identityFromConfig(p.Identity); err != nil {
			return nil, err
		}
		log.Info("node identity",
			zap.String("pubkey", hex.EncodeToString(id.PubKey[:])[:keyPrefixLen]))
	}
	return &engine{
		relay: relayName,
		p:     p,
		id:    id,
		bus:   b,
		log:   log,
		seen:  newSeenTable(p.DedupTTL, p.DedupEntries),
	}, nil
}

func (e *engine) Waveform() radio.Waveform { return e.p.Waveform }

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
	if e.txEnabled() {
		e.scheduleAdverts(time.Now())
		e.log.Info("transmit pipeline up",
			zap.String("mode", e.policy.Mode),
			zap.Int8("power_dbm", e.policy.PowerDBm),
			zap.Int("queue_depth", e.policy.QueueDepth))
	} else {
		e.log.Info("dry run: judging frames, transmitting nothing")
	}
	for {
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
			e.log.Debug("corrupt reception", zap.Error(err))
			e.bus.Publish(bus.FrameCorrupt{
				Relay: e.relay, At: time.Now(), Err: err.Error(),
			})
		case errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil:
			// The receive window closed: the pipeline's turn.
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
func (e *engine) receiveWindow(ctx context.Context) (context.Context, context.CancelFunc) {
	if !e.txEnabled() {
		return ctx, func() {}
	}
	if wait, ok := e.txWait(time.Now()); ok {
		return context.WithDeadline(ctx, time.Now().Add(wait))
	}
	return ctx, func() {}
}

func (e *engine) judge(dev radio.Device, frame radio.Frame) {
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
	})

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
	fields, advertOK, selfAdvert := describe(pkt, &judged, e.id)
	log = log.With(fields...)
	if first, dup := e.seen.witness(pkt.Hash(), id, frame.At); dup {
		log.Info("frame judged",
			zap.String("verdict", verdictDuplicate),
			zap.String("duplicate_of", first.Short()),
		)
		judged.Verdict, judged.DuplicateOf = verdictDuplicate, first.Short()
		e.bus.Publish(judged)
		return
	}

	verdict, why := e.verdict(pkt, advertOK, selfAdvert)
	judged.Verdict = verdict
	if why != "" && judged.Detail == "" {
		judged.Detail = why
	}
	log.Info("frame judged", zap.String("verdict", verdict), zap.String("why", why))
	e.bus.Publish(judged)

	if e.txEnabled() {
		e.relayFor(dev, pkt, verdict, id, frame.SNR)
	}
}
