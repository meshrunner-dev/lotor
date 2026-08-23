// Package meshcore is the MeshCore protocol engine. In this stage it
// is deliberately receive-only: it hears, parses, deduplicates and
// judges frames — logging what it *would* relay — without owning a
// transmit path at all. The dry run is how the judgement earns trust
// on a live mesh before it is allowed to key a transmitter.
package meshcore

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/protocol"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/txn"
)

func init() {
	protocol.Register("meshcore", protocol.Builder{Build: build, Presets: presets})
}

// params is the relay-side configuration: the waveform choice plus
// the engine's own knobs.
type params struct {
	radio.Waveform `yaml:",inline"`

	// TxPowerDBm is parsed and envelope-checked now so configs are
	// validated early, even though the engine cannot transmit yet.
	TxPowerDBm *int8 `yaml:"tx_power_dbm"`

	// DedupTTL bounds how long a packet hash suppresses its copies.
	DedupTTL time.Duration `yaml:"dedup_ttl"`
	// DedupEntries bounds the seen table's size.
	DedupEntries int `yaml:"dedup_entries"`
}

type engine struct {
	relay string
	p     params
	bus   *bus.Bus
	log   *zap.Logger
	seen  *seenTable
}

func build(relayName string, cfg map[string]any, b *bus.Bus, log *zap.Logger) (protocol.Engine, error) {
	p, err := config.Decode[params](cfg)
	if err != nil {
		return nil, fmt.Errorf("meshcore params: %w", err)
	}
	if p.FrequencyHz == 0 {
		return nil, errors.New("meshcore params: frequency_hz is required")
	}
	// Zero values give the reference's dedup: a fixed 160-entry ring,
	// no time bound. dedup_ttl adds an operator time bound on top.
	if p.DedupEntries == 0 {
		p.DedupEntries = referenceCapacity
	}
	return &engine{
		relay: relayName,
		p:     p,
		bus:   b,
		log:   log,
		seen:  newSeenTable(p.DedupTTL, p.DedupEntries),
	}, nil
}

func (e *engine) Waveform() radio.Waveform { return e.p.Waveform }

func (e *engine) Run(ctx context.Context, dev radio.Device) error {
	e.log.Info("dry run: judging frames, transmitting nothing")
	for {
		frame, err := dev.Receive(ctx)
		switch {
		case err == nil:
		case errors.Is(err, radio.ErrCorrupt):
			e.log.Debug("corrupt reception", zap.Error(err))
			continue
		case ctx.Err() != nil:
			return ctx.Err()
		default:
			return err
		}
		e.judge(frame)
	}
}

func (e *engine) judge(frame radio.Frame) {
	id := txn.New()
	log := e.log.With(zap.String("txn", id.Short()))
	log.Info("frame heard",
		zap.Int("bytes", len(frame.Payload)),
		zap.Float64("rssi_dbm", frame.RSSI),
		zap.Float64("snr_db", frame.SNR),
		zap.Duration("airtime", frame.Airtime),
	)
	e.bus.Publish(bus.FrameHeard{
		Relay: e.relay, Txn: id, At: frame.At,
		Bytes: len(frame.Payload), RSSI: frame.RSSI, SNR: frame.SNR,
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
	fields, advertOK := describe(pkt, &judged)
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

	verdict, why := e.verdict(pkt, advertOK)
	judged.Verdict = verdict
	if why != "" && judged.Detail == "" {
		judged.Detail = why
	}
	log.Info("frame judged", zap.String("verdict", verdict), zap.String("why", why))
	e.bus.Publish(judged)
}
