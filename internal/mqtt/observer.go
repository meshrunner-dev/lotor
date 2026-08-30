package mqtt

// The observer: one broker connection watching one relay through the
// bus, exactly as the sentinel watches it — another subscriber, never
// another hand in the pipeline. A slow broker costs messages (the
// subscription's bounded buffer, the counters below), never airtime.

import (
	"context"
	"encoding/hex"
	"sync"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/logging"
)

// TX modes: what sent frames are worth telling a broker about.
const (
	TXOff         = "off"
	TXSelfAdverts = "self-adverts" // the reference's default: presence, not traffic
	TXAll         = "all"
)

// Sink is one broker connection — the paho client in production, a
// recorder in tests.
type Sink interface {
	// Publish delivers one message, or says why it could not.
	Publish(topic string, qos byte, retain bool, payload []byte) error
	Connected() bool
	Close()
}

// Config is everything one observer needs decided.
type Config struct {
	Instance string // the config object's name, for logs
	Relay    string // whose frames to watch

	IATA, Token, Topic string

	Status, Packets, Raw bool
	RX                   bool
	TX                   string
	Types                []string // payload-type names; empty admits all
	StatusInterval       time.Duration
	// Retain marks the heartbeat and the neighbourhood, the two
	// messages whose latest edition is the interesting one.
	Retain bool

	// Connects signals each established broker session; the observer
	// answers every one with a fresh heartbeat instead of guessing
	// when the dial lands.
	Connects <-chan struct{}

	// Neighbors runs one neighbourhood round — walking the table and
	// asking each neighbour its scopes takes real airtime, so it is a
	// closure of the relay's, and nil means the feature is off. ran
	// false says the round could not run at all — a dry gate, a
	// cancelled cycle — and nothing is published, where an empty
	// neighbourhood is still an answer.
	Neighbors         func(ctx context.Context) (entries []NeighborEntry, queried int, ran bool)
	NeighborsInterval time.Duration
	// RegionSelf reads the self block of the neighbourhood message —
	// carried names and default — live and in ONE reading, so a
	// mutation between two calls cannot publish the list of one
	// version beside the default of another.
	RegionSelf func() (regions, defaultRegion string)

	// The watched relay's face: name on the air, public key in hex.
	Origin, OriginID string
	// The status message's fixed strings and its live half.
	Model, Firmware, Client, Radio string
	Health                         func() Health
}

// Counters is what an observer will admit to when asked.
type Counters struct {
	Published uint64
	// PublishErrors counts messages a broker refused or timed out;
	// BusDropped what the subscription lost to a slow consumer;
	// Filtered what the configuration declined on purpose.
	PublishErrors uint64
	BusDropped    uint64
	Filtered      uint64
	LastPublished time.Time
}

// Observer feeds one sink from the bus.
type Observer struct {
	cfg     Config
	sink    Sink
	log     *zap.Logger
	types   map[string]bool
	selfKey []byte

	mu sync.Mutex
	n  Counters
}

// New builds an observer; it runs nothing until Run.
func New(cfg Config, sink Sink, log *zap.Logger) *Observer {
	o := &Observer{cfg: cfg, sink: sink, log: log}
	if len(cfg.Types) > 0 {
		o.types = make(map[string]bool, len(cfg.Types))
		for _, t := range cfg.Types {
			o.types[t] = true
		}
	}
	if key, err := hex.DecodeString(cfg.OriginID); err == nil {
		o.selfKey = key
	}
	return o
}

// Counters reports the tally so far, the bus's own drop count folded
// in.
func (o *Observer) Counters(sub *bus.Subscription) Counters {
	o.mu.Lock()
	defer o.mu.Unlock()
	n := o.n
	if sub != nil {
		n.BusDropped = sub.Dropped()
	}
	return n
}

// Run consumes the subscription until the context ends. The caller
// owns the subscription's lifetime; the observer owns the sink's.
func (o *Observer) Run(ctx context.Context, sub *bus.Subscription) {
	logging.Trace(o.log, "observer loop started",
		zap.String("relay", o.cfg.Relay),
		zap.Duration("status_interval", o.cfg.StatusInterval),
		zap.Duration("neighbours_interval", o.cfg.NeighborsInterval))
	defer func() {
		o.sink.Close()
		logging.Trace(o.log, "observer loop stopped", zap.Error(ctx.Err()))
	}()
	var tickC, nbTickC <-chan time.Time
	if o.cfg.Status && o.cfg.StatusInterval > 0 {
		tick := time.NewTicker(o.cfg.StatusInterval)
		defer tick.Stop()
		tickC = tick.C
		// Without a connect signal the first beat goes out blind; with
		// one, it waits for the session instead of counting a failure.
		if o.cfg.Connects == nil {
			o.publishStatus(time.Now())
		}
	}
	if o.cfg.Neighbors != nil && o.cfg.NeighborsInterval > 0 {
		tick := time.NewTicker(o.cfg.NeighborsInterval)
		defer tick.Stop()
		nbTickC = tick.C
	}
	nbDone := make(chan struct{}, 1)
	nbBusy := false
	first := true
	for {
		select {
		case <-ctx.Done():
			return
		case <-o.cfg.Connects:
			logging.Trace(o.log, "observer broker session ready")
			o.publishStatus(time.Now())
			if first {
				first = false
				o.startNeighbors(ctx, &nbBusy, nbDone)
			}
		case at := <-tickC:
			logging.Trace(o.log, "observer status tick")
			o.publishStatus(at)
		case <-nbTickC:
			logging.Trace(o.log, "observer neighbourhood tick")
			o.startNeighbors(ctx, &nbBusy, nbDone)
		case <-nbDone:
			nbBusy = false
			logging.Trace(o.log, "observer neighbourhood worker idle")
		case ev, ok := <-sub.C:
			if !ok {
				return
			}
			o.event(ev)
		}
	}
}

// startNeighbors runs one neighbourhood round off the loop — asking
// each neighbour its scopes blocks for seconds per query, and frames
// must keep flowing meanwhile. One round at a time; a tick landing on
// a running round is dropped, not queued.
func (o *Observer) startNeighbors(ctx context.Context, busy *bool, done chan<- struct{}) {
	if o.cfg.Neighbors == nil {
		logging.Trace(o.log, "observer neighbourhood skipped", zap.String("reason", "disabled"))
		return
	}
	if *busy {
		logging.Trace(o.log, "observer neighbourhood skipped", zap.String("reason", "already-running"))
		return
	}
	*busy = true
	logging.Trace(o.log, "observer neighbourhood worker started")
	go func() {
		defer func() { done <- struct{}{} }()
		entries, queried, ran := o.cfg.Neighbors(ctx)
		if !ran {
			logging.Trace(o.log, "observer neighbourhood produced no snapshot",
				zap.String("reason", "round-not-run"))
			return
		}
		if ctx.Err() != nil {
			logging.Trace(o.log, "observer neighbourhood produced no snapshot",
				zap.String("reason", "cancelled"))
			return
		}
		selfRegions, selfDefault := "", ""
		if o.cfg.RegionSelf != nil {
			selfRegions, selfDefault = o.cfg.RegionSelf()
		}
		payload, err := NeighborsJSON(time.Now(), o.cfg.Origin, o.cfg.OriginID,
			selfRegions, selfDefault, entries, queried)
		if err != nil {
			o.log.Warn("observer payload not encoded",
				zap.String("class", TopicNeighbors), zap.Error(err))
			return
		}
		logging.Trace(o.log, "observer payload encoded",
			zap.String("class", TopicNeighbors), zap.Int("payload_bytes", len(payload)),
			zap.Int("neighbours", len(entries)), zap.Int("queried", queried))
		o.publish(TopicNeighbors, 0, payload)
	}()
}

func (o *Observer) event(ev bus.Event) {
	switch e := ev.(type) {
	case bus.FrameHeard:
		if e.Relay != o.cfg.Relay {
			return
		}
		if !o.cfg.RX {
			o.log.Debug("observer frame ignored",
				zap.String("direction", "rx"), zap.String("reason", "rx-disabled"))
			return
		}
		if len(e.Raw) == 0 {
			logging.Trace(o.log, "observer bus event ignored",
				zap.String("direction", "rx"), zap.String("reason", "empty-frame"))
			return
		}
		o.frame(e.Raw, e.At, true, e.SNR, e.RSSI)
	case bus.FrameSent:
		// A shadow emission never touched the air; telling a broker
		// it did would put frames on maps that no antenna radiated.
		if e.Relay != o.cfg.Relay {
			return
		}
		if e.Shadow {
			o.log.Debug("observer frame ignored",
				zap.String("direction", "tx"), zap.String("reason", "shadow"))
			return
		}
		if len(e.Raw) == 0 {
			logging.Trace(o.log, "observer bus event ignored",
				zap.String("direction", "tx"), zap.String("reason", "empty-frame"))
			return
		}
		if o.cfg.TX == TXOff || o.cfg.TX == "" {
			o.log.Debug("observer frame ignored",
				zap.String("direction", "tx"), zap.String("reason", "tx-disabled"))
			return
		}
		o.frame(e.Raw, e.At, false, 0, 0)
	}
}

// frame analyses one wire frame and publishes what the configuration
// asks for. A frame that does not parse is not an observer's to
// describe — the packets contract is an analysis, and the journal
// already records corruption.
func (o *Observer) frame(raw []byte, at time.Time, rx bool, snr, rssi float64) {
	if !o.cfg.Packets && !o.cfg.Raw {
		o.log.Debug("observer frame ignored",
			zap.String("direction", direction(rx)), zap.String("reason", "outputs-disabled"))
		return
	}
	pkt, err := meshcore.ParsePacket(raw)
	if err != nil {
		o.log.Debug("observer frame ignored",
			zap.String("direction", direction(rx)), zap.String("reason", "invalid-frame"),
			zap.Int("wire_bytes", len(raw)), zap.Error(err))
		return
	}
	if !rx && o.cfg.TX == TXSelfAdverts && !o.selfAdvert(pkt) {
		o.filtered("tx-policy", pkt, rx)
		return
	}
	if o.types != nil && !o.types[pkt.PayloadType().String()] {
		o.filtered("type-filter", pkt, rx)
		return
	}
	o.log.Debug("observer frame selected",
		zap.String("direction", direction(rx)),
		zap.String("packet_type", pkt.PayloadType().String()),
		zap.Int("wire_bytes", len(raw)))
	if o.cfg.Packets {
		payload, err := PacketJSON(raw, pkt, at, rx,
			o.cfg.Origin, o.cfg.OriginID, snr, rssi)
		if err != nil {
			o.log.Warn("observer payload not encoded",
				zap.String("class", TopicPackets), zap.Error(err))
		} else {
			logging.Trace(o.log, "observer payload encoded",
				zap.String("class", TopicPackets), zap.Int("payload_bytes", len(payload)))
			o.publish(TopicPackets, 0, payload)
		}
	}
	if o.cfg.Raw {
		payload, err := RawJSON(raw, at, o.cfg.Origin, o.cfg.OriginID)
		if err != nil {
			o.log.Warn("observer payload not encoded",
				zap.String("class", TopicRaw), zap.Error(err))
		} else {
			logging.Trace(o.log, "observer payload encoded",
				zap.String("class", TopicRaw), zap.Int("payload_bytes", len(payload)))
			o.publish(TopicRaw, 0, payload)
		}
	}
}

func direction(rx bool) string {
	if rx {
		return "rx"
	}
	return "tx"
}

// selfAdvert reports whether a sent frame is this node announcing
// itself — the one emission the default TX mode shares.
func (o *Observer) selfAdvert(pkt *meshcore.Packet) bool {
	if pkt.PayloadType() != meshcore.PayloadTypeAdvert ||
		len(pkt.Payload) < meshcore.PubKeySize || len(o.selfKey) != meshcore.PubKeySize {
		return false
	}
	for i := range o.selfKey {
		if pkt.Payload[i] != o.selfKey[i] {
			return false
		}
	}
	return true
}

func (o *Observer) publishStatus(at time.Time) {
	h := Health{}
	if o.cfg.Health != nil {
		h = o.cfg.Health()
	}
	payload, err := StatusJSON(at, o.cfg.Origin, o.cfg.OriginID,
		o.cfg.Model, o.cfg.Firmware, o.cfg.Radio, o.cfg.Client, h)
	if err != nil {
		o.log.Warn("observer payload not encoded",
			zap.String("class", TopicStatus), zap.Error(err))
		return
	}
	logging.Trace(o.log, "observer payload encoded",
		zap.String("class", TopicStatus), zap.Int("payload_bytes", len(payload)))
	// QoS 1, like the reference: the heartbeat is the one message
	// worth a broker's acknowledgement.
	o.publish(TopicStatus, 1, payload)
}

// retained says whether a message class carries the retain flag: the
// snapshots do, when the broker allows it; the frame stream never.
func (o *Observer) retained(class string) bool {
	return o.cfg.Retain && (class == TopicStatus || class == TopicNeighbors)
}

func (o *Observer) publish(class string, qos byte, payload []byte) {
	topic, err := BuildTopic(o.cfg.Topic, o.cfg.IATA, o.cfg.OriginID, o.cfg.Token, class)
	if err != nil {
		o.mu.Lock()
		o.n.PublishErrors++
		o.mu.Unlock()
		o.log.Warn("observer topic refused", zap.String("class", class), zap.Error(err))
		return
	}
	retain := o.retained(class)
	started := time.Now()
	err = o.sink.Publish(topic, qos, retain, payload)
	if err != nil {
		o.mu.Lock()
		o.n.PublishErrors++
		o.mu.Unlock()
		o.log.Debug("observer publish failed",
			zap.String("class", class), zap.String("topic", topic), zap.Error(err))
		return
	}
	logging.Trace(o.log, "observer broker publish completed",
		zap.String("class", class), zap.String("topic", topic),
		zap.Uint8("qos", qos), zap.Bool("retain", retain),
		zap.Int("payload_bytes", len(payload)), zap.Duration("elapsed", time.Since(started)),
		zap.Bool("success", true))
	o.mu.Lock()
	o.n.Published++
	o.n.LastPublished = time.Now()
	o.mu.Unlock()
	o.log.Debug("observer message published", zap.String("class", class), zap.String("topic", topic))
}

func (o *Observer) filtered(reason string, pkt *meshcore.Packet, rx bool) {
	o.mu.Lock()
	o.n.Filtered++
	o.mu.Unlock()
	o.log.Debug("observer frame filtered",
		zap.String("direction", direction(rx)), zap.String("reason", reason),
		zap.String("packet_type", pkt.PayloadType().String()))
}
