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
	// The self block of the neighbourhood message.
	SelfScopes, DefaultScope string

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
	defer o.sink.Close()
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
			o.publishStatus(time.Now())
			if first {
				first = false
				o.startNeighbors(ctx, &nbBusy, nbDone)
			}
		case at := <-tickC:
			o.publishStatus(at)
		case <-nbTickC:
			o.startNeighbors(ctx, &nbBusy, nbDone)
		case <-nbDone:
			nbBusy = false
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
	if o.cfg.Neighbors == nil || *busy {
		return
	}
	*busy = true
	go func() {
		defer func() { done <- struct{}{} }()
		entries, queried, ran := o.cfg.Neighbors(ctx)
		if !ran || ctx.Err() != nil {
			return
		}
		payload, err := NeighborsJSON(time.Now(), o.cfg.Origin, o.cfg.OriginID,
			o.cfg.SelfScopes, o.cfg.DefaultScope, entries, queried)
		if err != nil {
			return
		}
		o.publish(TopicNeighbors, 0, payload)
	}()
}

func (o *Observer) event(ev bus.Event) {
	switch e := ev.(type) {
	case bus.FrameHeard:
		if e.Relay != o.cfg.Relay || !o.cfg.RX || len(e.Raw) == 0 {
			return
		}
		o.frame(e.Raw, e.At, true, e.SNR, e.RSSI)
	case bus.FrameSent:
		// A shadow emission never touched the air; telling a broker
		// it did would put frames on maps that no antenna radiated.
		if e.Relay != o.cfg.Relay || e.Shadow || len(e.Raw) == 0 {
			return
		}
		if o.cfg.TX == TXOff || o.cfg.TX == "" {
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
		return
	}
	pkt, err := meshcore.ParsePacket(raw)
	if err != nil {
		return
	}
	if !rx && o.cfg.TX == TXSelfAdverts && !o.selfAdvert(pkt) {
		o.filtered()
		return
	}
	if o.types != nil && !o.types[pkt.PayloadType().String()] {
		o.filtered()
		return
	}
	if o.cfg.Packets {
		if payload, err := PacketJSON(raw, pkt, at, rx,
			o.cfg.Origin, o.cfg.OriginID, snr, rssi); err == nil {
			o.publish(TopicPackets, 0, payload)
		}
	}
	if o.cfg.Raw {
		if payload, err := RawJSON(raw, at, o.cfg.Origin, o.cfg.OriginID); err == nil {
			o.publish(TopicRaw, 0, payload)
		}
	}
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
		return
	}
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
		o.log.Warn("observer topic refused", zap.Error(err))
		return
	}
	if err := o.sink.Publish(topic, qos, o.retained(class), payload); err != nil {
		o.mu.Lock()
		o.n.PublishErrors++
		o.mu.Unlock()
		o.log.Debug("observer publish failed", zap.String("topic", topic), zap.Error(err))
		return
	}
	o.mu.Lock()
	o.n.Published++
	o.n.LastPublished = time.Now()
	o.mu.Unlock()
}

func (o *Observer) filtered() {
	o.mu.Lock()
	o.n.Filtered++
	o.mu.Unlock()
}
