package main

// The manager's observer wing: MQTT connections live and die with
// their configuration, bounced on their own — never bouncing the
// relay whose frames they merely watch — and re-bounced when that
// relay itself is rebuilt, because their captured face (name, key,
// waveform) must follow it.

import (
	"context"
	"fmt"
	"sort"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/cli"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/mqtt"
)

// observerSubBuffer bounds what a slow broker may lag behind before
// the bus starts counting drops against it.
const observerSubBuffer = 256

type managedObserver struct {
	cancel context.CancelFunc
	done   chan struct{}
	obs    *mqtt.Observer
	sub    *bus.Subscription
	broker *mqtt.Broker
	url    string
	relay  string
}

// observerRelay resolves which relay an observer watches: the named
// one, or the only one there is.
func (m *manager) observerRelay(mq config.MQTT) (string, error) {
	if mq.Relay != "" {
		if _, ok := m.file.Relays[mq.Relay]; !ok {
			return "", fmt.Errorf("no relay %q to observe", mq.Relay)
		}
		return mq.Relay, nil
	}
	if len(m.file.Relays) == 1 {
		for name := range m.file.Relays {
			return name, nil
		}
	}
	return "", fmt.Errorf("%d relays run here — relay= says which to observe", len(m.file.Relays))
}

// observerConfig assembles one observer's whole decision set from its
// stored shape and the watched relay's live face. The caller holds mu.
func (m *manager) observerConfig(name string, mq config.MQTT) (mqtt.Config, error) {
	relayName, err := m.observerRelay(mq)
	if err != nil {
		return mqtt.Config{}, err
	}
	info, ok := m.infos[relayName]
	if !ok {
		return mqtt.Config{}, fmt.Errorf("relay %q is not assembled", relayName)
	}
	if info.Identity == "" {
		return mqtt.Config{}, fmt.Errorf("relay %q has no identity — an observer without a device id has no topic", relayName)
	}
	cfg := mqtt.Config{
		Instance: name,
		Relay:    relayName,
		IATA:     mq.IATA,
		Token:    mq.Token,
		Topic:    mq.Topic,
		Raw:      mq.Raw,
		TX:       mq.TX,
		Types:    mq.Types,
		Origin:   info.NodeName,
		OriginID: info.Identity,
		Model:    info.Driver,
		Firmware: version,
		Client:   "lotor " + version,
		Radio: mqtt.RadioString(info.Waveform.FrequencyHz,
			uint32(max(info.Waveform.BandwidthHz, 0)), // a LoRa bandwidth, never negative
			info.Waveform.SpreadingFactor, info.Waveform.CodingRate),
		Health: m.observerHealth(relayName, time.Now()),
	}
	if cfg.Topic == "" {
		cfg.Topic = mqtt.DefaultTopic
	}
	if cfg.TX == "" {
		cfg.TX = mqtt.TXSelfAdverts
	}
	cfg.Status = mq.Status == nil || *mq.Status
	cfg.Packets = mq.Packets == nil || *mq.Packets
	cfg.RX = mq.RX == nil || *mq.RX
	cfg.StatusInterval = mq.StatusInterval
	if cfg.StatusInterval == 0 {
		cfg.StatusInterval = 5 * time.Minute
	}
	return cfg, nil
}

// observerHealth reads the watched relay's vitals each time the
// heartbeat asks, through the manager's live view — a bounced relay's
// successor answers, not a captured pointer to its predecessor.
func (m *manager) observerHealth(relayName string, started time.Time) func() mqtt.Health {
	return func() mqtt.Health {
		m.mu.Lock()
		info, ok := m.infos[relayName]
		m.mu.Unlock()
		h := mqtt.Health{}
		if !ok {
			return h
		}
		up := int(time.Since(started).Seconds())
		h.UptimeSecs = &up
		h.Repeat = "off"
		if info.TXMode == config.TXOnAir {
			h.Repeat = "on"
		}
		if info.NoiseFloor != nil {
			if nf, ok := info.NoiseFloor(); ok {
				floor := int(nf.DBm)
				h.NoiseFloor = &floor
			}
		}
		if info.Traffic != nil {
			sent, received, recvErrors, txAir, rxAir := info.Traffic()
			s, r, e := int(sent), int(received), int(recvErrors)
			tx, rx := int(txAir.Seconds()), int(rxAir.Seconds())
			h.PacketsSent, h.PacketsReceived, h.RecvErrors = &s, &r, &e
			h.TxAirSecs, h.RxAirSecs = &tx, &rx
		}
		return h
	}
}

// startObserver brings one connection up. A configuration that cannot
// observe — no such relay, no identity — logs and stays down rather
// than half-running; the operator reads why in the journal and in
// status. The caller holds mu.
func (m *manager) startObserver(ctx context.Context, name string) {
	mq, ok := m.file.MQTT[name]
	if !ok {
		return
	}
	log := m.log.Named("mqtt").With(zap.String("observer", name))
	cfg, err := m.observerConfig(name, mq)
	if err != nil {
		log.Error("observer not started", zap.Error(err))
		return
	}
	broker, err := mqtt.Dial(mq.URL, mq.Username, mq.Password, name, log)
	if err != nil {
		log.Error("observer not started", zap.Error(err))
		return
	}
	sub := m.bus.Subscribe(observerSubBuffer)
	obs := mqtt.New(cfg, broker, log)
	octx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	m.observers[name] = &managedObserver{
		cancel: cancel, done: done, obs: obs, sub: sub, broker: broker,
		url: mq.URL, relay: cfg.Relay,
	}
	m.wg.Go(func() {
		defer close(done)
		defer sub.Close()
		obs.Run(octx, sub)
	})
	log.Info("observer up", zap.String("broker", mq.URL), zap.String("relay", cfg.Relay))
}

// stopObserver takes one down and waits it out. The caller holds mu.
func (m *manager) stopObserver(name string) {
	h, ok := m.observers[name]
	if !ok {
		return
	}
	h.cancel()
	<-h.done
	delete(m.observers, name)
}

// bounceObserver restarts one connection under the daemon's own
// context — a mutation's order outlives the session that gave it.
// The caller holds mu.
func (m *manager) bounceObserver(name string) {
	m.stopObserver(name)
	m.startObserver(m.ctx, name)
}

// bounceObserversOf restarts every observer watching a relay that was
// just rebuilt: their captured face must follow the successor. The
// caller holds mu.
func (m *manager) bounceObserversOf(relayName string) {
	for name, h := range m.observers {
		if h.relay == relayName {
			m.bounceObserver(name)
		}
	}
}

// MQTTInfos is the console's live view of the observers.
func (m *manager) MQTTInfos() []cli.MQTTInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]cli.MQTTInfo, 0, len(m.observers))
	for name, h := range m.observers {
		out = append(out, cli.MQTTInfo{
			Name: name, URL: h.url, Relay: h.relay,
			Connected: h.broker.Connected,
			Counters: func() (uint64, uint64, uint64, uint64, time.Time) {
				n := h.obs.Counters(h.sub)
				return n.Published, n.PublishErrors, n.BusDropped, n.Filtered, n.LastPublished
			},
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
