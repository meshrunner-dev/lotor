package main

// The manager's observer wing: MQTT connections live and die with
// their configuration, bounced on their own — never bouncing the
// relay whose frames they merely watch — and re-bounced when that
// relay itself is rebuilt, because their captured face (name, key,
// waveform) must follow it.

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/cli"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/mqtt"
	enginemc "meshrunner.dev/lotor/internal/protocol/meshcore"
)

// observerSubBuffer bounds what a slow broker may lag behind before
// the bus starts counting drops against it.
const observerSubBuffer = 256

// neighboursFloor is the least a neighbourhood round may repeat:
// each one asks every neighbour over the air, and that is airtime
// the mesh pays for.
const neighboursFloor = time.Hour

type managedObserver struct {
	cancel context.CancelFunc
	done   chan struct{}
	obs    *mqtt.Observer
	sub    *bus.Subscription
	broker *mqtt.Broker
	url    string
	relay  string
}

// resolveMQTTParams turns one observer's layers into its effective
// parameter set, refusing shapes that could not observe — the same
// deep check a mutation runs before anything is written.
func resolveMQTTParams(mq config.MQTT) (mqtt.Params, error) {
	effective, _, err := mq.Layered.Resolve(mqtt.Presets())
	if err != nil {
		return mqtt.Params{}, err
	}
	p, err := config.Decode[mqtt.Params](effective)
	if err != nil {
		return mqtt.Params{}, err
	}
	if p.URL == "" {
		return mqtt.Params{}, errors.New("an observer needs url= — its own, or a profile's")
	}
	if !hasBrokerScheme(p.URL) {
		return mqtt.Params{}, fmt.Errorf("url %q — want tcp://, ssl://, ws:// or wss://", p.URL)
	}
	switch p.TX {
	case "", mqtt.TXOff, mqtt.TXSelfAdverts, mqtt.TXAll:
	default:
		return mqtt.Params{}, fmt.Errorf("tx %q — want off, self-adverts or all", p.TX)
	}
	if p.Neighbors && p.NeighborsInterval.Std() != 0 && p.NeighborsInterval.Std() < neighboursFloor {
		return mqtt.Params{}, fmt.Errorf(
			"neighbors_interval under %s — each round asks every neighbour over the air", neighboursFloor)
	}
	return p, nil
}

// observerRelay resolves which relay an observer watches: the named
// one, or the only one there is.
func (m *manager) observerRelay(relay string) (string, error) {
	if relay != "" {
		if _, ok := m.file.Relays[relay]; !ok {
			return "", fmt.Errorf("no relay %q to observe", relay)
		}
		return relay, nil
	}
	if len(m.file.Relays) == 1 {
		for name := range m.file.Relays {
			return name, nil
		}
	}
	return "", fmt.Errorf("%d relays run here — relay= says which to observe", len(m.file.Relays))
}

// observerConfig assembles one observer's whole decision set from its
// effective parameters and the watched relay's live face. The caller
// holds mu.
func (m *manager) observerConfig(name string, p mqtt.Params, log *zap.Logger) (mqtt.Config, error) {
	relayName, err := m.observerRelay(p.Relay)
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
		IATA:     p.IATA,
		Token:    p.Token,
		Topic:    p.Topic,
		Raw:      p.Raw,
		TX:       p.TX,
		Types:    p.Types,
		Retain:   p.Retain,
		Origin:   info.NodeName,
		OriginID: info.Identity,
		Model:    info.Driver,
		Firmware: version,
		Client:   "lotor " + version,
		Radio: mqtt.RadioString(info.Waveform.FrequencyHz,
			uint32(max(info.Waveform.BandwidthHz, 0)), // a LoRa bandwidth, never negative
			info.Waveform.SpreadingFactor, info.Waveform.CodingRate),
		Health:       m.observerHealth(relayName),
		SelfScopes:   strings.Join(info.Scopes, ","),
		DefaultScope: info.DefaultScope,
	}
	if cfg.Topic == "" {
		cfg.Topic = mqtt.DefaultTopic
	}
	if cfg.TX == "" {
		cfg.TX = mqtt.TXSelfAdverts
	}
	cfg.Status = p.Status == nil || *p.Status
	cfg.Packets = p.Packets == nil || *p.Packets
	cfg.RX = p.RX == nil || *p.RX
	cfg.StatusInterval = p.StatusInterval.Std()
	if cfg.StatusInterval == 0 {
		cfg.StatusInterval = 5 * time.Minute
	}
	if p.Neighbors {
		cfg.Neighbors = m.neighboursRound(relayName, log)
		cfg.NeighborsInterval = p.NeighborsInterval.Std()
		if cfg.NeighborsInterval == 0 {
			cfg.NeighborsInterval = 24 * time.Hour
		}
	}
	return cfg, nil
}

// observerDial builds the connection options: transport, keepalive,
// pinned chain, and the authentication the parameters choose — an
// audience mints a device token fresh at every connect, a username
// rides as-is ({pubkey} resolved to the watched relay's key), and
// silence connects anonymously.
func observerDial(p mqtt.Params, name string, info cli.RelayInfo,
	connects chan<- struct{}, log *zap.Logger,
) mqtt.Options {
	opts := mqtt.Options{
		URL:       p.URL,
		Instance:  name,
		Keepalive: p.Keepalive.Std(),
		CAFile:    p.CA,
		OnConnect: func() {
			select {
			case connects <- struct{}{}:
			default:
			}
		},
	}
	switch {
	case p.Audience != "":
		opts.Credentials = func() (string, string) {
			token, err := mqtt.AuthToken(info.Identity, p.Audience, p.TokenLifetime.Std(),
				time.Now(), func(msg []byte) ([]byte, error) {
					if info.Sign == nil {
						return nil, errors.New("the relay cannot sign — no identity")
					}
					return info.Sign(msg), nil
				})
			if err != nil {
				log.Warn("device token not minted", zap.Error(err))
				return "", ""
			}
			return mqtt.JWTUsername(info.Identity), token
		}
	case p.Username == "{pubkey}":
		opts.Username, opts.Password = info.Identity, p.Password
	default:
		opts.Username, opts.Password = p.Username, p.Password
	}
	return opts
}

// neighboursRound is the observer's periodic neighbourhood walk: list
// the direct neighbours, ask each its scopes over the air, and report
// each outcome honestly — responded, timeout, or send_failed. It runs
// off the observer's loop and reads the manager's live view, so a
// bounced relay's successor answers.
func (m *manager) neighboursRound(relayName string, log *zap.Logger,
) func(ctx context.Context) ([]mqtt.NeighborEntry, int) {
	return func(ctx context.Context) ([]mqtt.NeighborEntry, int) {
		m.mu.Lock()
		info, ok := m.infos[relayName]
		m.mu.Unlock()
		if !ok || info.Neighbours == nil {
			return nil, 0
		}
		rows := info.Neighbours()
		entries := make([]mqtt.NeighborEntry, 0, len(rows))
		queried := 0
		for _, nb := range rows {
			e := mqtt.NeighborEntry{
				PubKey: hex.EncodeToString(nb.PubKey[:]),
				SNR:    nb.SNR,
			}
			if nb.Heard.IsZero() {
				e.HeardUnknown = true
			} else {
				e.HeardSecsAgo = int(time.Since(nb.Heard).Seconds())
			}
			if info.AskScopes != nil && ctx.Err() == nil {
				queried++
				scopes, err := info.AskScopes(nb.PubKey[:])
				switch {
				case err == nil:
					e.Status = "responded"
					e.Scopes = strings.Join(scopes, ",")
				case errors.Is(err, enginemc.ErrNoAnswer):
					e.Status = "timeout"
				default:
					e.Status = "send_failed"
					log.Debug("neighbour scopes not asked",
						zap.String("neighbour", e.PubKey[:12]), zap.Error(err))
				}
			} else {
				e.Status = "send_failed"
			}
			entries = append(entries, e)
		}
		return entries, queried
	}
}

// observerHealth reads the watched relay's vitals each time the
// heartbeat asks, through the manager's live view — a bounced relay's
// successor answers, not a captured pointer to its predecessor.
func (m *manager) observerHealth(relayName string) func() mqtt.Health {
	return func() mqtt.Health {
		m.mu.Lock()
		info, ok := m.infos[relayName]
		m.mu.Unlock()
		h := mqtt.Health{}
		if !ok {
			return h
		}
		up := int(time.Since(info.Started).Seconds())
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
	p, err := resolveMQTTParams(mq)
	if err != nil {
		log.Error("observer not started", zap.Error(err))
		return
	}
	cfg, err := m.observerConfig(name, p, log)
	if err != nil {
		log.Error("observer not started", zap.Error(err))
		return
	}
	connects := make(chan struct{}, 1)
	cfg.Connects = connects
	broker, err := mqtt.Dial(observerDial(p, name, m.infos[cfg.Relay], connects, log), log)
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
		url: p.URL, relay: cfg.Relay,
	}
	m.wg.Go(func() {
		defer close(done)
		defer sub.Close()
		obs.Run(octx, sub)
	})
	log.Info("observer up", zap.String("broker", p.URL), zap.String("relay", cfg.Relay))
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
