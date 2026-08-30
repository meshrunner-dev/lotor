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
	"sync"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/cli"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/logging"
	"meshrunner.dev/lotor/internal/mqtt"
	"meshrunner.dev/lotor/internal/product"
	enginemc "meshrunner.dev/lotor/internal/protocol/meshcore"
)

// observerSubBuffer bounds what a slow broker may lag behind before
// the bus starts counting drops against it.
const observerSubBuffer = 256

// neighboursFloor is the least a neighbourhood round may repeat:
// each one asks every neighbour over the air, and that is airtime
// the mesh pays for — negligible at this pace, rude much below it.
const neighboursFloor = 30 * time.Minute

type managedObserver struct {
	cancel context.CancelFunc
	done   chan struct{}
	obs    *mqtt.Observer
	sub    *bus.Subscription
	broker *mqtt.Broker
	url    string
	relay  string
	// live gates this incarnation's connection callbacks: closed at
	// stop, so a late Paho notification cannot speak after it.
	live *liveGate
}

// decodeMQTTParams resolves the persisted shape shared by active and parked
// observers. IATA identifies the observation site, not merely one topic
// placeholder, so every observer carries it even when its template omits it.
func decodeMQTTParams(mq config.MQTT) (mqtt.Params, error) {
	effective, _, err := mq.Layered.Resolve(mqtt.Presets())
	if err != nil {
		return mqtt.Params{}, err
	}
	p, err := config.Decode[mqtt.Params](effective)
	if err != nil {
		return mqtt.Params{}, err
	}
	if p.IATA == "" {
		return mqtt.Params{}, errors.New("iata= is required for every observer")
	}
	iata, err := mqtt.NormalizeIATA(p.IATA)
	if err != nil {
		return mqtt.Params{}, err
	}
	p.IATA = iata
	return p, nil
}

// resolveMQTTParams turns one observer's layers into its effective
// parameter set, refusing shapes that could not observe — the same
// deep check a mutation runs before anything is written.
func resolveMQTTParams(mq config.MQTT) (mqtt.Params, error) {
	p, err := decodeMQTTParams(mq)
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
	if p.NeighboursInterval.Std() != 0 && p.NeighboursInterval.Std() < neighboursFloor {
		return mqtt.Params{}, fmt.Errorf(
			"neighbours_interval under %s — each round asks every neighbour over the air", neighboursFloor)
	}
	if err := mqtt.ValidOwner(p.Owner); err != nil {
		return mqtt.Params{}, err
	}
	// The topic must already build with what is configured — finding a
	// hole per published frame is a journal full of refusals, not an
	// answer the operator can act on.
	topic := p.Topic
	if topic == "" {
		topic = mqtt.DefaultTopic
	}
	if _, err := mqtt.BuildTopic(topic, p.IATA, "-", p.Token, mqtt.TopicStatus); err != nil {
		return mqtt.Params{}, fmt.Errorf("nothing could be published: %w", err)
	}
	return p, nil
}

// observerRelay resolves which relay an observer watches: the named
// one, or the only one there is.
func (m *manager) observerRelay(relay string) (string, error) {
	return observerRelayIn(m.file, relay)
}

// observerRelayIn is the same resolution against any file: a pure
// judgement, which is what lets the reconciliation compare where an
// observer would land before and after a topology change.
func observerRelayIn(f *config.File, relay string) (string, error) {
	if relay != "" {
		if _, ok := f.Relays[relay]; !ok {
			return "", fmt.Errorf("no relay %q to observe", relay)
		}
		return relay, nil
	}
	if len(f.Relays) == 1 {
		for name := range f.Relays {
			return name, nil
		}
	}
	return "", fmt.Errorf("%d relays run here — relay= says which to observe", len(f.Relays))
}

// observerConfig assembles one observer's whole decision set from its
// effective parameters and the watched relay's live face. The caller
// holds mu.
func (m *manager) observerConfig(name string, p mqtt.Params, log *zap.Logger) (mqtt.Config, error) {
	relayName, err := m.observerRelay(p.Relay)
	if err != nil {
		return mqtt.Config{}, err
	}
	m.viewMu.RLock()
	info, ok := m.infos[relayName]
	m.viewMu.RUnlock()
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
		OriginID: mqtt.DeviceID(info.Identity),
		Model:    info.Driver,
		Firmware: version,
		Client:   product.Slug + "/" + version,
		Radio: mqtt.RadioString(info.Waveform.FrequencyHz,
			uint32(max(info.Waveform.BandwidthHz, 0)), // a LoRa bandwidth, never negative
			info.Waveform.SpreadingFactor, info.Waveform.CodingRate),
		Health:     m.observerHealth(relayName),
		RegionSelf: regionSelfOf(info),
	}
	if p.Origin != "" {
		// The operator may publish under another banner than the air's.
		cfg.Origin = p.Origin
	}
	if cfg.Topic == "" {
		cfg.Topic = mqtt.DefaultTopic
	}
	if cfg.TX == "" {
		cfg.TX = mqtt.TXSelfAdverts
	}
	cfg.Packets = p.Packets == nil || *p.Packets
	cfg.RX = p.RX == nil || *p.RX
	// The interval is the heartbeat's whole switch: absent beats at
	// the default, an explicit zero is silence.
	cfg.StatusInterval = 5 * time.Minute
	if p.StatusInterval != nil {
		cfg.StatusInterval = p.StatusInterval.Std()
	}
	cfg.Status = cfg.StatusInterval > 0
	if p.NeighboursInterval.Std() > 0 {
		cfg.Neighbors = m.neighboursRound(relayName, log)
		cfg.NeighborsInterval = p.NeighboursInterval.Std()
	}
	return cfg, nil
}

// observerDial builds the connection options: transport, keepalive,
// pinned chain, and the authentication the parameters choose — an
// audience mints a device token fresh at every connect, a username
// rides as-is ({pubkey} resolved to the watched relay's key), and
// silence connects anonymously.
func observerDial(p mqtt.Params, name string, info cli.RelayInfo,
	connects chan<- struct{}, publish func(bus.Event), log *zap.Logger,
) mqtt.Options {
	opts := mqtt.Options{
		URL:       p.URL,
		Instance:  name,
		Keepalive: p.Keepalive.Std(),
		CAFile:    p.CA,
		OnTransition: func(state, cause string) {
			publish(bus.ObserverState{
				Observer: name, At: time.Now(), State: state, Cause: cause,
			})
		},
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
			token, err := mqtt.AuthToken(info.Identity, p.Audience, p.Owner, p.TokenLifetime.Std(),
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
		opts.Username, opts.Password = mqtt.DeviceID(info.Identity), p.Password
	default:
		opts.Username, opts.Password = p.Username, p.Password
	}
	return opts
}

// neighboursRound is the observer's periodic neighbourhood cycle,
// two-staged the way the ecosystem publishes it: a zero-hop discover
// refreshes the table and its window is waited out, then each
// neighbour is asked its scopes over the air, each outcome reported
// honestly — responded, timeout, or send_failed. The snapshot is
// rebuilt every cycle rather than trusted to age well: a table two
// seconds old publishes what the air answers, not an empty page. It
// runs off the observer's loop and reads the manager's live view, so
// a bounced relay's successor answers.
func (m *manager) neighboursRound(relayName string, log *zap.Logger,
) func(ctx context.Context) ([]mqtt.NeighborEntry, int, bool) {
	return func(ctx context.Context) ([]mqtt.NeighborEntry, int, bool) {
		m.viewMu.RLock()
		info, ok := m.infos[relayName]
		m.viewMu.RUnlock()
		if !ok || info.Neighbours == nil {
			logging.Trace(log, "neighbourhood round skipped",
				zap.String("reason", "relay-view-unavailable"))
			return nil, 0, false
		}
		// A round is emissions: discover, then a question per
		// neighbour. A gate that cannot key the radio has nothing to
		// run — and publishing all-send_failed would dress a posture
		// up as an outage.
		if info.TXMode != config.TXOnAir && info.TXMode != config.TXOnAirZeroHop {
			log.Info("neighbourhood round skipped — the transmit gate does not key the radio",
				zap.String("tx_mode", info.TXMode))
			return nil, 0, false
		}
		if !refreshNeighbours(ctx, info, log) {
			return nil, 0, false
		}
		rows := info.Neighbours()
		entries := make([]mqtt.NeighborEntry, 0, len(rows))
		queried := 0
		for _, nb := range rows {
			e := mqtt.NeighborEntry{
				PubKey: mqtt.DeviceID(hex.EncodeToString(nb.PubKey[:])),
				SNR:    nb.SNR,
			}
			if nb.Heard.IsZero() {
				e.HeardUnknown = true
			} else {
				e.HeardSecsAgo = int(time.Since(nb.Heard).Seconds())
			}
			if info.AskRegions != nil && ctx.Err() == nil {
				queried++
				regions, err := info.AskRegions(nb.PubKey[:])
				switch {
				case err == nil:
					e.Status = "responded"
					e.Regions = strings.Join(regions, ",")
					log.Debug("neighbour scopes answered",
						zap.String("neighbour", e.PubKey[:12]), zap.Int("regions", len(regions)))
				case errors.Is(err, enginemc.ErrNoAnswer):
					e.Status = "timeout"
					log.Debug("neighbour scopes timed out", zap.String("neighbour", e.PubKey[:12]))
				default:
					e.Status = "send_failed"
					log.Debug("neighbour scopes not asked",
						zap.String("neighbour", e.PubKey[:12]), zap.Error(err))
				}
			} else {
				e.Status = "send_failed"
				log.Debug("neighbour scopes not asked",
					zap.String("neighbour", e.PubKey[:12]), zap.String("reason", "unavailable"))
			}
			entries = append(entries, e)
		}
		log.Info("neighbourhood round done",
			zap.Int("neighbours", len(entries)), zap.Int("queried", queried))
		return entries, queried, true
	}
}

// refreshNeighbours runs the cycle's first stage: a zero-hop
// discover, its answers drained until the window closes so the table
// is current when the questions begin. A relay that cannot scan —
// dry gate, a scan already running — degrades to the table as it
// stands, said in the journal. false means the round was cancelled.
func refreshNeighbours(ctx context.Context, info cli.RelayInfo, log *zap.Logger) bool {
	if info.Discover == nil {
		logging.Trace(log, "neighbourhood refresh skipped", zap.String("reason", "unsupported"))
		return true
	}
	answers, until, err := info.Discover()
	if errors.Is(err, enginemc.ErrScanListening) {
		// Another round already keyed the scan; its answers land in
		// the shared table, so joining is waiting the window out.
		return joinScanWindow(ctx, info, log)
	}
	if err != nil {
		log.Debug("neighbourhood refresh skipped — table as it stands", zap.Error(err))
		return true
	}
	log.Info("neighbourhood refresh started", zap.Time("window_closes", until))
	// The engine closes the channel at the window's end; the deadline
	// below is the belt for an engine that cannot say so in time —
	// the round proceeds with the table as it stands rather than
	// hanging on a quiet channel.
	overdue := time.NewTimer(time.Until(until) + 30*time.Second)
	defer overdue.Stop()
	received := 0
	for {
		select {
		case <-ctx.Done():
			logging.Trace(log, "neighbourhood refresh cancelled", zap.Int("answers", received))
			return false
		case <-overdue.C:
			log.Debug("neighbourhood refresh window never closed — table as it stands")
			return true
		case _, more := <-answers:
			if !more {
				logging.Trace(log, "neighbourhood refresh completed", zap.Int("answers", received))
				return true
			}
			received++
		}
	}
}

// joinScanWindow waits out the scan another asker keyed — the shared
// table fills either way — and proceeds with what it gathered. false
// only when cancelled.
func joinScanWindow(ctx context.Context, info cli.RelayInfo, log *zap.Logger) bool {
	until := time.Now().Add(65 * time.Second) // a window and a breath, when nobody will say
	if info.ScanWindow != nil {
		if end, ok := info.ScanWindow(); ok {
			until = end.Add(2 * time.Second)
		}
	}
	logging.Trace(log, "joining the scan already listening", zap.Time("window_closes", until))
	select {
	case <-ctx.Done():
		return false
	case <-time.After(time.Until(until)):
		return true
	}
}

// observerHealth reads the watched relay's vitals each time the
// heartbeat asks, through the manager's live view — a bounced relay's
// successor answers, not a captured pointer to its predecessor.
func (m *manager) observerHealth(relayName string) func() mqtt.Health {
	return func() mqtt.Health {
		// viewMu, never mu: this runs inside the observer's Run loop,
		// which stopObserver joins while holding mu — the heartbeat
		// racing a bounce used to deadlock the whole daemon here.
		m.viewMu.RLock()
		info, ok := m.infos[relayName]
		m.viewMu.RUnlock()
		h := mqtt.Health{}
		if m.sen != nil {
			if jh := m.sen.Health(); !jh.Healthy || jh.Failures > 0 {
				degraded := !jh.Healthy
				failures := int(jh.Failures)
				h.JournalDegraded = &degraded
				h.JournalFailures = &failures
				h.JournalLastErr = jh.LastErr
				h.JournalLastFailAt = jh.LastFailAt
			}
		}
		if !ok {
			return h
		}
		up := int(time.Since(info.Started).Seconds())
		h.UptimeSecs = &up
		h.Repeat = repeatOff
		if info.TXMode == config.TXOnAir {
			h.Repeat = repeatOn
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
	if mq.Disabled {
		delete(m.obsCause, name)
		log.Info("observer disabled — not started")
		return
	}
	p, err := resolveMQTTParams(mq)
	if err != nil {
		m.observerDown(name, err, log)
		return
	}
	cfg, err := m.observerConfig(name, p, log)
	if err != nil {
		m.observerDown(name, err, log)
		return
	}
	auth := "anonymous"
	if p.Audience != "" {
		auth = "device-token"
	} else if p.Username != "" {
		auth = "static"
	}
	logging.Trace(log, "observer assembled",
		zap.String("relay", cfg.Relay), zap.String("iata", cfg.IATA),
		zap.String("topic_template", cfg.Topic), zap.String("auth", auth),
		zap.Bool("rx", cfg.RX), zap.String("tx", cfg.TX),
		zap.Bool("packets", cfg.Packets), zap.Bool("raw", cfg.Raw),
		zap.Bool("status", cfg.Status), zap.Duration("status_interval", cfg.StatusInterval),
		zap.Duration("neighbours_interval", cfg.NeighborsInterval), zap.Bool("retain", cfg.Retain))
	connects := make(chan struct{}, 1)
	cfg.Connects = connects
	m.viewMu.RLock()
	dialInfo := m.infos[cfg.Relay]
	m.viewMu.RUnlock()
	// "started" goes out BEFORE the dial: Paho's callbacks run
	// concurrently and a fast local broker connected before the start
	// event landed, writing history backwards. The liveness gate
	// keeps a late callback of THIS incarnation from speaking after
	// its stop — or into its successor's timeline.
	live := newLiveGate()
	m.bus.Publish(bus.ObserverState{Observer: name, At: time.Now(), State: "started"})
	broker, err := mqtt.Dial(observerDial(p, name, dialInfo, connects, live.gate(m.bus.Publish), log), log)
	if err != nil {
		m.observerDown(name, err, log)
		return
	}
	sub := m.bus.Subscribe(observerSubBuffer)
	obs := mqtt.New(cfg, broker, log)
	octx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	m.observers[name] = &managedObserver{
		cancel: cancel, done: done, obs: obs, sub: sub, broker: broker,
		url: p.URL, relay: cfg.Relay, live: live,
	}
	m.wg.Go(func() {
		defer close(done)
		defer sub.Close()
		obs.Run(octx, sub)
	})
	delete(m.obsCause, name)
	log.Info("observer started", zap.String("broker", p.URL), zap.String("relay", cfg.Relay))
}

// observerDown records why an observer is not running: the cause the
// status line shows, the transition the journal keeps — not only a
// log line that scrolled away. The caller holds mu.
func (m *manager) observerDown(name string, err error, log *zap.Logger) {
	m.obsCause[name] = err.Error()
	m.bus.Publish(bus.ObserverState{
		Observer: name, At: time.Now(), State: "down", Cause: err.Error(),
	})
	log.Error("observer not started", zap.Error(err))
}

// stopObserver takes one down and waits it out. The caller holds mu.
func (m *manager) stopObserver(name string) {
	h, ok := m.observers[name]
	if !ok {
		return
	}
	log := m.log.Named("mqtt").With(zap.String("observer", name))
	logging.Trace(log, "observer stopping", zap.String("relay", h.relay))
	if h.live != nil {
		// When Close returns, no callback of this incarnation is
		// still in flight: "stopped" lands after its last word, and
		// a successor's "started" after that.
		h.live.Close()
	}
	h.cancel()
	<-h.done
	delete(m.observers, name)
	m.bus.Publish(bus.ObserverState{Observer: name, At: time.Now(), State: "stopped"})
	log.Info("observer stopped")
}

// reconcileObservers realigns every observer with the relay topology
// after a relay was created or removed. An empty relay= means "the
// only relay", and that phrase changes meaning with the map: each
// observer whose resolution changed is stopped, bounced or started,
// so the running state is exactly what a restart would produce —
// never a connection kept alive to the captured face of a relay the
// tree no longer holds. The caller holds mu.
func (m *manager) reconcileObservers() {
	for name, mq := range m.file.MQTT {
		if mq.Disabled {
			continue
		}
		log := m.log.Named("mqtt").With(zap.String("observer", name))
		var target string
		p, err := resolveMQTTParams(mq)
		if err == nil {
			target, err = m.observerRelay(p.Relay)
		}
		h, live := m.observers[name]
		switch {
		case live && err != nil:
			logging.Trace(log, "observer topology invalidated", zap.Error(err))
			m.stopObserver(name)
			m.observerDown(name, err, log)
		case live && h.relay != target:
			logging.Trace(log, "observer relay changed",
				zap.String("from", h.relay), zap.String("to", target))
			m.bounceObserver(name)
		case !live && err == nil:
			logging.Trace(log, "observer topology became usable", zap.String("relay", target))
			m.startObserver(m.ctx, name)
		case !live && m.obsCause[name] != err.Error():
			// Still down, but the reason moved with the topology: the
			// status must say why it is down now, not why it once was.
			m.observerDown(name, err, log)
		}
	}
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

// MQTTInfos is the console's live view of the observers — the running
// ones with their counters, and the configured-but-not-running ones
// (disabled, or refused at start) so the listing and the tree still
// see every object there is.
func (m *manager) MQTTInfos() []cli.MQTTInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]cli.MQTTInfo, 0, len(m.file.MQTT))
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
	for name, mq := range m.file.MQTT {
		if _, live := m.observers[name]; live {
			continue
		}
		row := cli.MQTTInfo{Name: name, Disabled: mq.Disabled, Down: m.obsCause[name]}
		if p, err := resolveMQTTParams(mq); err == nil {
			row.URL = p.URL
			if relayName, err := m.observerRelay(p.Relay); err == nil {
				row.Relay = relayName
			}
		}
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// regionSelfOf reads the region state live at each publication — the
// table mutates over the air, and a snapshot taken at observer
// assembly would publish a policy the relay no longer runs — and in
// one snapshot, so the list and the default always describe the same
// version of the table.
func regionSelfOf(info cli.RelayInfo) func() (string, string) {
	if info.Regions == nil {
		return func() (string, string) { return "", "" }
	}
	return func() (string, string) {
		r, err := info.Regions()
		if err != nil {
			return "", ""
		}
		return strings.Join(r.Served, ","), r.Default
	}
}

// liveGate closes one incarnation's timeline: a Paho callback that
// fires after the observer stopped must not write into history —
// least of all into a successor's. The gate is a barrier, not a mere
// filter: a bare flag would silence only the callbacks that START
// late, while one already past the check could still publish after
// the stop. Publishes hold the gate while they speak and Close takes
// the same gate, so its return means the timeline is sealed.
type liveGate struct {
	mu   sync.Mutex
	live bool
}

func newLiveGate() *liveGate { return &liveGate{live: true} }

// gate wraps a bus publish behind the incarnation's liveness. The
// bus never blocks, so holding the gate through the call is cheap.
func (g *liveGate) gate(publish func(bus.Event)) func(bus.Event) {
	return func(ev bus.Event) {
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.live {
			publish(ev)
		}
	}
}

// Close seals the gate and waits out any publish already in flight.
func (g *liveGate) Close() {
	g.mu.Lock()
	g.live = false
	g.mu.Unlock()
}
