// Package room is the MeshCore room server, the first application type:
// a mesh identity clients log into to post text and receive what others
// posted, under an access list its admin governs. This cut holds an
// identity, follows a radio, hears the mesh and announces itself: its
// adverts travel the shared origination pipeline, so a shadow room
// spends the duty it would have spent and an on-air room keys the
// radio for them. Logins, posts and pushes arrive with the shared
// server kernel, in the order the design document lays out.
package room

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/application"
	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/correlation"
	"meshrunner.dev/lotor/internal/logging"
	"meshrunner.dev/lotor/internal/meshcorecfg"
	"meshrunner.dev/lotor/internal/origin"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/schema"

	mesh "meshrunner.dev/pkg/meshcore"
)

const (
	protocolName = "meshcore"
	// The type is spelled with its protocol on purpose: one word an
	// operator types, and a word that could never be mistaken for
	// another mesh's room when a second protocol arrives.
	typeName = "meshcore-room"

	// The reference room server's defaults: a flood advert every 47
	// hours, a zero-hop one every two minutes, a ring of 32 posts.
	defaultFloodAdvert = 47 * time.Hour
	defaultLocalAdvert = 2 * time.Minute
	defaultHistory     = 32
	maxHistory         = 4096
	maxNodeName        = 31
	maxPassword        = 15

	rfRetry = 5 * time.Second

	// The reference dispatcher's priorities for what a room sends of
	// its own accord: a flooded advert yields to everything else.
	prioAdvertFlood = 3
	prioAdvertLocal = 0
	// The room's outbound queue is small on purpose: it originates
	// little, and a backlog here is a radio that is not answering.
	defaultQueueDepth = 8
)

func init() {
	application.Register(typeName, application.Builder{
		Protocol: protocolName,
		Build:    build, Check: check, Asks: asks,
		Presets: meshcorecfg.Presets(), Schema: roomSchema(),
	})
}

func roomSchema() []schema.Attr {
	out := meshcorecfg.WaveformSchema()
	return append(out,
		schema.Attr{Name: "tx_power_dbm", Type: schema.Int,
			Doc: "room transmit power in dBm; the radio envelope must allow it"},
		schema.Attr{Name: "duty_cycle_pct", Type: schema.Float,
			Doc: "accounted airtime budget per sliding hour, percent"},
		schema.Attr{Name: "identity", Type: schema.String, Secret: true,
			Doc: `the room private key, hex — or "new" to mint one`},
		schema.Attr{Name: "node_name", Type: schema.String,
			Doc: "what this room calls itself on the mesh"},
		schema.Attr{Name: "node_lat", Type: schema.Float,
			Doc: "advertised latitude, degrees (with node_lon; both zero advertises no location)"},
		schema.Attr{Name: "node_lon", Type: schema.Float,
			Doc: "advertised longitude, degrees"},
		schema.Attr{Name: "admin_password", Type: schema.String, Secret: true,
			Doc: "the word that logs a client in as admin; empty closes the admin door"},
		schema.Attr{Name: "guest_password", Type: schema.String, Secret: true,
			Doc: "the room password — a client offering it may read and post; empty closes that door"},
		schema.Attr{Name: "allow_read_only", Type: schema.Bool,
			Doc: "admit an unknown password as a guest who may read but never post"},
		schema.Attr{Name: "advert_flood_interval", Type: schema.Duration,
			Doc: "how often the room floods its advert (0 = never; the reference's 47h)"},
		schema.Attr{Name: "advert_local_interval", Type: schema.Duration,
			Doc: "how often the room adverts zero-hop (0 = never; the reference's 2m)"},
		schema.Attr{Name: "history", Type: schema.Int,
			Doc: "posts kept, oldest overwritten first (0 takes the reference's 32)"},
		schema.Attr{Name: "persist_history", Type: schema.Bool,
			Doc: "keep the posts across a restart; false is the reference's RAM ring"},
	)
}

type params struct {
	radio.Waveform `yaml:",inline"`

	TXPowerDBm     int8          `yaml:"tx_power_dbm"`
	DutyCyclePct   float64       `yaml:"duty_cycle_pct"`
	Identity       string        `yaml:"identity"`
	NodeName       string        `yaml:"node_name"`
	NodeLat        float64       `yaml:"node_lat"`
	NodeLon        float64       `yaml:"node_lon"`
	AdminPassword  string        `yaml:"admin_password"`
	GuestPassword  string        `yaml:"guest_password"`
	AllowReadOnly  bool          `yaml:"allow_read_only"`
	FloodAdvert    time.Duration `yaml:"advert_flood_interval"`
	LocalAdvert    time.Duration `yaml:"advert_local_interval"`
	History        int           `yaml:"history"`
	PersistHistory bool          `yaml:"persist_history"`
}

// resolve decodes and judges the contributed configuration. Absent
// intervals take the reference's; a configured zero means never.
func resolve(cfg map[string]any) (params, *mesh.LocalIdentity, error) {
	p, err := config.Decode[params](cfg)
	if err != nil {
		return p, nil, fmt.Errorf("meshcore room params: %w", err)
	}
	if _, set := cfg["advert_flood_interval"]; !set {
		p.FloodAdvert = defaultFloodAdvert
	}
	if _, set := cfg["advert_local_interval"]; !set {
		p.LocalAdvert = defaultLocalAdvert
	}
	if _, set := cfg["persist_history"]; !set {
		p.PersistHistory = true
	}
	if p.History == 0 {
		p.History = defaultHistory
	}
	if err := validateAir(p); err != nil {
		return p, nil, err
	}
	if err := validateRoom(p); err != nil {
		return p, nil, err
	}
	if p.Identity == "" {
		return p, nil, errors.New("meshcore room params: identity is required")
	}
	id, err := meshcorecfg.Identity(p.Identity)
	if err != nil {
		return p, nil, err
	}
	return p, id, nil
}

// validateAir judges the waveform and the airtime budget — what the
// radio will be asked for.
func validateAir(p params) error {
	if p.FrequencyHz == 0 {
		return errors.New("meshcore room params: frequency_hz is required")
	}
	if p.SpreadingFactor < 5 || p.SpreadingFactor > 12 {
		return fmt.Errorf("meshcore room params: spreading_factor %d — want 5..12", p.SpreadingFactor)
	}
	if p.BandwidthHz < 7_000 || p.BandwidthHz > 500_000 {
		return fmt.Errorf("meshcore room params: bandwidth_hz %d — want 7000..500000", p.BandwidthHz)
	}
	if p.CodingRate < 5 || p.CodingRate > 8 {
		return fmt.Errorf("meshcore room params: coding_rate %d — want 5..8", p.CodingRate)
	}
	if p.DutyCyclePct < 0 || p.DutyCyclePct > 100 {
		return fmt.Errorf("meshcore room params: duty_cycle_pct %g — want 0..100", p.DutyCyclePct)
	}
	return nil
}

// validateRoom judges what the room says about itself: its name, its
// doors, its clocks and its memory.
func validateRoom(p params) error {
	if len(p.NodeName) > maxNodeName {
		return fmt.Errorf("meshcore room params: node_name exceeds %d bytes", maxNodeName)
	}
	// The client clamps what it sends to fifteen characters; a longer
	// word here would be one nobody could ever offer.
	if len(p.AdminPassword) > maxPassword || len(p.GuestPassword) > maxPassword {
		return fmt.Errorf("meshcore room params: passwords are at most %d characters", maxPassword)
	}
	if p.AdminPassword != "" && p.AdminPassword == p.GuestPassword {
		return errors.New("meshcore room params: the admin and guest passwords must differ")
	}
	if p.FloodAdvert < 0 || p.LocalAdvert < 0 {
		return errors.New("meshcore room params: advert intervals cannot be negative")
	}
	if p.History < 0 || p.History > maxHistory {
		return fmt.Errorf("meshcore room params: history %d — want 1..%d", p.History, maxHistory)
	}
	if (p.NodeLat < -90 || p.NodeLat > 90) || (p.NodeLon < -180 || p.NodeLon > 180) {
		return errors.New("meshcore room params: node_lat/node_lon out of range")
	}
	return nil
}

func check(cfg map[string]any) error {
	_, _, err := resolve(cfg)
	return err
}

func asks(cfg map[string]any) (application.RadioDemand, error) {
	p, _, err := resolve(cfg)
	if err != nil {
		return application.RadioDemand{}, err
	}
	return application.RadioDemand{
		Waveform: p.Waveform, PowerDBm: p.TXPowerDBm, DutyCyclePct: p.DutyCyclePct,
	}, nil
}

// service is one room. Everything below mu is read by Info from any
// goroutine; the RF loop and the advert clock write it.
type service struct {
	name      string
	radioName string
	p         params
	id        *mesh.LocalIdentity
	log       *zap.Logger
	tx        application.TXPolicy
	pipeline  *origin.Pipeline

	mu         sync.Mutex
	state      application.State
	cause      string
	rf         application.RFState
	rfCause    string
	binding    *radio.Binding
	duty       *radio.AirtimeLedger
	rfDevice   radio.Device
	rfWake     chan struct{}
	heard      uint64
	corrupt    uint64
	advertsDue uint64
	sent       uint64
	dropped    uint64
}

func build(spec application.Spec) (application.Service, error) {
	p, id, err := resolve(spec.Config)
	if err != nil {
		return nil, err
	}
	if p.NodeName == "" {
		p.NodeName = "room-" + hex.EncodeToString(id.PubKey[:3])
	}
	log := spec.Log
	if log == nil {
		log = zap.NewNop()
	}
	queueDepth := spec.TX.QueueDepth
	if queueDepth <= 0 {
		queueDepth = defaultQueueDepth
	}
	s := &service{
		name: spec.Name, radioName: spec.Radio, p: p, id: id, log: log, tx: spec.TX,
		state: application.StateStarting, rfWake: make(chan struct{}, 1),
		pipeline: origin.New(origin.Config{
			SourceKind: bus.SourceApplication, Source: spec.Name, Bus: spec.Bus, Log: log,
		}, queueDepth),
	}
	if spec.Radio == "" {
		s.rf = application.RFDetached
	} else {
		s.rf = application.RFDown
	}
	return s, nil
}

// Run serves the room until ctx ends: the advert clocks, the outbound
// pipeline and the radio session, each on its own goroutine.
func (s *service) Run(ctx context.Context) error {
	s.setLifecycle(application.StateRunning, nil)
	s.log.Info("room up", zap.String("node", s.p.NodeName),
		zap.String("pubkey", hex.EncodeToString(s.id.PubKey[:6])),
		zap.String("tx", s.gate()),
		zap.Int("history", s.p.History), zap.Bool("persist_history", s.p.PersistHistory))
	var wg sync.WaitGroup
	wg.Go(func() { s.runAdverts(ctx) })
	wg.Go(func() { s.runTX(ctx) })
	s.runRF(ctx)
	wg.Wait()
	s.setLifecycle(application.StateStopped, nil)
	return nil
}

// gate is the origination mode, dry when nothing said otherwise.
func (s *service) gate() string {
	if s.tx.Mode == "" {
		return config.TXDry
	}
	return s.tx.Mode
}

// runAdverts keeps the reference's two clocks and, in dry mode, says
// what it would have sent. The flood advert re-arms both so the two
// never coincide, as the reference's loop does.
func (s *service) runAdverts(ctx context.Context) {
	if s.p.FloodAdvert == 0 && s.p.LocalAdvert == 0 {
		<-ctx.Done()
		return
	}
	flood, local := timerOrNever(s.p.FloodAdvert), timerOrNever(s.p.LocalAdvert)
	for {
		select {
		case <-ctx.Done():
			return
		case <-flood:
			s.advertDue("advert-flood")
			flood = timerOrNever(s.p.FloodAdvert)
			local = timerOrNever(s.p.LocalAdvert)
		case <-local:
			s.advertDue("advert-local")
			local = timerOrNever(s.p.LocalAdvert)
		}
	}
}

func timerOrNever(d time.Duration) <-chan time.Time {
	if d <= 0 {
		return nil
	}
	return time.After(d)
}

// advertDue composes the advert a clock asked for and hands it to the
// pipeline — flooded at the reference's low priority, or zero-hop for
// the neighbourhood. A dry gate composes and counts and sends nothing:
// the room's account of what it would have said.
func (s *service) advertDue(kind string) {
	pkt, err := s.selfAdvert(time.Now())
	if err != nil {
		s.log.Warn("advert not composed", zap.String("kind", kind), zap.Error(err))
		return
	}
	priority := uint8(prioAdvertFlood)
	if kind == "advert-local" {
		pkt.Header = mesh.MakeHeader(mesh.RouteDirect, mesh.PayloadTypeAdvert, mesh.PayloadVer1)
		pkt.SetPathHashCount(0)
		priority = prioAdvertLocal
	}
	s.mu.Lock()
	s.advertsDue++
	s.mu.Unlock()
	if s.gate() == config.TXDry {
		s.log.Debug("advert due, gate is dry", zap.String("kind", kind),
			zap.Int("bytes", len(pkt.Payload)))
		return
	}
	raw, err := pkt.MarshalBinary()
	if err != nil {
		s.log.Warn("advert not marshalled", zap.String("kind", kind), zap.Error(err))
		return
	}
	item := origin.Emission{Frame: raw, Subject: pkt, Correlation: correlation.New(), Kind: kind, Priority: priority}
	if !s.pipeline.Queue.Offer(item) {
		s.pipeline.Drop(item, "queue-full")
	}
}

// runTX drains the pipeline's queue with the radio the room holds at
// that instant, and keeps the tally.
func (s *service) runTX(ctx context.Context) {
	for ctx.Err() == nil {
		item, ok := s.pipeline.Queue.TakeUntil(ctx, time.Now().Add(time.Second))
		if !ok {
			continue
		}
		s.mu.Lock()
		device, ledger, power := s.rfDevice, s.duty, s.p.TXPowerDBm
		s.mu.Unlock()
		out := s.pipeline.Emit(ctx, item, device, ledger, origin.Policy{
			Mode: s.gate(), LBTThresholdDB: s.tx.LBTThresholdDB, LBTExhausted: s.tx.LBTExhausted, CAD: s.tx.CAD,
		}, power)
		s.mu.Lock()
		switch {
		case out.Sent:
			s.sent++
		case out.Dropped != "":
			s.dropped++
		}
		s.mu.Unlock()
	}
}

func (s *service) selfAdvert(at time.Time) (*mesh.Packet, error) {
	data := &mesh.AdvertData{Type: mesh.AdvTypeRoom, Name: s.p.NodeName}
	if s.p.NodeLat != 0 || s.p.NodeLon != 0 {
		data.HasLoc = true
		data.LatE6 = int32(s.p.NodeLat * 1e6)
		data.LonE6 = int32(s.p.NodeLon * 1e6)
	}
	return mesh.BuildAdvert(s.id, at, data)
}

// runRF follows the binding the manager supplies: open a logical
// session, hear the mesh, start over when it ends. The station's shape.
func (s *service) runRF(ctx context.Context) {
	for ctx.Err() == nil {
		s.mu.Lock()
		binding := s.binding
		s.mu.Unlock()
		if binding == nil {
			s.waitRF(ctx)
			continue
		}
		device, err := binding.OpenContext(ctx)
		if err != nil {
			if ctx.Err() == nil && !errors.Is(err, radio.ErrBindingClosed) {
				logging.Trace(s.log, "room radio session unavailable", zap.Error(err))
			}
			s.setRF(application.RFDown, err)
			s.waitRF(ctx)
			continue
		}
		err = device.Configure(s.p.Waveform)
		if err == nil {
			err = device.StartReceive()
		}
		if err == nil {
			s.setRF(application.RFActive, nil)
			s.mu.Lock()
			s.rfDevice = device
			s.mu.Unlock()
			err = s.receiveRF(ctx, device)
			s.mu.Lock()
			s.rfDevice = nil
			s.mu.Unlock()
		}
		_ = device.Close()
		if ctx.Err() == nil && err != nil && !errors.Is(err, radio.ErrControllerDown) {
			logging.Trace(s.log, "room radio session ended", zap.Error(err))
		}
		if ctx.Err() == nil {
			s.setRF(application.RFDown, err)
		}
	}
}

func (s *service) waitRF(ctx context.Context) {
	select {
	case <-ctx.Done():
	case <-s.rfWake:
	case <-time.After(rfRetry):
	}
}

func (s *service) receiveRF(ctx context.Context, device radio.Device) error {
	for {
		frame, err := device.Receive(ctx)
		if errors.Is(err, radio.ErrCorrupt) {
			s.mu.Lock()
			s.corrupt++
			s.mu.Unlock()
			continue
		}
		if err != nil {
			return err
		}
		s.mu.Lock()
		s.heard++
		s.mu.Unlock()
		logging.Trace(s.log, "room heard a frame", zap.String("corr", frame.Correlation.Short()))
	}
}

func (s *service) setRF(state application.RFState, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.binding == nil {
		return
	}
	s.rf = state
	s.rfCause = ""
	if err != nil {
		s.rfCause = err.Error()
	}
}

func (s *service) setLifecycle(state application.State, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = state
	s.cause = ""
	if err != nil {
		s.cause = err.Error()
	}
}

// AttachRadio supplies or withdraws the RF capability without touching
// the room's state.
func (s *service) AttachRadio(name string, binding *radio.Binding, duty *radio.AirtimeLedger, cause string) {
	s.mu.Lock()
	s.radioName, s.binding, s.duty, s.rfCause = name, binding, duty, cause
	if name == "" {
		s.rf, s.rfCause = application.RFDetached, ""
	} else {
		s.rf = application.RFDown
	}
	select {
	case s.rfWake <- struct{}{}:
	default:
	}
	s.mu.Unlock()
	s.log.Debug("room radio attachment changed", zap.String("radio", name),
		zap.Bool("attached", binding != nil), zap.String("cause", cause))
}

// RadioDemand is the configured waveform: nothing changes it over the
// air in this cut.
func (s *service) RadioDemand() application.RadioDemand {
	return application.RadioDemand{
		Waveform: s.p.Waveform, PowerDBm: s.p.TXPowerDBm, DutyCyclePct: s.p.DutyCyclePct,
	}
}

func (s *service) Info() application.Info {
	s.mu.Lock()
	defer s.mu.Unlock()
	return application.Info{
		Name: s.name, Protocol: protocolName, Type: typeName, Radio: s.radioName,
		State: s.state, Cause: s.cause, RF: s.rf, RFCause: s.rfCause,
		Waveform: s.p.Waveform, PublicKey: hex.EncodeToString(s.id.PubKey[:]),
		Summary: map[string]string{
			"node":        s.p.NodeName,
			"heard":       strconv.FormatUint(s.heard, 10),
			"corrupt":     strconv.FormatUint(s.corrupt, 10),
			"adverts due": strconv.FormatUint(s.advertsDue, 10),
			"sent":        strconv.FormatUint(s.sent, 10),
			"dropped":     strconv.FormatUint(s.dropped, 10),
			"tx":          s.gate(),
			"history":     "0 / " + strconv.Itoa(s.p.History),
		},
	}
}
