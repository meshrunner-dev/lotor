package meshcore

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net"
	"sync"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/logging"
	"meshrunner.dev/lotor/internal/meshcorecfg"
	"meshrunner.dev/lotor/internal/product"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/station"

	mesh "meshrunner.dev/pkg/meshcore"
	"meshrunner.dev/pkg/meshcore/companion"
)

const (
	protocolName     = "meshcore"
	protocolVersion  = 13
	defaultContacts  = 100
	defaultChannels  = 8
	defaultMailbox   = 16
	defaultAirFactor = 1_000
	maxStationName   = 31
	maxChannelName   = 31
	maxChannelSlots  = 255
	maxContactSlots  = 510
	maxMailboxSlots  = 4096
	maxConnectionPIN = math.MaxUint32
	maxSignData      = 8 * 1024
	maxConnections   = 16
)

func init() {
	station.Register(protocolName, station.Builder{
		Build: build, Check: check, Asks: asks,
		Presets: meshcorecfg.Presets(), Schema: stationSchema(),
	})
}

type params struct {
	radio.Waveform `yaml:",inline"`

	TXPowerDBm     int8    `yaml:"tx_power_dbm"`
	DutyCyclePct   float64 `yaml:"duty_cycle_pct"`
	Identity       string  `yaml:"identity"`
	NodeName       string  `yaml:"node_name"`
	PIN            uint64  `yaml:"pin"`
	NodeLat        float64 `yaml:"node_lat"`
	NodeLon        float64 `yaml:"node_lon"`
	MultiACKs      int     `yaml:"multi_acks"`
	AdvertLoc      int     `yaml:"advert_loc_policy"`
	TelemetryMode  int     `yaml:"telemetry_mode"`
	ManualContacts bool    `yaml:"manual_add_contacts"`
	PathHashMode   int     `yaml:"path_hash_mode"`
	RXDelayMilli   uint32  `yaml:"rx_delay_milli"`
	AirFactorMilli uint32  `yaml:"airtime_factor_milli"`
	MaxContacts    int     `yaml:"max_contacts"`
	MaxChannels    int     `yaml:"max_channels"`
	MailboxCap     int     `yaml:"mailbox_capacity"`
}

func resolve(cfg map[string]any) (params, *mesh.LocalIdentity, error) {
	p, err := config.Decode[params](cfg)
	if err != nil {
		return p, nil, fmt.Errorf("meshcore station params: %w", err)
	}
	if err := validateAir(p); err != nil {
		return p, nil, err
	}
	if _, configured := cfg["airtime_factor_milli"]; !configured {
		p.AirFactorMilli = defaultAirFactor
	}
	if err := validatePreferences(p); err != nil {
		return p, nil, err
	}
	if err := normalizeCapacities(&p); err != nil {
		return p, nil, err
	}
	if p.Identity == "" {
		return p, nil, errors.New("meshcore station params: identity is required")
	}
	id, err := meshcorecfg.Identity(p.Identity)
	if err != nil {
		return p, nil, err
	}
	return p, id, nil
}

func validateAir(p params) error {
	if p.FrequencyHz == 0 {
		return errors.New("meshcore station params: frequency_hz is required")
	}
	if p.FrequencyHz%1_000 != 0 {
		return fmt.Errorf("meshcore station params: frequency_hz %d cannot be represented by "+
			"the companion protocol's kHz field", p.FrequencyHz)
	}
	if p.SpreadingFactor < 5 || p.SpreadingFactor > 12 {
		return fmt.Errorf("meshcore station params: spreading_factor %d — want 5..12", p.SpreadingFactor)
	}
	if p.BandwidthHz < 7_000 || p.BandwidthHz > 500_000 {
		return fmt.Errorf("meshcore station params: bandwidth_hz %d — want 7000..500000", p.BandwidthHz)
	}
	if p.CodingRate < 5 || p.CodingRate > 8 {
		return fmt.Errorf("meshcore station params: coding_rate %d — want 5..8", p.CodingRate)
	}
	if p.DutyCyclePct <= 0 || math.IsNaN(p.DutyCyclePct) || math.IsInf(p.DutyCyclePct, 0) || p.DutyCyclePct > 100 {
		return fmt.Errorf("meshcore station params: duty_cycle_pct %g — want a finite value in (0,100]",
			p.DutyCyclePct)
	}
	return nil
}

func validatePreferences(p params) error {
	if len(p.NodeName) > maxStationName {
		return fmt.Errorf("meshcore station params: node_name is %d bytes — at most %d fit",
			len(p.NodeName), maxStationName)
	}
	if p.PIN > maxConnectionPIN {
		return fmt.Errorf("meshcore station params: pin %d does not fit uint32", p.PIN)
	}
	if p.NodeLat < -90 || p.NodeLat > 90 || p.NodeLon < -180 || p.NodeLon > 180 {
		return fmt.Errorf("meshcore station params: invalid location %g,%g", p.NodeLat, p.NodeLon)
	}
	if p.MultiACKs < 0 || p.MultiACKs > math.MaxUint8 {
		return fmt.Errorf("meshcore station params: multi_acks %d — want 0..255", p.MultiACKs)
	}
	if p.AdvertLoc < 0 || p.AdvertLoc > math.MaxUint8 || p.TelemetryMode < 0 || p.TelemetryMode > 0x3f {
		return errors.New("meshcore station params: advert_loc_policy must fit one byte and telemetry_mode six bits")
	}
	if p.PathHashMode < 0 || p.PathHashMode > 2 {
		return fmt.Errorf("meshcore station params: path_hash_mode %d — want 0..2", p.PathHashMode)
	}
	return nil
}

func normalizeCapacities(p *params) error {
	if p.MaxContacts == 0 {
		p.MaxContacts = defaultContacts
	}
	if p.MaxChannels == 0 {
		p.MaxChannels = defaultChannels
	}
	if p.MailboxCap == 0 {
		p.MailboxCap = defaultMailbox
	}
	if p.MaxContacts < 2 || p.MaxContacts > maxContactSlots || p.MaxContacts%2 != 0 {
		return fmt.Errorf("meshcore station params: max_contacts %d — want an even value in 2..%d",
			p.MaxContacts, maxContactSlots)
	}
	if p.MaxChannels < 1 || p.MaxChannels > maxChannelSlots {
		return fmt.Errorf("meshcore station params: max_channels %d — want 1..%d",
			p.MaxChannels, maxChannelSlots)
	}
	if p.MailboxCap < 1 || p.MailboxCap > maxMailboxSlots {
		return fmt.Errorf("meshcore station params: mailbox_capacity %d — want 1..%d",
			p.MailboxCap, maxMailboxSlots)
	}
	return nil
}

func check(cfg map[string]any) error {
	_, _, err := resolve(cfg)
	return err
}

func asks(cfg map[string]any) (station.RadioDemand, error) {
	p, _, err := resolve(cfg)
	return station.RadioDemand{
		Waveform: p.Waveform, PowerDBm: p.TXPowerDBm, DutyCyclePct: p.DutyCyclePct,
	}, err
}

func build(spec station.Spec) (station.Service, error) {
	p, id, err := resolve(spec.Config)
	if err != nil {
		return nil, err
	}
	log := spec.Log
	if log == nil {
		log = zap.NewNop()
	}
	s := &service{
		name: spec.Name, listen: spec.Listen, radioName: spec.Radio,
		p: p, id: id, log: log, buildVersion: spec.Build.Version,
		channels: make(map[uint8]channel), contacts: make(map[[mesh.PubKeySize]byte]contactEntry),
		state: station.StateStarting, stateStore: spec.State, txPolicy: spec.TX, bus: spec.Bus,
		rfWake: make(chan struct{}, 1), startedAt: time.Now(),
		connections: make(map[[mesh.PubKeySize]byte]remoteConnection),
	}
	queueDepth := spec.TX.QueueDepth
	if queueDepth <= 0 {
		queueDepth = 32
	}
	s.outbound = newEmissionQueue(queueDepth)
	// The declarative station configuration is the virtual equivalent of the
	// firmware image defaults restored after formatting its filesystem.
	s.factoryState = s.snapshotLocked()
	loadCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.loadState(loadCtx); err != nil {
		return nil, err
	}
	if spec.Build.SourceTime.IsZero() {
		s.buildDate = "local build"
	} else {
		s.buildDate = spec.Build.SourceTime.UTC().Format("2 Jan 2006")
	}
	if s.buildVersion == "" {
		s.buildVersion = "0.0.0-dev"
	}
	if spec.Radio == "" {
		s.rf = station.RFDetached
	} else {
		s.rf = station.RFDown
	}
	return s, nil
}

type channel struct {
	name   string
	secret [16]byte
}

type contactEntry struct {
	info      companion.Contact
	advert    []byte
	order     uint64
	syncSince uint32
	ephemeral bool
}

type ackExpectation struct {
	crc  uint32
	at   time.Time
	used bool
}

// stationStats are the counters attributable to this virtual station. They do
// not expose another attachment's traffic when the physical radio is shared.
type stationStats struct {
	received, sent                uint32
	sentFlood, sentDirect         uint32
	receivedFlood, receivedDirect uint32
	receiveErrors                 uint32
	txAir, rxAir                  time.Duration
	lastRSSIDBm, lastSNRx4        int8
}

type service struct {
	name, listen, radioName string
	p                       params
	id                      *mesh.LocalIdentity
	log                     *zap.Logger
	buildDate, buildVersion string

	mu           sync.Mutex
	writeMu      sync.Mutex
	state        station.State
	cause        string
	rf           station.RFState
	listener     net.Listener
	client       net.Conn
	generation   uint64
	disconnect   uint64
	remote       string
	clockDelta   time.Duration
	lastUnique   uint32
	appVersion   uint8
	autoFlags    uint8
	autoHops     uint8
	channels     map[uint8]channel
	contacts     map[[mesh.PubKeySize]byte]contactEntry
	nextContact  uint64
	defaultKey   [16]byte
	defaultScope string
	sendScope    [16]byte
	sendUnscoped bool
	mailbox      [][]byte
	seen         packetRing
	expectedACKs [8]ackExpectation
	nextACK      int
	binding      *radio.Binding
	rfCause      string
	stateStore   station.StateStore
	txPolicy     station.TXPolicy
	bus          *bus.Bus
	duty         *radio.AirtimeLedger
	rfWake       chan struct{}
	rfDevice     radio.Device
	outbound     *emissionQueue
	startedAt    time.Time
	stats        stationStats
	signData     []byte
	pending      pendingRequest
	connections  map[[mesh.PubKeySize]byte]remoteConnection
	advertPaths  [16]advertPath
	factoryState persistedState
}

func (s *service) Run(ctx context.Context) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.listen)
	if err != nil {
		s.setLifecycle(station.StateError, err)
		return err
	}
	s.mu.Lock()
	s.listener = ln
	s.listen = ln.Addr().String()
	s.state, s.cause = station.StateRunning, ""
	s.mu.Unlock()
	s.log.Info("station listening", zap.String("listen", ln.Addr().String()))
	rfCtx, cancelRF := context.WithCancel(ctx)
	rfDone := make(chan struct{})
	go func() {
		defer close(rfDone)
		s.runRF(rfCtx)
	}()
	stop := context.AfterFunc(ctx, func() { s.closeIO() })
	defer func() {
		stop()
		cancelRF()
		s.closeIO()
		if ctx.Err() != nil {
			s.setLifecycle(station.StateStopped, nil)
		}
		<-rfDone
	}()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			s.setLifecycle(station.StateError, err)
			return err
		}
		generation := s.replaceClient(conn)
		go s.serveClient(ctx, conn, generation)
	}
}

func (s *service) closeIO() {
	s.mu.Lock()
	ln, client := s.listener, s.client
	s.listener, s.client = nil, nil
	s.remote = ""
	s.mu.Unlock()
	if client != nil {
		_ = client.Close()
	}
	if ln != nil {
		_ = ln.Close()
	}
}

func (s *service) replaceClient(conn net.Conn) uint64 {
	s.mu.Lock()
	old := s.client
	s.generation++
	generation := s.generation
	s.client = conn
	s.disconnect = 0
	s.remote = conn.RemoteAddr().String()
	s.mu.Unlock()
	if old != nil {
		_ = old.Close()
	}
	s.log.Debug("companion client connected", zap.String("remote", conn.RemoteAddr().String()),
		zap.Bool("replaced", old != nil))
	return generation
}

func (s *service) serveClient(ctx context.Context, conn net.Conn, generation uint64) {
	defer func() {
		_ = conn.Close()
		s.mu.Lock()
		current := s.generation == generation && s.client == conn
		if current {
			s.client, s.remote = nil, ""
		}
		s.mu.Unlock()
		if current {
			s.log.Debug("companion client disconnected")
		}
	}()
	for ctx.Err() == nil {
		cmd, responses, alive := s.readCommand(conn)
		if !alive {
			return
		}
		if !s.currentClient(conn, generation) {
			return
		}
		if cmd != nil {
			responses = s.handle(ctx, cmd)
		}
		if !s.writeResponses(conn, generation, responses) {
			return
		}
		if s.consumeDisconnect(generation) {
			return
		}
	}
}

func (s *service) consumeDisconnect(generation uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.disconnect != generation {
		return false
	}
	s.disconnect = 0
	return true
}

func (s *service) readCommand(conn net.Conn) (companion.Command, []companion.Response, bool) {
	for {
		frame, err := companion.ReadFrame(conn, companion.ToDevice)
		if err != nil {
			if errors.Is(err, companion.ErrDirection) || errors.Is(err, companion.ErrPayloadTooLarge) ||
				errors.Is(err, companion.ErrEmptyPayload) {
				logging.Trace(s.log, "companion frame refused", zap.Error(err))
				continue
			}
			return nil, nil, false
		}
		cmd, err := companion.DecodeCommand(frame.Payload)
		if err == nil {
			return cmd, nil, true
		}
		code := companion.ErrorIllegalArgument
		if errors.Is(err, companion.ErrUnsupportedVariant) {
			code = companion.ErrorUnsupportedCommand
		}
		return nil, []companion.Response{companion.ErrorResponse{Code: code}}, true
	}
}

func (s *service) writeResponses(conn net.Conn, generation uint64, responses []companion.Response) bool {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	for _, response := range responses {
		if !s.currentClient(conn, generation) {
			return false
		}
		payload, err := companion.MarshalResponse(response)
		if err != nil {
			s.log.Error("companion response encode", zap.Error(err))
			return false
		}
		if err := companion.WriteFrame(conn, companion.ToApplication, payload); err != nil {
			return false
		}
	}
	return true
}

func (s *service) currentClient(conn net.Conn, generation uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.client == conn && s.generation == generation
}

func (s *service) handle(ctx context.Context, cmd companion.Command) []companion.Response {
	s.mu.Lock()
	if responses, handled := s.handleQuery(cmd); handled {
		s.mu.Unlock()
		return responses
	}
	if responses, handled := s.handleTransmission(cmd); handled {
		s.mu.Unlock()
		return responses
	}
	before := s.snapshotLocked()
	responses := s.handleMutation(cmd)
	if err := s.persistLocked(ctx, before); err != nil {
		s.log.Error("companion state persistence failed", zap.Error(err))
		s.mu.Unlock()
		return errorResponses(companion.ErrorFileIO)
	}
	dropped := []emission(nil)
	if lifecycleSucceeded(cmd, responses) {
		dropped = s.resetRuntimeLocked()
		s.disconnect = s.generation
	}
	s.mu.Unlock()
	for _, item := range dropped {
		s.txDrop(item, "station-restart")
	}
	return responses
}

func lifecycleSucceeded(cmd companion.Command, responses []companion.Response) bool {
	switch cmd.(type) {
	case companion.Reboot:
		return len(responses) == 0
	case companion.FactoryReset:
		return len(responses) == 1 && responses[0] == companion.StatusResponse(companion.ResponseOK)
	default:
		return false
	}
}

func (s *service) handleQuery(cmd companion.Command) ([]companion.Response, bool) {
	if responses, handled := s.handleRuntimeQuery(cmd); handled {
		return responses, true
	}
	switch c := cmd.(type) {
	case companion.DeviceQuery:
		s.appVersion = c.TargetVersion
		return []companion.Response{companion.DeviceInfo{
			ProtocolVersion: protocolVersion, MaxContacts: uint16(s.p.MaxContacts),
			MaxChannels: uint8(s.p.MaxChannels), PIN: uint32(s.p.PIN), BuildDate: s.buildDate,
			Model: product.Name + " Virtual Station", FirmwareVersion: s.buildVersion,
			Repeat: false, PathHashMode: uint8(s.p.PathHashMode),
		}}, true
	case companion.AppStart:
		return []companion.Response{s.selfInfo()}, true
	case companion.GetContacts:
		return s.getContacts(c), true
	case companion.ContactKey:
		if c.Kind == companion.CommandGetContactByKey {
			return s.getContact(c.PublicKey), true
		}
	case companion.ExportContact:
		return s.exportContact(c), true
	case companion.SimpleCommand:
		if responses, handled := s.handleIdentityQuery(c.Kind); handled {
			return responses, true
		}
		if c.Kind == companion.CommandSyncNextMessage {
			return nil, false
		}
		if c.Kind == companion.CommandGetCustomVars {
			return []companion.Response{companion.CustomVars{}}, true
		}
		return s.handleSimple(c.Kind), true
	case companion.UnknownCommand:
		return errorResponses(companion.ErrorUnsupportedCommand), true
	default:
		return nil, false
	}
	return nil, false
}

func (s *service) handleRuntimeQuery(cmd companion.Command) ([]companion.Response, bool) {
	switch c := cmd.(type) {
	case companion.GetStats:
		return s.getStats(c.Type), true
	case companion.GetAdvertPath:
		return s.getAdvertPath(c.PublicKey), true
	case companion.SignData:
		return s.appendSignData(c.Data), true
	case companion.ContactRequest:
		switch c.Kind {
		case companion.CommandHasConnection:
			s.pruneConnectionsLocked(time.Now())
			if _, exists := s.connections[c.PublicKey]; exists {
				return okResponses(), true
			}
			return errorResponses(companion.ErrorNotFound), true
		case companion.CommandLogout:
			delete(s.connections, c.PublicKey)
			return okResponses(), true
		default:
			return nil, false
		}
	default:
		return nil, false
	}
}

func (s *service) handleMutation(cmd companion.Command) []companion.Response {
	if responses, handled := s.handleContactMutation(cmd); handled {
		return responses
	}
	if responses, handled := s.handleConfigurationMutation(cmd); handled {
		return responses
	}
	if responses, handled := s.handlePreferenceMutation(cmd); handled {
		return responses
	}
	switch c := cmd.(type) {
	case companion.SetDeviceTime:
		return s.setDeviceTime(c.UnixSeconds)
	case companion.SimpleCommand:
		return s.handleSimple(c.Kind)
	case companion.SetRadioParams:
		return s.setRadioParams(c)
	case companion.SetRadioTXPower:
		return s.setRadioPower(c.PowerDBm)
	case companion.Reboot:
		// The reference immediately reboots and writes no response.
		return nil
	case companion.FactoryReset:
		return s.restoreFactoryLocked()
	default:
		return errorResponses(companion.ErrorUnsupportedCommand)
	}
}

func (s *service) restoreFactoryLocked() []companion.Response {
	waveform := s.factoryState.Waveform.radio()
	if s.binding != nil && waveform != s.p.Waveform {
		if err := s.binding.SetWaveform(waveform); err != nil {
			return errorResponses(companion.ErrorIllegalArgument)
		}
	}
	s.restoreLocked(s.factoryState)
	return okResponses()
}

func (s *service) resetRuntimeLocked() []emission {
	dropped := s.outbound.drain()
	s.lastUnique = 0
	s.appVersion = 0
	s.sendScope = [16]byte{}
	s.sendUnscoped = false
	s.seen = packetRing{}
	s.expectedACKs = [8]ackExpectation{}
	s.nextACK = 0
	s.startedAt = time.Now()
	s.stats = stationStats{}
	s.signData = nil
	s.pending = pendingRequest{}
	s.connections = make(map[[mesh.PubKeySize]byte]remoteConnection)
	s.advertPaths = [16]advertPath{}
	for key, entry := range s.contacts {
		if entry.ephemeral {
			delete(s.contacts, key)
		}
	}
	return dropped
}

func (s *service) handlePreferenceMutation(cmd companion.Command) ([]companion.Response, bool) {
	switch c := cmd.(type) {
	case companion.SetAdvertName:
		if len(c.Name) > maxStationName {
			c.Name = c.Name[:maxStationName]
		}
		s.p.NodeName = c.Name
		return okResponses(), true
	case companion.SetAdvertLocation:
		if c.LatitudeE6 < -90_000_000 || c.LatitudeE6 > 90_000_000 ||
			c.LongitudeE6 < -180_000_000 || c.LongitudeE6 > 180_000_000 {
			return errorResponses(companion.ErrorIllegalArgument), true
		}
		s.p.NodeLat = float64(c.LatitudeE6) / 1e6
		s.p.NodeLon = float64(c.LongitudeE6) / 1e6
		return okResponses(), true
	case companion.SetTuningParams:
		s.p.RXDelayMilli, s.p.AirFactorMilli = c.RXDelayMilli, c.AirtimeFactorMilli
		return okResponses(), true
	case companion.SetDevicePIN:
		if c.PIN != 0 && (c.PIN < 100_000 || c.PIN > 999_999) {
			return errorResponses(companion.ErrorIllegalArgument), true
		}
		s.p.PIN = uint64(c.PIN)
		return okResponses(), true
	case companion.ImportPrivateKey:
		identity, err := mesh.LocalIdentityFromKeys(c.PrivateKey[:], nil)
		if err != nil || !identity.FirmwareImportable() {
			return errorResponses(companion.ErrorIllegalArgument), true
		}
		s.id = identity
		clear(s.expectedACKs[:])
		s.pending = pendingRequest{}
		clear(s.connections)
		return okResponses(), true
	case companion.SetCustomVar:
		// Virtual stations currently expose no target-specific sensor
		// settings, exactly the empty SensorManager reference behaviour.
		return errorResponses(companion.ErrorIllegalArgument), true
	default:
		return nil, false
	}
}

func (s *service) handleIdentityQuery(kind companion.CommandCode) ([]companion.Response, bool) {
	switch kind {
	case companion.CommandExportPrivateKey:
		key := companion.PrivateKey{}
		copy(key.Key[:], s.id.PrvKey())
		return []companion.Response{key}, true
	case companion.CommandSignStart:
		s.signData = make([]byte, 0, maxSignData)
		return []companion.Response{companion.SignStart{MaxBytes: maxSignData}}, true
	case companion.CommandSignFinish:
		if s.signData == nil {
			return errorResponses(companion.ErrorBadState), true
		}
		raw := s.id.Sign(s.signData)
		s.signData = nil
		signature := companion.Signature{}
		copy(signature.Value[:], raw)
		return []companion.Response{signature}, true
	default:
		return nil, false
	}
}

func (s *service) appendSignData(data []byte) []companion.Response {
	if s.signData == nil {
		return errorResponses(companion.ErrorBadState)
	}
	if len(data) > maxSignData-len(s.signData) {
		return errorResponses(companion.ErrorTableFull)
	}
	s.signData = append(s.signData, data...)
	return okResponses()
}

func (s *service) handleConfigurationMutation(cmd companion.Command) ([]companion.Response, bool) {
	switch c := cmd.(type) {
	case companion.GetChannel:
		return s.getChannel(c.Index), true
	case companion.SetChannel:
		return s.setChannel(c), true
	case companion.SetAutoAddConfig:
		s.autoFlags, s.autoHops = c.Flags, c.MaxHops
		return okResponses(), true
	case companion.SetPathHashMode:
		if c.Mode > 2 {
			return errorResponses(companion.ErrorIllegalArgument), true
		}
		s.p.PathHashMode = int(c.Mode)
		return okResponses(), true
	case companion.SetOtherParams:
		s.p.ManualContacts = c.ManualContacts
		if c.HasTelemetry {
			// The reference stores three two-bit telemetry permissions and
			// drops the two unused high bits when it reports them again.
			s.p.TelemetryMode = int(c.TelemetryMode & 0x3f)
		}
		if c.HasAdvertLoc {
			s.p.AdvertLoc = int(c.AdvertLocPolicy)
		}
		if c.HasMultiACKs {
			s.p.MultiACKs = int(c.MultiACKs)
		}
		return okResponses(), true
	case companion.SetDefaultFloodScope:
		return s.setDefaultFloodScope(c), true
	case companion.SetFloodScope:
		return s.setFloodScope(c), true
	}
	return nil, false
}

func (s *service) handleContactMutation(cmd companion.Command) ([]companion.Response, bool) {
	switch c := cmd.(type) {
	case companion.AddUpdateContact:
		return s.addUpdateContact(c), true
	case companion.ImportContact:
		return s.importContact(c.Packet), true
	case companion.ContactKey:
		if c.Kind == companion.CommandResetPath {
			return s.resetContactPath(c.PublicKey), true
		}
		if c.Kind == companion.CommandRemoveContact {
			return s.removeContact(c.PublicKey), true
		}
	}
	return nil, false
}

func (s *service) setDeviceTime(seconds uint32) []companion.Response {
	current := time.Now().Add(s.clockDelta)
	requested := time.Unix(int64(seconds), 0)
	if requested.Before(current.Truncate(time.Second)) {
		return errorResponses(companion.ErrorIllegalArgument)
	}
	s.clockDelta = time.Until(requested)
	return okResponses()
}

func (s *service) uniqueTimestampLocked() uint32 {
	now := uint32(time.Now().Add(s.clockDelta).Unix())
	if now <= s.lastUnique {
		now = s.lastUnique + 1
	}
	s.lastUnique = now
	return now
}

func okResponses() []companion.Response {
	return []companion.Response{companion.StatusResponse(companion.ResponseOK)}
}

func errorResponses(code companion.ErrorCode) []companion.Response {
	return []companion.Response{companion.ErrorResponse{Code: code}}
}

func (s *service) setRadioParams(c companion.SetRadioParams) []companion.Response {
	if c.Repeat || c.FrequencyKHz < 150_000 || c.FrequencyKHz > 2_500_000 ||
		c.Spreading < 5 || c.Spreading > 12 || c.CodingRate < 5 || c.CodingRate > 8 ||
		c.BandwidthHz < 7_000 || c.BandwidthHz > 500_000 {
		return errorResponses(companion.ErrorIllegalArgument)
	}
	waveform := s.p.Waveform
	waveform.FrequencyHz = c.FrequencyKHz * 1_000
	waveform.BandwidthHz = int(c.BandwidthHz)
	waveform.SpreadingFactor = int(c.Spreading)
	waveform.CodingRate = int(c.CodingRate)
	if s.binding != nil {
		if err := s.binding.SetWaveform(waveform); err != nil {
			return errorResponses(companion.ErrorIllegalArgument)
		}
	}
	s.p.Waveform = waveform
	return okResponses()
}

func (s *service) setRadioPower(power int8) []companion.Response {
	// Lotor's first station radio is the SX1262, whose reference companion
	// range is -9..22 dBm. The physical envelope is judged again at admission.
	if power < -9 || power > 22 {
		return errorResponses(companion.ErrorIllegalArgument)
	}
	if s.rfDevice != nil {
		if err := s.rfDevice.Envelope().Permits(s.p.Waveform, power, true); err != nil {
			return errorResponses(companion.ErrorIllegalArgument)
		}
	}
	s.p.TXPowerDBm = power
	return okResponses()
}

func (s *service) getChannel(index uint8) []companion.Response {
	ch, exists := s.channels[index]
	if !exists || int(index) >= s.p.MaxChannels {
		return errorResponses(companion.ErrorNotFound)
	}
	return []companion.Response{companion.ChannelInfo{Index: index, Name: ch.name, Secret: ch.secret}}
}

func (s *service) setChannel(c companion.SetChannel) []companion.Response {
	if int(c.Index) >= s.p.MaxChannels {
		return errorResponses(companion.ErrorNotFound)
	}
	name := c.Name
	if len(name) > maxChannelName {
		name = name[:maxChannelName]
	}
	s.channels[c.Index] = channel{name: name, secret: c.Secret}
	return okResponses()
}

func (s *service) setDefaultFloodScope(c companion.SetDefaultFloodScope) []companion.Response {
	if c.Clear {
		s.defaultScope, s.defaultKey = "", [16]byte{}
		return okResponses()
	}
	if c.Name == "" || len(c.Name) > 30 {
		return errorResponses(companion.ErrorIllegalArgument)
	}
	s.defaultScope, s.defaultKey = c.Name, c.Key
	return okResponses()
}

func (s *service) setFloodScope(c companion.SetFloodScope) []companion.Response {
	// The originated-send override is session state, just as in the
	// reference. A detached station keeps it for its next attached send.
	s.sendUnscoped = c.Unscoped
	s.sendScope = [16]byte{}
	if !c.Null && !c.Unscoped {
		s.sendScope = c.Key
	}
	return okResponses()
}

func (s *service) handleSimple(kind companion.CommandCode) []companion.Response {
	switch kind {
	case companion.CommandGetDeviceTime:
		return []companion.Response{companion.CurrentTime{
			UnixSeconds: uint32(time.Now().Add(s.clockDelta).Unix()),
		}}
	case companion.CommandSyncNextMessage:
		if len(s.mailbox) == 0 {
			return []companion.Response{companion.StatusResponse(companion.ResponseNoMoreMessages)}
		}
		response := append([]byte(nil), s.mailbox[0]...)
		s.mailbox = s.mailbox[1:]
		return []companion.Response{companion.EncodedResponse{Payload: response}}
	case companion.CommandGetBatteryAndStorage:
		return []companion.Response{companion.BatteryAndStorage{}}
	case companion.CommandGetTuningParams:
		return []companion.Response{companion.TuningParams{
			RXDelayMilli: s.p.RXDelayMilli, AirtimeFactorMilli: s.p.AirFactorMilli,
		}}
	case companion.CommandGetAutoAddConfig:
		return []companion.Response{companion.AutoAddConfig{Flags: s.autoFlags, MaxHops: s.autoHops}}
	case companion.CommandGetAllowedRepeatFreq:
		return []companion.Response{companion.AllowedRepeatFrequencies{}}
	case companion.CommandGetDefaultFloodScope:
		return []companion.Response{companion.DefaultFloodScope{
			Clear: s.defaultScope == "", Name: s.defaultScope, Key: s.defaultKey,
		}}
	case companion.CommandSendSelfAdvert:
		return []companion.Response{companion.StatusResponse(companion.ResponseDisabled)}
	default:
		return errorResponses(companion.ErrorUnsupportedCommand)
	}
}

func (s *service) getStats(kind companion.StatsType) []companion.Response {
	switch kind {
	case companion.StatsCore:
		uptime := max(time.Duration(0), time.Since(s.startedAt)) / time.Second
		return []companion.Response{companion.CoreStats{
			UptimeSeconds: uint32(min(uptime, time.Duration(math.MaxUint32))),
			QueueLength:   uint8(min(s.outbound.len(), math.MaxUint8)),
		}}
	case companion.StatsRadio:
		noiseFloor := int16(0)
		if s.rfDevice != nil {
			if noise, ok := s.rfDevice.NoiseFloor(); ok {
				noiseFloor = int16(max(float64(math.MinInt16), min(float64(math.MaxInt16), math.Trunc(noise.DBm))))
			}
		}
		return []companion.Response{companion.RadioStats{
			NoiseFloorDBm: noiseFloor, LastRSSIDBm: s.stats.lastRSSIDBm, LastSNRx4: s.stats.lastSNRx4,
			TXAirSeconds: durationSeconds(s.stats.txAir), RXAirSeconds: durationSeconds(s.stats.rxAir),
		}}
	case companion.StatsPackets:
		return []companion.Response{companion.PacketStats{
			Received: s.stats.received, Sent: s.stats.sent,
			SentFlood: s.stats.sentFlood, SentDirect: s.stats.sentDirect,
			ReceivedFlood: s.stats.receivedFlood, ReceivedDirect: s.stats.receivedDirect,
			ReceiveErrors: s.stats.receiveErrors,
		}}
	default:
		return errorResponses(companion.ErrorIllegalArgument)
	}
}

func durationSeconds(value time.Duration) uint32 {
	seconds := max(time.Duration(0), value) / time.Second
	return uint32(min(seconds, time.Duration(math.MaxUint32)))
}

func (s *service) selfInfo() companion.SelfInfo {
	lat := int32(math.Round(s.p.NodeLat * 1e6))
	lon := int32(math.Round(s.p.NodeLon * 1e6))
	return companion.SelfInfo{
		AdvertType: mesh.AdvTypeChat, TXPowerDBm: s.p.TXPowerDBm,
		MaxTXPowerDBm: s.p.TXPowerDBm, PublicKey: s.id.PubKey,
		LatitudeE6: lat, LongitudeE6: lon, MultiACKs: uint8(s.p.MultiACKs),
		AdvertLocPolicy: uint8(s.p.AdvertLoc), TelemetryMode: uint8(s.p.TelemetryMode),
		ManualContacts: s.p.ManualContacts, FrequencyKHz: s.p.FrequencyHz / 1_000,
		BandwidthHz: uint32(s.p.BandwidthHz), Spreading: uint8(s.p.SpreadingFactor),
		CodingRate: uint8(s.p.CodingRate), Name: s.p.NodeName,
	}
}

func (s *service) Info() station.Info {
	s.mu.Lock()
	defer s.mu.Unlock()
	rf, rfCause := s.rf, s.rfCause
	if s.binding != nil {
		state, cause := s.binding.State()
		rfCause = cause
		switch state {
		case radio.BindingActive:
			rf = station.RFActive
		case radio.BindingBlocked:
			rf = station.RFBlocked
		case radio.BindingDown:
			rf = station.RFDown
		}
	}
	return station.Info{
		Name: s.name, Protocol: protocolName, Listen: s.listen, Radio: s.radioName,
		State: s.state, Cause: s.cause, RF: rf, RFCause: rfCause, Connected: s.client != nil,
		Remote: s.remote, Mailbox: len(s.mailbox), MailboxCap: s.p.MailboxCap,
		Waveform: s.p.Waveform, PublicKey: hex.EncodeToString(s.id.PubKey[:]),
	}
}

func (s *service) AttachRadio(name string, binding *radio.Binding, duty *radio.AirtimeLedger, cause string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.radioName, s.binding, s.duty, s.rfCause = name, binding, duty, cause
	if name == "" {
		s.rf = station.RFDetached
		s.rfCause = ""
	} else {
		s.rf = station.RFDown
	}
	select {
	case s.rfWake <- struct{}{}:
	default:
	}
}

func (s *service) RadioDemand() station.RadioDemand {
	s.mu.Lock()
	defer s.mu.Unlock()
	return station.RadioDemand{
		Waveform: s.p.Waveform, PowerDBm: s.p.TXPowerDBm, DutyCyclePct: s.p.DutyCyclePct,
	}
}

func (s *service) setLifecycle(state station.State, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = state
	s.cause = ""
	if err != nil {
		s.cause = err.Error()
	}
}
