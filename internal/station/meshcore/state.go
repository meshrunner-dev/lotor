package meshcore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"meshrunner.dev/lotor/internal/radio"

	mesh "meshrunner.dev/pkg/meshcore"
	"meshrunner.dev/pkg/meshcore/companion"
)

const persistedStateVersion = 1

type persistedChannel struct {
	Index  uint8    `json:"index"`
	Name   string   `json:"name"`
	Secret [16]byte `json:"secret"`
}

type persistedContact struct {
	PublicKey        [32]byte `json:"publicKey"`
	Type             uint8    `json:"type"`
	Flags            uint8    `json:"flags"`
	PathLen          uint8    `json:"pathLen"`
	Path             [64]byte `json:"path"`
	Name             string   `json:"name"`
	LastAdvertUnix   uint32   `json:"lastAdvertUnix"`
	LatitudeE6       int32    `json:"latitudeE6"`
	LongitudeE6      int32    `json:"longitudeE6"`
	LastModifiedUnix uint32   `json:"lastModifiedUnix"`
	Advert           []byte   `json:"advert,omitempty"`
	Order            uint64   `json:"order"`
}

type persistedWaveform struct {
	FrequencyHz     uint32 `json:"frequencyHz"`
	SpreadingFactor int    `json:"spreadingFactor"`
	BandwidthHz     int    `json:"bandwidthHz"`
	CodingRate      int    `json:"codingRate"`
	Preamble        int    `json:"preamble"`
	SyncWord        byte   `json:"syncWord"`
	CRC             bool   `json:"crc"`
}

func persistWaveform(w radio.Waveform) persistedWaveform {
	return persistedWaveform{
		FrequencyHz: w.FrequencyHz, SpreadingFactor: w.SpreadingFactor,
		BandwidthHz: w.BandwidthHz, CodingRate: w.CodingRate, Preamble: w.Preamble,
		SyncWord: w.SyncWord, CRC: w.CRC,
	}
}

func (w persistedWaveform) radio() radio.Waveform {
	return radio.Waveform{
		FrequencyHz: w.FrequencyHz, SpreadingFactor: w.SpreadingFactor,
		BandwidthHz: w.BandwidthHz, CodingRate: w.CodingRate, Preamble: w.Preamble,
		SyncWord: w.SyncWord, CRC: w.CRC,
	}
}

// persistedState contains only state the companion protocol may own. The
// configured identity seeds a new station, then an imported identity becomes
// durable here. Capacity and regulatory policy remain configuration-owned.
type persistedState struct {
	Version int `json:"version"`

	Waveform       persistedWaveform  `json:"waveform"`
	TXPowerDBm     int8               `json:"txPowerDbm"`
	NodeName       string             `json:"nodeName"`
	NodeLat        float64            `json:"nodeLat"`
	NodeLon        float64            `json:"nodeLon"`
	PIN            uint64             `json:"pin"`
	MultiACKs      int                `json:"multiAcks"`
	AdvertLoc      int                `json:"advertLocPolicy"`
	TelemetryMode  int                `json:"telemetryMode"`
	ManualContact  bool               `json:"manualAddContacts"`
	PathHashMode   int                `json:"pathHashMode"`
	RXDelayMilli   uint32             `json:"rxDelayMilli"`
	AirFactorMilli uint32             `json:"airtimeFactorMilli"`
	PrivateKey     []byte             `json:"privateKey,omitempty"`
	ClockDelta     int64              `json:"clockDeltaNs"`
	AutoFlags      uint8              `json:"autoAddFlags"`
	AutoHops       uint8              `json:"autoAddMaxHops"`
	DefaultScope   string             `json:"defaultScope"`
	DefaultKey     [16]byte           `json:"defaultScopeKey"`
	Channels       []persistedChannel `json:"channels,omitempty"`
	Contacts       []persistedContact `json:"contacts,omitempty"`
	Mailbox        [][]byte           `json:"mailbox,omitempty"`
}

func (s *service) snapshotLocked() persistedState {
	state := persistedState{
		Version: persistedStateVersion, Waveform: persistWaveform(s.p.Waveform), TXPowerDBm: s.p.TXPowerDBm,
		NodeName: s.p.NodeName, NodeLat: s.p.NodeLat, NodeLon: s.p.NodeLon, PIN: s.p.PIN,
		MultiACKs: s.p.MultiACKs, AdvertLoc: s.p.AdvertLoc, TelemetryMode: s.p.TelemetryMode,
		ManualContact: s.p.ManualContacts, PathHashMode: s.p.PathHashMode,
		RXDelayMilli: s.p.RXDelayMilli, AirFactorMilli: s.p.AirFactorMilli,
		PrivateKey: s.id.PrvKey(),
		ClockDelta: int64(s.clockDelta), AutoFlags: s.autoFlags, AutoHops: s.autoHops,
		DefaultScope: s.defaultScope, DefaultKey: s.defaultKey,
		Channels: make([]persistedChannel, 0, len(s.channels)),
		Contacts: make([]persistedContact, 0, len(s.contacts)),
		Mailbox:  make([][]byte, len(s.mailbox)),
	}
	for i := range s.mailbox {
		state.Mailbox[i] = append([]byte(nil), s.mailbox[i]...)
	}
	for index, ch := range s.channels {
		state.Channels = append(state.Channels, persistedChannel{Index: index, Name: ch.name, Secret: ch.secret})
	}
	sort.Slice(state.Channels, func(i, j int) bool { return state.Channels[i].Index < state.Channels[j].Index })
	for _, entry := range s.contacts {
		info := entry.info
		state.Contacts = append(state.Contacts, persistedContact{
			PublicKey: info.PublicKey, Type: info.Type, Flags: info.Flags, PathLen: info.PathLen,
			Path: info.Path, Name: info.Name, LastAdvertUnix: info.LastAdvertUnix,
			LatitudeE6: info.LatitudeE6, LongitudeE6: info.LongitudeE6,
			LastModifiedUnix: info.LastModifiedUnix, Advert: append([]byte(nil), entry.advert...), Order: entry.order,
		})
	}
	sort.Slice(state.Contacts, func(i, j int) bool { return state.Contacts[i].Order < state.Contacts[j].Order })
	return state
}

func (s *service) restoreLocked(state persistedState) {
	s.p.Waveform, s.p.TXPowerDBm = state.Waveform.radio(), state.TXPowerDBm
	s.p.NodeName, s.p.NodeLat, s.p.NodeLon = state.NodeName, state.NodeLat, state.NodeLon
	s.p.PIN, s.p.MultiACKs = state.PIN, state.MultiACKs
	s.p.AdvertLoc, s.p.TelemetryMode = state.AdvertLoc, state.TelemetryMode
	s.p.ManualContacts, s.p.PathHashMode = state.ManualContact, state.PathHashMode
	s.p.RXDelayMilli, s.p.AirFactorMilli = state.RXDelayMilli, state.AirFactorMilli
	if len(state.PrivateKey) == mesh.PrvKeySize {
		identity, err := mesh.LocalIdentityFromKeys(state.PrivateKey, nil)
		if err == nil {
			s.id = identity
		}
	}
	s.clockDelta = time.Duration(state.ClockDelta)
	s.autoFlags, s.autoHops = state.AutoFlags, state.AutoHops
	s.defaultScope, s.defaultKey = state.DefaultScope, state.DefaultKey
	s.channels = make(map[uint8]channel, len(state.Channels))
	for _, item := range state.Channels {
		s.channels[item.Index] = channel{name: item.Name, secret: item.Secret}
	}
	s.contacts = make(map[[mesh.PubKeySize]byte]contactEntry, len(state.Contacts))
	s.nextContact = 0
	for _, item := range state.Contacts {
		info := companion.Contact{
			PublicKey: item.PublicKey, Type: item.Type, Flags: item.Flags, PathLen: item.PathLen,
			Path: item.Path, Name: item.Name, LastAdvertUnix: item.LastAdvertUnix,
			LatitudeE6: item.LatitudeE6, LongitudeE6: item.LongitudeE6,
			LastModifiedUnix: item.LastModifiedUnix,
		}
		s.contacts[item.PublicKey] = contactEntry{
			info: info, advert: append([]byte(nil), item.Advert...), order: item.Order,
		}
		s.nextContact = max(s.nextContact, item.Order)
	}
	s.mailbox = make([][]byte, len(state.Mailbox))
	for i := range state.Mailbox {
		s.mailbox[i] = append([]byte(nil), state.Mailbox[i]...)
	}
}

func (s *service) loadState(ctx context.Context) error {
	if s.stateStore == nil {
		return nil
	}
	raw, exists, err := s.stateStore.LoadStationState(ctx, s.name)
	if err != nil || !exists {
		return err
	}
	var state persistedState
	if err := json.Unmarshal(raw, &state); err != nil {
		return fmt.Errorf("meshcore station state: %w", err)
	}
	if state.Version != persistedStateVersion {
		return fmt.Errorf("meshcore station state: version %d, want %d", state.Version, persistedStateVersion)
	}
	if err := s.validateState(state); err != nil {
		return err
	}
	s.restoreLocked(state)
	return nil
}

func (s *service) validateState(state persistedState) error {
	check := s.p
	check.Waveform, check.TXPowerDBm = state.Waveform.radio(), state.TXPowerDBm
	check.NodeName, check.NodeLat, check.NodeLon = state.NodeName, state.NodeLat, state.NodeLon
	check.PIN, check.MultiACKs = state.PIN, state.MultiACKs
	check.AdvertLoc, check.TelemetryMode = state.AdvertLoc, state.TelemetryMode
	check.ManualContacts, check.PathHashMode = state.ManualContact, state.PathHashMode
	check.RXDelayMilli, check.AirFactorMilli = state.RXDelayMilli, state.AirFactorMilli
	if err := validateAir(check); err != nil {
		return fmt.Errorf("meshcore station state: %w", err)
	}
	if err := validatePreferences(check); err != nil {
		return fmt.Errorf("meshcore station state: %w", err)
	}
	if len(state.PrivateKey) != 0 {
		identity, err := mesh.LocalIdentityFromKeys(state.PrivateKey, nil)
		if err != nil || !identity.FirmwareImportable() {
			return errors.New("meshcore station state: invalid private identity")
		}
	}
	if err := s.validateChannels(state.Channels); err != nil {
		return err
	}
	if err := s.validateContacts(state.Contacts); err != nil {
		return err
	}
	return s.validateMailbox(state.Mailbox)
}

func (s *service) validateChannels(channels []persistedChannel) error {
	seen := make(map[uint8]struct{}, len(channels))
	for _, item := range channels {
		if int(item.Index) >= s.p.MaxChannels || len(item.Name) > maxChannelName {
			return fmt.Errorf("meshcore station state: invalid channel %d", item.Index)
		}
		if _, duplicate := seen[item.Index]; duplicate {
			return fmt.Errorf("meshcore station state: duplicate channel %d", item.Index)
		}
		seen[item.Index] = struct{}{}
	}
	return nil
}

func (s *service) validateContacts(contacts []persistedContact) error {
	if len(contacts) > s.p.MaxContacts {
		return fmt.Errorf("meshcore station state: %d contacts exceed capacity %d", len(contacts), s.p.MaxContacts)
	}
	contactKeys := make(map[[mesh.PubKeySize]byte]struct{}, len(contacts))
	for _, item := range contacts {
		if _, duplicate := contactKeys[item.PublicKey]; duplicate {
			return fmt.Errorf("meshcore station state: duplicate contact %x", item.PublicKey[:6])
		}
		contactKeys[item.PublicKey] = struct{}{}
		if item.PathLen != 0xff && !mesh.ValidPathLen(item.PathLen) {
			return fmt.Errorf("meshcore station state: invalid contact path 0x%02x", item.PathLen)
		}
		if len(item.Name) > maxStationName {
			return fmt.Errorf("meshcore station state: contact name exceeds %d bytes", maxStationName)
		}
		if len(item.Advert) > 0 {
			packet, err := mesh.ParsePacket(item.Advert)
			if err != nil || packet.PayloadType() != mesh.PayloadTypeAdvert {
				return errors.New("meshcore station state: invalid contact advert")
			}
		}
	}
	return nil
}

func (s *service) validateMailbox(mailbox [][]byte) error {
	if len(mailbox) > s.p.MailboxCap {
		return fmt.Errorf("meshcore station state: %d mailbox entries exceed capacity %d",
			len(mailbox), s.p.MailboxCap)
	}
	for _, payload := range mailbox {
		if _, err := companion.MarshalResponse(companion.EncodedResponse{Payload: payload}); err != nil {
			return fmt.Errorf("meshcore station state: invalid mailbox response: %w", err)
		}
	}
	return nil
}

func (s *service) persistLocked(ctx context.Context, before persistedState) error {
	if s.stateStore == nil {
		return nil
	}
	after := s.snapshotLocked()
	beforeRaw, err := json.Marshal(before) //nolint:gosec // this state deliberately owns the imported identity
	if err != nil {
		return err
	}
	afterRaw, err := json.Marshal(after) //nolint:gosec // persistence is the companion import contract
	if err != nil {
		return err
	}
	if string(beforeRaw) == string(afterRaw) {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.stateStore.SaveStationState(ctx, s.name, afterRaw); err != nil {
		oldWaveform := before.Waveform.radio()
		s.restoreLocked(before)
		if s.binding != nil && oldWaveform != after.Waveform.radio() {
			_ = s.binding.SetWaveform(oldWaveform)
		}
		return err
	}
	return nil
}
