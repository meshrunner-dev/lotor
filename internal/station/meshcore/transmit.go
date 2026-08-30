package meshcore

import (
	"bytes"
	"strings"
	"time"

	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/correlation"

	mesh "meshrunner.dev/pkg/meshcore"
	"meshrunner.dev/pkg/meshcore/companion"
)

const (
	stationMaxText      = 10 * mesh.CipherBlockSize
	stationMaxGroupData = mesh.MaxPacketPayload - mesh.CipherBlockSize - 3
	stationTimeoutBase  = 500 * time.Millisecond
)

func (s *service) handleTransmission(command companion.Command) ([]companion.Response, bool) {
	switch cmd := command.(type) {
	case companion.SendSelfAdvert:
		return s.sendSelfAdvert(cmd), true
	case companion.SendText:
		return s.sendText(cmd), true
	case companion.SendChannelText:
		return s.sendChannelText(cmd), true
	case companion.SendChannelData:
		return s.sendChannelData(cmd), true
	case companion.SendRawData:
		return s.sendRawData(cmd), true
	case companion.SendLogin:
		return s.sendLogin(cmd), true
	case companion.ContactRequest:
		if cmd.Kind == companion.CommandSendStatusRequest {
			return s.sendStatusRequest(cmd.PublicKey), true
		}
	case companion.SendTelemetryRequest:
		return s.sendTelemetryRequest(cmd), true
	case companion.ContactDataRequest:
		return s.sendContactDataRequest(cmd), true
	case companion.SendPathDiscovery:
		return s.sendPathDiscovery(cmd.PublicKey), true
	case companion.SendTracePath:
		return s.sendTrace(cmd), true
	case companion.SendControlData:
		return s.sendControlData(cmd.Data), true
	case companion.SendRawPacket:
		return s.sendRawPacket(cmd), true
	case companion.ContactKey:
		if cmd.Kind == companion.CommandShareContact {
			return s.shareContact(cmd.PublicKey), true
		}
	}
	return nil, false
}

func (s *service) sendSelfAdvert(command companion.SendSelfAdvert) []companion.Response {
	packet, err := s.selfAdvert(time.Now().Add(s.clockDelta))
	if err != nil {
		return errorResponses(companion.ErrTableFull)
	}
	kind := "station-advert-local"
	if command.Flood {
		s.routeFlood(packet)
		kind = "station-advert-flood"
	} else {
		s.routeDirect(packet, 0, nil)
	}
	if response := s.submitLocked(packet, kind); response != nil {
		return response
	}
	return okResponses()
}

func (s *service) sendText(command companion.SendText) []companion.Response {
	if command.TextType != mesh.TxtTypePlain && command.TextType != mesh.TxtTypeCLIData {
		return errorResponses(companion.ErrUnsupportedCommand)
	}
	contact, exists := s.contactByPrefix(command.RecipientPrefix[:])
	if !exists {
		return errorResponses(companion.ErrNotFound)
	}
	text := command.Text
	if at := strings.IndexByte(text, 0); at >= 0 {
		text = text[:at]
	}
	limit := stationMaxText
	if command.Attempt > 3 {
		limit -= 2
	}
	if len(text) > limit {
		return errorResponses(companion.ErrTableFull)
	}
	secret, err := s.id.SharedSecret(contact.info.PublicKey[:])
	if err != nil {
		return errorResponses(companion.ErrIllegalArgument)
	}
	sentAt := time.Unix(int64(command.UnixSeconds), 0)
	if command.TextType == mesh.TxtTypeCLIData {
		sentAt = time.Unix(int64(s.uniqueTimestampLocked()), 0)
	}
	plain := mesh.BuildTextPlaintextAttempt(sentAt, command.TextType, text, int(command.Attempt))
	packet, err := mesh.BuildDatagram(mesh.PayloadTypeTxtMsg,
		contact.info.PublicKey[:mesh.PathHashSize], s.id.PubKey[:mesh.PathHashSize], secret, plain)
	if err != nil {
		return errorResponses(companion.ErrTableFull)
	}
	flood := contact.info.PathLen == 0xff
	kind := "station-message-direct"
	if flood {
		s.routeFlood(packet)
		kind = "station-message-flood"
	} else {
		pathBytes := pathByteLen(contact.info.PathLen)
		s.routeDirect(packet, contact.info.PathLen, contact.info.Path[:pathBytes])
	}
	if response := s.submitLocked(packet, kind); response != nil {
		return response
	}
	expectedACK := uint32(0)
	if command.TextType == mesh.TxtTypePlain {
		head := plain[:5+len(text)]
		expectedACK = mesh.AckCRC(head, s.id.PubKey[:])
		s.expectedACKs[s.nextACK] = ackExpectation{crc: expectedACK, at: time.Now(), used: true}
		s.nextACK = (s.nextACK + 1) % len(s.expectedACKs)
	}
	return []companion.Response{companion.Sent{
		Flood: flood, ExpectedACK: expectedACK, TimeoutMillis: s.estimateTimeout(packet, flood),
	}}
}

func (s *service) sendChannelText(command companion.SendChannelText) []companion.Response {
	if command.TextType != mesh.TxtTypePlain {
		return errorResponses(companion.ErrUnsupportedCommand)
	}
	item, exists := s.channels[command.Channel]
	if !exists {
		return errorResponses(companion.ErrNotFound)
	}
	channel, err := mesh.NewGroupChannel(item.secret[:])
	if err != nil {
		return errorResponses(companion.ErrIllegalArgument)
	}
	text := command.Text
	if room := stationMaxText - len(s.p.NodeName) - 2; len(text) > room {
		text = text[:room]
	}
	plain := mesh.BuildGroupText(time.Unix(int64(command.UnixSeconds), 0), s.p.NodeName, text)
	packet, err := mesh.BuildGroupDatagram(mesh.PayloadTypeGrpTxt, channel, plain)
	if err != nil {
		return errorResponses(companion.ErrTableFull)
	}
	s.routeFlood(packet)
	if response := s.submitLocked(packet, "station-channel-text"); response != nil {
		return response
	}
	return okResponses()
}

func (s *service) sendChannelData(command companion.SendChannelData) []companion.Response {
	if command.DataType == 0 || len(command.Data) > stationMaxGroupData {
		return errorResponses(companion.ErrIllegalArgument)
	}
	item, exists := s.channels[command.Channel]
	if !exists {
		return errorResponses(companion.ErrNotFound)
	}
	channel, err := mesh.NewGroupChannel(item.secret[:])
	if err != nil {
		return errorResponses(companion.ErrIllegalArgument)
	}
	plain, err := mesh.BuildGroupData(command.DataType, command.Data)
	if err != nil {
		return errorResponses(companion.ErrIllegalArgument)
	}
	packet, err := mesh.BuildGroupDatagram(mesh.PayloadTypeGrpData, channel, plain)
	if err != nil {
		return errorResponses(companion.ErrTableFull)
	}
	if command.PathLen == 0xff {
		s.routeFlood(packet)
	} else {
		s.routeDirect(packet, command.PathLen, command.Path)
	}
	if response := s.submitLocked(packet, "station-channel-data"); response != nil {
		return response
	}
	return okResponses()
}

func (s *service) shareContact(publicKey [mesh.PubKeySize]byte) []companion.Response {
	entry, exists := s.contacts[publicKey]
	if !exists {
		return errorResponses(companion.ErrNotFound)
	}
	if len(entry.advert) == 0 {
		return errorResponses(companion.ErrTableFull)
	}
	packet, err := mesh.ParsePacket(entry.advert)
	if err != nil {
		return errorResponses(companion.ErrFileIO)
	}
	s.routeDirect(packet, 0, nil)
	if response := s.submitLocked(packet, "station-contact-share"); response != nil {
		return response
	}
	return okResponses()
}

func (s *service) contactByPrefix(prefix []byte) (contactEntry, bool) {
	for _, entry := range s.orderedContacts() {
		if bytes.Equal(entry.info.PublicKey[:len(prefix)], prefix) {
			return entry, true
		}
	}
	return contactEntry{}, false
}

func (s *service) routeFlood(packet *mesh.Packet) {
	packet.Header = mesh.MakeHeader(mesh.RouteFlood, packet.PayloadType(), packet.PayloadVer())
	packet.SetPathHashSizeAndCount(s.p.PathHashMode+1, 0)
	packet.Path = nil
	packet.TransportCodes = [2]uint16{}
	if s.sendUnscoped {
		return
	}
	key := s.sendScope
	if key == ([16]byte{}) {
		key = s.defaultKey
	}
	mesh.TransportKey(key).Scope(packet)
}

func (*service) routeDirect(packet *mesh.Packet, pathLen uint8, path []byte) {
	packet.Header = mesh.MakeHeader(mesh.RouteDirect, packet.PayloadType(), packet.PayloadVer())
	packet.PathLen = pathLen
	packet.Path = append([]byte(nil), path...)
	packet.TransportCodes = [2]uint16{}
}

func pathByteLen(pathLen uint8) int {
	return int(pathLen&63) * (int(pathLen>>6) + 1)
}

func (s *service) submitLocked(packet *mesh.Packet, kind string) []companion.Response {
	return s.submitAtLocked(packet, kind, time.Time{})
}

func (s *service) submitAtLocked(packet *mesh.Packet, kind string, notBefore time.Time) []companion.Response {
	return s.submitAtPriorityLocked(packet, kind, notBefore, referencePriority(packet))
}

func (s *service) submitAtPriorityLocked(packet *mesh.Packet, kind string, notBefore time.Time,
	priority uint8,
) []companion.Response {
	if s.txPolicy.Mode == "" || s.txPolicy.Mode == config.TXDry || s.rfDevice == nil || s.duty == nil {
		return []companion.Response{companion.StatusResponse(companion.ResponseDisabled)}
	}
	item := emission{
		packet: packet, correlation: correlation.New(), kind: kind,
		notBefore: notBefore, priority: priority,
	}
	if s.outbound.offer(item) {
		s.seen.mark(packet.Hash())
		return nil
	}
	return errorResponses(companion.ErrTableFull)
}

func referencePriority(packet *mesh.Packet) uint8 {
	if packet.IsRouteFlood() {
		switch packet.PayloadType() {
		case mesh.PayloadTypePath:
			return 2
		case mesh.PayloadTypeAdvert:
			return 3
		default:
			return 1
		}
	}
	if packet.PayloadType() == mesh.PayloadTypeTrace {
		return 5
	}
	if packet.PayloadType() == mesh.PayloadTypePath {
		return 1
	}
	return 0
}

func (s *service) estimateTimeout(packet *mesh.Packet, flood bool) uint32 {
	airtime := time.Duration(0)
	if s.rfDevice != nil {
		airtime = s.rfDevice.Airtime(packet.RawLength())
	}
	if flood {
		return uint32((stationTimeoutBase + 16*airtime).Milliseconds())
	}
	perHop := 6*airtime + 250*time.Millisecond
	return uint32((stationTimeoutBase + time.Duration(packet.PathHashCount()+1)*perHop).Milliseconds())
}
