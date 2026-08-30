package meshcore

import (
	"bytes"
	"fmt"
	"time"

	"meshrunner.dev/lotor/internal/correlation"
	"meshrunner.dev/lotor/internal/radio"

	mesh "meshrunner.dev/pkg/meshcore"
	"meshrunner.dev/pkg/meshcore/companion"
)

type pendingKind uint8

const (
	pendingNone pendingKind = iota
	pendingLogin
	pendingStatus
	pendingTelemetry
	pendingBinary
	pendingDiscovery
)

type pendingRequest struct {
	kind      pendingKind
	tag       uint32
	publicKey [mesh.PubKeySize]byte
}

type remoteConnection struct {
	keepAlive   time.Duration
	activeAt    time.Time
	nextPing    time.Time
	expectedACK uint32
}

func (s *service) sendRawData(command companion.SendRawData) []companion.Response {
	if len(command.Path) > 63 || len(command.Data) < 4 {
		return errorResponses(companion.ErrUnsupportedCommand)
	}
	packet, err := mesh.BuildRawCustom(command.Data)
	if err != nil {
		return errorResponses(companion.ErrTableFull)
	}
	s.routeDirect(packet, uint8(len(command.Path)), command.Path)
	if responses := s.submitLocked(packet, "station-raw-data"); responses != nil {
		return responses
	}
	return okResponses()
}

func (s *service) sendLogin(command companion.SendLogin) []companion.Response {
	contact, exists := s.contacts[command.PublicKey]
	if !exists {
		return errorResponses(companion.ErrNotFound)
	}
	password := command.Password
	if len(password) > 15 {
		password = password[:15]
	}
	tag := s.uniqueTimestampLocked()
	var packet *mesh.Packet
	var err error
	if contact.info.Type == mesh.AdvTypeRoom {
		packet, _, err = mesh.BuildRoomLoginReq(s.id, contact.info.PublicKey[:], tag, contact.syncSince, password)
	} else {
		packet, _, err = mesh.BuildLoginReq(s.id, contact.info.PublicKey[:], tag, password)
	}
	if err != nil {
		return errorResponses(companion.ErrTableFull)
	}
	flood := s.routeContact(packet, contact, false)
	if responses := s.submitLocked(packet, "station-login"); responses != nil {
		return responses
	}
	s.pending = pendingRequest{kind: pendingLogin, tag: tag, publicKey: command.PublicKey}
	return []companion.Response{companion.Sent{
		Flood: flood, ExpectedACK: companion.LoginExpectedACK(command.PublicKey),
		TimeoutMillis: s.estimateTimeout(packet, flood),
	}}
}

func (s *service) sendStatusRequest(publicKey [mesh.PubKeySize]byte) []companion.Response {
	body, err := mesh.FrameStatusRequest()
	if err != nil {
		return errorResponses(companion.ErrTableFull)
	}
	return s.sendKnownRequest(publicKey, body, pendingStatus, "station-status-request", false)
}

func (s *service) sendTelemetryRequest(command companion.SendTelemetryRequest) []companion.Response {
	if command.Self {
		encoder := mesh.NewLPPEncoder()
		_ = encoder.Add(mesh.LPPReading{Channel: 1, Type: mesh.LPPVoltage, Value: float64(0)})
		body := make([]byte, 0, 7+len(encoder.Bytes()))
		body = append(body, 0)
		body = append(body, s.id.PubKey[:6]...)
		body = append(body, encoder.Bytes()...)
		return []companion.Response{companion.Push{Code: companion.PushTelemetryResponse, Body: body}}
	}
	body, err := mesh.FrameTelemetryRequest(0)
	if err != nil {
		return errorResponses(companion.ErrTableFull)
	}
	return s.sendKnownRequest(command.PublicKey, body, pendingTelemetry, "station-telemetry-request", false)
}

func (s *service) sendContactDataRequest(command companion.ContactDataRequest) []companion.Response {
	if command.Kind == companion.CommandSendBinaryRequest {
		return s.sendKnownRequest(command.PublicKey, command.Data, pendingBinary,
			"station-binary-request", false)
	}
	if command.Kind != companion.CommandSendAnonymousRequest {
		return errorResponses(companion.ErrUnsupportedCommand)
	}
	contact, known := s.contacts[command.PublicKey]
	if !known {
		s.expireAnonymousContactLocked()
		s.nextContact++
		contact = contactEntry{
			info: companion.Contact{
				PublicKey: command.PublicKey, Type: mesh.AdvTypeNone, PathLen: 0,
				LastModifiedUnix: uint32(time.Now().Add(s.clockDelta).Unix()),
			},
			order: s.nextContact, ephemeral: true,
		}
		s.contacts[command.PublicKey] = contact
	}
	secret, err := s.id.SharedSecret(command.PublicKey[:])
	if err != nil {
		return errorResponses(companion.ErrIllegalArgument)
	}
	tag := s.uniqueTimestampLocked()
	packet, err := mesh.BuildAnonDatagram(command.PublicKey[:mesh.PathHashSize], s.id.PubKey[:], secret,
		mesh.FrameAdmin(tag, command.Data))
	if err != nil {
		return errorResponses(companion.ErrTableFull)
	}
	flood := s.routeContact(packet, contact, false)
	if responses := s.submitLocked(packet, "station-anonymous-request"); responses != nil {
		return responses
	}
	s.pending = pendingRequest{kind: pendingBinary, tag: tag, publicKey: command.PublicKey}
	return []companion.Response{companion.Sent{
		Flood: flood, ExpectedACK: tag, TimeoutMillis: s.estimateTimeout(packet, flood),
	}}
}

func (s *service) expireAnonymousContactLocked() {
	count := 0
	var oldestKey [mesh.PubKeySize]byte
	var oldest contactEntry
	found := false
	for key, entry := range s.contacts {
		if !entry.ephemeral {
			continue
		}
		count++
		if !found || entry.info.LastModifiedUnix < oldest.info.LastModifiedUnix ||
			(entry.info.LastModifiedUnix == oldest.info.LastModifiedUnix && entry.order < oldest.order) {
			oldestKey, oldest, found = key, entry, true
		}
	}
	if count >= maxAnonContacts && found {
		delete(s.contacts, oldestKey)
	}
}

func (s *service) sendKnownRequest(publicKey [mesh.PubKeySize]byte, body []byte,
	kind pendingKind, emissionKind string, forceFlood bool,
) []companion.Response {
	contact, exists := s.contacts[publicKey]
	if !exists {
		return errorResponses(companion.ErrNotFound)
	}
	secret, err := s.id.SharedSecret(contact.info.PublicKey[:])
	if err != nil {
		return errorResponses(companion.ErrIllegalArgument)
	}
	tag := s.uniqueTimestampLocked()
	packet, err := mesh.BuildRequest(s.id, contact.info.PublicKey[:], secret, tag, body)
	if err != nil {
		return errorResponses(companion.ErrTableFull)
	}
	flood := s.routeContact(packet, contact, forceFlood)
	if responses := s.submitLocked(packet, emissionKind); responses != nil {
		return responses
	}
	s.pending = pendingRequest{kind: kind, tag: tag, publicKey: publicKey}
	return []companion.Response{companion.Sent{
		Flood: flood, ExpectedACK: tag, TimeoutMillis: s.estimateTimeout(packet, flood),
	}}
}

func (s *service) sendPathDiscovery(publicKey [mesh.PubKeySize]byte) []companion.Response {
	body, err := mesh.FrameTelemetryRequest(^uint8(1))
	if err != nil {
		return errorResponses(companion.ErrTableFull)
	}
	return s.sendKnownRequest(publicKey, body, pendingDiscovery, "station-path-discovery", true)
}

func (s *service) routeContact(packet *mesh.Packet, contact contactEntry, forceFlood bool) bool {
	if forceFlood || contact.info.PathLen == 0xff {
		s.routeFlood(packet)
		return true
	}
	pathLen := contact.info.PathLen
	s.routeDirect(packet, pathLen, contact.info.Path[:pathByteLen(pathLen)])
	return false
}

func (s *service) sendControlData(data []byte) []companion.Response {
	packet, err := mesh.BuildControl(data)
	if err != nil {
		return errorResponses(companion.ErrTableFull)
	}
	s.routeDirect(packet, 0, nil)
	if responses := s.submitLocked(packet, "station-control-data"); responses != nil {
		return responses
	}
	return okResponses()
}

func (s *service) sendRawPacket(command companion.SendRawPacket) []companion.Response {
	packet, err := mesh.ParsePacket(command.Packet)
	if err != nil {
		return errorResponses(companion.ErrIllegalArgument)
	}
	kind := fmt.Sprintf("station-raw-packet-p%d", command.Priority)
	if responses := s.submitAtPriorityLocked(packet, kind, time.Time{}, command.Priority); responses != nil {
		return responses
	}
	return okResponses()
}

func (s *service) receiveRemoteResponse(packet *mesh.Packet, correlations ...correlation.ID) {
	datagram, err := mesh.ParseDatagram(packet.Payload)
	if err != nil {
		return
	}
	s.mu.Lock()
	pending := s.pending
	contacts := s.orderedContacts()
	_, pendingKnown := s.contacts[pending.publicKey]
	identity := s.id
	s.mu.Unlock()
	if !bytes.Equal(datagram.DestHash, identity.PubKey[:mesh.PathHashSize]) {
		return
	}
	if pending.kind != pendingNone && !pendingKnown {
		contacts = append(contacts, contactEntry{info: companion.Contact{PublicKey: pending.publicKey}})
	}
	for _, contact := range contacts {
		if !bytes.Equal(datagram.SrcHash, contact.info.PublicKey[:mesh.PathHashSize]) {
			continue
		}
		secret, err := identity.SharedSecret(contact.info.PublicKey[:])
		if err != nil {
			continue
		}
		plain, err := datagram.Open(secret)
		if err == nil {
			s.consumeRemoteResponse(contact.info.PublicKey, plain, firstCorrelation(correlations))
			return
		}
	}
}

func (s *service) consumeRemoteResponse(publicKey [mesh.PubKeySize]byte, plain []byte,
	corr correlation.ID,
) {
	tag, body, err := mesh.UnframeAdmin(plain)
	if err != nil {
		return
	}
	s.mu.Lock()
	pending := s.pending
	if pending.kind == pendingNone || pending.publicKey != publicKey {
		s.mu.Unlock()
		return
	}
	var response companion.Response
	switch pending.kind {
	case pendingLogin:
		s.pending = pendingRequest{}
		response = s.loginPushLocked(publicKey, plain)
	case pendingStatus:
		if len(body) > 0 {
			s.pending = pendingRequest{}
			pushBody := append([]byte{0}, publicKey[:6]...)
			response = companion.Push{Code: companion.PushStatusResponse, Body: append(pushBody, body...)}
		}
	case pendingTelemetry:
		if len(body) > 0 && tag == pending.tag {
			s.pending = pendingRequest{}
			pushBody := append([]byte{0}, publicKey[:6]...)
			response = companion.Push{Code: companion.PushTelemetryResponse, Body: append(pushBody, body...)}
		}
	case pendingBinary:
		if len(body) > 0 && tag == pending.tag {
			s.pending = pendingRequest{}
			response = companion.Push{Code: companion.PushBinaryResponse,
				Body: append([]byte{0}, plain...)}
		}
	case pendingDiscovery, pendingNone:
	}
	s.mu.Unlock()
	if response != nil {
		s.push(response, corr)
	}
}

func (s *service) loginPushLocked(publicKey [mesh.PubKeySize]byte, plain []byte) companion.Response {
	_, body, err := mesh.UnframeAdmin(plain)
	if err == nil && bytes.HasPrefix(body, []byte("OK")) {
		return companion.Push{Code: companion.PushLoginSuccess,
			Body: append([]byte{0}, publicKey[:6]...)}
	}
	reply, err := mesh.ParseLoginReply(plain)
	if err == nil && reply.Result == mesh.LoginOK {
		keepAlive := time.Duration(reply.KeepAlive) * 16 * time.Second
		if keepAlive > 0 {
			if _, exists := s.connections[publicKey]; exists || len(s.connections) < maxConnections {
				now := time.Now()
				s.connections[publicKey] = remoteConnection{
					keepAlive: keepAlive, activeAt: now, nextPing: now.Add(keepAlive),
				}
			}
		}
		isAdmin := byte(0)
		if reply.IsAdmin {
			isAdmin = 1
		}
		pushBody := append([]byte{isAdmin}, publicKey[:6]...)
		pushBody = append(pushBody, mesh.FrameAdmin(reply.Clock, nil)...)
		pushBody = append(pushBody, reply.Permissions, reply.FirmwareLevel)
		return companion.Push{Code: companion.PushLoginSuccess, Body: pushBody}
	}
	return companion.Push{Code: companion.PushLoginFail, Body: append([]byte{0}, publicKey[:6]...)}
}

func (s *service) pruneConnectionsLocked(now time.Time) {
	for key, connection := range s.connections {
		if !connection.activeAt.IsZero() &&
			now.Sub(connection.activeAt) >= connection.keepAlive*5/2 {
			delete(s.connections, key)
		}
	}
}

func (s *service) checkConnections() {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.pruneConnectionsLocked(now)
	for key, connection := range s.connections {
		if now.Before(connection.nextPing) {
			continue
		}
		connection.nextPing = now.Add(connection.keepAlive)
		contact, exists := s.contacts[key]
		if !exists || contact.info.PathLen == 0xff {
			s.connections[key] = connection
			continue
		}
		secret, err := s.id.SharedSecret(key[:])
		if err != nil {
			s.connections[key] = connection
			continue
		}
		tag := s.uniqueTimestampLocked()
		body := mesh.FrameKeepAliveRequest(contact.syncSince)
		packet, err := mesh.BuildRequest(s.id, key[:], secret, tag, body)
		if err != nil {
			s.connections[key] = connection
			continue
		}
		s.routeContact(packet, contact, false)
		if responses := s.submitLocked(packet, "station-keep-alive"); responses == nil {
			connection.expectedACK = mesh.AckCRC(mesh.FrameAdmin(tag, body), s.id.PubKey[:])
		}
		s.connections[key] = connection
	}
}

func (s *service) receivePathDiscovery(contact contactEntry, packet *mesh.Packet, path *mesh.PathReturn,
	corr correlation.ID,
) bool {
	tag, err := mesh.ParseAck(path.Extra)
	if err != nil {
		return false
	}
	s.mu.Lock()
	pending := s.pending
	if pending.kind != pendingDiscovery || pending.publicKey != contact.info.PublicKey || pending.tag != tag {
		s.mu.Unlock()
		return false
	}
	s.pending = pendingRequest{}
	s.mu.Unlock()
	body := append([]byte{0}, contact.info.PublicKey[:6]...)
	body = append(body, path.PathLen)
	body = append(body, path.Path...)
	body = append(body, packet.PathLen)
	body = append(body, packet.Path...)
	s.push(companion.Push{Code: companion.PushPathDiscoveryResponse, Body: body}, corr)
	return true
}

func (s *service) receiveRaw(packet *mesh.Packet, frame radio.Frame) {
	if !packet.IsRouteDirect() || packet.PathHashCount() != 0 {
		return
	}
	body := make([]byte, 0, 3+len(packet.Payload))
	body = append(body, mesh.EncodeSNR(frame.SNR), byte(int8(frame.RSSI)), 0xff)
	s.push(companion.Push{Code: companion.PushRawData, Body: append(body, packet.Payload...)}, frame.Correlation)
}

func (s *service) receiveControl(packet *mesh.Packet, frame radio.Frame) {
	if !packet.IsRouteDirect() || packet.PathHashCount() != 0 || len(packet.Payload) == 0 || packet.Payload[0]&0x80 == 0 {
		return
	}
	body := make([]byte, 0, 3+len(packet.Payload))
	body = append(body, mesh.EncodeSNR(frame.SNR), byte(int8(frame.RSSI)), packet.PathLen)
	s.push(companion.Push{Code: companion.PushControlData, Body: append(body, packet.Payload...)}, frame.Correlation)
}
