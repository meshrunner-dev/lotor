package meshcore

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"math"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/correlation"
	"meshrunner.dev/lotor/internal/logging"
	"meshrunner.dev/lotor/internal/radio"

	mesh "meshrunner.dev/pkg/meshcore"
	"meshrunner.dev/pkg/meshcore/companion"
)

const rfRetry = time.Second

func (s *service) runRF(ctx context.Context) {
	txDone := make(chan struct{})
	go func() {
		defer close(txDone)
		s.runTX(ctx)
	}()
	defer func() { <-txDone }()
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
				logging.Trace(s.log, "station radio session unavailable", zap.Error(err))
			}
			s.waitRF(ctx)
			continue
		}
		err = device.Configure(s.RadioDemand().Waveform)
		if err == nil {
			err = device.StartReceive()
		}
		if err == nil {
			err = s.receiveRF(ctx, device)
		}
		s.clearRFDevice(device)
		_ = device.Close()
		if ctx.Err() == nil && err != nil && !errors.Is(err, radio.ErrControllerDown) {
			logging.Trace(s.log, "station radio session ended", zap.Error(err))
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
	s.mu.Lock()
	s.rfDevice = device
	s.mu.Unlock()
	for {
		frame, err := device.Receive(ctx)
		if errors.Is(err, radio.ErrCorrupt) {
			s.recordCorruptReception(frame)
			logging.Trace(s.log, "station radio corrupt reception",
				zap.String("corr", frame.Correlation.Short()), zap.Error(err))
			continue
		}
		if err != nil {
			return err
		}
		s.processRF(ctx, frame)
	}
}

func (s *service) clearRFDevice(device radio.Device) {
	s.mu.Lock()
	if s.rfDevice == device {
		s.rfDevice = nil
	}
	s.mu.Unlock()
}

func (s *service) processRF(ctx context.Context, frame radio.Frame) {
	ctx = s.beginRFReception(ctx, frame)
	packet, err := mesh.ParsePacket(frame.Payload)
	if err != nil {
		s.log.Debug("station frame malformed", zap.String("corr", frame.Correlation.Short()), zap.Error(err))
		return
	}
	s.recordReceivedRoute(packet)
	s.mu.Lock()
	duplicate := s.seen.witness(packet.Hash())
	s.mu.Unlock()
	if duplicate {
		s.log.Debug("station frame duplicate", zap.String("corr", frame.Correlation.Short()))
		return
	}
	s.logReception(frame, packet)
	switch packet.PayloadType() {
	case mesh.PayloadTypeAck:
		s.receiveACK(packet.Payload, frame.Correlation)
	case mesh.PayloadTypeMultipart:
		multipart, err := mesh.ParseMultipart(packet.Payload)
		if err == nil && multipart.Inner == mesh.PayloadTypeAck {
			s.receiveACK(multipart.Data, frame.Correlation)
		}
	case mesh.PayloadTypeAdvert:
		s.receiveAdvert(ctx, packet)
	case mesh.PayloadTypePath:
		s.receivePath(ctx, packet)
	case mesh.PayloadTypeResponse:
		s.receiveRemoteResponse(packet, frame.Correlation)
	case mesh.PayloadTypeTxtMsg:
		s.receiveDirectText(ctx, packet, frame)
	case mesh.PayloadTypeGrpTxt, mesh.PayloadTypeGrpData:
		s.receiveGroup(ctx, packet, frame)
	case mesh.PayloadTypeRawCustom:
		s.receiveRaw(packet, frame)
	case mesh.PayloadTypeControl:
		s.receiveControl(packet, frame)
	case mesh.PayloadTypeTrace:
		s.receiveTrace(packet, frame)
	default:
		return
	}
}

func (s *service) logReception(frame radio.Frame, packet *mesh.Packet) {
	trafficFields := []zap.Field{
		zap.String("corr", frame.Correlation.Short()),
		zap.String("type", packet.PayloadType().String()), zap.String("route", packet.Route().String()),
	}
	if frame.HasRFMeasurements() {
		s.log.Debug("station frame received", trafficFields...)
		logging.Trace(s.log, "station radio reception",
			zap.String("corr", frame.Correlation.Short()), zap.Float64("rssi_dbm", frame.RSSI),
			zap.Float64("snr_db", frame.SNR), zap.Float64("frequency_error_hz", frame.FreqErrHz),
			zap.Duration("airtime", frame.Airtime), zap.Int("bytes", len(frame.Payload)))
	} else {
		trafficFields = append(trafficFields, zap.String("binding", frame.Binding))
		localFields := []zap.Field{
			zap.String("corr", frame.Correlation.Short()), zap.String("binding", frame.Binding),
		}
		if !frame.CausedBy.IsZero() {
			trafficFields = append(trafficFields, zap.String("caused_by", frame.CausedBy.Short()))
			localFields = append(localFields, zap.String("caused_by", frame.CausedBy.Short()))
		}
		s.log.Debug("station frame handed over", trafficFields...)
		logging.Trace(s.log, "station local frame hand-over", localFields...)
	}
}

func (s *service) beginRFReception(ctx context.Context, frame radio.Frame) context.Context {
	s.recordReception(frame)
	// Handed-over frames included: applications date a contact from
	// this log, and PUSH_CODE_ADVERT carries no time.
	s.pushRawReception(frame)
	return correlation.WithContext(ctx, frame.Correlation)
}

func (s *service) recordReception(frame radio.Frame) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.received++
	if !frame.HasRFMeasurements() {
		// Handed over by a peer: a packet, counted as one, but the
		// antenna did nothing. StatsRadio is the antenna's account,
		// and the airtime went on transmitting — already counted by
		// the binding that keyed the chip.
		return
	}
	s.stats.rxAir += max(time.Duration(0), frame.Airtime)
	s.stats.lastRSSIDBm = int8(max(float64(math.MinInt8), min(float64(math.MaxInt8), math.Trunc(frame.RSSI))))
	s.stats.lastSNRx4 = int8(mesh.EncodeSNR(frame.SNR))
}

func (s *service) recordCorruptReception(frame radio.Frame) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.receiveErrors++
	s.stats.rxAir += max(time.Duration(0), frame.Airtime)
	// lastRSSI and lastSNR hold the last frame a demodulator produced;
	// a CRC refusal has no measurement of its own to contribute.
}

func (s *service) recordReceivedRoute(packet *mesh.Packet) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if packet.IsRouteFlood() {
		s.stats.receivedFlood++
	} else if packet.IsRouteDirect() {
		s.stats.receivedDirect++
	}
}

func (s *service) pushRawReception(frame radio.Frame) {
	body := make([]byte, 0, 2+len(frame.Payload))
	body = append(body, mesh.EncodeSNR(frame.SNR), byte(int8(math.Round(frame.RSSI))))
	body = append(body, frame.Payload...)
	if 1+len(body) <= companion.MaxPayload {
		s.push(companion.Push{Code: companion.PushLogRXData, Body: body}, frame.Correlation)
	}
}

func (s *service) receiveAdvert(ctx context.Context, packet *mesh.Packet) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.mu.Lock()
	before := s.snapshotLocked()
	result, err := s.storeAdvert(packet, true)
	if err == nil && result.stored {
		err = s.persistLocked(ctx, before)
	}
	var responses []companion.Response
	if err == nil {
		responses = s.advertResponsesLocked(result, packet)
	}
	s.mu.Unlock()
	if err != nil {
		s.log.Error("station advert state failed", zap.Error(err))
		return
	}
	if result.stored {
		fields := []zap.Field{
			zap.String("contact", contactPrefix(result.contact.PublicKey)),
			zap.Bool("created", result.created), zap.Bool("evicted", result.hadEviction),
		}
		if corr, ok := correlation.FromContext(ctx); ok {
			fields = append(fields, zap.String("corr", corr.Short()))
		}
		s.log.Debug("station contact advert stored", fields...)
	}
	for _, response := range responses {
		s.push(response, correlationFromContext(ctx))
	}
}

func (s *service) advertResponsesLocked(result advertStoreResult,
	packet *mesh.Packet,
) []companion.Response {
	responses := make([]companion.Response, 0, 2)
	if result.hadEviction {
		responses = append(responses, companion.Push{
			Code: companion.PushContactDeleted, Body: result.evicted[:],
		})
	}
	if result.announce {
		s.cacheAdvertPathLocked(result.contact.PublicKey, packet)
		if result.created {
			wire, _ := companion.MarshalResponse(companion.ContactResponse{Contact: result.contact})
			responses = append(responses, companion.Push{Code: companion.PushNewAdvert, Body: wire[1:]})
		} else {
			responses = append(responses, companion.Push{
				Code: companion.PushAdvert, Body: result.contact.PublicKey[:],
			})
		}
	}
	if result.full {
		responses = append(responses, companion.Push{Code: companion.PushContactsFull})
	}
	return responses
}

func (s *service) receiveDirectText(ctx context.Context, packet *mesh.Packet, frame radio.Frame) {
	identity := s.identitySnapshot()
	datagram, err := mesh.ParseDatagram(packet.Payload)
	if err != nil || !bytes.Equal(datagram.DestHash, identity.PubKey[:mesh.PathHashSize]) {
		return
	}
	s.mu.Lock()
	contacts := s.orderedContacts()
	s.mu.Unlock()
	for _, contact := range contacts {
		if !bytes.Equal(datagram.SrcHash, contact.info.PublicKey[:mesh.PathHashSize]) {
			continue
		}
		secret, err := identity.SharedSecret(contact.info.PublicKey[:])
		if err != nil {
			continue
		}
		plain, err := datagram.Open(secret)
		if err != nil {
			continue
		}
		message, err := mesh.ParseTextPlaintext(plain)
		if err != nil || (message.Type != mesh.TxtTypePlain && message.Type != mesh.TxtTypeCLIData &&
			message.Type != mesh.TxtTypeSignedPlain) {
			return
		}
		pathLen := uint8(0xff)
		if packet.IsRouteFlood() {
			pathLen = packet.PathLen
		}
		s.mu.Lock()
		appVersion := s.appVersion
		s.mu.Unlock()
		var response companion.Response = companion.ContactMessageV3{
			SNRx4: int8(mesh.EncodeSNR(frame.SNR)), SenderPrefix: [6]byte(contact.info.PublicKey[:6]),
			PathLen: pathLen, TextType: message.Type, UnixSeconds: uint32(message.Timestamp.Unix()),
			SignedPrefix: message.SignedPrefix, Text: message.Text,
		}
		if appVersion < 3 {
			response = companion.ContactMessage{
				SenderPrefix: [6]byte(contact.info.PublicKey[:6]), PathLen: pathLen,
				TextType: message.Type, UnixSeconds: uint32(message.Timestamp.Unix()),
				SignedPrefix: message.SignedPrefix, Text: message.Text,
			}
		}
		syncSince := uint32(0)
		if message.Type == mesh.TxtTypeSignedPlain {
			syncSince = uint32(message.Timestamp.Unix())
		}
		s.enqueueContactMailbox(ctx, contact.info.PublicKey, syncSince, response)
		s.replyToText(identity, contact, packet, plain, message)
		return
	}
}

func (s *service) replyToText(identity *mesh.LocalIdentity, contact contactEntry,
	received *mesh.Packet, plain []byte,
	message *mesh.TextPlaintext,
) {
	ackKey := contact.info.PublicKey[:]
	if message.Type == mesh.TxtTypeSignedPlain {
		ackKey = identity.PubKey[:]
	}
	ack, err := mesh.BuildTextAckBody(plain, ackKey)
	if err != nil {
		return
	}
	if received.IsRouteFlood() {
		s.sendPathReturn(identity, contact, received, ack)
		return
	}
	if len(ack) > 0 {
		s.sendACK(contact, ack)
	}
}

func (s *service) sendPathReturn(identity *mesh.LocalIdentity, contact contactEntry,
	received *mesh.Packet, ack []byte,
) {
	secret, err := identity.SharedSecret(contact.info.PublicKey[:])
	if err != nil {
		return
	}
	extraType := uint8(0)
	if len(ack) > 0 {
		extraType = uint8(mesh.PayloadTypeAck)
	}
	packet, err := mesh.BuildPathReturn(contact.info.PublicKey[:mesh.PathHashSize],
		identity.PubKey[:mesh.PathHashSize], secret, received.PathLen, received.Path, extraType, ack)
	if err != nil {
		return
	}
	s.mu.Lock()
	s.routeFlood(packet)
	_ = s.submitAtLocked(packet, "station-path-return", time.Now().Add(200*time.Millisecond))
	s.mu.Unlock()
}

func (s *service) sendACK(contact contactEntry, ack []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pathLen := contact.info.PathLen
	if pathLen == 0xff {
		packet, err := mesh.BuildAck(ack)
		if err != nil {
			return
		}
		s.routeFlood(packet)
		_ = s.submitAtLocked(packet, "station-ack-flood", time.Now().Add(200*time.Millisecond))
		return
	}
	path := contact.info.Path[:pathByteLen(pathLen)]
	delay := 200 * time.Millisecond
	if s.p.MultiACKs > 0 {
		packet, err := mesh.BuildMultiAck(ack, 1)
		if err == nil {
			s.routeDirect(packet, pathLen, path)
			_ = s.submitAtLocked(packet, "station-multi-ack", time.Now().Add(delay))
		}
		delay += 300 * time.Millisecond
	}
	packet, err := mesh.BuildAck(ack)
	if err == nil {
		s.routeDirect(packet, pathLen, path)
		_ = s.submitAtLocked(packet, "station-ack-direct", time.Now().Add(delay))
	}
}

func (s *service) receiveACK(payload []byte, correlations ...correlation.ID) {
	crc, err := mesh.ParseAck(payload)
	if err != nil {
		return
	}
	s.mu.Lock()
	var matched ackExpectation
	for i := range s.expectedACKs {
		if s.expectedACKs[i].used && s.expectedACKs[i].crc == crc {
			matched = s.expectedACKs[i]
			s.expectedACKs[i] = ackExpectation{}
			break
		}
	}
	if !matched.used {
		for key, connection := range s.connections {
			if connection.expectedACK == crc && crc != 0 {
				connection.expectedACK = 0
				connection.activeAt = time.Now()
				connection.nextPing = connection.activeAt.Add(connection.keepAlive)
				s.connections[key] = connection
				s.mu.Unlock()
				return
			}
		}
	}
	s.mu.Unlock()
	if !matched.used {
		return
	}
	trip := max(0, time.Since(matched.at).Milliseconds())
	s.push(companion.SendConfirmed{
		ExpectedACK: crc,
		TripMillis:  uint32(min(trip, int64(^uint32(0)))),
	}, firstCorrelation(correlations))
}

func (s *service) receivePath(ctx context.Context, packet *mesh.Packet) {
	identity := s.identitySnapshot()
	datagram, err := mesh.ParseDatagram(packet.Payload)
	if err != nil || !bytes.Equal(datagram.DestHash, identity.PubKey[:mesh.PathHashSize]) {
		return
	}
	s.mu.Lock()
	contacts := s.orderedContacts()
	s.mu.Unlock()
	for _, candidate := range contacts {
		if !bytes.Equal(datagram.SrcHash, candidate.info.PublicKey[:mesh.PathHashSize]) {
			continue
		}
		secret, err := identity.SharedSecret(candidate.info.PublicKey[:])
		if err != nil {
			continue
		}
		plain, err := datagram.Open(secret)
		if err != nil {
			continue
		}
		path, err := mesh.DecodePathReturn(plain)
		if err != nil {
			return
		}
		corr := correlationFromContext(ctx)
		if path.ExtraType == uint8(mesh.PayloadTypeResponse) &&
			s.receivePathDiscovery(candidate, packet, path, corr) {
			return
		}
		if !s.storeContactPath(ctx, candidate.info.PublicKey, path) {
			return
		}
		s.push(companion.Push{Code: companion.PushPathUpdated, Body: candidate.info.PublicKey[:]}, corr)
		if path.ExtraType == uint8(mesh.PayloadTypeAck) {
			s.receiveACK(path.Extra, corr)
		}
		// A question we flooded is answered inside the path return
		// that teaches the way back, so the answer arrives as this
		// packet's extra rather than as a response of its own. Route
		// learnt first, then the answer: the reply may set the client
		// asking again, and it should ask down the fresh route.
		if path.ExtraType == uint8(mesh.PayloadTypeResponse) {
			s.consumeRemoteResponse(candidate.info.PublicKey, path.Extra, corr)
		}
		if packet.IsRouteFlood() {
			s.sendReciprocalPath(identity, candidate, packet, path, secret)
		}
		return
	}
}

func (s *service) storeContactPath(ctx context.Context, publicKey [mesh.PubKeySize]byte,
	path *mesh.PathReturn,
) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, exists := s.contacts[publicKey]
	if !exists {
		return false
	}
	before := s.snapshotLocked()
	entry.info.PathLen = path.PathLen
	clear(entry.info.Path[:])
	copy(entry.info.Path[:], path.Path)
	entry.info.LastModifiedUnix = s.uniqueTimestampLocked()
	s.contacts[publicKey] = entry
	if err := s.persistLocked(ctx, before); err != nil {
		s.log.Error("station contact path persistence failed", zap.Error(err))
		return false
	}
	fields := []zap.Field{zap.String("contact", contactPrefix(publicKey)), zap.Uint8("path_len", path.PathLen)}
	if corr, ok := correlation.FromContext(ctx); ok {
		fields = append(fields, zap.String("corr", corr.Short()))
	}
	s.log.Debug("station contact path changed", fields...)
	return true
}

func (s *service) sendReciprocalPath(identity *mesh.LocalIdentity, contact contactEntry,
	received *mesh.Packet,
	returned *mesh.PathReturn, secret []byte,
) {
	packet, err := mesh.BuildPathReturn(contact.info.PublicKey[:mesh.PathHashSize],
		identity.PubKey[:mesh.PathHashSize], secret, received.PathLen, received.Path, 0, nil)
	if err != nil {
		return
	}
	s.mu.Lock()
	s.routeDirect(packet, returned.PathLen, returned.Path)
	_ = s.submitAtLocked(packet, "station-path-reciprocal", time.Now().Add(500*time.Millisecond))
	s.mu.Unlock()
}

func (s *service) receiveGroup(ctx context.Context, packet *mesh.Packet, frame radio.Frame) {
	datagram, err := mesh.ParseGroupDatagram(packet.Payload)
	if err != nil {
		return
	}
	s.mu.Lock()
	channels := make(map[uint8]channel, len(s.channels))
	maps.Copy(channels, s.channels)
	s.mu.Unlock()
	for index, item := range channels {
		group, err := mesh.NewGroupChannel(item.secret[:])
		if err != nil || !bytes.Equal(group.Hash, datagram.ChannelHash) {
			continue
		}
		plain, err := datagram.Open(group)
		if err != nil {
			continue
		}
		pathLen := uint8(0xff)
		if packet.IsRouteFlood() {
			pathLen = packet.PathLen
		}
		if packet.PayloadType() == mesh.PayloadTypeGrpTxt {
			message, err := mesh.ParseGroupText(plain)
			if err == nil {
				text := message.Text
				if message.Sender != "" {
					text = message.Sender + ": " + text
				}
				s.enqueueGroupText(ctx, index, pathLen, frame.SNR,
					uint32(message.Timestamp.Unix()), text)
			}
			return
		}
		message, err := mesh.ParseGroupData(plain)
		if err == nil {
			s.enqueueMailbox(ctx, companion.ChannelData{
				SNRx4: int8(mesh.EncodeSNR(frame.SNR)), Channel: index, PathLen: pathLen,
				DataType: message.Type, Data: message.Data,
			})
		}
		return
	}
}

func (s *service) enqueueGroupText(ctx context.Context, channel, pathLen uint8, snr float64,
	timestamp uint32, text string,
) {
	s.mu.Lock()
	appVersion := s.appVersion
	s.mu.Unlock()
	var response companion.Response = companion.ChannelMessageV3{
		SNRx4: int8(mesh.EncodeSNR(snr)), Channel: channel, PathLen: pathLen,
		TextType: mesh.TxtTypePlain, UnixSeconds: timestamp, Text: text,
	}
	if appVersion < 3 {
		response = companion.ChannelMessage{
			Channel: channel, PathLen: pathLen, TextType: mesh.TxtTypePlain,
			UnixSeconds: timestamp, Text: text,
		}
	}
	s.enqueueMailbox(ctx, response)
}

func (s *service) identitySnapshot() *mesh.LocalIdentity {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.id
}

func (s *service) enqueueMailbox(ctx context.Context, response companion.Response) {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	s.mu.Lock()
	before := s.snapshotLocked()
	result, encodeErr := s.enqueueMailboxLocked(response, correlationFromContext(ctx))
	var err error
	if result.accepted {
		err = s.persistLocked(ctx, before)
	}
	s.mu.Unlock()
	if encodeErr != nil {
		s.log.Error("station mailbox encode failed", zap.Error(encodeErr))
		return
	}
	if err != nil {
		s.log.Error("station mailbox persistence failed", zap.Error(err))
		return
	}
	s.logMailboxEnqueue(result)
	if result.accepted {
		s.push(companion.MessagesWaiting{}, result.corr)
	}
}

func (s *service) push(response companion.Response, correlations ...correlation.ID) {
	corr := firstCorrelation(correlations)
	payload, err := companion.MarshalResponse(response)
	if err != nil {
		fields := []zap.Field{zap.Error(err)}
		if !corr.IsZero() {
			fields = append(fields, zap.String("corr", corr.Short()))
		}
		s.log.Error("companion push encode failed", fields...)
		return
	}
	s.mu.Lock()
	conn, generation := s.client, s.generation
	s.mu.Unlock()
	if conn == nil {
		return
	}
	item := companionPush{conn: conn, generation: generation, payload: payload, corr: corr}
	select {
	case s.pushes <- item:
	default:
		fields := []zap.Field{zap.Uint8("code", responseCode(payload)), zap.Int("bytes", len(payload))}
		if !corr.IsZero() {
			fields = append(fields, zap.String("corr", corr.Short()))
		}
		s.log.Warn("companion push queue full, dropping push", fields...)
	}
}

func (s *service) runPushes(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case item := <-s.pushes:
			s.writePush(item)
		}
	}
}

func (s *service) writePush(item companionPush) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if !s.currentClient(item.conn, item.generation) {
		return
	}
	fields := []zap.Field{
		zap.Uint8("code", responseCode(item.payload)),
		zap.Int("bytes", len(item.payload)),
	}
	if !item.corr.IsZero() {
		fields = append(fields, zap.String("corr", item.corr.Short()))
	}
	if err := item.conn.SetWriteDeadline(time.Now().Add(companionWriteTimeout)); err != nil {
		logging.Trace(s.log, "companion push deadline failed", append(fields, zap.Error(err))...)
		_ = item.conn.Close()
		return
	}
	if err := companion.WriteFrame(item.conn, companion.ToApplication, item.payload); err != nil {
		logging.Trace(s.log, "companion push failed", append(fields, zap.Error(err))...)
		_ = item.conn.Close()
		return
	}
	_ = item.conn.SetWriteDeadline(time.Time{})
	logging.Trace(s.log, "companion push sent", fields...)
}

func (s *service) runTX(ctx context.Context) {
	nextConnections := time.Now().Add(time.Second)
	for {
		if !time.Now().Before(nextConnections) {
			s.checkConnections()
			nextConnections = time.Now().Add(time.Second)
		}
		item, ok := s.outbound.TakeUntil(ctx, nextConnections)
		if !ok {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		s.transmit(ctx, item)
		if ctx.Err() != nil {
			return
		}
	}
}

// transmit carries one emission through the shared pipeline with the
// radio this station holds right now, and keeps the tally of what
// went on the air.
func (s *service) transmit(ctx context.Context, item emission) {
	s.mu.Lock()
	device, ledger, policy, power := s.rfDevice, s.duty, s.txPolicy, s.p.TXPowerDBm
	s.mu.Unlock()
	out := s.pipeline.Emit(ctx, item, device, ledger, originPolicy(policy), power)
	if !out.Sent {
		return
	}
	if packet, ok := item.Subject.(*mesh.Packet); ok {
		s.recordTransmission(packet, out.Airtime)
	}
}

func (s *service) recordTransmission(packet *mesh.Packet, airtime time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.sent++
	s.stats.txAir += max(time.Duration(0), airtime)
	if packet.IsRouteFlood() {
		s.stats.sentFlood++
	} else if packet.IsRouteDirect() {
		s.stats.sentDirect++
	}
}

// txDrop refuses one emission for a reason the journal records.
func (s *service) txDrop(item emission, reason string) {
	s.pipeline.Drop(item, reason)
}
