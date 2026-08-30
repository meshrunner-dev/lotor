package meshcore

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"errors"
	"maps"
	"math"
	rand "math/rand/v2"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/correlation"
	"meshrunner.dev/lotor/internal/logging"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/station"

	mesh "meshrunner.dev/pkg/meshcore"
	"meshrunner.dev/pkg/meshcore/companion"
)

const (
	rfRetry         = time.Second
	stationLBTBound = 4 * time.Second
	stationLBTRetry = 200 * time.Millisecond
	stationDutyWait = 10 * time.Minute
)

type emission struct {
	packet      *mesh.Packet
	correlation correlation.ID
	kind        string
	notBefore   time.Time
	priority    uint8
}

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
			s.pushRawReception(frame)
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
	s.recordReception(frame)
	s.pushRawReception(frame)
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
	s.log.Debug("station frame received", zap.String("corr", frame.Correlation.Short()),
		zap.String("type", packet.PayloadType().String()), zap.String("route", packet.Route().String()))
	logging.Trace(s.log, "station radio reception",
		zap.String("corr", frame.Correlation.Short()), zap.Float64("rssi_dbm", frame.RSSI),
		zap.Float64("snr_db", frame.SNR), zap.Float64("frequency_error_hz", frame.FreqErrHz),
		zap.Duration("airtime", frame.Airtime), zap.Int("bytes", len(frame.Payload)))
	switch packet.PayloadType() {
	case mesh.PayloadTypeAck:
		s.receiveACK(packet.Payload)
	case mesh.PayloadTypeMultipart:
		multipart, err := mesh.ParseMultipart(packet.Payload)
		if err == nil && multipart.Inner == mesh.PayloadTypeAck {
			s.receiveACK(multipart.Data)
		}
	case mesh.PayloadTypeAdvert:
		s.receiveAdvert(ctx, packet)
	case mesh.PayloadTypePath:
		s.receivePath(ctx, packet)
	case mesh.PayloadTypeResponse:
		s.receiveRemoteResponse(packet)
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

func (s *service) recordReception(frame radio.Frame) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.received++
	s.stats.rxAir += max(time.Duration(0), frame.Airtime)
	s.stats.lastRSSIDBm = int8(max(float64(math.MinInt8), min(float64(math.MaxInt8), math.Trunc(frame.RSSI))))
	s.stats.lastSNRx4 = snrQuarter(frame.SNR)
}

func (s *service) recordCorruptReception(frame radio.Frame) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stats.receiveErrors++
	s.stats.rxAir += max(time.Duration(0), frame.Airtime)
	s.stats.lastRSSIDBm = int8(max(float64(math.MinInt8), min(float64(math.MaxInt8), math.Trunc(frame.RSSI))))
	s.stats.lastSNRx4 = snrQuarter(frame.SNR)
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
	body = append(body, byte(snrQuarter(frame.SNR)), byte(int8(math.Round(frame.RSSI))))
	body = append(body, frame.Payload...)
	if 1+len(body) <= companion.MaxPayload {
		s.push(companion.Push{Code: companion.PushLogRXData, Body: body})
	}
}

func (s *service) receiveAdvert(ctx context.Context, packet *mesh.Packet) {
	s.mu.Lock()
	before := s.snapshotLocked()
	stored, created, err := s.storeAdvert(packet, true)
	if err == nil && stored {
		err = s.persistLocked(ctx, before)
	}
	var response companion.Response
	if err == nil && stored {
		advert, _ := mesh.ParseAdvert(packet.Payload)
		entry := s.contacts[advert.Identity.PubKey]
		s.cacheAdvertPathLocked(entry.info.PublicKey, packet)
		if created {
			wire, _ := companion.MarshalResponse(companion.ContactResponse{Contact: entry.info})
			response = companion.Push{Code: companion.PushNewAdvert, Body: wire[1:]}
		} else {
			response = companion.Push{Code: companion.PushAdvert, Body: entry.info.PublicKey[:]}
		}
	}
	s.mu.Unlock()
	if err != nil {
		s.log.Error("station advert state failed", zap.Error(err))
		return
	}
	if response != nil {
		s.push(response)
	}
}

func (s *service) receiveDirectText(ctx context.Context, packet *mesh.Packet, frame radio.Frame) {
	datagram, err := mesh.ParseDatagram(packet.Payload)
	if err != nil || !bytes.Equal(datagram.DestHash, s.id.PubKey[:mesh.PathHashSize]) {
		return
	}
	s.mu.Lock()
	contacts := s.orderedContacts()
	s.mu.Unlock()
	for _, contact := range contacts {
		if !bytes.Equal(datagram.SrcHash, contact.info.PublicKey[:mesh.PathHashSize]) {
			continue
		}
		secret, err := s.id.SharedSecret(contact.info.PublicKey[:])
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
		var prefix []byte
		if message.Type == mesh.TxtTypeSignedPlain && len(plain) >= 9 {
			prefix = append([]byte(nil), plain[5:9]...)
		}
		pathLen := uint8(0xff)
		if packet.IsRouteFlood() {
			pathLen = packet.PathLen
		}
		s.mu.Lock()
		appVersion := s.appVersion
		s.mu.Unlock()
		var response companion.Response = companion.ContactMessageV3{
			SNRx4: snrQuarter(frame.SNR), SenderPrefix: [6]byte(contact.info.PublicKey[:6]),
			PathLen: pathLen, TextType: message.Type, UnixSeconds: uint32(message.Timestamp.Unix()),
			SignedPrefix: prefix, Text: message.Text,
		}
		if appVersion < 3 {
			response = companion.ContactMessage{
				SenderPrefix: [6]byte(contact.info.PublicKey[:6]), PathLen: pathLen,
				TextType: message.Type, UnixSeconds: uint32(message.Timestamp.Unix()),
				SignedPrefix: prefix, Text: message.Text,
			}
		}
		syncSince := uint32(0)
		if message.Type == mesh.TxtTypeSignedPlain {
			syncSince = uint32(message.Timestamp.Unix())
		}
		s.enqueueContactMailbox(ctx, contact.info.PublicKey, syncSince, response)
		s.replyToText(contact, packet, plain, message)
		return
	}
}

func (s *service) replyToText(contact contactEntry, received *mesh.Packet, plain []byte,
	message *mesh.TextPlaintext,
) {
	var ack []byte
	switch message.Type {
	case mesh.TxtTypePlain:
		ack = make([]byte, 6)
		binary.LittleEndian.PutUint32(ack, mesh.AckCRC(plain[:5+len(message.Text)], contact.info.PublicKey[:]))
		if tail := 5 + len(message.Text) + 1; tail < len(plain) {
			ack[4] = plain[tail]
		}
		_, _ = crand.Read(ack[5:])
	case mesh.TxtTypeCLIData:
		// A flooded command reply teaches us its sender's path but expects
		// no ACK body.
	case mesh.TxtTypeSignedPlain:
		ack = make([]byte, 4)
		binary.LittleEndian.PutUint32(ack,
			mesh.AckCRC(plain[:min(len(plain), 9+len(message.Text))], s.id.PubKey[:]))
	default:
		return
	}
	if received.IsRouteFlood() {
		s.sendPathReturn(contact, received, ack)
		return
	}
	if len(ack) > 0 {
		s.sendACK(contact, ack)
	}
}

func (s *service) sendPathReturn(contact contactEntry, received *mesh.Packet, ack []byte) {
	secret, err := s.id.SharedSecret(contact.info.PublicKey[:])
	if err != nil {
		return
	}
	extraType := uint8(0)
	if len(ack) > 0 {
		extraType = uint8(mesh.PayloadTypeAck)
	}
	packet, err := mesh.BuildPathReturn(contact.info.PublicKey[:mesh.PathHashSize],
		s.id.PubKey[:mesh.PathHashSize], secret, received.PathLen, received.Path, extraType, ack)
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

func (s *service) receiveACK(payload []byte) {
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
	body := make([]byte, 0, 8)
	body = binary.LittleEndian.AppendUint32(body, crc)
	body = binary.LittleEndian.AppendUint32(body, uint32(min(trip, int64(^uint32(0)))))
	s.push(companion.Push{Code: companion.PushSendConfirmed, Body: body})
}

func (s *service) receivePath(ctx context.Context, packet *mesh.Packet) {
	datagram, err := mesh.ParseDatagram(packet.Payload)
	if err != nil || !bytes.Equal(datagram.DestHash, s.id.PubKey[:mesh.PathHashSize]) {
		return
	}
	s.mu.Lock()
	contacts := s.orderedContacts()
	s.mu.Unlock()
	for _, candidate := range contacts {
		if !bytes.Equal(datagram.SrcHash, candidate.info.PublicKey[:mesh.PathHashSize]) {
			continue
		}
		secret, err := s.id.SharedSecret(candidate.info.PublicKey[:])
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
		if path.ExtraType == uint8(mesh.PayloadTypeResponse) &&
			s.receivePathDiscovery(candidate, packet, path) {
			return
		}
		if !s.storeContactPath(ctx, candidate.info.PublicKey, path) {
			return
		}
		s.push(companion.Push{Code: companion.PushPathUpdated, Body: candidate.info.PublicKey[:]})
		if path.ExtraType == uint8(mesh.PayloadTypeAck) {
			s.receiveACK(path.Extra)
		}
		if packet.IsRouteFlood() {
			s.sendReciprocalPath(candidate, packet, path, secret)
		}
		return
	}
}

func (s *service) storeContactPath(ctx context.Context, publicKey [mesh.PubKeySize]byte,
	path *mesh.PathReturn,
) bool {
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
	return true
}

func (s *service) sendReciprocalPath(contact contactEntry, received *mesh.Packet,
	returned *mesh.PathReturn, secret []byte,
) {
	packet, err := mesh.BuildPathReturn(contact.info.PublicKey[:mesh.PathHashSize],
		s.id.PubKey[:mesh.PathHashSize], secret, received.PathLen, received.Path, 0, nil)
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
				text := plain[5:]
				if end := bytes.IndexByte(text, 0); end >= 0 {
					text = text[:end]
				}
				s.enqueueGroupText(ctx, index, pathLen, frame.SNR,
					uint32(message.Timestamp.Unix()), string(text))
			}
			return
		}
		if len(plain) >= 3 && int(plain[2]) <= len(plain)-3 {
			s.enqueueMailbox(ctx, companion.ChannelData{
				SNRx4: snrQuarter(frame.SNR), Channel: index, PathLen: pathLen,
				DataType: uint16(plain[0]) | uint16(plain[1])<<8,
				Data:     append([]byte(nil), plain[3:3+int(plain[2])]...),
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
		SNRx4: snrQuarter(snr), Channel: channel, PathLen: pathLen,
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

func snrQuarter(snr float64) int8 {
	return int8(max(-128, min(127, math.Round(snr*4))))
}

func (s *service) enqueueMailbox(ctx context.Context, response companion.Response) {
	s.mu.Lock()
	before := s.snapshotLocked()
	accepted := s.enqueueMailboxLocked(response)
	var err error
	if accepted {
		err = s.persistLocked(ctx, before)
	}
	s.mu.Unlock()
	if err != nil {
		s.log.Error("station mailbox persistence failed", zap.Error(err))
		return
	}
	if accepted {
		s.push(companion.MessagesWaiting{})
	}
}

func (s *service) push(response companion.Response) {
	payload, err := companion.MarshalResponse(response)
	if err != nil {
		return
	}
	s.mu.Lock()
	conn, generation := s.client, s.generation
	s.mu.Unlock()
	if conn == nil {
		return
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if !s.currentClient(conn, generation) {
		return
	}
	if err := companion.WriteFrame(conn, companion.ToApplication, payload); err != nil {
		logging.Trace(s.log, "companion push failed", zap.Error(err))
	}
}

func (s *service) runTX(ctx context.Context) {
	nextConnections := time.Now().Add(time.Second)
	for {
		if !time.Now().Before(nextConnections) {
			s.checkConnections()
			nextConnections = time.Now().Add(time.Second)
		}
		item, ok := s.outbound.takeUntil(ctx, nextConnections)
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

func (s *service) transmit(ctx context.Context, item emission) {
	s.mu.Lock()
	device, ledger, policy, power := s.rfDevice, s.duty, s.txPolicy, s.p.TXPowerDBm
	s.mu.Unlock()
	if device == nil || ledger == nil {
		s.txDrop(item, "radio-down")
		return
	}
	raw, err := item.packet.MarshalBinary()
	if err != nil {
		s.txDrop(item, "malformed")
		return
	}
	airtime := device.Airtime(len(raw))
	reservation := s.reserveDuty(ctx, ledger, airtime, item)
	if reservation == nil {
		return
	}
	defer reservation.Cancel()
	if !s.stationClearChannel(ctx, device, policy, item) {
		return
	}
	shadow := policy.Mode == config.TXShadow
	at, actualAir, actualPower := time.Now(), airtime, power
	var txErr error
	if !shadow {
		report, err := device.Transmit(ctx, raw, power)
		txErr = err
		if report.Airtime > 0 {
			at, actualAir, actualPower = report.At, report.Airtime, report.PowerDBm
		}
		if err != nil && report.Airtime == 0 {
			s.txDrop(item, "tx-failed")
			return
		}
	}
	reservation.Commit(at, actualAir)
	s.recordTransmission(item.packet, actualAir)
	if s.bus != nil {
		s.bus.Publish(bus.FrameSent{SourceKind: bus.SourceStation, Source: s.name,
			Correlation: item.correlation, At: at,
			Airtime: actualAir, PowerDBm: actualPower, Kind: item.kind, Shadow: shadow, Raw: raw})
	}
	s.log.Debug("station frame sent", zap.String("corr", item.correlation.Short()),
		zap.String("kind", item.kind), zap.Uint8("priority", item.priority),
		zap.Bool("shadow", shadow), zap.Error(txErr))
	logging.Trace(s.log, "station tx emission accounted", zap.String("corr", item.correlation.Short()),
		zap.Uint8("priority", item.priority), zap.Duration("airtime", actualAir),
		zap.Int8("power_dbm", actualPower), zap.Bool("shadow", shadow))
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

func (s *service) reserveDuty(ctx context.Context, ledger *radio.AirtimeLedger,
	airtime time.Duration, item emission,
) *radio.AirtimeReservation {
	deadline := time.Now().Add(stationDutyWait)
	for {
		now := time.Now()
		reservation, freeAt, never := ledger.Reserve(now, airtime)
		if reservation != nil {
			return reservation
		}
		if never || freeAt.After(deadline) {
			s.txDrop(item, "duty")
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(max(0, freeAt.Sub(now))):
		}
	}
}

func (s *service) stationClearChannel(ctx context.Context, device radio.Device,
	policy station.TXPolicy, item emission,
) bool {
	if !policy.CAD {
		return true
	}
	deadline := time.Now().Add(stationLBTBound)
	for {
		busy, err := device.AssessChannel(ctx, policy.LBTThresholdDB)
		if errors.Is(err, radio.ErrBusyReceiving) {
			s.requeue(item)
			return false
		}
		if err != nil {
			s.txDrop(item, "lbt-failed")
			return false
		}
		if !busy {
			return true
		}
		if time.Now().After(deadline) {
			if policy.LBTExhausted == config.LBTDrop {
				s.txDrop(item, "lbt")
				return false
			}
			s.log.Warn("station channel busy past the LBT bound, transmitting anyway",
				zap.String("corr", item.correlation.Short()))
			return true
		}
		retry := stationLBTRetry/2 + rand.N(stationLBTRetry) //nolint:gosec // timing jitter, not security
		select {
		case <-ctx.Done():
			return false
		case <-time.After(retry):
		}
	}
}

func (s *service) requeue(item emission) {
	if !s.outbound.offer(item) {
		s.txDrop(item, "queue-full")
	}
}

func (s *service) txDrop(item emission, reason string) {
	s.log.Debug("station frame dropped", zap.String("corr", item.correlation.Short()),
		zap.String("kind", item.kind), zap.Uint8("priority", item.priority), zap.String("reason", reason))
	if s.bus != nil {
		s.bus.Publish(bus.TxDropped{SourceKind: bus.SourceStation, Source: s.name,
			Correlation: item.correlation,
			At:          time.Now(), Reason: reason, Kind: item.kind})
	}
}
