package meshcore

import (
	"bytes"
	"context"
	"errors"
	"maps"
	"math"
	"math/rand/v2"
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
	packet, err := mesh.ParsePacket(frame.Payload)
	if err != nil {
		s.log.Debug("station frame malformed", zap.String("corr", frame.Correlation.Short()), zap.Error(err))
		return
	}
	s.log.Debug("station frame received", zap.String("corr", frame.Correlation.Short()),
		zap.String("type", packet.PayloadType().String()), zap.String("route", packet.Route().String()))
	logging.Trace(s.log, "station radio reception",
		zap.String("corr", frame.Correlation.Short()), zap.Float64("rssi_dbm", frame.RSSI),
		zap.Float64("snr_db", frame.SNR), zap.Float64("frequency_error_hz", frame.FreqErrHz),
		zap.Duration("airtime", frame.Airtime), zap.Int("bytes", len(frame.Payload)))
	switch packet.PayloadType() {
	case mesh.PayloadTypeAdvert:
		s.receiveAdvert(ctx, packet)
	case mesh.PayloadTypeTxtMsg:
		s.receiveDirectText(ctx, packet, frame)
	case mesh.PayloadTypeGrpTxt, mesh.PayloadTypeGrpData:
		s.receiveGroup(ctx, packet, frame)
	default:
		return
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
		s.enqueueMailbox(ctx, companion.ContactMessageV3{
			SNRx4: snrQuarter(frame.SNR), SenderPrefix: [6]byte(contact.info.PublicKey[:6]),
			PathLen: pathLen, TextType: message.Type, UnixSeconds: uint32(message.Timestamp.Unix()),
			SignedPrefix: prefix, Text: message.Text,
		})
		return
	}
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
				s.enqueueMailbox(ctx, companion.ChannelMessageV3{
					SNRx4: snrQuarter(frame.SNR), Channel: index, PathLen: pathLen,
					TextType: mesh.TxtTypePlain, UnixSeconds: uint32(message.Timestamp.Unix()),
					Text: string(text),
				})
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
	for {
		select {
		case <-ctx.Done():
			return
		case item := <-s.outbound:
			s.transmit(ctx, item)
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
	if s.bus != nil {
		s.bus.Publish(bus.FrameSent{Relay: s.name, Correlation: item.correlation, At: at,
			Airtime: actualAir, PowerDBm: actualPower, Kind: item.kind, Shadow: shadow, Raw: raw})
	}
	s.log.Debug("station frame sent", zap.String("corr", item.correlation.Short()),
		zap.String("kind", item.kind), zap.Bool("shadow", shadow), zap.Error(txErr))
	logging.Trace(s.log, "station tx emission accounted", zap.String("corr", item.correlation.Short()),
		zap.Duration("airtime", actualAir), zap.Int8("power_dbm", actualPower), zap.Bool("shadow", shadow))
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
	select {
	case s.outbound <- item:
	default:
		s.txDrop(item, "queue-full")
	}
}

func (s *service) txDrop(item emission, reason string) {
	s.log.Debug("station frame dropped", zap.String("corr", item.correlation.Short()),
		zap.String("kind", item.kind), zap.String("reason", reason))
	if s.bus != nil {
		s.bus.Publish(bus.TxDropped{Relay: s.name, Correlation: item.correlation,
			At: time.Now(), Reason: reason, Kind: item.kind})
	}
}
