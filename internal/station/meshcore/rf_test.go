package meshcore

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/correlation"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/station"

	mesh "meshrunner.dev/pkg/meshcore"
	"meshrunner.dev/pkg/meshcore/companion"
)

type stationRadio struct {
	transmits int
	assesses  int
	noise     radio.NoiseFloor
	noiseOK   bool
	assessErr error
	txStarted chan struct{}
	txRelease chan struct{}
}

func (*stationRadio) Envelope() radio.Envelope {
	return radio.Envelope{MaxTxPowerSet: true, MaxTxPowerDBm: 22, ChipMinDBm: -9, ChipMaxDBm: 22}
}
func (*stationRadio) Configure(radio.Waveform) error { return nil }
func (*stationRadio) StartReceive() error            { return nil }
func (*stationRadio) Receive(ctx context.Context) (radio.Frame, error) {
	<-ctx.Done()
	return radio.Frame{}, ctx.Err()
}
func (r *stationRadio) NoiseFloor() (radio.NoiseFloor, bool) { return r.noise, r.noiseOK }
func (*stationRadio) NoiseStarved() uint64                   { return 0 }
func (*stationRadio) ChipStats() (radio.ChipStats, bool)     { return radio.ChipStats{}, false }
func (r *stationRadio) Transmit(ctx context.Context, _ []byte, power int8) (radio.TxReport, error) {
	r.transmits++
	if r.txStarted != nil {
		close(r.txStarted)
	}
	if r.txRelease != nil {
		select {
		case <-ctx.Done():
			return radio.TxReport{}, ctx.Err()
		case <-r.txRelease:
		}
	}
	return radio.TxReport{At: time.Now(), Airtime: 100 * time.Millisecond, PowerDBm: power}, nil
}
func (r *stationRadio) AssessChannel(context.Context, float64) (bool, error) {
	r.assesses++
	return false, r.assessErr
}
func (*stationRadio) Airtime(int) time.Duration { return 100 * time.Millisecond }
func (*stationRadio) Close() error              { return nil }

func TestRFGroupMailboxSurvivesRestartAndSync(t *testing.T) {
	store := &memoryStationState{}
	spec := testSpec(t)
	spec.State = store
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	_ = svc.handle(t.Context(), companion.DeviceQuery{TargetVersion: 3})
	secret := [16]byte{1, 2, 3}
	_ = svc.handle(t.Context(), companion.SetChannel{Index: 2, Name: "ops", Secret: secret})
	group, err := mesh.NewGroupChannel(secret[:])
	if err != nil {
		t.Fatal(err)
	}
	packet, err := mesh.BuildGroupDatagram(mesh.PayloadTypeGrpTxt, group,
		mesh.BuildGroupText(time.Unix(1_800_000_000, 0), "bob", "hello"))
	if err != nil {
		t.Fatal(err)
	}
	packet.SetPathHashSizeAndCount(2, 1)
	packet.Path = []byte{4, 5}
	raw, _ := packet.MarshalBinary()
	svc.processRF(t.Context(), radio.Frame{Payload: raw, SNR: 2.25})
	if len(svc.mailbox) != 1 || svc.mailbox[0][0] != byte(companion.ResponseChannelMessageV3) {
		t.Fatalf("mailbox = % X", svc.mailbox)
	}
	svc.processRF(t.Context(), radio.Frame{Payload: raw, SNR: 2.25})
	if len(svc.mailbox) != 1 {
		t.Fatalf("duplicate group message entered mailbox %d times", len(svc.mailbox))
	}

	built, err = build(spec)
	if err != nil {
		t.Fatal(err)
	}
	restored := requireService(t, built)
	responses := restored.handle(t.Context(), companion.SimpleCommand{Kind: companion.CommandSyncNextMessage})
	if len(responses) != 1 {
		t.Fatalf("sync = %#v", responses)
	}
	payload, err := companion.MarshalResponse(responses[0])
	if err != nil || payload[0] != byte(companion.ResponseChannelMessageV3) ||
		!bytes.Equal(payload[4:7], []byte{2, 0x41, mesh.TxtTypePlain}) || string(payload[11:]) != "bob: hello" {
		t.Fatalf("mailbox response = % X, %v", payload, err)
	}
	built, err = build(spec)
	if err != nil {
		t.Fatal(err)
	}
	if got := requireService(t, built).mailbox; len(got) != 0 {
		t.Fatalf("popped mailbox restored: % X", got)
	}
}

func TestAdvertCapacityNotifiesTheCompanion(t *testing.T) {
	buildService := func(t *testing.T) *service {
		t.Helper()
		spec := testSpec(t)
		spec.Config["max_contacts"] = 2
		built, err := build(spec)
		if err != nil {
			t.Fatal(err)
		}
		return requireService(t, built)
	}
	advert := func(t *testing.T, value byte, timestamp int64) (*mesh.Packet, [mesh.PubKeySize]byte) {
		t.Helper()
		peer := testPeer(t, value)
		packet, err := mesh.BuildAdvert(peer, time.Unix(timestamp, 0), &mesh.AdvertData{
			Type: mesh.AdvTypeChat, Name: "peer",
		})
		if err != nil {
			t.Fatal(err)
		}
		packet.SetPathHashSizeAndCount(1, 1)
		packet.Path = []byte{value}
		return packet, peer.PubKey
	}

	t.Run("full", func(t *testing.T) {
		svc := buildService(t)
		for value := byte(1); value <= 2; value++ {
			packet, _ := advert(t, value, 1_800_000_000+int64(value))
			svc.receiveAdvert(t.Context(), packet)
		}
		app := attachApplication(t, svc)
		packet, _ := advert(t, 3, 1_800_000_003)
		got := readPushesAfter(t, app, 2, func() { svc.receiveAdvert(t.Context(), packet) })
		if got[0][0] != byte(companion.PushNewAdvert) ||
			!bytes.Equal(got[1], []byte{byte(companion.PushContactsFull)}) {
			t.Fatalf("full-table pushes = % X", got)
		}
	})

	t.Run("overwrite", func(t *testing.T) {
		svc := buildService(t)
		var oldest [mesh.PubKeySize]byte
		for value := byte(1); value <= 2; value++ {
			packet, publicKey := advert(t, value, 1_800_000_000+int64(value))
			if value == 1 {
				oldest = publicKey
			}
			svc.receiveAdvert(t.Context(), packet)
		}
		_ = svc.handle(t.Context(), companion.SetAutoAddConfig{Flags: 1})
		app := attachApplication(t, svc)
		packet, newest := advert(t, 3, 1_800_000_003)
		got := readPushesAfter(t, app, 2, func() { svc.receiveAdvert(t.Context(), packet) })
		if got[0][0] != byte(companion.PushContactDeleted) || !bytes.Equal(got[0][1:], oldest[:]) {
			t.Fatalf("contact-deleted push = % X", got[0])
		}
		if got[1][0] != byte(companion.PushNewAdvert) || len(got[1]) < 33 ||
			!bytes.Equal(got[1][1:33], newest[:]) {
			t.Fatalf("new-advert push = % X", got[1])
		}
		if contact := svc.contacts[newest].info; contact.PathLen != 0xff {
			t.Fatalf("advert inbound path became contact route: %+v", contact)
		}
	})

	t.Run("manual", func(t *testing.T) {
		svc := buildService(t)
		svc.p.ManualContacts = true
		app := attachApplication(t, svc)
		packet, publicKey := advert(t, 4, 1_800_000_004)
		got := readPushAfter(t, app, func() { svc.receiveAdvert(t.Context(), packet) })
		if got[0] != byte(companion.PushNewAdvert) || len(svc.contacts) != 0 {
			t.Fatalf("manual advert push = % X contacts=%d", got, len(svc.contacts))
		}
		path := svc.handle(t.Context(), companion.GetAdvertPath{PublicKey: publicKey})
		if len(path) != 1 {
			t.Fatalf("manual advert path = %#v", path)
		}
	})
}

func TestFloodedDirectTextQueuesACKPathReturn(t *testing.T) {
	spec := testSpec(t)
	spec.TX = station.TXPolicy{Mode: config.TXShadow, QueueDepth: 4}
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	svc.rfDevice = &stationRadio{}
	svc.duty = radio.NewAirtimeLedger(time.Hour, nil)
	peer, err := mesh.NewLocalIdentity(bytes.NewReader(bytes.Repeat([]byte{11}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	_ = svc.handle(t.Context(), companion.AddUpdateContact{Contact: companion.Contact{
		PublicKey: peer.PubKey, Type: mesh.AdvTypeChat, PathLen: 0xff, Name: "peer",
	}})
	secret, err := peer.SharedSecret(svc.id.PubKey[:])
	if err != nil {
		t.Fatal(err)
	}
	plain := mesh.BuildTextPlaintext(time.Unix(1_800_000_000, 0), mesh.TxtTypePlain, "private")
	packet, err := mesh.BuildDatagram(mesh.PayloadTypeTxtMsg,
		svc.id.PubKey[:mesh.PathHashSize], peer.PubKey[:mesh.PathHashSize], secret, plain)
	if err != nil {
		t.Fatal(err)
	}
	packet.SetPathHashSizeAndCount(1, 2)
	packet.Path = []byte{4, 5}
	raw, _ := packet.MarshalBinary()
	svc.processRF(t.Context(), radio.Frame{Payload: raw})
	item := takeEmission(t, svc)
	if item.packet.PayloadType() != mesh.PayloadTypePath || !item.packet.IsRouteFlood() ||
		item.notBefore.IsZero() {
		t.Fatalf("path reply = %+v", item)
	}
	datagram, err := mesh.ParseDatagram(item.packet.Payload)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := datagram.Open(secret)
	if err != nil {
		t.Fatal(err)
	}
	returned, err := mesh.DecodePathReturn(opened)
	if err != nil || returned.PathLen != packet.PathLen || !bytes.Equal(returned.Path, packet.Path) ||
		returned.ExtraType != uint8(mesh.PayloadTypeAck) || len(returned.Extra) < 4 ||
		binary.LittleEndian.Uint32(returned.Extra) != mesh.AckCRC(plain, peer.PubKey[:]) {
		t.Fatalf("returned path = %+v, %v", returned, err)
	}
	svc.processRF(t.Context(), radio.Frame{Payload: raw})
	if duplicate, ok := pollEmission(svc); ok {
		t.Fatalf("duplicate text queued another reply: %+v", duplicate)
	}
}

func TestACKPushConfirmsAnExpectedSend(t *testing.T) {
	spec := testSpec(t)
	spec.TX = station.TXPolicy{Mode: config.TXShadow, QueueDepth: 4}
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	svc.rfDevice = &stationRadio{}
	svc.duty = radio.NewAirtimeLedger(time.Hour, nil)
	peer := [32]byte{1, 2, 3}
	_ = svc.handle(t.Context(), companion.AddUpdateContact{Contact: companion.Contact{
		PublicKey: peer, Type: mesh.AdvTypeChat, PathLen: 0xff, Name: "peer",
	}})
	responses := svc.handle(t.Context(), companion.SendText{
		TextType: mesh.TxtTypePlain, UnixSeconds: 1_800_000_000,
		RecipientPrefix: [6]byte{1, 2, 3}, Text: "hello",
	})
	sent, ok := responses[0].(companion.Sent)
	if !ok {
		t.Fatalf("send = %#v", responses)
	}
	_ = takeEmission(t, svc)

	stationSide, appSide := net.Pipe()
	defer stationSide.Close()
	defer appSide.Close()
	svc.mu.Lock()
	svc.client, svc.generation = stationSide, 1
	svc.mu.Unlock()
	writerCtx, cancelWriter := context.WithCancel(t.Context())
	defer cancelWriter()
	go svc.runPushes(writerCtx)
	frames := make(chan [][]byte, 1)
	go func() {
		got := make([][]byte, 0, 2)
		for range 2 {
			frame, readErr := companion.ReadFrame(appSide, companion.ToApplication)
			if readErr != nil {
				return
			}
			got = append(got, frame.Payload)
		}
		frames <- got
	}()
	ack := make([]byte, 4)
	binary.LittleEndian.PutUint32(ack, sent.ExpectedACK)
	packet, err := mesh.BuildAck(ack)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := packet.MarshalBinary()
	svc.processRF(t.Context(), radio.Frame{Payload: raw, SNR: 1, RSSI: -90})
	got := <-frames
	if len(got) != 2 || got[0][0] != byte(companion.PushLogRXData) ||
		got[1][0] != byte(companion.PushSendConfirmed) ||
		binary.LittleEndian.Uint32(got[1][1:5]) != sent.ExpectedACK {
		t.Fatalf("ACK pushes = % X", got)
	}
}

func TestReceivedPathPersistsAndQueuesReciprocal(t *testing.T) {
	store := &memoryStationState{}
	spec := testSpec(t)
	spec.State = store
	spec.TX = station.TXPolicy{Mode: config.TXShadow, QueueDepth: 4}
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	svc.rfDevice = &stationRadio{}
	svc.duty = radio.NewAirtimeLedger(time.Hour, nil)
	peer, err := mesh.NewLocalIdentity(bytes.NewReader(bytes.Repeat([]byte{12}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	_ = svc.handle(t.Context(), companion.AddUpdateContact{Contact: companion.Contact{
		PublicKey: peer.PubKey, Type: mesh.AdvTypeChat, PathLen: 0xff, Name: "peer",
	}})
	secret, _ := peer.SharedSecret(svc.id.PubKey[:])
	packet, err := mesh.BuildPathReturn(svc.id.PubKey[:mesh.PathHashSize],
		peer.PubKey[:mesh.PathHashSize], secret, 2, []byte{9, 10}, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	packet.SetPathHashSizeAndCount(1, 1)
	packet.Path = []byte{7}
	raw, _ := packet.MarshalBinary()
	svc.processRF(t.Context(), radio.Frame{Payload: raw})
	contact := svc.contacts[peer.PubKey].info
	if contact.PathLen != 2 || contact.Path[0] != 9 || contact.Path[1] != 10 || contact.LastModifiedUnix == 0 {
		t.Fatalf("stored contact path = %+v", contact)
	}
	item := takeEmission(t, svc)
	if item.kind != "station-path-reciprocal" || !item.packet.IsRouteDirect() ||
		item.packet.PathLen != 2 || !bytes.Equal(item.packet.Path, []byte{9, 10}) {
		t.Fatalf("reciprocal = %+v", item)
	}
	restored, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	if got := requireService(t, restored).contacts[peer.PubKey].info; got.PathLen != 2 || got.Path[1] != 10 {
		t.Fatalf("restored path = %+v", got)
	}
}

func TestRFDirectTextDecryptsForKnownContact(t *testing.T) {
	built, err := build(testSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	_ = svc.handle(t.Context(), companion.DeviceQuery{TargetVersion: 3})
	peer, err := mesh.NewLocalIdentity(bytes.NewReader(bytes.Repeat([]byte{7}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	_ = svc.handle(t.Context(), companion.AddUpdateContact{Contact: companion.Contact{
		PublicKey: peer.PubKey, Type: mesh.AdvTypeChat, PathLen: 0xff, Name: "peer",
	}})
	secret, err := peer.SharedSecret(svc.id.PubKey[:])
	if err != nil {
		t.Fatal(err)
	}
	packet, err := mesh.BuildDatagram(mesh.PayloadTypeTxtMsg,
		svc.id.PubKey[:mesh.PathHashSize], peer.PubKey[:mesh.PathHashSize], secret,
		mesh.BuildTextPlaintext(time.Unix(1_800_000_000, 0), mesh.TxtTypePlain, "private"))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := packet.MarshalBinary()
	svc.processRF(t.Context(), radio.Frame{Payload: raw, SNR: -1.5})
	if len(svc.mailbox) != 1 || svc.mailbox[0][0] != byte(companion.ResponseContactMessageV3) ||
		string(svc.mailbox[0][16:]) != "private" {
		t.Fatalf("direct mailbox = % X", svc.mailbox)
	}
}

func TestLegacyApplicationGetsPreV3MailboxFrames(t *testing.T) {
	built, err := build(testSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	_ = svc.handle(t.Context(), companion.DeviceQuery{TargetVersion: 2})
	secret := [16]byte{1, 2, 3}
	_ = svc.handle(t.Context(), companion.SetChannel{Index: 2, Name: "ops", Secret: secret})
	group, err := mesh.NewGroupChannel(secret[:])
	if err != nil {
		t.Fatal(err)
	}
	packet, err := mesh.BuildGroupDatagram(mesh.PayloadTypeGrpTxt, group,
		mesh.BuildGroupText(time.Unix(1_800_000_000, 0), "bob", "legacy"))
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := packet.MarshalBinary()
	svc.processRF(t.Context(), radio.Frame{Payload: raw, SNR: 2.25})
	if len(svc.mailbox) != 1 || svc.mailbox[0][0] != byte(companion.ResponseChannelMessage) ||
		string(svc.mailbox[0][8:]) != "bob: legacy" {
		t.Fatalf("legacy mailbox = % X", svc.mailbox)
	}
}

func TestShadowStationConsumesSharedLedgerWithoutKeying(t *testing.T) {
	spec := testSpec(t)
	spec.TX = station.TXPolicy{
		Mode: config.TXShadow, CAD: true, LBTExhausted: config.LBTDrop, QueueDepth: 4,
	}
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	device := &stationRadio{}
	svc.rfDevice = device
	svc.duty = radio.NewAirtimeLedger(time.Hour, nil)
	peer := [32]byte{1, 2, 3}
	_ = svc.handle(t.Context(), companion.AddUpdateContact{Contact: companion.Contact{
		PublicKey: peer, Type: mesh.AdvTypeChat, PathLen: 0xff, Name: "peer",
	}})
	responses := svc.handle(t.Context(), companion.SendText{
		TextType: mesh.TxtTypePlain, UnixSeconds: 1_800_000_000,
		RecipientPrefix: [6]byte{1, 2, 3}, Text: "hello",
	})
	if len(responses) != 1 {
		t.Fatalf("send = %#v", responses)
	}
	item := takeEmission(t, svc)
	svc.transmit(t.Context(), item)
	if device.transmits != 0 || device.assesses != 1 || svc.duty.Usage(time.Now()) != 100*time.Millisecond {
		t.Fatalf("shadow: transmits %d assesses %d usage %s",
			device.transmits, device.assesses, svc.duty.Usage(time.Now()))
	}
	if svc.stats.sent != 1 || svc.stats.sentFlood != 1 || svc.stats.txAir != 100*time.Millisecond {
		t.Fatalf("shadow station stats = %+v", svc.stats)
	}
}

func TestStationReceptionBusyRequeueIsPacedAndKeepsItsBound(t *testing.T) {
	spec := testSpec(t)
	spec.TX = station.TXPolicy{
		Mode: config.TXOnAir, CAD: true, LBTExhausted: config.LBTDrop, QueueDepth: 4,
	}
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	device := &stationRadio{assessErr: radio.ErrBusyReceiving}
	svc.rfDevice = device
	svc.duty = radio.NewAirtimeLedger(time.Hour, nil)
	packet, err := mesh.BuildAck([]byte{1, 2, 3, 4})
	if err != nil {
		t.Fatal(err)
	}
	item := emission{packet: packet, correlation: correlation.New(), kind: "station-test"}
	now := time.Now()
	svc.transmit(t.Context(), item)
	if device.assesses != 1 || svc.outbound.len() != 1 {
		t.Fatalf("paced requeue: assessments %d queue %d", device.assesses, svc.outbound.len())
	}
	requeued, ok := svc.outbound.takeUntil(t.Context(), now.Add(time.Second))
	if !ok || requeued.busySince.IsZero() || !requeued.notBefore.After(now) {
		t.Fatalf("requeued emission = %+v, ok %t", requeued, ok)
	}

	requeued.busySince = time.Now().Add(-stationLBTBound - time.Second)
	svc.transmit(t.Context(), requeued)
	if device.assesses != 2 || svc.outbound.len() != 0 {
		t.Fatalf("exhausted reception retry: assessments %d queue %d", device.assesses, svc.outbound.len())
	}
}

func TestStartedStationTransmitFinishesAfterSessionCancellation(t *testing.T) {
	spec := testSpec(t)
	spec.TX = station.TXPolicy{Mode: config.TXOnAir, QueueDepth: 4}
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	device := &stationRadio{txStarted: make(chan struct{}), txRelease: make(chan struct{})}
	svc.rfDevice = device
	svc.duty = radio.NewAirtimeLedger(time.Hour, nil)
	packet, err := mesh.BuildAck([]byte{1, 2, 3, 4})
	if err != nil {
		t.Fatal(err)
	}
	item := emission{packet: packet, correlation: correlation.New(), kind: "station-test"}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		svc.transmit(ctx, item)
		close(done)
	}()
	<-device.txStarted
	cancel()
	select {
	case <-done:
		t.Fatal("started transmit returned before the hardware completed")
	case <-time.After(20 * time.Millisecond):
	}
	close(device.txRelease)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("started transmit did not finish")
	}
	if got := svc.duty.Usage(time.Now()); got != 100*time.Millisecond {
		t.Fatalf("accounted airtime = %s", got)
	}
}

func TestStationStatsStayLocalToTheAttachment(t *testing.T) {
	built, err := build(testSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	svc.startedAt = time.Now().Add(-42 * time.Second)
	svc.rfDevice = &stationRadio{noise: radio.NoiseFloor{DBm: -105.75}, noiseOK: true}
	svc.stats = stationStats{
		received: 11, sent: 7, sentFlood: 3, sentDirect: 4,
		receivedFlood: 5, receivedDirect: 6, receiveErrors: 2,
		txAir: 4*time.Second + 900*time.Millisecond, rxAir: 8*time.Second + 100*time.Millisecond,
		lastRSSIDBm: -91, lastSNRx4: 13,
	}

	responses := svc.handle(t.Context(), companion.GetStats{Type: companion.StatsCore})
	core, ok := responses[0].(companion.CoreStats)
	if !ok || core.UptimeSeconds != 42 || core.QueueLength != 0 || core.BatteryMillivolts != 0 {
		t.Fatalf("core stats = %#v", responses)
	}
	responses = svc.handle(t.Context(), companion.GetStats{Type: companion.StatsRadio})
	rf, ok := responses[0].(companion.RadioStats)
	if !ok || rf.NoiseFloorDBm != -105 || rf.LastRSSIDBm != -91 || rf.LastSNRx4 != 13 ||
		rf.TXAirSeconds != 4 || rf.RXAirSeconds != 8 {
		t.Fatalf("radio stats = %#v", responses)
	}
	responses = svc.handle(t.Context(), companion.GetStats{Type: companion.StatsPackets})
	packets, ok := responses[0].(companion.PacketStats)
	if !ok || packets != (companion.PacketStats{
		Received: 11, Sent: 7, SentFlood: 3, SentDirect: 4,
		ReceivedFlood: 5, ReceivedDirect: 6, ReceiveErrors: 2,
	}) {
		t.Fatalf("packet stats = %#v", responses)
	}
	responses = svc.handle(t.Context(), companion.GetStats{Type: 99})
	if len(responses) != 1 || responses[0] != (companion.ErrorResponse{Code: companion.ErrIllegalArgument}) {
		t.Fatalf("invalid stats response = %#v", responses)
	}
}

func TestStationTextMatchesReferenceLimitsClockAndTimeout(t *testing.T) {
	spec := testSpec(t)
	spec.TX = station.TXPolicy{Mode: config.TXShadow, QueueDepth: 4}
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	svc.rfDevice = &stationRadio{}
	svc.duty = radio.NewAirtimeLedger(time.Hour, nil)
	peer, err := mesh.NewLocalIdentity(bytes.NewReader(bytes.Repeat([]byte{8}, 64)))
	if err != nil {
		t.Fatal(err)
	}
	path := [mesh.MaxPathSize]byte{9, 10}
	_ = svc.handle(t.Context(), companion.AddUpdateContact{Contact: companion.Contact{
		PublicKey: peer.PubKey, Type: mesh.AdvTypeChat, PathLen: 2, Path: path, Name: "peer",
	}})
	responses := svc.handle(t.Context(), companion.SendText{
		TextType: mesh.TxtTypeCLIData, UnixSeconds: 1,
		RecipientPrefix: [6]byte(peer.PubKey[:6]), Text: "status",
	})
	sent, ok := responses[0].(companion.Sent)
	if !ok || sent.Flood || sent.TimeoutMillis != 3_050 || sent.ExpectedACK != 0 {
		t.Fatalf("sent = %#v", responses)
	}
	item := takeEmission(t, svc)
	datagram, err := mesh.ParseDatagram(item.packet.Payload)
	if err != nil {
		t.Fatal(err)
	}
	secret, err := peer.SharedSecret(svc.id.PubKey[:])
	if err != nil {
		t.Fatal(err)
	}
	plain, err := datagram.Open(secret)
	if err != nil {
		t.Fatal(err)
	}
	message, err := mesh.ParseTextPlaintext(plain)
	if err != nil || message.Timestamp.Unix() == 1 || message.Text != "status" {
		t.Fatalf("CLI plaintext = %+v, %v", message, err)
	}

	tooLong := string(bytes.Repeat([]byte{'x'}, stationMaxText+1))
	responses = svc.handle(t.Context(), companion.SendText{
		TextType: mesh.TxtTypePlain, RecipientPrefix: [6]byte(peer.PubKey[:6]), Text: tooLong,
	})
	if got, _ := companion.MarshalResponse(responses[0]); !bytes.Equal(got,
		[]byte{byte(companion.ResponseError), byte(companion.ErrTableFull)}) {
		t.Fatalf("long text = % X", got)
	}

	_ = svc.handle(t.Context(), companion.AddUpdateContact{Contact: companion.Contact{
		PublicKey: peer.PubKey, Type: mesh.AdvTypeChat, PathLen: 0xff, Name: "peer",
	}})
	responses = svc.handle(t.Context(), companion.SendText{
		TextType: mesh.TxtTypePlain, RecipientPrefix: [6]byte(peer.PubKey[:6]), Text: "flood",
	})
	sent, ok = responses[0].(companion.Sent)
	if !ok || !sent.Flood || sent.TimeoutMillis != 2_100 || sent.ExpectedACK == 0 {
		t.Fatalf("flood sent = %#v", responses)
	}
}

func TestStationGroupTextTruncatesAtTheReferenceBoundary(t *testing.T) {
	spec := testSpec(t)
	spec.TX = station.TXPolicy{Mode: config.TXShadow, QueueDepth: 2}
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	svc.rfDevice = &stationRadio{}
	svc.duty = radio.NewAirtimeLedger(time.Hour, nil)
	secret := [16]byte{1, 2, 3}
	_ = svc.handle(t.Context(), companion.SetChannel{Index: 1, Name: "ops", Secret: secret})
	text := string(bytes.Repeat([]byte{'z'}, stationMaxText+20))
	responses := svc.handle(t.Context(), companion.SendChannelText{
		Channel: 1, TextType: mesh.TxtTypePlain, UnixSeconds: 1_800_000_000, Text: text,
	})
	if got, _ := companion.MarshalResponse(responses[0]); !bytes.Equal(got, []byte{byte(companion.ResponseOK)}) {
		t.Fatalf("send group = % X", got)
	}
	item := takeEmission(t, svc)
	datagram, err := mesh.ParseGroupDatagram(item.packet.Payload)
	if err != nil {
		t.Fatal(err)
	}
	group, err := mesh.NewGroupChannel(secret[:])
	if err != nil {
		t.Fatal(err)
	}
	plain, err := datagram.Open(group)
	if err != nil {
		t.Fatal(err)
	}
	message, err := mesh.ParseGroupText(plain)
	if err != nil || message.Sender != svc.p.NodeName ||
		len(message.Sender)+2+len(message.Text) != stationMaxText {
		t.Fatalf("group plaintext = %+v, %v", message, err)
	}
}

func TestDryStationRejectsTransmissionWithAnApplicationTerminalError(t *testing.T) {
	core, observed := observer.New(zapcore.DebugLevel)
	spec := testSpec(t)
	spec.Log = zap.New(core)
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	responses := svc.handle(t.Context(), companion.SendChannelText{
		Channel: 0, TextType: mesh.TxtTypePlain, UnixSeconds: 1_800_000_000, Text: "hello",
	})
	if len(responses) != 1 || responses[0] != (companion.ErrorResponse{Code: companion.ErrBadState}) {
		t.Fatalf("dry send responses = %#v", responses)
	}
	refused := observed.FilterMessage("station frame refused").All()
	if len(refused) != 1 {
		t.Fatalf("dry send refusal logs = %#v", refused)
	}
	fields := refused[0].ContextMap()
	if fields["reason"] != "dry" || fields["kind"] != "station-channel-text" || fields["corr"] == "" {
		t.Fatalf("dry send refusal fields = %#v", fields)
	}
}
