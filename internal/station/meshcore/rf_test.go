package meshcore

import (
	"bytes"
	"context"
	"testing"
	"time"

	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/station"

	mesh "meshrunner.dev/pkg/meshcore"
	"meshrunner.dev/pkg/meshcore/companion"
)

type stationRadio struct {
	transmits int
	assesses  int
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
func (*stationRadio) NoiseFloor() (radio.NoiseFloor, bool) { return radio.NoiseFloor{}, false }
func (*stationRadio) NoiseStarved() uint64                 { return 0 }
func (*stationRadio) ChipStats() (radio.ChipStats, bool)   { return radio.ChipStats{}, false }
func (r *stationRadio) Transmit(_ context.Context, _ []byte, power int8) (radio.TxReport, error) {
	r.transmits++
	return radio.TxReport{At: time.Now(), Airtime: 100 * time.Millisecond, PowerDBm: power}, nil
}
func (r *stationRadio) AssessChannel(context.Context, float64) (bool, error) {
	r.assesses++
	return false, nil
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

func TestRFDirectTextDecryptsForKnownContact(t *testing.T) {
	built, err := build(testSpec(t))
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
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
	item := <-svc.outbound
	svc.transmit(t.Context(), item)
	if device.transmits != 0 || device.assesses != 1 || svc.duty.Usage(time.Now()) != 100*time.Millisecond {
		t.Fatalf("shadow: transmits %d assesses %d usage %s",
			device.transmits, device.assesses, svc.duty.Usage(time.Now()))
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
	item := <-svc.outbound
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
		[]byte{byte(companion.ResponseError), byte(companion.ErrorTableFull)}) {
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
	item := <-svc.outbound
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
