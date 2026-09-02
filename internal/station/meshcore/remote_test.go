package meshcore

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/station"

	mesh "meshrunner.dev/pkg/meshcore"
	"meshrunner.dev/pkg/meshcore/companion"
)

func testRFService(t *testing.T) *service {
	t.Helper()
	spec := testSpec(t)
	spec.TX = station.TXPolicy{Mode: config.TXShadow, QueueDepth: 8}
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	svc.rfDevice = &stationRadio{}
	svc.duty = radio.NewAirtimeLedger(time.Hour, nil)
	return svc
}

func testPeer(t *testing.T, value byte) *mesh.LocalIdentity {
	t.Helper()
	peer, err := mesh.LocalIdentityFromSeed(bytes.Repeat([]byte{value}, mesh.SeedSize))
	if err != nil {
		t.Fatal(err)
	}
	return peer
}

func addPeer(t *testing.T, svc *service, peer *mesh.LocalIdentity, pathLen uint8, path []byte) {
	t.Helper()
	var storedPath [mesh.MaxPathSize]byte
	copy(storedPath[:], path)
	responses := svc.handle(t.Context(), companion.AddUpdateContact{Contact: companion.Contact{
		PublicKey: peer.PubKey, Type: mesh.AdvTypeChat, PathLen: pathLen, Path: storedPath, Name: "peer",
	}})
	if len(responses) != 1 || responses[0] != companion.StatusResponse(companion.ResponseOK) {
		t.Fatalf("add peer = %#v", responses)
	}
}

func attachApplication(t *testing.T, svc *service) net.Conn {
	t.Helper()
	stationSide, appSide := net.Pipe()
	svc.mu.Lock()
	svc.client = stationSide
	svc.generation++
	svc.mu.Unlock()
	writerCtx, cancelWriter := context.WithCancel(t.Context())
	go svc.runPushes(writerCtx)
	t.Cleanup(func() {
		cancelWriter()
		_ = stationSide.Close()
		_ = appSide.Close()
	})
	return appSide
}

func readPushAfter(t *testing.T, app net.Conn, action func()) []byte {
	t.Helper()
	done := make(chan struct{})
	go func() {
		action()
		close(done)
	}()
	if err := app.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	frame, err := companion.ReadFrame(app, companion.ToApplication)
	if err != nil {
		t.Fatal(err)
	}
	<-done
	return frame.Payload
}

func readPushesAfter(t *testing.T, app net.Conn, count int, action func()) [][]byte {
	t.Helper()
	done := make(chan struct{})
	go func() {
		action()
		close(done)
	}()
	frames := make([][]byte, 0, count)
	for range count {
		if err := app.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		frame, err := companion.ReadFrame(app, companion.ToApplication)
		if err != nil {
			t.Fatal(err)
		}
		frames = append(frames, frame.Payload)
	}
	<-done
	return frames
}

func TestRemoteLoginAndStatusRequestRoundTrip(t *testing.T) {
	svc := testRFService(t)
	peer := testPeer(t, 23)
	addPeer(t, svc, peer, 0, nil)
	secret, err := peer.SharedSecret(svc.id.PubKey[:])
	if err != nil {
		t.Fatal(err)
	}

	responses := svc.handle(t.Context(), companion.SendLogin{
		PublicKey: peer.PubKey, Password: "0123456789abcdef-extra",
	})
	sent, ok := responses[0].(companion.Sent)
	if !ok || sent.Flood || sent.ExpectedACK != binary.LittleEndian.Uint32(peer.PubKey[:4]) {
		t.Fatalf("login sent = %#v", responses)
	}
	loginEmission := takeEmission(t, svc)
	anonymous, err := mesh.ParseAnonDatagram(emissionPacket(loginEmission).Payload)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := anonymous.Open(secret)
	if err != nil {
		t.Fatal(err)
	}
	_, password, err := mesh.AnonPassword(plain)
	if err != nil || password != "0123456789abcde" {
		t.Fatalf("login password = %q, %v", password, err)
	}

	app := attachApplication(t, svc)
	loginReply, err := mesh.FrameLoginReply(mesh.LoginReply{
		Clock: 0x11223344, Result: mesh.LoginOK, KeepAlive: 2,
		IsAdmin: true, Permissions: 0x05, FirmwareLevel: 13,
	})
	if err != nil {
		t.Fatal(err)
	}
	packet, err := mesh.BuildDatagram(mesh.PayloadTypeResponse,
		svc.id.PubKey[:mesh.PathHashSize], peer.PubKey[:mesh.PathHashSize], secret, loginReply)
	if err != nil {
		t.Fatal(err)
	}
	got := readPushAfter(t, app, func() { svc.receiveRemoteResponse(packet) })
	wantPrefix := append([]byte{byte(companion.PushLoginSuccess), 1}, peer.PubKey[:6]...)
	wantPrefix = append(wantPrefix, 0x44, 0x33, 0x22, 0x11, 0x05, 13)
	if !bytes.Equal(got, wantPrefix) {
		t.Fatalf("login push = % X, want % X", got, wantPrefix)
	}
	responses = svc.handle(t.Context(), companion.ContactRequest{
		Kind: companion.CommandHasConnection, PublicKey: peer.PubKey,
	})
	if len(responses) != 1 || responses[0] != companion.StatusResponse(companion.ResponseOK) {
		t.Fatalf("has connection = %#v", responses)
	}
	svc.mu.Lock()
	connection := svc.connections[peer.PubKey]
	connection.nextPing = time.Now().Add(-time.Second)
	svc.connections[peer.PubKey] = connection
	svc.mu.Unlock()
	svc.checkConnections()
	keepAlive := takeEmission(t, svc)
	keepDatagram, err := mesh.ParseDatagram(emissionPacket(keepAlive).Payload)
	if err != nil {
		t.Fatal(err)
	}
	keepPlain, err := keepDatagram.Open(secret)
	if err != nil {
		t.Fatal(err)
	}
	_, keepBody, err := mesh.UnframeAdmin(keepPlain)
	if err != nil || keepBody[0] != mesh.ReqKeepAlive {
		t.Fatalf("keep-alive body = % X, %v", keepBody, err)
	}
	svc.mu.Lock()
	expectedKeepACK := svc.connections[peer.PubKey].expectedACK
	svc.mu.Unlock()
	if expectedKeepACK == 0 {
		t.Fatal("keep-alive has no expected ACK")
	}
	ack := make([]byte, 4)
	binary.LittleEndian.PutUint32(ack, expectedKeepACK)
	svc.receiveACK(ack)
	svc.mu.Lock()
	remainingKeepACK := svc.connections[peer.PubKey].expectedACK
	svc.mu.Unlock()
	if remainingKeepACK != 0 {
		t.Fatalf("keep-alive ACK remained %#x", remainingKeepACK)
	}

	responses = svc.handle(t.Context(), companion.ContactRequest{
		Kind: companion.CommandSendStatusRequest, PublicKey: peer.PubKey,
	})
	sent, ok = responses[0].(companion.Sent)
	if !ok || sent.ExpectedACK == 0 {
		t.Fatalf("status sent = %#v", responses)
	}
	statusEmission := takeEmission(t, svc)
	datagram, err := mesh.ParseDatagram(emissionPacket(statusEmission).Payload)
	if err != nil {
		t.Fatal(err)
	}
	request, err := datagram.Open(secret)
	if err != nil {
		t.Fatal(err)
	}
	tag, body, err := mesh.UnframeAdmin(request)
	if err != nil || tag != sent.ExpectedACK || body[0] != mesh.ReqGetStatus {
		t.Fatalf("status request tag=%d body=% X err=%v", tag, body, err)
	}
	responsePacket, err := mesh.BuildResponse(svc.id.PubKey[:mesh.PathHashSize],
		peer.PubKey[:mesh.PathHashSize], secret, tag, []byte{9, 8, 7})
	if err != nil {
		t.Fatal(err)
	}
	got = readPushAfter(t, app, func() { svc.receiveRemoteResponse(responsePacket) })
	wantPrefix = append([]byte{byte(companion.PushStatusResponse), 0}, peer.PubKey[:6]...)
	wantPrefix = append(wantPrefix, 9, 8, 7)
	if len(got) < len(wantPrefix) || !bytes.Equal(got[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("status push = % X, want prefix % X", got, wantPrefix)
	}
}

func TestRoomLoginAndKeepAliveCarryPersistedSyncCursor(t *testing.T) {
	store := &memoryStationState{}
	spec := testSpec(t)
	spec.State = store
	spec.TX = station.TXPolicy{Mode: config.TXShadow, QueueDepth: 8}
	built, err := build(spec)
	if err != nil {
		t.Fatal(err)
	}
	svc := requireService(t, built)
	svc.rfDevice = &stationRadio{}
	svc.duty = radio.NewAirtimeLedger(time.Hour, nil)
	room := testPeer(t, 37)
	responses := svc.handle(t.Context(), companion.AddUpdateContact{Contact: companion.Contact{
		PublicKey: room.PubKey, Type: mesh.AdvTypeRoom, PathLen: 0, Name: "room",
	}})
	if len(responses) != 1 || responses[0] != companion.StatusResponse(companion.ResponseOK) {
		t.Fatalf("add room = %#v", responses)
	}
	secret, err := room.SharedSecret(svc.id.PubKey[:])
	if err != nil {
		t.Fatal(err)
	}
	const syncSince = uint32(1_800_000_000)
	plain := mesh.BuildTextPlaintext(time.Unix(int64(syncSince), 0), mesh.TxtTypeSignedPlain,
		string([]byte{1, 2, 3, 4})+"signed")
	packet, err := mesh.BuildDatagram(mesh.PayloadTypeTxtMsg,
		svc.id.PubKey[:mesh.PathHashSize], room.PubKey[:mesh.PathHashSize], secret, plain)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := packet.MarshalBinary()
	svc.processRF(t.Context(), radio.Frame{Payload: raw})
	if svc.contacts[room.PubKey].syncSince != syncSince {
		t.Fatalf("sync cursor = %d, want %d", svc.contacts[room.PubKey].syncSince, syncSince)
	}

	built, err = build(spec)
	if err != nil {
		t.Fatal(err)
	}
	svc = requireService(t, built)
	svc.rfDevice = &stationRadio{}
	svc.duty = radio.NewAirtimeLedger(time.Hour, nil)
	responses = svc.handle(t.Context(), companion.SendLogin{PublicKey: room.PubKey, Password: "guest"})
	if _, ok := responses[0].(companion.Sent); !ok {
		t.Fatalf("room login = %#v", responses)
	}
	emission := takeEmission(t, svc)
	anon, err := mesh.ParseAnonDatagram(emissionPacket(emission).Payload)
	if err != nil {
		t.Fatal(err)
	}
	opened, err := anon.Open(secret)
	if err != nil {
		t.Fatal(err)
	}
	_, body, err := mesh.UnframeAdmin(opened)
	if err != nil || len(body) < 9 || binary.LittleEndian.Uint32(body[:4]) != syncSince ||
		string(body[4:9]) != "guest" {
		t.Fatalf("room login body = % X, %v", body, err)
	}

	svc.connections[room.PubKey] = remoteConnection{
		keepAlive: time.Minute, activeAt: time.Now(), nextPing: time.Now().Add(-time.Second),
	}
	svc.checkConnections()
	emission = takeEmission(t, svc)
	datagram, err := mesh.ParseDatagram(emissionPacket(emission).Payload)
	if err != nil {
		t.Fatal(err)
	}
	opened, err = datagram.Open(secret)
	if err != nil {
		t.Fatal(err)
	}
	_, body, err = mesh.UnframeAdmin(opened)
	if err != nil || len(body) < 5 || body[0] != mesh.ReqKeepAlive ||
		binary.LittleEndian.Uint32(body[1:5]) != syncSince {
		t.Fatalf("room keep-alive body = % X, %v", body, err)
	}
}

func TestAnonymousRequestCreatesOnlyAReferenceCompatibleVolatileContact(t *testing.T) {
	svc := testRFService(t)
	peer := testPeer(t, 29)
	responses := svc.handle(t.Context(), companion.ContactDataRequest{
		Kind: companion.CommandSendAnonymousRequest, PublicKey: peer.PubKey,
		Data: []byte{mesh.AnonReqClock},
	})
	sent, ok := responses[0].(companion.Sent)
	contact := svc.contacts[peer.PubKey]
	if !ok || sent.Flood || sent.ExpectedACK == 0 || len(svc.contacts) != 1 || !contact.ephemeral ||
		contact.info.Type != mesh.AdvTypeNone || contact.info.PathLen != 0 {
		t.Fatalf("anonymous request = %#v contacts=%d", responses, len(svc.contacts))
	}
	emission := takeEmission(t, svc)
	if emissionPacket(emission).PayloadType() != mesh.PayloadTypeAnonReq || emissionPacket(emission).PathHashCount() != 0 {
		t.Fatalf("anonymous emission = %#v", emissionPacket(emission))
	}
	listed := svc.handle(t.Context(), companion.GetContacts{})
	if start, ok := listed[0].(companion.ContactsStart); !ok || start.Count != 0 {
		t.Fatalf("anonymous contact leaked into contact list: %#v", listed)
	}
	oldest := peer.PubKey
	for value := byte(30); value < 38; value++ {
		candidate := testPeer(t, value)
		responses = svc.handle(t.Context(), companion.ContactDataRequest{
			Kind: companion.CommandSendAnonymousRequest, PublicKey: candidate.PubKey,
			Data: []byte{mesh.AnonReqClock},
		})
		if _, ok := responses[0].(companion.Sent); !ok {
			t.Fatalf("anonymous slot %d = %#v", value, responses)
		}
		_ = takeEmission(t, svc)
	}
	if len(svc.contacts) != maxAnonContacts {
		t.Fatalf("anonymous contact slots = %d, want %d", len(svc.contacts), maxAnonContacts)
	}
	if _, exists := svc.contacts[oldest]; exists {
		t.Fatalf("oldest anonymous contact %x was not replaced", oldest[:6])
	}
	svc.mu.Lock()
	_ = svc.resetRuntimeLocked()
	svc.mu.Unlock()
	if len(svc.contacts) != 0 {
		t.Fatalf("volatile anonymous contact survived restart: %+v", svc.contacts)
	}
}

func TestRawPacketCommandControlsStationQueuePriority(t *testing.T) {
	svc := testRFService(t)
	low, err := mesh.BuildRawCustom([]byte{1, 2, 3, 4})
	if err != nil {
		t.Fatal(err)
	}
	high, err := mesh.BuildRawCustom([]byte{5, 6, 7, 8})
	if err != nil {
		t.Fatal(err)
	}
	lowRaw, _ := low.MarshalBinary()
	highRaw, _ := high.MarshalBinary()
	for _, command := range []companion.SendRawPacket{
		{Priority: 5, Packet: lowRaw}, {Priority: 1, Packet: highRaw},
	} {
		responses := svc.handle(t.Context(), command)
		if len(responses) != 1 || responses[0] != companion.StatusResponse(companion.ResponseOK) {
			t.Fatalf("raw packet response = %#v", responses)
		}
	}
	first := takeEmission(t, svc)
	second := takeEmission(t, svc)
	if first.Priority != 1 || second.Priority != 5 || emissionPacket(first).Hash() != high.Hash() ||
		emissionPacket(second).Hash() != low.Hash() {
		t.Fatalf("raw packet order = %d/%x then %d/%x",
			first.Priority, emissionPacket(first).Hash(), second.Priority, emissionPacket(second).Hash())
	}
}

func TestSelfTelemetryAndRawPushes(t *testing.T) {
	svc := testRFService(t)
	responses := svc.handle(t.Context(), companion.SendTelemetryRequest{Self: true})
	push, ok := responses[0].(companion.Push)
	if !ok || push.Code != companion.PushTelemetryResponse || len(push.Body) < 11 ||
		!bytes.Equal(push.Body[1:7], svc.id.PubKey[:6]) {
		t.Fatalf("self telemetry = %#v", responses)
	}
	readings, err := mesh.LPPDecode(push.Body[7:])
	if err != nil || len(readings) != 1 || readings[0].Type != mesh.LPPVoltage || readings[0].Value != float64(0) {
		t.Fatalf("self telemetry readings = %#v, %v", readings, err)
	}

	app := attachApplication(t, svc)
	raw, err := mesh.BuildRawCustom([]byte{1, 2, 3, 4})
	if err != nil {
		t.Fatal(err)
	}
	svc.routeDirect(raw, 0, nil)
	got := readPushAfter(t, app, func() {
		svc.receiveRaw(raw, radio.Frame{SNR: 1.25, RSSI: -91.75})
	})
	if !bytes.Equal(got, []byte{byte(companion.PushRawData), 5, 0xa5, 0xff, 1, 2, 3, 4}) {
		t.Fatalf("raw push = % X", got)
	}

	control, err := mesh.BuildControl([]byte{0x80, 7})
	if err != nil {
		t.Fatal(err)
	}
	svc.routeDirect(control, 0, nil)
	got = readPushAfter(t, app, func() {
		svc.receiveControl(control, radio.Frame{SNR: -1, RSSI: -95})
	})
	if !bytes.Equal(got, []byte{byte(companion.PushControlData), 0xfc, 0xa1, 0, 0x80, 7}) {
		t.Fatalf("control push = % X", got)
	}
}

func TestPathDiscoveryPushDoesNotMutateTheContactPath(t *testing.T) {
	svc := testRFService(t)
	peer := testPeer(t, 31)
	addPeer(t, svc, peer, 0xff, nil)
	const tag = 0x11223344
	svc.pending = pendingRequest{kind: pendingDiscovery, tag: tag, publicKey: peer.PubKey}
	secret, err := peer.SharedSecret(svc.id.PubKey[:])
	if err != nil {
		t.Fatal(err)
	}
	packet, err := mesh.BuildPathReturn(svc.id.PubKey[:mesh.PathHashSize],
		peer.PubKey[:mesh.PathHashSize], secret, 2, []byte{9, 10},
		uint8(mesh.PayloadTypeResponse), mesh.FrameAdmin(tag, []byte{1}))
	if err != nil {
		t.Fatal(err)
	}
	packet.SetPathHashSizeAndCount(1, 1)
	packet.Path = []byte{7}
	app := attachApplication(t, svc)
	got := readPushAfter(t, app, func() { svc.receivePath(t.Context(), packet) })
	want := append([]byte{byte(companion.PushPathDiscoveryResponse), 0}, peer.PubKey[:6]...)
	want = append(want, 2, 9, 10, 1, 7)
	if !bytes.Equal(got, want) {
		t.Fatalf("path discovery push = % X, want % X", got, want)
	}
	if svc.contacts[peer.PubKey].info.PathLen != 0xff {
		t.Fatalf("discovery persisted path %+v", svc.contacts[peer.PubKey].info)
	}
	if emission, ok := pollEmission(svc); ok {
		t.Fatalf("discovery queued reciprocal path: %+v", emission)
	}
}
