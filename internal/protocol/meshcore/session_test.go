package meshcore

import (
	"encoding/binary"
	"testing"
	"time"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/txn"
)

// login builds a guest login as a companion sends it: an ANON_REQ
// whose plaintext is timestamp ‖ password (a C string).
func login(t *testing.T, self, peer *meshcore.LocalIdentity, ts uint32, password string, flood bool) (radio.Frame, []byte) {
	t.Helper()
	secret, err := peer.SharedSecret(self.PubKey[:])
	if err != nil {
		t.Fatal(err)
	}
	plain := binary.LittleEndian.AppendUint32(nil, ts)
	plain = append(plain, password...)
	plain = append(plain, 0)
	pkt, err := meshcore.BuildAnonDatagram(self.PubKey[:1], peer.PubKey[:], secret, plain)
	if err != nil {
		t.Fatal(err)
	}
	route := meshcore.RouteDirect
	if flood {
		route = meshcore.RouteFlood
	}
	pkt.Header = meshcore.MakeHeader(route, meshcore.PayloadTypeAnonReq, meshcore.PayloadVer1)
	raw, err := pkt.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return radio.Frame{Payload: raw, At: time.Now(), SNR: 8, RSSI: -70}, secret
}

// request builds a logged-in guest's REQ.
func request(t *testing.T, self, peer *meshcore.LocalIdentity, ts uint32, body []byte) radio.Frame {
	t.Helper()
	secret, err := peer.SharedSecret(self.PubKey[:])
	if err != nil {
		t.Fatal(err)
	}
	pkt, err := meshcore.BuildRequest(peer, self.PubKey[:], secret, ts, body)
	if err != nil {
		t.Fatal(err)
	}
	pkt.Header = meshcore.MakeHeader(meshcore.RouteDirect, meshcore.PayloadTypeReq, meshcore.PayloadVer1)
	raw, err := pkt.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return radio.Frame{Payload: raw, At: time.Now(), SNR: 8, RSSI: -70}
}

// openReply parses a transmitted RESPONSE and opens it as the client.
func openReply(t *testing.T, raw, secret []byte) (uint32, []byte) {
	t.Helper()
	pkt, err := meshcore.ParsePacket(raw)
	if err != nil {
		t.Fatal(err)
	}
	if pkt.PayloadType() != meshcore.PayloadTypeResponse {
		t.Fatalf("reply type = %v", pkt.PayloadType())
	}
	dg, err := meshcore.ParseDatagram(pkt.Payload)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := dg.Open(secret)
	if err != nil {
		t.Fatal(err)
	}
	ts, body, err := meshcore.UnframeAdmin(plain)
	if err != nil {
		t.Fatal(err)
	}
	return ts, body
}

func TestGuestLoginAndStatus(t *testing.T) {
	e, dev, sub, peer := txRig(t, "on-air")
	e.p.GuestPassword = "raccoon"
	runEngine(t, e, dev)

	frame, secret := login(t, e.id, peer, 100, "raccoon", false)
	dev.frames <- frame
	if sent := awaitSent(t, sub); sent.Kind != "login-resp" {
		t.Fatalf("sent = %+v", sent)
	}
	_, body := openReply(t, <-dev.sent, secret)
	if len(body) < 9 || body[0] != respServerLoginOK || body[2] != 0 || body[3] != permGuest {
		t.Fatalf("login reply = % x — want OK, no admin, guest perms", body)
	}

	// The session works: ask for the status.
	dev.frames <- request(t, e.id, peer, 101, []byte{reqTypeGetStatus, 0, 0, 0, 0})
	if sent := awaitSent(t, sub); sent.Kind != "req-resp" {
		t.Fatalf("sent = %+v", sent)
	}
	tag, blob := openReply(t, <-dev.sent, secret)
	if tag != 101 {
		t.Fatalf("tag = %d, want the request timestamp reflected", tag)
	}
	// The reference's RepeaterStats is 56 bytes; the opened datagram
	// carries block-cipher padding past it.
	if len(blob) < 56 {
		t.Fatalf("status blob = %d bytes, want at least the reference's 56", len(blob))
	}
	// Replay: the same timestamp must be refused.
	dev.frames <- request(t, e.id, peer, 101, []byte{reqTypeGetStatus, 0, 0, 0, 0})
	select {
	case raw := <-dev.sent:
		t.Fatalf("a replayed request was answered: % x", raw[:8])
	case <-time.After(700 * time.Millisecond):
	}
}

func TestWrongPasswordIsSilence(t *testing.T) {
	e, dev, _, peer := txRig(t, "on-air")
	e.p.GuestPassword = "raccoon"
	e.queue.depth = 8
	for i, pw := range []string{"admin", "wrong", ""} {
		frame, _ := login(t, e.id, peer, uint32(200+i), pw, false)
		pkt, err := meshcore.ParsePacket(frame.Payload)
		if err != nil {
			t.Fatal(err)
		}
		e.respondAnon(dev, pkt, txn.New())
	}
	if n := len(e.queue.entries); n != 0 {
		t.Fatalf("%d replies queued — wrong or blank passwords must be silence", n)
	}
}

func TestFloodLoginEarnsAPathReturn(t *testing.T) {
	// A stranger logging in from across the mesh has no path to us:
	// the reply is a PATH return that both teaches it and carries the
	// answer, flooded back.
	e, dev, sub, peer := txRig(t, "on-air")
	e.p.GuestPassword = "raccoon"
	runEngine(t, e, dev)

	frame, secret := login(t, e.id, peer, 300, "raccoon", true)
	dev.frames <- frame
	if sent := awaitSent(t, sub); sent.Kind != "login-resp" {
		t.Fatalf("sent = %+v", sent)
	}
	pkt, err := meshcore.ParsePacket(<-dev.sent)
	if err != nil {
		t.Fatal(err)
	}
	if pkt.PayloadType() != meshcore.PayloadTypePath || !pkt.IsRouteFlood() {
		t.Fatalf("reply = %v %v, want a flooded PATH return", pkt.PayloadType(), pkt.Route())
	}
	dg, err := meshcore.ParseDatagram(pkt.Payload)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := dg.Open(secret)
	if err != nil {
		t.Fatal(err)
	}
	pr, err := meshcore.DecodePathReturn(plain)
	if err != nil {
		t.Fatal(err)
	}
	if meshcore.PayloadType(pr.ExtraType) != meshcore.PayloadTypeResponse || len(pr.Extra) < 9 {
		t.Fatalf("path return extra: type %d, %d bytes", pr.ExtraType, len(pr.Extra))
	}
}

func TestNeighboursAnswerListsWhoWeHear(t *testing.T) {
	e, dev, sub, peer := txRig(t, "on-air")
	e.p.GuestPassword = "raccoon"
	var third [32]byte
	third[0] = 0xAB
	e.neighbours.put(third, 9.25, time.Now().Add(-90*time.Second))
	runEngine(t, e, dev)

	frame, secret := login(t, e.id, peer, 400, "raccoon", false)
	dev.frames <- frame
	awaitSent(t, sub)
	<-dev.sent

	// version 0, count 10, offset 0, order newest, 8-byte prefixes,
	// then the 4-byte uniqueness blob.
	args := []byte{reqTypeGetNeighbrs, 0, 10, 0, 0, 0, 8, 1, 2, 3, 4}
	dev.frames <- request(t, e.id, peer, 401, args)
	awaitSent(t, sub)
	_, body := openReply(t, <-dev.sent, secret)
	total := binary.LittleEndian.Uint16(body[0:2])
	returned := binary.LittleEndian.Uint16(body[2:4])
	if total != 1 || returned != 1 {
		t.Fatalf("total=%d returned=%d", total, returned)
	}
	row := body[4:]
	if row[0] != 0xAB {
		t.Fatalf("prefix = % x", row[:8])
	}
	ago := binary.LittleEndian.Uint32(row[8:12])
	if ago < 85 || ago > 95 {
		t.Fatalf("heard %d s ago, want ~90", ago)
	}
	if int8(row[12]) != int8(9.25*4) {
		t.Fatalf("snr byte = %d", int8(row[12]))
	}
}
