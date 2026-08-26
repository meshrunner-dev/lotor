package meshcore

import (
	"encoding/binary"
	"encoding/hex"
	"testing"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/txn"
)

// nowTS is where the tests put their clock: the freshness gate reads
// a login's stamp against ours, so a 1970 counter would be a
// recording, not a request.
// The base is taken once, so a stamp built at the top of a test and
// the assertion that reads it back agree even across a second boundary.
var testEpoch = uint32(time.Now().Unix())

func nowTS(offset uint32) uint32 { return testEpoch + offset }

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

	frame, secret := login(t, e.id, peer, nowTS(100), "raccoon", false)
	dev.frames <- frame
	if sent := awaitSent(t, sub); sent.Kind != "login-resp" {
		t.Fatalf("sent = %+v", sent)
	}
	_, body := openReply(t, <-dev.sent, secret)
	if len(body) < 9 || body[0] != respLoginOK || body[2] != 0 || body[3] != permGuest {
		t.Fatalf("login reply = % x — want OK, no admin, guest perms", body)
	}

	// The session works: ask for the status.
	dev.frames <- request(t, e.id, peer, nowTS(101), []byte{reqTypeGetStatus, 0, 0, 0, 0})
	if sent := awaitSent(t, sub); sent.Kind != "req-resp" {
		t.Fatalf("sent = %+v", sent)
	}
	tag, blob := openReply(t, <-dev.sent, secret)
	if tag != nowTS(101) {
		t.Fatalf("tag = %d, want the request timestamp reflected", tag)
	}
	// The reference's RepeaterStats is 56 bytes; the opened datagram
	// carries block-cipher padding past it.
	if len(blob) < 56 {
		t.Fatalf("status blob = %d bytes, want at least the reference's 56", len(blob))
	}
	// Replay: the same timestamp must be refused.
	dev.frames <- request(t, e.id, peer, nowTS(101), []byte{reqTypeGetStatus, 0, 0, 0, 0})
	select {
	case raw := <-dev.sent:
		t.Fatalf("a replayed request was answered: % x", raw[:8])
	case <-time.After(700 * time.Millisecond):
	}
}

func TestWrongPasswordIsSilence(t *testing.T) {
	e, _, _, peer := txRig(t, "on-air")
	e.p.GuestPassword = "raccoon"
	e.queue.depth = 8
	for i, pw := range []string{"admin", "wrong", ""} {
		frame, _ := login(t, e.id, peer, nowTS(uint32(200+i)), pw, false)
		pkt, err := meshcore.ParsePacket(frame.Payload)
		if err != nil {
			t.Fatal(err)
		}
		e.respondAnon(rxOf(e, pkt), txn.New())
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

	frame, secret := login(t, e.id, peer, nowTS(300), "raccoon", true)
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

	frame, secret := login(t, e.id, peer, nowTS(400), "raccoon", false)
	dev.frames <- frame
	awaitSent(t, sub)
	<-dev.sent

	// version 0, count 10, offset 0, order newest, 8-byte prefixes,
	// then the 4-byte uniqueness blob.
	args := []byte{reqTypeGetNeighbours, 0, 10, 0, 0, 0, 8, 1, 2, 3, 4}
	dev.frames <- request(t, e.id, peer, nowTS(401), args)
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

func TestDryEngineSurvivesAStrangersRequest(t *testing.T) {
	// A relay that never transmits still judges everything it hears,
	// and judging an authenticated request consults the session table.
	// That table must exist in every mode: when it did not, one packet
	// from anybody — no session, no password, no MAC — killed the
	// daemon.
	seed := make([]byte, meshcore.SeedSize)
	seed[0] = 5
	id, err := meshcore.LocalIdentityFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	built, err := build("dry-relay", map[string]any{
		"frequency_hz": 869_618_000,
		"identity":     hex.EncodeToString(seed),
	}, bus.New(), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	e, ok := built.(*engine)
	if !ok {
		t.Fatalf("build returned %T", built)
	}
	if e.txEnabled() {
		t.Fatal("the rig is meant to be dry")
	}

	// A request addressed to our key prefix, from nobody at all.
	pkt := &meshcore.Packet{
		Header: meshcore.MakeHeader(meshcore.RouteDirect,
			meshcore.PayloadTypeReq, meshcore.PayloadVer1),
		Payload: append([]byte{id.PubKey[0], 0x99}, make([]byte, 24)...),
	}
	raw, err := pkt.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	e.judge(newFakeDevice(), radio.Frame{Payload: raw, At: time.Now(), SNR: 5})
	// Surviving the call is the assertion.
}

func TestEveryLoginAttemptCostsAToken(t *testing.T) {
	// A limiter that only sees successes bounds the honest client and
	// lets the guesser run free.
	e, _, _, peer := txRig(t, "shadow")
	e.p.GuestPassword = "open-sesame"
	e.queue.depth = 16

	for i := range loginLimitMax {
		frame, _ := login(t, e.id, peer, nowTS(uint32(i+1)), "wrong", false)
		pkt, err := meshcore.ParsePacket(frame.Payload)
		if err != nil {
			t.Fatal(err)
		}
		e.respondAnon(rxOf(e, pkt), txn.New())
	}
	if n := len(e.queue.entries); n != 0 {
		t.Fatalf("%d replies to wrong passwords", n)
	}
	// The window is spent: even the right password waits its turn.
	frame, _ := login(t, e.id, peer, nowTS(99), "open-sesame", false)
	pkt, err := meshcore.ParsePacket(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	e.respondAnon(rxOf(e, pkt), txn.New())
	if n := len(e.queue.entries); n != 0 {
		t.Fatalf("%d replies past the exhausted window — guessing is unbounded", n)
	}
}

func TestOneSessionCannotFloodTheMesh(t *testing.T) {
	// Every authenticated answer floods, so a guest polling in a loop
	// would spend the whole mesh's airtime. Its own budget bounds it.
	e, dev, sub, peer := txRig(t, "shadow")
	e.p.GuestPassword = "raccoon"
	e.queue.depth = 64
	runEngine(t, e, dev)

	frame, _ := login(t, e.id, peer, nowTS(600), "raccoon", false)
	dev.frames <- frame
	if sent := awaitSent(t, sub); sent.Kind != "login-resp" {
		t.Fatalf("sent = %+v", sent)
	}
	for i := range sessionLimitMax + 3 {
		dev.frames <- request(t, e.id, peer, nowTS(uint32(601+i)),
			[]byte{reqTypeGetStatus, 0, 0, 0, 0})
	}

	answers, refused := 0, 0
	deadline := time.After(3 * time.Second)
	for answers+refused < sessionLimitMax+3 {
		select {
		case ev := <-sub.C:
			switch v := ev.(type) {
			case bus.FrameSent:
				if v.Kind == "req-resp" {
					answers++
				}
			case bus.TxDropped:
				if v.Reason == "rate-limited" {
					refused++
				}
			}
		case <-deadline:
			t.Fatalf("%d answered, %d refused — the rest never resolved", answers, refused)
		}
	}
	if answers != sessionLimitMax || refused != 3 {
		t.Fatalf("%d answers and %d refusals, want %d and 3", answers, refused, sessionLimitMax)
	}
}

func TestKeepAliveKeepsTheSessionAlive(t *testing.T) {
	// A companion that sends a keep-alive instead of polling must not
	// be the one retired first.
	e, _, _, peer := txRig(t, "shadow")
	e.p.GuestPassword = "raccoon"
	frame, _ := login(t, e.id, peer, nowTS(700), "raccoon", false)
	pkt, err := meshcore.ParsePacket(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	e.respondAnon(rxOf(e, pkt), txn.New())

	c := e.acl.get(peer.PubKey[:])
	if c == nil {
		t.Fatal("login left no session")
	}
	c.lastActive = time.Now().Add(-30 * time.Minute)

	req, err := meshcore.ParsePacket(request(t, e.id, peer, nowTS(701),
		[]byte{reqTypeKeepAlive, 0, 0, 0, 0}).Payload)
	if err != nil {
		t.Fatal(err)
	}
	e.respondRequest(rxOf(e, req), txn.New())

	if again := e.acl.get(peer.PubKey[:]); again == nil {
		t.Fatal("the session was retired")
	} else if time.Since(again.lastActive) > time.Minute {
		t.Fatalf("keep-alive left lastActive at %v — it keeps nothing alive", again.lastActive)
	}
}

func TestIdleSessionsRetire(t *testing.T) {
	a := newACL()
	var key [meshcore.PubKeySize]byte
	key[0] = 0xAB
	a.put(&client{pubKey: key, lastActive: time.Now().Add(-2 * sessionIdle)})
	if a.get(key[:]) != nil {
		t.Fatal("an idle session answered as live")
	}
	if got := a.matching(0xAB); len(got) != 0 {
		t.Fatalf("%d idle sessions still matched", len(got))
	}
}
