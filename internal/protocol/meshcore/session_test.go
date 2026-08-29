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
	e.p.GuestAccess, e.p.GuestPassword = guestPassword, "raccoon"
	runEngine(t, e, dev)

	frame, secret := login(t, e.id, peer, nowTS(100), "raccoon", false)
	dev.frames <- frame
	if sent := awaitSent(t, sub); sent.Kind != "login-resp" {
		t.Fatalf("sent = %+v", sent)
	}
	_, body := openReply(t, <-dev.sent, secret)
	if len(body) < 9 || body[0] != meshcore.LoginOK || body[2] != 0 || body[3] != permGuest {
		t.Fatalf("login reply = % x — want OK, no admin, guest perms", body)
	}

	// The session works: ask for the status.
	dev.frames <- request(t, e.id, peer, nowTS(101), []byte{meshcore.ReqGetStatus, 0, 0, 0, 0})
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
	dev.frames <- request(t, e.id, peer, nowTS(101), []byte{meshcore.ReqGetStatus, 0, 0, 0, 0})
	select {
	case raw := <-dev.sent:
		t.Fatalf("a replayed request was answered: % x", raw[:8])
	case <-time.After(700 * time.Millisecond):
	}
}

func TestWrongPasswordIsSilence(t *testing.T) {
	e, _, _, peer := txRig(t, "on-air")
	e.p.GuestAccess, e.p.GuestPassword = guestPassword, "raccoon"
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
	e.p.GuestAccess, e.p.GuestPassword = guestPassword, "raccoon"
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
	e.p.GuestAccess, e.p.GuestPassword = guestPassword, "raccoon"
	var third [32]byte
	third[0] = 0xAB
	e.neighbours.put(third, "", 9.25, time.Now().Add(-90*time.Second))
	runEngine(t, e, dev)

	frame, secret := login(t, e.id, peer, nowTS(400), "raccoon", false)
	dev.frames <- frame
	awaitSent(t, sub)
	<-dev.sent

	// version 0, count 10, offset 0, order newest, 8-byte prefixes,
	// then the 4-byte uniqueness blob.
	args := []byte{meshcore.ReqGetNeighbours, 0, 10, 0, 0, 0, 8, 1, 2, 3, 4}
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

func TestAGuesserCannotLockTheOwnerOut(t *testing.T) {
	// The reference bounds logins not at all, and neither does this
	// engine any more: a failed attempt earns silence, so a guesser
	// pays the airtime of every try and gains nothing — while the
	// honest word must work immediately after any burst of wrong
	// ones. The old login limiter had it backwards: a companion's
	// retry burst after a restart locked its own owner out.
	e, _, _, peer := txRig(t, "shadow")
	e.p.GuestAccess, e.p.GuestPassword = guestPassword, "open-sesame"
	e.queue.depth = 16

	for i := range 12 {
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
	// The right password is served at once, whatever came before.
	frame, _ := login(t, e.id, peer, nowTS(99), "open-sesame", false)
	pkt, err := meshcore.ParsePacket(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	e.respondAnon(rxOf(e, pkt), txn.New())
	if n := len(e.queue.entries); n != 1 {
		t.Fatalf("%d replies to the right password after a guess burst, want 1", n)
	}
}

func TestARoutedSessionIsNeverCharged(t *testing.T) {
	// The budget exists for amplification, and a client that taught a
	// route home amplifies nothing: its answers walk one directed
	// path, and flow as freely as the reference serves them.
	e, _, _, peer := txRig(t, "shadow")
	e.p.GuestAccess, e.p.GuestPassword = guestPassword, "raccoon"
	e.queue.depth = 64
	frame, _ := login(t, e.id, peer, nowTS(700), "raccoon", false)
	pkt, err := meshcore.ParsePacket(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	e.respondAnon(rxOf(e, pkt), txn.New())
	c := e.acl.get(peer.PubKey[:])
	if c == nil {
		t.Fatal("no session after login")
	}
	c.out = &outPath{pathLen: 0, path: nil, learned: time.Now()} // adjacent

	served := len(e.queue.entries)
	for i := range sessionLimitMax + 4 {
		req := request(t, e.id, peer, nowTS(uint32(701+i)), []byte{meshcore.ReqGetStatus, 0, 0, 0, 0})
		rpkt, err := meshcore.ParsePacket(req.Payload)
		if err != nil {
			t.Fatal(err)
		}
		rx := rxOf(e, rpkt)
		if _, _, handled := e.reqVerdict(rx); !handled {
			t.Fatal("request not recognised")
		}
		e.respondRequest(rx, txn.New())
	}
	if n := len(e.queue.entries) - served; n != sessionLimitMax+4 {
		t.Fatalf("%d answers down the taught route, want all %d", n, sessionLimitMax+4)
	}
}

func TestOneSessionCannotFloodTheMesh(t *testing.T) {
	// Every authenticated answer floods, so a guest polling in a loop
	// would spend the whole mesh's airtime. Its own budget bounds it.
	e, dev, sub, peer := txRig(t, "shadow")
	e.p.GuestAccess, e.p.GuestPassword = guestPassword, "raccoon"
	e.queue.depth = 64
	runEngine(t, e, dev)

	frame, _ := login(t, e.id, peer, nowTS(600), "raccoon", false)
	dev.frames <- frame
	if sent := awaitSent(t, sub); sent.Kind != "login-resp" {
		t.Fatalf("sent = %+v", sent)
	}
	for i := range sessionLimitMax + 3 {
		dev.frames <- request(t, e.id, peer, nowTS(uint32(601+i)),
			[]byte{meshcore.ReqGetStatus, 0, 0, 0, 0})
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
	e.p.GuestAccess, e.p.GuestPassword = guestPassword, "raccoon"
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
		[]byte{meshcore.ReqKeepAlive, 0, 0, 0, 0}).Payload)
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
	a := newACL(nil)
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

func TestGuestAccessModes(t *testing.T) {
	// Blocked by default; a password is enough to mean "password";
	// an open door has to be named, and cannot also carry a credential.
	for _, c := range []struct {
		access, password, want string
		refused                bool
	}{
		{"", "", guestBlocked, false},
		{"", "hunter2", guestPassword, false},
		{guestBlocked, "", guestBlocked, false},
		{guestOpen, "", guestOpen, false},
		{guestPassword, "hunter2", guestPassword, false},
		{guestBlocked, "hunter2", "", true},
		{guestOpen, "hunter2", "", true},
		{guestPassword, "", "", true},
		{"maybe", "", "", true},
	} {
		p := params{GuestAccess: c.access, GuestPassword: c.password}
		err := normalizeGuest(&p)
		if c.refused {
			if err == nil {
				t.Errorf("access %q with password %q was accepted", c.access, c.password)
			}
			continue
		}
		if err != nil {
			t.Errorf("access %q with password %q refused: %v", c.access, c.password, err)
		} else if p.GuestAccess != c.want {
			t.Errorf("access %q resolved to %q, want %q", c.access, p.GuestAccess, c.want)
		}
	}
}

func TestOpenGuestNeedsNoPassword(t *testing.T) {
	e, dev, sub, peer := txRig(t, "shadow")
	e.p.GuestAccess = guestOpen
	runEngine(t, e, dev)

	frame, _ := login(t, e.id, peer, nowTS(900), "", false)
	dev.frames <- frame
	if sent := awaitSent(t, sub); sent.Kind != "login-resp" {
		t.Fatalf("sent = %+v, want an open door to answer", sent)
	}
	if e.acl.get(peer.PubKey[:]) == nil {
		t.Fatal("the open login left no session")
	}
}

func TestBlockedGuestAnswersNobody(t *testing.T) {
	e, dev, sub, peer := txRig(t, "shadow")
	e.p.GuestAccess = guestBlocked
	runEngine(t, e, dev)
	frame, _ := login(t, e.id, peer, nowTS(950), "", false)
	dev.frames <- frame

	select {
	case ev := <-sub.C:
		if s, ok := ev.(bus.FrameSent); ok {
			t.Fatalf("a blocked door answered: %+v", s)
		}
	case <-time.After(500 * time.Millisecond):
	}
}

func TestAFreshLoginDropsTheRouteTheOldConversationTaught(t *testing.T) {
	// The scenario off the air: a companion logs in, teaches a route,
	// then reconfigures itself and logs in again — direct. The reply
	// must not ride the dead route: the login opened a new
	// conversation, and whatever the old one taught belongs to it.
	e, _, _, peer := txRig(t, "on-air")
	e.p.GuestAccess, e.p.GuestPassword = guestPassword, "raccoon"
	e.queue.depth = 8

	frame, _ := login(t, e.id, peer, nowTS(300), "raccoon", false)
	pkt, err := meshcore.ParsePacket(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	e.respondAnon(rxOf(e, pkt), txn.New())
	// Every login installs a fresh session object in the table's
	// place — the composed-then-installed discipline a replayed login
	// must not be able to short-circuit — so the route is read back
	// from the table, never from a pointer taken before.
	session := func() *client {
		t.Helper()
		c := e.acl.get(peer.PubKey[:])
		if c == nil {
			t.Fatal("the login made no session")
		}
		return c
	}
	session().out = &outPath{pathLen: 2, path: []byte{0x4f, 0xa2}, learned: time.Now()}

	frame, _ = login(t, e.id, peer, nowTS(301), "raccoon", false)
	if pkt, err = meshcore.ParsePacket(frame.Payload); err != nil {
		t.Fatal(err)
	}
	e.respondAnon(rxOf(e, pkt), txn.New())
	if session().out != nil {
		t.Error("the stale route survived the new login")
	}
	if n := len(e.queue.entries); n < 2 {
		t.Fatalf("%d replies queued, want 2", n)
	}
	reply := e.queue.entries[len(e.queue.entries)-1].pkt
	if !reply.IsRouteFlood() {
		t.Errorf("the reply rode a route the login just invalidated: header %#x", reply.Header)
	}

	// A blank-password recheck is a step inside the same conversation,
	// and keeps the route it stands on.
	session().out = &outPath{pathLen: 0, path: []byte{}, learned: time.Now()}
	frame, _ = login(t, e.id, peer, nowTS(302), "", false)
	if pkt, err = meshcore.ParsePacket(frame.Payload); err != nil {
		t.Fatal(err)
	}
	e.respondAnon(rxOf(e, pkt), txn.New())
	if session().out == nil {
		t.Error("a recheck dropped the route it should stand on")
	}

	// And a flood login rediscovers, whatever it carried: the asker
	// itself said it has no route, so ours to it is suspect too.
	frame, _ = login(t, e.id, peer, nowTS(303), "raccoon", true)
	if pkt, err = meshcore.ParsePacket(frame.Payload); err != nil {
		t.Fatal(err)
	}
	e.respondAnon(rxOf(e, pkt), txn.New())
	if session().out != nil {
		t.Error("a flood login kept a route it must rediscover")
	}
}

// withHops re-marshals a direct frame with hops still on its path, the
// shape a routed packet has in transit.
func withHops(t *testing.T, frame radio.Frame, hops ...byte) radio.Frame {
	t.Helper()
	pkt, err := meshcore.ParsePacket(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	pkt.Path, pkt.PathLen = hops, uint8(len(hops))
	raw, err := pkt.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	frame.Payload = raw
	return frame
}

func TestATransitCopyMustNotDeduplicateTheDelivery(t *testing.T) {
	// Seen off the air: a companion logs in along a configured path.
	// The packet is heard twice — once with hops remaining, in transit
	// between the repeaters it names, and once with the path consumed,
	// which is the delivery. The transit copy is not ours and must not
	// poison the duplicate table against the delivery.
	e, dev, sub, peer := txRig(t, "on-air")
	e.p.GuestAccess, e.p.GuestPassword = guestPassword, "raccoon"
	e.queue.depth = 8

	frame, _ := login(t, e.id, peer, nowTS(400), "raccoon", false)
	e.judge(dev, withHops(t, frame, 0x77, 0x33))
	if v := awaitJudged(t, sub); v.Verdict != verdictNotAddressed {
		t.Fatalf("the transit copy was judged %q, want %q", v.Verdict, verdictNotAddressed)
	}
	e.judge(dev, frame)
	v := awaitJudged(t, sub)
	if v.Verdict != verdictAnon {
		t.Fatalf("the delivery was judged %q, want %q — the transit copy poisoned the table",
			v.Verdict, verdictAnon)
	}
	if n := len(e.queue.entries); n != 1 {
		t.Fatalf("%d replies queued, want the login answered once", n)
	}

	// The other half of the reference's rule still holds: a packet we
	// relay is witnessed, so its echo cannot be relayed twice.
	frame2, _ := login(t, e.id, peer, nowTS(401), "raccoon", false)
	relayed := withHops(t, frame2, e.id.PubKey[0], 0x33)
	e.judge(dev, relayed)
	if v := awaitJudged(t, sub); v.Verdict != verdictRelayDirect {
		t.Fatalf("the next-hop copy was judged %q, want %q", v.Verdict, verdictRelayDirect)
	}
	e.judge(dev, relayed)
	if v := awaitJudged(t, sub); v.Verdict != verdictDuplicate {
		t.Fatalf("the echo of a relayed packet was judged %q, want %q", v.Verdict, verdictDuplicate)
	}
}

// awaitJudged reads bus events until a judgement arrives.
func awaitJudged(t *testing.T, sub *bus.Subscription) bus.FrameJudged {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case ev := <-sub.C:
			if j, ok := ev.(bus.FrameJudged); ok {
				return j
			}
		case <-deadline:
			t.Fatal("no judgement arrived")
		}
	}
}

func TestAdminLoginGrantsTheRole(t *testing.T) {
	e, dev, sub, peer := txRig(t, "on-air")
	e.p.GuestAccess, e.p.GuestPassword = guestPassword, "raccoon"
	e.p.AdminPassword = "mask"
	runEngine(t, e, dev)

	frame, secret := login(t, e.id, peer, nowTS(300), "mask", false)
	dev.frames <- frame
	if sent := awaitSent(t, sub); sent.Kind != "login-resp" {
		t.Fatalf("sent = %+v", sent)
	}
	_, body := openReply(t, <-dev.sent, secret)
	if len(body) < 4 || body[0] != meshcore.LoginOK || body[2] != 1 || body[3]&permRoleMask != permAdmin {
		t.Fatalf("login reply = % x — want OK, admin bit, admin perms", body)
	}

	// A password login sets the role the password earns, demotion
	// included — the reference rewrites the bits on every one; only
	// the blank in-ACL recheck keeps a granted role.
	frame, secret = login(t, e.id, peer, nowTS(301), "raccoon", false)
	dev.frames <- frame
	if sent := awaitSent(t, sub); sent.Kind != "login-resp" {
		t.Fatalf("sent = %+v", sent)
	}
	if _, body := openReply(t, <-dev.sent, secret); body[2] != 0 {
		t.Fatalf("the guest word kept the admin role: % x", body)
	}
	// And the admin word takes it back.
	frame, secret = login(t, e.id, peer, nowTS(302), "mask", false)
	dev.frames <- frame
	awaitSent(t, sub)
	if _, body := openReply(t, <-dev.sent, secret); body[2] != 1 {
		t.Fatalf("the admin word did not restore the role: % x", body)
	}
}

func TestAnEmptyAdminPasswordGrantsNothing(t *testing.T) {
	// OTA admin is off until a password exists: an empty submission
	// must not match an empty setting.
	e, _, _, peer := txRig(t, "on-air")
	e.p.GuestAccess = guestBlocked
	e.queue.depth = 8
	frame, _ := login(t, e.id, peer, nowTS(310), "", false)
	pkt, err := meshcore.ParsePacket(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	e.respondAnon(rxOf(e, pkt), txn.New())
	if n := len(e.queue.entries); n != 0 {
		t.Fatalf("an empty password earned %d replies", n)
	}
}

func TestAdminEqualsGuestIsRefused(t *testing.T) {
	p := params{NodeName: "x", GuestAccess: guestPassword,
		GuestPassword: "same", AdminPassword: "same"}
	if err := normalizeGuest(&p); err == nil {
		t.Fatal("one word granting two roles was accepted")
	}
}

func TestAdminOnlyPostureStillAcceptsItsAdmin(t *testing.T) {
	// Guests blocked, administration from the field: an ordinary
	// posture, and gating logins on the guest mode alone locked the
	// owner out of their own repeater.
	e, dev, sub, peer := txRig(t, "on-air")
	e.p.GuestAccess, e.p.AdminPassword = guestBlocked, "mask"
	runEngine(t, e, dev)

	frame, secret := login(t, e.id, peer, nowTS(600), "mask", false)
	dev.frames <- frame
	if sent := awaitSent(t, sub); sent.Kind != "login-resp" {
		t.Fatalf("the admin was refused its own door: %+v", sent)
	}
	if _, body := openReply(t, <-dev.sent, secret); body[2] != 1 {
		t.Fatalf("login reply = % x — want the admin bit", body)
	}

	// A guest word still earns nothing here.
	frame, _ = login(t, e.id, peer, nowTS(601), "raccoon", false)
	dev.frames <- frame
	select {
	case raw := <-dev.sent:
		t.Fatalf("a guest password opened a blocked door: % x", raw[:8])
	case <-time.After(700 * time.Millisecond):
	}
}
