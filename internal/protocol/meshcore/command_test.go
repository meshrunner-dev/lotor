package meshcore

import (
	"testing"
	"time"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/radio"
)

// commandFrame is a companion sending one CLI line to this node.
func commandFrame(t *testing.T, self, peer *meshcore.LocalIdentity,
	at time.Time, line string,
) radio.Frame {
	t.Helper()
	secret, err := peer.SharedSecret(self.PubKey[:])
	if err != nil {
		t.Fatal(err)
	}
	plain := meshcore.BuildTextPlaintext(at, meshcore.TxtTypeCLIData, line)
	pkt, err := meshcore.BuildDatagram(meshcore.PayloadTypeTxtMsg,
		self.PubKey[:meshcore.PathHashSize], peer.PubKey[:meshcore.PathHashSize],
		secret, plain)
	if err != nil {
		t.Fatal(err)
	}
	pkt.Header = meshcore.MakeHeader(meshcore.RouteDirect,
		meshcore.PayloadTypeTxtMsg, meshcore.PayloadVer1)
	raw, err := pkt.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return radio.Frame{Payload: raw, At: time.Now()}
}

func TestAnAdminCommandRunsAndAnswers(t *testing.T) {
	e, dev, sub, peer := txRig(t, "on-air")
	e.p.AdminPassword = "mask"
	var ran []string
	e.AttachCommands(func(line string, admin []byte) string {
		ran = append(ran, line)
		return "name: Raccoon City"
	})
	runEngine(t, e, dev)

	frame, secret := login(t, e.id, peer, nowTS(400), "mask", false)
	dev.frames <- frame
	awaitSent(t, sub)
	<-dev.sent

	at := time.Unix(int64(nowTS(410)), 0)
	dev.frames <- commandFrame(t, e.id, peer, at, "get name")
	if sent := awaitSent(t, sub); sent.Kind != "cmd-resp" {
		t.Fatalf("sent = %+v", sent)
	}
	raw := <-dev.sent
	pkt, err := meshcore.ParsePacket(raw)
	if err != nil {
		t.Fatal(err)
	}
	if pkt.PayloadType() != meshcore.PayloadTypeTxtMsg {
		t.Fatalf("answer rode as %s, want TXT_MSG", pkt.PayloadType())
	}
	d, err := meshcore.ParseDatagram(pkt.Payload)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := d.Open(secret)
	if err != nil {
		t.Fatal(err)
	}
	text, err := meshcore.ParseTextPlaintext(plain)
	if err != nil {
		t.Fatal(err)
	}
	if text.Text != "name: Raccoon City" || text.Type != meshcore.TxtTypeCLIData {
		t.Errorf("answer = %q type=%d", text.Text, text.Type)
	}
	if len(ran) != 1 || ran[0] != "get name" {
		t.Fatalf("the door saw %v", ran)
	}

	// A retry — the same timestamp again — must not run the line
	// twice: the mutation already took.
	dev.frames <- commandFrame(t, e.id, peer, at, "get name")
	select {
	case <-dev.sent:
	case <-time.After(700 * time.Millisecond):
	}
	if len(ran) != 1 {
		t.Errorf("a retry ran the command again: %v", ran)
	}
}

func TestAGuestCommandIsNotAdministration(t *testing.T) {
	e, dev, sub, peer := txRig(t, "on-air")
	e.p.GuestAccess, e.p.GuestPassword = guestPassword, "raccoon"
	ran := 0
	e.AttachCommands(func(string, []byte) string { ran++; return "should not run" })
	runEngine(t, e, dev)

	frame, _ := login(t, e.id, peer, nowTS(500), "raccoon", false)
	dev.frames <- frame
	awaitSent(t, sub)
	<-dev.sent

	dev.frames <- commandFrame(t, e.id, peer, time.Unix(int64(nowTS(510)), 0), "set name pwned")
	select {
	case raw := <-dev.sent:
		t.Fatalf("a guest's command was answered: % x", raw[:8])
	case <-time.After(700 * time.Millisecond):
	}
	if ran != 0 {
		t.Errorf("a guest reached the mutation door %d times", ran)
	}
}

func TestAMutatingCommandDoesNotBlockTheLoop(t *testing.T) {
	// The freeze: a set command called the manager synchronously from
	// this very goroutine, and the bounce it triggered waited for the
	// goroutine to die — deadlock. The command hook must return at
	// once, whatever the line does. Here the hook stands in for the
	// manager, and the test proves the engine answers and keeps
	// serving.
	e, dev, sub, peer := txRig(t, "on-air")
	e.p.AdminPassword = "mask"
	done := make(chan struct{})
	e.AttachCommands(func(line string, admin []byte) string {
		close(done) // a real hook queues async and returns; never blocks
		return "applied — tx_power_dbm will change, relay restarting"
	})
	runEngine(t, e, dev)

	frame, secret := login(t, e.id, peer, nowTS(800), "mask", false)
	dev.frames <- frame
	awaitSent(t, sub)
	<-dev.sent

	dev.frames <- commandFrame(t, e.id, peer, time.Unix(int64(nowTS(810)), 0), "set tx 6")
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the command hook was never reached — the loop is stuck")
	}
	if sent := awaitSent(t, sub); sent.Kind != "cmd-resp" {
		t.Fatalf("no reply to the set: %+v", sent)
	}
	raw := <-dev.sent
	pkt, _ := meshcore.ParsePacket(raw)
	d, _ := meshcore.ParseDatagram(pkt.Payload)
	plain, err := d.Open(secret)
	if err != nil {
		t.Fatal(err)
	}
	text, err := meshcore.ParseTextPlaintext(plain)
	if err != nil {
		t.Fatal(err)
	}
	if text.Text == "" {
		t.Fatal("the admin got no OK back")
	}

	// The loop still serves the next thing: a plain status request
	// after the set is answered normally.
	dev.frames <- request(t, e.id, peer, nowTS(811), []byte{meshcore.ReqGetStatus, 0, 0, 0, 0})
	if sent := awaitSent(t, sub); sent.Kind != "req-resp" {
		t.Fatalf("the loop stopped serving after a set: %+v", sent)
	}
}

func TestTheAccessListAnswersItsAdminAlone(t *testing.T) {
	e, dev, sub, peer := txRig(t, "on-air")
	e.p.AdminPassword = "mask"
	e.p.GuestAccess, e.p.GuestPassword = guestPassword, "raccoon"
	e.AttachSessions(newMemStore())
	runEngine(t, e, dev)

	// A grant to a third key, so the list has something to say. A
	// real identity: the grant derives a secret, and a byte pattern
	// is not a curve point.
	seed := make([]byte, meshcore.SeedSize)
	seed[0] = 7
	thirdID, err := meshcore.LocalIdentityFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	third := thirdID.PubKey
	if err := e.Grant(third[:], PermReadWrite); err != nil {
		t.Fatal(err)
	}

	frame, secret := login(t, e.id, peer, nowTS(900), "mask", false)
	dev.frames <- frame
	awaitSent(t, sub)
	<-dev.sent

	dev.frames <- request(t, e.id, peer, nowTS(901), []byte{meshcore.ReqGetAccessList, 0, 0, 0, 0})
	if sent := awaitSent(t, sub); sent.Kind != "req-resp" {
		t.Fatalf("no access list: %+v", sent)
	}
	tag, body := openReply(t, <-dev.sent, secret)
	if tag != nowTS(901) {
		t.Fatalf("tag = %d", tag)
	}
	// Seven bytes per entry: the six-byte prefix and the byte. The
	// admin session and the grant are here; the padding past the
	// entries is the cipher's, all zero, and a zero role is skipped
	// on the wire so the tail reads unambiguously.
	if len(body) < 14 {
		t.Fatalf("body = % x", body)
	}
	found := map[byte]byte{}
	for i := 0; i+7 <= len(body); i += 7 {
		if body[i+6]&permRoleMask == permGuest {
			continue // padding, or nothing
		}
		found[body[i]] = body[i+6]
	}
	if found[third[0]]&permRoleMask != PermReadWrite {
		t.Errorf("the grant is missing or wrong: %v", found)
	}
	if found[peer.PubKey[0]]&permRoleMask != PermAdmin {
		t.Errorf("the asking admin is missing: %v", found)
	}

	// A guest asking earns silence, the reference's own gate.
	gfr, _ := login(t, e.id, peer, nowTS(910), "raccoon", false)
	dev.frames <- gfr
	awaitSent(t, sub)
	<-dev.sent
	dev.frames <- request(t, e.id, peer, nowTS(911), []byte{meshcore.ReqGetAccessList, 0, 0, 0, 0})
	select {
	case raw := <-dev.sent:
		t.Fatalf("a guest read the access list: % x", raw[:8])
	case <-time.After(700 * time.Millisecond):
	}
}

func TestTelemetryMaskReachesTheSensors(t *testing.T) {
	// The contract the sensor work will land on: the wire's reserved
	// byte arrives inverted, a guest is forced past it to zero, and
	// the hook always runs — which sensor a mask admits is the
	// sensors' own judgement.
	e, dev, sub, peer := txRig(t, "on-air")
	e.p.AdminPassword = "mask"
	e.p.GuestAccess, e.p.GuestPassword = guestPassword, "raccoon"
	var masks []byte
	e.AttachTelemetry(func(permMask byte, enc *meshcore.LPPEncoder) error {
		masks = append(masks, permMask)
		return nil
	})
	runEngine(t, e, dev)

	frame, _ := login(t, e.id, peer, nowTS(950), "mask", false)
	dev.frames <- frame
	awaitSent(t, sub)
	<-dev.sent
	// An admin asking with reserved byte 0x0F gets ^0x0F = 0xF0.
	dev.frames <- request(t, e.id, peer, nowTS(951), []byte{meshcore.ReqGetTelemetry, 0x0F, 0, 0, 0})
	awaitSent(t, sub)
	<-dev.sent

	frame, _ = login(t, e.id, peer, nowTS(960), "raccoon", false)
	dev.frames <- frame
	awaitSent(t, sub)
	<-dev.sent
	// A guest asking for everything is forced to the base readings.
	dev.frames <- request(t, e.id, peer, nowTS(961), []byte{meshcore.ReqGetTelemetry, 0x00, 0, 0, 0})
	awaitSent(t, sub)
	<-dev.sent

	if len(masks) != 2 || masks[0] != 0xF0 || masks[1] != 0x00 {
		t.Fatalf("masks = %#v — want [0xF0 0x00]", masks)
	}
}
