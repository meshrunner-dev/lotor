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
