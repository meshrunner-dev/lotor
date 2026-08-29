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
