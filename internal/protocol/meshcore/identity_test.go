package meshcore

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"testing"
	"time"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/bus"
)

func TestIdentityFormats(t *testing.T) {
	ref, err := meshcore.NewLocalIdentity(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	prv := ref.PrvKey()

	// The reference CLI's prv.key form: 64-byte expanded key, public
	// key derived — the migration path from an existing node.
	fromPrv, err := identityFromConfig(hex.EncodeToString(prv))
	if err != nil {
		t.Fatal(err)
	}
	if fromPrv.PubKey != ref.PubKey {
		t.Fatal("prv.key import derived a different node")
	}

	// The 96-byte pair, and its rejection when the halves disagree.
	pair := hex.EncodeToString(append(append([]byte{}, prv...), ref.PubKey[:]...))
	if _, err := identityFromConfig(pair); err != nil {
		t.Fatalf("key pair refused: %v", err)
	}
	other, _ := meshcore.NewLocalIdentity(rand.Reader)
	bad := hex.EncodeToString(append(append([]byte{}, prv...), other.PubKey[:]...))
	if _, err := identityFromConfig(bad); err == nil {
		t.Fatal("mismatched key pair accepted")
	}

	// A seed round-trips deterministically.
	seed := "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f"
	a, err := identityFromConfig(seed)
	if err != nil {
		t.Fatal(err)
	}
	b, err := identityFromConfig(seed)
	if err != nil {
		t.Fatal(err)
	}
	if a.PubKey != b.PubKey {
		t.Fatal("seed expansion is not deterministic")
	}

	// Garbage is loud.
	for _, junk := range []string{"zz", "abcd", hex.EncodeToString(make([]byte, 48))} {
		if _, err := identityFromConfig(junk); err == nil {
			t.Errorf("accepted %q", junk)
		}
	}
}

// identifiedEngine builds an engine owning a fresh identity.
func identifiedEngine(t *testing.T) (*engine, *bus.Subscription) {
	t.Helper()
	e, sub := testEngine(t)
	id, err := meshcore.NewLocalIdentity(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	e.id = id
	return e, sub
}

func TestDirectAddressingJudgedWithIdentity(t *testing.T) {
	e, sub := identifiedEngine(t)

	ours := e.id.PubKey[0]
	toUs := []byte{0x02 | 0x02<<2, 0x02, ours, 0x99, 0x01, 0x02, 0x03}
	notUs := []byte{0x02 | 0x02<<2, 0x02, ^ours, 0x99, 0x01, 0x02, 0x04}
	e.judge(newFakeDevice(), frame(toUs))
	e.judge(newFakeDevice(), frame(notUs))

	judged := drainJudged(t, sub)
	if judged[0].Verdict != "would-relay-direct" {
		t.Errorf("addressed to us = %q", judged[0].Verdict)
	}
	if judged[1].Verdict != "direct-not-addressed" {
		t.Errorf("addressed elsewhere = %q", judged[1].Verdict)
	}
}

func TestFloodLoopDetected(t *testing.T) {
	e, sub := identifiedEngine(t)

	ours := e.id.PubKey[0]
	looped := []byte{0x01 | 0x05<<2, 0x03, 0x11, ours, 0x22, 0xDD, 0xEE}
	clean := []byte{0x01 | 0x05<<2, 0x03, 0x11, ^ours, 0x22, 0xDD, 0xEF}
	e.judge(newFakeDevice(), frame(looped))
	e.judge(newFakeDevice(), frame(clean))

	judged := drainJudged(t, sub)
	if judged[0].Verdict != "would-drop-flood-loop" {
		t.Errorf("looped flood = %q", judged[0].Verdict)
	}
	if judged[1].Verdict != "would-relay-flood" {
		t.Errorf("clean flood = %q", judged[1].Verdict)
	}
}

func TestSelfAdvertRecognised(t *testing.T) {
	e, sub := identifiedEngine(t)

	pkt, err := meshcore.BuildAdvert(e.id, time.Now(), &meshcore.AdvertData{
		Type: meshcore.AdvTypeRepeater, Name: "us",
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := pkt.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	e.judge(newFakeDevice(), frame(raw))

	judged := drainJudged(t, sub)
	if judged[0].Verdict != "self-advert" {
		t.Errorf("own echo = %q", judged[0].Verdict)
	}
	if judged[0].Node != "" {
		t.Errorf("a self advert must not enter the directory, node = %q", judged[0].Node)
	}
}

func TestTraceNextHopJudgedWithIdentity(t *testing.T) {
	e, sub := identifiedEngine(t)

	payload := func(next byte) []byte {
		p := make([]byte, 9, 11)
		binary.LittleEndian.PutUint32(p[0:], 1)
		binary.LittleEndian.PutUint32(p[4:], 2)
		p[8] = 0
		return append(p, next, 0xA2)
	}
	toUs := append([]byte{0x02 | 0x09<<2, 0x00}, payload(e.id.PubKey[0])...)
	notUs := append([]byte{0x02 | 0x09<<2, 0x00}, payload(^e.id.PubKey[0])...)
	e.judge(newFakeDevice(), frame(toUs))
	e.judge(newFakeDevice(), frame(notUs))

	judged := drainJudged(t, sub)
	if judged[0].Verdict != "would-relay-trace" {
		t.Errorf("trace next hop us = %q", judged[0].Verdict)
	}
	if judged[1].Verdict != "trace-not-addressed" {
		t.Errorf("trace next hop elsewhere = %q", judged[1].Verdict)
	}
}
