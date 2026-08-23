package meshcore

import (
	"crypto/rand"
	"encoding/binary"
	"path/filepath"
	"testing"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/bus"
)

func TestIdentityPersistsAcrossLoads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.id")
	first, err := loadOrCreateIdentity(path, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	second, err := loadOrCreateIdentity(path, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	if first.PubKey != second.PubKey {
		t.Fatal("reloaded identity differs — the node changed its name overnight")
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
	e.judge(frame(toUs))
	e.judge(frame(notUs))

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
	e.judge(frame(looped))
	e.judge(frame(clean))

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
	e.judge(frame(raw))

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
	e.judge(frame(toUs))
	e.judge(frame(notUs))

	judged := drainJudged(t, sub)
	if judged[0].Verdict != "would-relay-trace" {
		t.Errorf("trace next hop us = %q", judged[0].Verdict)
	}
	if judged[1].Verdict != "trace-not-addressed" {
		t.Errorf("trace next hop elsewhere = %q", judged[1].Verdict)
	}
}
