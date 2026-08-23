package meshcore

import (
	"encoding/binary"
	"testing"
)

// Reference gates, walked one by one. Raw frames:
// [header][transport?][path_len][path…][payload…].
func judgeOne(t *testing.T, raw []byte) string {
	t.Helper()
	e, sub := testEngine(t)
	e.judge(frame(raw))
	judged := drainJudged(t, sub)
	if len(judged) != 1 {
		t.Fatalf("judged %d frames", len(judged))
	}
	return judged[0].Verdict
}

func TestFloodGates(t *testing.T) {
	// CONTROL and TRACE are never re-flooded by the reference.
	if v := judgeOne(t, []byte{0x01 | 0x0B<<2, 0x00, 0x80}); v != "would-drop-flood-type" {
		t.Errorf("flood CONTROL = %q", v)
	}
	if v := judgeOne(t, []byte{0x01 | 0x09<<2, 0x00, 1, 2, 3, 4, 5, 6, 7, 8, 0}); v != "would-drop-flood-type" {
		t.Errorf("flood TRACE = %q", v)
	}
	// RAW_CUSTOM: "don't flood route these".
	if v := judgeOne(t, []byte{0x01 | 0x0F<<2, 0x00, 0xAA}); v != "would-drop-flood-type" {
		t.Errorf("flood RAW_CUSTOM = %q", v)
	}
}

func TestFloodPathCapacity(t *testing.T) {
	// GRP_TXT flood, 32 hops of 2-byte hashes: appending one more
	// entry would exceed the reference's 64-byte path.
	full := make([]byte, 0, 68)
	full = append(full, 0x01|0x05<<2, 32|0x40)
	full = append(full, make([]byte, 64)...) // 32 × 2-byte hashes
	full = append(full, 0xDD, 0xEE)
	if v := judgeOne(t, full); v != "would-drop-flood-path-full" {
		t.Errorf("full path = %q", v)
	}
	// 31 hops of 2 bytes still fits one more.
	fits := make([]byte, 0, 66)
	fits = append(fits, 0x01|0x05<<2, 31|0x40)
	fits = append(fits, make([]byte, 62)...)
	fits = append(fits, 0xDD, 0xEE)
	if v := judgeOne(t, fits); v != "would-relay-flood" {
		t.Errorf("path with room = %q", v)
	}
}

func TestUnsupportedVersion(t *testing.T) {
	// Version bits 01: the reference dispatcher rejects at parse.
	if v := judgeOne(t, []byte{0x40 | 0x01 | 0x05<<2, 0x00, 0xDD}); v != "unsupported-version" {
		t.Errorf("version 1 = %q", v)
	}
}

func TestTraceWalk(t *testing.T) {
	tracePayload := func() []byte {
		p := make([]byte, 9, 11)
		binary.LittleEndian.PutUint32(p[0:], 0x11111111) // tag
		binary.LittleEndian.PutUint32(p[4:], 0x22222222) // auth
		p[8] = 0                                         // flags: 1-byte target hashes
		return append(p, 0xA1, 0xA2)                     // two target hops
	}
	// Nothing walked yet: in transit.
	transit := append([]byte{0x02 | 0x09<<2, 0x00}, tracePayload()...)
	if v := judgeOne(t, transit); v != "trace-transit" {
		t.Errorf("trace at hop 0 = %q", v)
	}
	// Two SNR bytes accumulated: the whole target path is walked.
	arrived := append([]byte{0x02 | 0x09<<2, 0x02, 0x10, 0x14}, tracePayload()...)
	if v := judgeOne(t, arrived); v != "trace-arrived" {
		t.Errorf("trace at end = %q", v)
	}
}

func TestSeenRingEvictsOldestByCapacity(t *testing.T) {
	e, sub := testEngine(t)
	e.seen = newSeenTable(0, 2) // capacity 2, no time bound

	a := []byte{0x01 | 0x05<<2, 0x00, 0x01}
	b := []byte{0x01 | 0x05<<2, 0x00, 0x02}
	c := []byte{0x01 | 0x05<<2, 0x00, 0x03}
	e.judge(frame(a))
	e.judge(frame(b))
	e.judge(frame(c)) // evicts a
	e.judge(frame(a)) // no longer remembered: judged afresh
	e.judge(frame(c)) // still remembered: duplicate

	judged := drainJudged(t, sub)
	want := []string{
		"would-relay-flood", "would-relay-flood", "would-relay-flood",
		"would-relay-flood", "duplicate",
	}
	for i, w := range want {
		if judged[i].Verdict != w {
			t.Errorf("frame %d: %q, want %q", i, judged[i].Verdict, w)
		}
	}
}
