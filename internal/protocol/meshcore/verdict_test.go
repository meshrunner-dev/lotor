package meshcore

import (
	"crypto/rand"
	"encoding/binary"
	"testing"
	"time"

	"meshrunner.dev/pkg/meshcore"
)

// Reference gates, walked one by one. Raw frames:
// [header][transport?][path_len][path…][payload…].
func judgeOne(t *testing.T, raw []byte) string {
	t.Helper()
	e, sub := testEngine(t)
	e.judge(newFakeDevice(), frame(raw))
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
	full = append(full, 0xDD, 0xEE, 0x11, 0x22, 0x33)
	if v := judgeOne(t, full); v != "would-drop-flood-path-full" {
		t.Errorf("full path = %q", v)
	}
	// 31 hops of 2 bytes still fits one more.
	fits := make([]byte, 0, 66)
	fits = append(fits, 0x01|0x05<<2, 31|0x40)
	fits = append(fits, make([]byte, 62)...)
	fits = append(fits, 0xDD, 0xEE, 0x11, 0x22, 0x33)
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

	// Whole envelopes: dest, src, then MAC and data. A payload too
	// short for its type is refused before the ring is consulted, so
	// a ring test must not lean on one.
	a := []byte{0x01 | 0x05<<2, 0x00, 0xDD, 0xEE, 0x11, 0x22, 0x01}
	b := []byte{0x01 | 0x05<<2, 0x00, 0xDD, 0xEE, 0x11, 0x22, 0x02}
	c := []byte{0x01 | 0x05<<2, 0x00, 0xDD, 0xEE, 0x11, 0x22, 0x03}
	e.judge(newFakeDevice(), frame(a))
	e.judge(newFakeDevice(), frame(b))
	e.judge(newFakeDevice(), frame(c)) // evicts a
	e.judge(newFakeDevice(), frame(a)) // no longer remembered: judged afresh
	e.judge(newFakeDevice(), frame(c)) // still remembered: duplicate

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

func TestTruncatedFloodsAreNotRelayed(t *testing.T) {
	// The reference reaches its routing only past a length gate: an
	// incomplete ACK or data packet is released, never carried.
	// Judging on the payload type alone had this node spreading
	// through its own neighbourhood what every reference repeater
	// stops. The boundary is the codec's, asked rather than copied.
	cases := []struct {
		name  string
		ptype byte
		below []byte // one byte short of a whole envelope
		whole []byte
	}{
		// ACK: four CRC bytes.
		{"ack", byte(meshcore.PayloadTypeAck), []byte{1, 2, 3}, []byte{1, 2, 3, 4}},
		// Datagram types: dest, src, then MAC and at least a byte.
		{"req", byte(meshcore.PayloadTypeReq),
			[]byte{0xDD, 0xEE, 0x11, 0x22}, []byte{0xDD, 0xEE, 0x11, 0x22, 0x33}},
		{"txt", byte(meshcore.PayloadTypeTxtMsg),
			[]byte{0xDD, 0xEE, 0x11, 0x22}, []byte{0xDD, 0xEE, 0x11, 0x22, 0x34}},
		{"path", byte(meshcore.PayloadTypePath),
			[]byte{0xDD, 0xEE, 0x11, 0x22}, []byte{0xDD, 0xEE, 0x11, 0x22, 0x35}},
		{"response", byte(meshcore.PayloadTypeResponse),
			[]byte{0xDD, 0xEE, 0x11, 0x22}, []byte{0xDD, 0xEE, 0x11, 0x22, 0x36}},
		// Group types: channel hash, then MAC and at least a byte.
		{"grp txt", byte(meshcore.PayloadTypeGrpTxt),
			[]byte{0xDD, 0x11, 0x22}, []byte{0xDD, 0x11, 0x22, 0x33}},
		{"grp data", byte(meshcore.PayloadTypeGrpData),
			[]byte{0xDD, 0x11, 0x22}, []byte{0xDD, 0x11, 0x22, 0x34}},
	}
	for _, c := range cases {
		short := append([]byte{0x01 | c.ptype<<2, 0x00}, c.below...)
		if v := judgeOne(t, short); v != "would-drop-flood-truncated" {
			t.Errorf("%s one byte short = %q", c.name, v)
		}
		whole := append([]byte{0x01 | c.ptype<<2, 0x00}, c.whole...)
		if v := judgeOne(t, whole); v != "would-relay-flood" {
			t.Errorf("%s at the boundary = %q", c.name, v)
		}
	}
	// An ANON_REQ carries a whole public key before its MAC, so the
	// boundary sits far higher — and a short one is released by the
	// anonymous path before the flood gate ever sees it, which is the
	// same refusal under the reference's own word for it.
	anonShort := append([]byte{0x01 | byte(meshcore.PayloadTypeAnonReq)<<2, 0x00, 0xDD}, make([]byte, 33)...)
	if v := judgeOne(t, anonShort); v != "ignored" {
		t.Errorf("anon one byte short = %q", v)
	}
	anonWhole := append([]byte{0x01 | byte(meshcore.PayloadTypeAnonReq)<<2, 0x00, 0xDD}, make([]byte, 35)...)
	if v := judgeOne(t, anonWhole); v != "would-relay-flood" {
		t.Errorf("anon at the boundary = %q", v)
	}
}

func TestOnlyTheHighBitControlSubsetIsAnswered(t *testing.T) {
	// The reference singles out CONTROL whose first payload byte has
	// its high bit set: those it answers zero-hop and releases
	// otherwise. Everything else is ordinary directed traffic, and
	// swallowing it would break this relay's transparency to a
	// control type it does not itself speak.
	e, sub := identifiedEngine(t)
	ours := e.id.PubKey[0]

	// High bit set, path walked: released, as the reference does.
	e.judge(newFakeDevice(), frame([]byte{0x02 | 0x0B<<2, 0x01, ours, 0x80, 0x01}))
	// High bit clear, our hash at the path head: ours to relay.
	e.judge(newFakeDevice(), frame([]byte{0x02 | 0x0B<<2, 0x01, ours, 0x04, 0x02}))
	// High bit clear, someone else's hop: not ours.
	e.judge(newFakeDevice(), frame([]byte{0x02 | 0x0B<<2, 0x01, ^ours, 0x04, 0x03}))
	// High bit set, zero hops: the subset the reference answers.
	e.judge(newFakeDevice(), frame([]byte{0x02 | 0x0B<<2, 0x00, 0x80, 0x05}))

	want := []string{"ignored", "would-relay-direct", "direct-not-addressed", "heard-zero-hop"}
	judged := drainJudged(t, sub)
	if len(judged) != len(want) {
		t.Fatalf("judged %d frames, want %d", len(judged), len(want))
	}
	for i, w := range want {
		if judged[i].Verdict != w {
			t.Errorf("frame %d: verdict %q, want %q", i, judged[i].Verdict, w)
		}
	}
}

func TestARejectedVersionTouchesNothing(t *testing.T) {
	// A version this engine says it cannot read must have no effect
	// at all: the reference refuses it in the dispatcher, before any
	// application decoding. A v2 advert wearing a v1 shape and a
	// valid signature was naming neighbours on its way to being
	// called unsupported.
	e, sub := identifiedEngine(t)
	peer, err := meshcore.NewLocalIdentity(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pkt, err := meshcore.BuildAdvert(peer, time.Now(), &meshcore.AdvertData{
		Type: meshcore.AdvTypeRepeater, Name: "ghost",
	})
	if err != nil {
		t.Fatal(err)
	}
	// The same advert, wearing a version we do not serve.
	pkt.Header = meshcore.MakeHeader(meshcore.RouteDirect,
		meshcore.PayloadTypeAdvert, meshcore.PayloadVer1+1)
	raw, err := pkt.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	e.judge(newFakeDevice(), frame(raw))

	judged := drainJudged(t, sub)
	if len(judged) != 1 || judged[0].Verdict != "unsupported-version" {
		t.Fatalf("judged %+v", judged)
	}
	if n := len(e.Neighbours()); n != 0 {
		t.Errorf("a rejected version named %d neighbours", n)
	}
	// Nor does it take a place in the duplicate table: the same bytes
	// at a version we do serve are judged afresh, not as a copy.
	pkt.Header = meshcore.MakeHeader(meshcore.RouteDirect,
		meshcore.PayloadTypeAdvert, meshcore.PayloadVer1)
	raw, err = pkt.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	e.judge(newFakeDevice(), frame(raw))
	judged = drainJudged(t, sub)
	if len(judged) != 1 || judged[0].Verdict == "duplicate" {
		t.Fatalf("the rejected frame reserved a slot: %+v", judged)
	}
}

func TestATraceVerdictPromisesOnlyWhatTheWalkAllows(t *testing.T) {
	// A trace path carries one raw SNR byte per hop, never node
	// hashes. A descriptor claiming a wider hop used to earn
	// would-relay-trace from the verdict and then a deterministic
	// refusal from AppendTraceHop, so dry promised what on-air could
	// not do. The codec refuses the shape now, and both agree.
	e, sub := identifiedEngine(t)
	payload := make([]byte, 9, 11)
	binary.LittleEndian.PutUint32(payload[0:], 1)
	binary.LittleEndian.PutUint32(payload[4:], 2)
	payload = append(payload, e.id.PubKey[0], 0xA2)

	wide := append([]byte{0x02 | 0x09<<2, 0x40}, payload...) // two-byte hops
	e.judge(newFakeDevice(), frame(wide))
	narrow := append([]byte{0x02 | 0x09<<2, 0x00}, payload...)
	e.judge(newFakeDevice(), frame(narrow))

	judged := drainJudged(t, sub)
	if len(judged) != 2 {
		t.Fatalf("judged %d frames", len(judged))
	}
	if judged[0].Verdict != "ignored" {
		t.Errorf("a trace the walk would refuse was judged %q", judged[0].Verdict)
	}
	if judged[1].Verdict != "would-relay-trace" {
		t.Errorf("a walkable trace was judged %q", judged[1].Verdict)
	}
	// And what the verdict promised, the transform delivers.
	pkt, err := meshcore.ParsePacket(narrow)
	if err != nil {
		t.Fatal(err)
	}
	if err := pkt.AppendTraceHop(6); err != nil {
		t.Errorf("the promised relay could not be made: %v", err)
	}
}
