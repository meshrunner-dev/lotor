package meshcore

import (
	"testing"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/radio"
)

func testEngine(t *testing.T) (*engine, *bus.Subscription) {
	t.Helper()
	b := bus.New()
	sub := b.Subscribe(16)
	t.Cleanup(sub.Close)
	eng, err := build("test", map[string]any{
		"frequency_hz":     uint32(869_618_000),
		"spreading_factor": 8,
		"bandwidth_hz":     62_500,
		"coding_rate":      8,
		"preamble":         32,
		"sync_word":        0x12,
		"crc":              true,
	}, b, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	e, ok := eng.(*engine)
	if !ok {
		t.Fatalf("engine type %T", eng)
	}
	return e, sub
}

func frame(payload []byte) radio.Frame {
	return radio.Frame{Payload: payload, At: time.Now()}
}

// Raw MeshCore frames: [header][path_len][path…][payload…], with the
// route in the header's low bits and the payload type above them.
var (
	floodAdvert = []byte{0x01 | 0x04<<2, 0x00, 0xAA, 0xBB, 0xCC}
	zeroHopCtl  = []byte{0x02 | 0x0B<<2, 0x00, 0x80, 0x04}
	directPath  = []byte{0x02 | 0x02<<2, 0x02, 0x11, 0x22, 0x01, 0x02, 0x03}
)

func drainJudged(t *testing.T, sub *bus.Subscription) []bus.FrameJudged {
	t.Helper()
	var out []bus.FrameJudged
	for {
		select {
		case ev := <-sub.C:
			if j, ok := ev.(bus.FrameJudged); ok {
				out = append(out, j)
			}
		default:
			return out
		}
	}
}

func TestVerdicts(t *testing.T) {
	e, sub := testEngine(t)

	e.judge(frame(floodAdvert))
	e.judge(frame(zeroHopCtl))
	e.judge(frame(directPath))
	e.judge(frame([]byte{0x01})) // truncated

	judged := drainJudged(t, sub)
	want := []string{"would-relay-flood", "heard-zero-hop", "would-relay-direct", "malformed"}
	if len(judged) != len(want) {
		t.Fatalf("judged %d frames, want %d", len(judged), len(want))
	}
	for i, w := range want {
		if judged[i].Verdict != w {
			t.Errorf("frame %d: verdict %q, want %q", i, judged[i].Verdict, w)
		}
	}
}

func TestDuplicateChainsToFirstTransaction(t *testing.T) {
	e, sub := testEngine(t)

	e.judge(frame(floodAdvert))
	e.judge(frame(floodAdvert))

	judged := drainJudged(t, sub)
	if len(judged) != 2 {
		t.Fatalf("judged %d frames", len(judged))
	}
	if judged[1].Verdict != "duplicate" {
		t.Fatalf("second copy verdict = %q", judged[1].Verdict)
	}
	if judged[1].DuplicateOf != judged[0].Txn.Short() {
		t.Errorf("duplicate_of = %q, want the first transaction %q",
			judged[1].DuplicateOf, judged[0].Txn.Short())
	}
}

func TestSeenTableExpires(t *testing.T) {
	e, sub := testEngine(t)
	e.seen = newSeenTable(time.Millisecond, 8)

	e.judge(frame(floodAdvert))
	time.Sleep(5 * time.Millisecond)
	e.judge(frame(floodAdvert))

	judged := drainJudged(t, sub)
	if judged[1].Verdict != "would-relay-flood" {
		t.Errorf("expired hash still judged %q", judged[1].Verdict)
	}
}
