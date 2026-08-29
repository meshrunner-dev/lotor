package meshcore

import (
	"math"
	"testing"
	"time"

	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

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

func TestDutyCyclePctIsFiniteAndBounded(t *testing.T) {
	base := map[string]any{"frequency_hz": 869_618_000}
	for _, good := range []float64{0, 10, 100} {
		base["duty_cycle_pct"] = good
		if _, err := paramsFrom(base); err != nil {
			t.Errorf("duty_cycle_pct %v refused: %v", good, err)
		}
	}
	for _, bad := range []float64{-0.01, 100.01, math.NaN(), math.Inf(1), math.Inf(-1)} {
		base["duty_cycle_pct"] = bad
		if _, err := paramsFrom(base); err == nil {
			t.Errorf("duty_cycle_pct %v accepted", bad)
		}
	}
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
	// Transport routes carry a 4-byte transport-code block after the
	// header, before the path descriptor.
	transportFlood = []byte{0x00 | 0x05<<2, 0x01, 0x02, 0x03, 0x04, 0x00, 0xDD, 0xEE}
	transportPath  = []byte{0x03 | 0x00<<2, 0x01, 0x02, 0x03, 0x04, 0x01, 0x33, 0x01, 0x02}
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

	e.judge(newFakeDevice(), frame(floodAdvert)) // raw bytes: no valid signature
	e.judge(newFakeDevice(), frame(zeroHopCtl))
	e.judge(newFakeDevice(), frame(directPath))
	e.judge(newFakeDevice(), frame(transportFlood))
	e.judge(newFakeDevice(), frame(transportPath))
	e.judge(newFakeDevice(), frame([]byte{0x01})) // truncated

	judged := drainJudged(t, sub)
	want := []string{
		"would-drop-invalid-advert", "heard-zero-hop", "direct-not-addressed",
		"would-drop-flood-scoped", "direct-not-addressed", "malformed",
	}
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

	e.judge(newFakeDevice(), frame(floodAdvert))
	e.judge(newFakeDevice(), frame(floodAdvert))

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

	grpTxt := []byte{0x01 | 0x05<<2, 0x00, 0xDD, 0xEE, 0x11, 0x22, 0x33}
	e.judge(newFakeDevice(), frame(grpTxt))
	time.Sleep(5 * time.Millisecond)
	e.judge(newFakeDevice(), frame(grpTxt))

	judged := drainJudged(t, sub)
	if judged[1].Verdict != "would-relay-flood" {
		t.Errorf("expired hash still judged %q", judged[1].Verdict)
	}
}

func TestTxPowerReadsTheScalarWhateverItsTag(t *testing.T) {
	// A value that crossed the console arrives as a string whatever
	// it spells: "0" set by an operator is the figure 0 imported
	// from a file. Seen on the air as an export nobody could paste
	// back.
	for _, c := range []struct {
		in       string
		explicit bool
		dbm      int8
	}{
		{`tx_power_dbm: 7`, true, 7},
		{`tx_power_dbm: "0"`, true, 0},
		{`tx_power_dbm: "-5"`, true, -5},
		{`tx_power_dbm: auto`, false, 0},
		{`tx_power_dbm: ""`, false, 0},
	} {
		var p struct {
			TX txPower `yaml:"tx_power_dbm"`
		}
		if err := yaml.Unmarshal([]byte(c.in), &p); err != nil {
			t.Errorf("%s: %v", c.in, err)
			continue
		}
		if p.TX.explicit != c.explicit || p.TX.dbm != c.dbm {
			t.Errorf("%s = %+v", c.in, p.TX)
		}
	}
	var p struct {
		TX txPower `yaml:"tx_power_dbm"`
	}
	if err := yaml.Unmarshal([]byte(`tx_power_dbm: loud`), &p); err == nil {
		t.Error("a word that is not auto parsed as a power")
	}
}

func TestCheckRefusesEverythingBuildRefuses(t *testing.T) {
	// The console dry-runs a set through check and commits if it
	// passes; the relay then runs build. A value check accepts and
	// build rejects is a set that reports "applied" and a relay that
	// dies at its next start — seen on the air with identity=new,
	// which build read as hex and could not.
	base := func(identity string) map[string]any {
		return map[string]any{
			"frequency_hz": 869618000, "spreading_factor": 8, "bandwidth_hz": 62500,
			"coding_rate": 8, "preamble": 32, "sync_word": 0x12, "crc": true,
			"duty_cycle_pct": 10.0, "node_name": "n", "identity": identity,
		}
	}
	for _, identity := range []string{"new", "banana", "abcd", ""} {
		cfg := base(identity)
		checkErr := check(cfg)
		_, buildErr := build("r", cfg, bus.New(), zap.NewNop())
		if (checkErr == nil) != (buildErr == nil) {
			t.Errorf("identity=%q: check=%v but build=%v — they must agree",
				identity, checkErr, buildErr)
		}
	}
	// And session_limit, the other thing build alone used to police.
	cfg := base("")
	cfg["session_limit"] = -1
	if check(cfg) == nil {
		t.Error("check accepted a negative session_limit")
	}
}
