package cli

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/sentinel"
	"meshrunner.dev/lotor/internal/txn"
)

type script struct {
	io.Reader
	io.Writer
}

// run drives a whole session from a command script and returns the
// transcript.
func run(t *testing.T, deps Deps, commands ...string) string {
	t.Helper()
	var out bytes.Buffer
	in := strings.NewReader(strings.Join(commands, "\n") + "\nquit\n")
	Serve(context.Background(), script{Reader: in, Writer: &out}, deps)
	return out.String()
}

func testDeps(t *testing.T) Deps {
	t.Helper()
	b := bus.New()
	sen, err := sentinel.Open(context.Background(), sentinel.MemoryJournal, time.Hour, b, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	return Deps{
		Version: "test",
		Started: time.Now().Add(-90 * time.Minute),
		Bus:     b,
		Relays: []RelayInfo{{
			Name: "meshcore-868", Protocol: "meshcore",
			Radio: "slot1", Driver: "sx126x-spi",
			Waveform: radio.Waveform{
				FrequencyHz: 869_618_000, SpreadingFactor: 8,
				BandwidthHz: 62_500, CodingRate: 8, Preamble: 32,
				SyncWord: 0x12, CRC: true,
			},
			State: func() string { return "running" },
		}},
		Sentinel: sen,
		Traces: map[string][]config.Trace{
			"radio slot1": {
				{Key: "busy_pin", Source: "profile:rak6421-13300x-slot1", Value: 24},
				{Key: "spi", Source: "override:rak6421-13300x-slot1", Value: "/dev/spidev0.0"},
			},
		},
	}
}

// seed journals one advert and its duplicate, returning both txns.
func seed(t *testing.T, deps Deps) (orig, dup txn.ID) {
	t.Helper()
	ctx := context.Background()
	orig, dup = txn.New(), txn.New()
	heard := func(id txn.ID, rssi float64) bus.FrameHeard {
		return bus.FrameHeard{Relay: "meshcore-868", Txn: id, At: time.Now(),
			Bytes: 132, RSSI: rssi, SNR: 8.5, Airtime: 1295 * time.Millisecond}
	}
	judged := bus.FrameJudged{Relay: "meshcore-868", Txn: orig,
		Verdict: "would-relay-flood", Type: "ADVERT", Route: "FLOOD", PathLen: 5,
		Node: "Radio-Club", PubKey: "17c74bb65391", Detail: "repeater"}
	for _, ev := range []bus.Event{
		heard(orig, -69), judged,
		heard(dup, -29),
		bus.FrameJudged{Relay: "meshcore-868", Txn: dup, Verdict: "duplicate",
			DuplicateOf: orig.Short(), Type: "ADVERT", Route: "FLOOD", PathLen: 5},
	} {
		deps.Sentinel.Process(ctx, ev)
	}
	return orig, dup
}

func TestStatusAndHelp(t *testing.T) {
	out := run(t, testDeps(t), "status", "help")
	for _, want := range []string{"lotor test", "meshcore-868", "running", "869.618 MHz",
		"journalling", "config show"} {
		if !strings.Contains(out, want) {
			t.Errorf("transcript lacks %q:\n%s", want, out)
		}
	}
}

func TestConfigShowProvenance(t *testing.T) {
	out := run(t, testDeps(t), "config show radio slot1")
	if !strings.Contains(out, "override:rak6421-13300x-slot1") ||
		!strings.Contains(out, "profile:rak6421-13300x-slot1") {
		t.Errorf("provenance missing:\n%s", out)
	}
}

func TestFramesAndChain(t *testing.T) {
	deps := testDeps(t)
	orig, dup := seed(t, deps)

	out := run(t, deps, "frames", "txn "+dup.Short()[:4])
	for _, want := range []string{
		`"Radio-Club" (repeater)`,
		"duplicate → " + orig.Short(),
		orig.Short(), // the chain pulls the original in
		"would-relay-flood",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("transcript lacks %q:\n%s", want, out)
		}
	}
}

func TestNodesDirectory(t *testing.T) {
	deps := testDeps(t)
	seed(t, deps)

	out := run(t, deps, "nodes")
	if !strings.Contains(out, "Radio-Club") || !strings.Contains(out, "17c74bb65391") ||
		!strings.Contains(out, "repeater") {
		t.Errorf("directory incomplete:\n%s", out)
	}
}

func TestErrorsAreOneLiners(t *testing.T) {
	deps := testDeps(t)
	out := run(t, deps,
		"nope",
		"frames --relay meshcore-433",
		"txn ffff",
	)
	for _, want := range []string{
		`error: unknown command "nope"`,
		`error: no relay "meshcore-433" (relays: meshcore-868)`,
		`error: no transaction matching "ffff"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("transcript lacks %q:\n%s", want, out)
		}
	}
}

func TestNoSentinelIsHonest(t *testing.T) {
	deps := testDeps(t)
	deps.Sentinel = nil
	out := run(t, deps, "frames", "nodes", "status")
	if strings.Count(out, "no sentinel configured") != 2 {
		t.Errorf("expected two honest refusals:\n%s", out)
	}
	if !strings.Contains(out, "sentinel  none") &&
		!strings.Contains(out, "sentinel\tnone") &&
		!strings.Contains(out, "sentinel   none") {
		t.Errorf("status should say sentinel none:\n%s", out)
	}
}

func TestIACStripper(t *testing.T) {
	// telnet client opening: IAC DO(253) opt, IAC SB(250)…IAC SE(240),
	// escaped 0xFF, then a command line.
	raw := make([]byte, 0, 32)
	raw = append(raw, 255, 253, 31, 255, 250, 31, 0, 80, 0, 24, 255, 240, 255, 255)
	raw = append(raw, []byte("status\n")...)
	got, err := io.ReadAll(&iacStripper{r: bytes.NewReader(raw)})
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "\xffstatus\n" {
		t.Errorf("stripped = %q", got)
	}
}
