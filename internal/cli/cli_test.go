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
	sen, err := sentinel.Open(context.Background(), sentinel.MemoryJournal, time.Hour, 0, b, zap.NewNop())
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
			NoiseFloor: func() (radio.NoiseFloor, bool) {
				return radio.NoiseFloor{DBm: -104, At: time.Now().Add(-3 * time.Second)}, true
			},
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

func TestHelpKnowsEachCommand(t *testing.T) {
	deps := testDeps(t)
	out := run(t, deps, "noise --help", "help noise", "noise help", "status extra")
	if strings.Count(out, "noise [--relay R]") != 2 {
		t.Errorf("per-command help missing:\n%s", out)
	}
	if strings.Contains(out, "daemon overview") {
		t.Errorf("per-command help dumped the full list:\n%s", out)
	}
	if !strings.Contains(out, `unknown argument "help"`) {
		t.Errorf("stray positional swallowed:\n%s", out)
	}
	if !strings.Contains(out, "status takes no arguments") {
		t.Errorf("status accepted stray arguments:\n%s", out)
	}
	if bad := run(t, deps, "help nope"); !strings.Contains(bad, `unknown command "nope"`) {
		t.Errorf("help accepted an unknown command:\n%s", bad)
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

func TestNoiseFloorIsShown(t *testing.T) {
	deps := testDeps(t)
	out := run(t, deps, "status", "relay show meshcore-868")
	if strings.Count(out, "-104 dBm (3s ago)") != 2 {
		t.Errorf("noise floor missing from status or relay show:\n%s", out)
	}
	// Before the first measurement converges, the shell says so.
	deps.Relays[0].NoiseFloor = func() (radio.NoiseFloor, bool) { return radio.NoiseFloor{}, false }
	out = run(t, deps, "status")
	if !strings.Contains(out, "floor calibrating") {
		t.Errorf("calibrating state hidden:\n%s", out)
	}
}

func TestNoiseHistoryCommand(t *testing.T) {
	deps := testDeps(t)
	at := time.Now().Add(-30 * time.Minute)
	for _, dbm := range []float64{-104, -98} {
		deps.Sentinel.Process(context.Background(), bus.NoiseFloor{
			Relay: "meshcore-868", At: at, DBm: dbm})
	}
	out := run(t, deps, "noise", "noise --last 7d")
	if !strings.Contains(out, "current  -104 dBm (3s ago)") {
		t.Errorf("noise lacks the live value:\n%s", out)
	}
	if !strings.Contains(out, "min -104.0") || !strings.Contains(out, "max -98.0") {
		t.Errorf("noise lacks the consolidated bucket:\n%s", out)
	}
	if bad := run(t, deps, "noise --last nope"); !strings.Contains(bad, "--last wants a duration") {
		t.Errorf("bad span accepted:\n%s", bad)
	}
}

func TestWatchValidatesItsRelayFilter(t *testing.T) {
	// A typo'd --relay must error like the query path does, not stream
	// nothing forever.
	deps := testDeps(t)
	out := run(t, deps, "frames watch --relay meshcor-868")
	if !strings.Contains(out, `error: no relay "meshcor-868"`) {
		t.Errorf("watch accepted an unknown relay:\n%s", out)
	}
}

func TestNoiseIsVisible(t *testing.T) {
	deps := testDeps(t)
	deps.Sentinel.Process(context.Background(), bus.FrameCorrupt{
		Relay: "meshcore-868", At: time.Now(), Err: "sx126x: crc mismatch"})
	out := run(t, deps, "sentinel", "relay show meshcore-868")
	if !strings.Contains(out, "noise") || !strings.Contains(out, "1 corrupt reception") {
		t.Errorf("corrupt receptions invisible:\n%s", out)
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

func TestSplitArgsHonoursQuotes(t *testing.T) {
	got := splitArgs(`node "FR91 🦝 Wanadoo" --json`)
	want := []string{"node", "FR91 🦝 Wanadoo", "--json"}
	if len(got) != len(want) {
		t.Fatalf("splitArgs = %q", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("arg %d = %q, want %q", i, got[i], want[i])
		}
	}
	if got := splitArgs(`say ""`); len(got) != 2 || got[1] != "" {
		t.Errorf("empty quoted arg = %q", got)
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
