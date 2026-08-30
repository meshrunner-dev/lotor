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
	"meshrunner.dev/lotor/internal/relay"
	"meshrunner.dev/lotor/internal/schema"
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
	sen, err := sentinel.Open(context.Background(), sentinel.MemoryJournal, time.Hour, 0, 0, b, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	return Deps{
		Version:  "test",
		Started:  time.Now().Add(-90 * time.Minute),
		Bus:      b,
		Kinds:    testKinds(),
		Sessions: NewSessions(),
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
		Radios: []RadioInfo{{
			Name: "slot1", Driver: "sx126x-spi", Relay: "meshcore-868",
		}},
		Traces: map[string][]config.Trace{
			"radio slot1": {
				{Key: "busy_pin", Source: "profile:rak6421-13300x-slot1", Value: 24},
				{Key: "driver", Source: "config", Value: "sx126x-spi"},
				{Key: "profile", Source: "config", Value: "rak6421-13300x-slot1"},
				{Key: "spi", Source: "override:rak6421-13300x-slot1", Value: "/dev/spidev0.0"},
			},
			"relay meshcore-868": {
				{Key: "identity", Source: "override:eu-868-narrow", Value: "b5445dd625d531fc"},
				{Key: "node_name", Source: "override:eu-868-narrow", Value: "test 🦝"},
			},
		},
	}
}

// testKinds is a hand-rolled vocabulary: the shape the wiring builds,
// without dragging the protocol and driver registries into CLI tests.
func testKinds() []schema.Kind {
	return []schema.Kind{
		{
			Name: "relay", Doc: "one protocol instance", ChoiceAttr: "protocol",
			Attrs: []schema.Attr{{Name: "protocol", Type: schema.String,
				Enum: []string{"meshcore"}, Doc: "the protocol"}},
			Contributed: func(choice string) []schema.Attr {
				if choice != "meshcore" {
					return nil
				}
				return []schema.Attr{
					{Name: "node_name", Type: schema.String, Doc: "the name on the air"},
					{Name: "identity", Type: schema.String, Secret: true, Doc: "the private key"},
				}
			},
		},
		{
			Name: "radio", Doc: "one transceiver", ChoiceAttr: "driver",
			Attrs: []schema.Attr{
				{Name: "driver", Type: schema.String,
					Enum: []string{"sx126x-spi"}, Doc: "the driver"},
				{Name: "profile", Type: schema.String, Doc: "the board preset"},
			},
			Profiles: func(choice string) []string {
				if choice == "" || choice == "sx126x-spi" {
					return []string{"lyra-zerow-station-g3", "rak6421-13300x-slot1"}
				}
				return nil
			},
			Contributed: func(choice string) []schema.Attr {
				if choice != "sx126x-spi" {
					return nil
				}
				return []schema.Attr{{Name: "spi", Type: schema.String, Doc: "the SPI device"}}
			},
		},
		{
			Name: "mqtt", Doc: "observer connections",
			Attrs: []schema.Attr{
				{Name: "profile", Type: schema.String, Doc: "the preset"},
				{Name: "url", Type: schema.String, Doc: "the broker"},
			},
			Profiles: func(string) []string {
				return []string{"analyzer-eu", "analyzer-us", "meshmapper"}
			},
		},
		{Name: "sentinel", Doc: "the journal", Singleton: true,
			Attrs: config.SentinelAttrs()},
		{Name: "system", Doc: "what this installation calls itself", Singleton: true,
			Attrs: config.SystemAttrs()},
	}
}

// seed journals one advert and its duplicate, returning both txns.
func seed(t *testing.T, deps Deps) (orig, dup txn.ID) {
	t.Helper()
	ctx := context.Background()
	orig, dup = txn.New(), txn.New()
	// The judged event carries the reception whole — the journal's
	// one archive event since the atomic contract.
	judgedAt := func(id txn.ID, rssi float64) bus.FrameJudged {
		return bus.FrameJudged{Relay: "meshcore-868", Txn: id, At: time.Now(),
			Bytes: 132, RSSI: rssi, SNR: 8.5, Airtime: 1295 * time.Millisecond}
	}
	judged := judgedAt(orig, -69)
	judged.Verdict, judged.Type, judged.Route, judged.PathLen = "would-relay-flood", "ADVERT", "FLOOD", 5
	judged.Node, judged.PubKey, judged.Detail = "Radio-Club", "17c74bb65391", "repeater"
	dupJudged := judgedAt(dup, -29)
	dupJudged.Verdict, dupJudged.DuplicateOf = "duplicate", orig.Short()
	dupJudged.Type, dupJudged.Route, dupJudged.PathLen = "ADVERT", "FLOOD", 5
	for _, ev := range []bus.Event{judged, dupJudged} {
		deps.Sentinel.Process(ctx, ev)
	}
	return orig, dup
}

func TestStatusAndHelp(t *testing.T) {
	out := run(t, testDeps(t), "status", "help")
	for _, want := range []string{"Lotor test", "meshcore-868", "running", "869.618 MHz",
		"journalling", "frames watch"} {
		if !strings.Contains(out, want) {
			t.Errorf("transcript lacks %q:\n%s", want, out)
		}
	}
	// The listing is a table of contents: flags live in each
	// command's own help.
	if strings.Contains(out, "--") {
		t.Errorf("top-level help leaks flags:\n%s", out)
	}
}

func TestHelpKnowsEachCommand(t *testing.T) {
	deps := testDeps(t)
	out := run(t, deps, "noise ?", "help noise", "noise help", "status extra")
	if strings.Count(out, "noise [relay=<name>]") != 2 {
		t.Errorf("per-command help missing:\n%s", out)
	}
	if strings.Contains(out, "daemon overview") {
		t.Errorf("per-command help dumped the full list:\n%s", out)
	}
	if !strings.Contains(out, `unknown argument "help"`) {
		t.Errorf("stray positional swallowed:\n%s", out)
	}
	if !strings.Contains(out, `unknown argument "extra" — try "status ?"`) {
		t.Errorf("status accepted stray arguments:\n%s", out)
	}
	if bad := run(t, deps, "help nope"); !strings.Contains(bad, `unknown command "nope"`) {
		t.Errorf("help accepted an unknown command:\n%s", bad)
	}
}

func TestBannerNamesThePrivilege(t *testing.T) {
	deps := testDeps(t)
	if out := run(t, deps, "status"); !strings.Contains(out, "read-only console") {
		t.Errorf("default privilege should read as read-only:\n%s", out)
	}
	deps.Privilege = Admin
	if out := run(t, deps, "status"); !strings.Contains(out, "admin console") {
		t.Errorf("admin session not announced:\n%s", out)
	}
}

func TestCommandTableIsCoherent(t *testing.T) {
	// The table is the single source of truth; every entry must be
	// runnable and answer --help, aliases included.
	deps := testDeps(t)
	for _, c := range commands {
		if c.run == nil {
			t.Errorf("command %q has no implementation", c.name)
		}
		if len(c.forms) == 0 {
			t.Errorf("command %q has no help forms", c.name)
		}
		out := run(t, deps, c.name+" ?")
		if strings.Contains(out, "error:") || !strings.Contains(out, c.name) {
			t.Errorf("%s ? misbehaves:\n%s", c.name, out)
		}
	}
}

func TestPerCommandFlagsAreEnforced(t *testing.T) {
	// The flag grammar is per command now: a flag another command owns
	// is an error here, never silently swallowed.
	out := run(t, testDeps(t), "nodes last=5", "noise type=ADVERT", "exit")
	if !strings.Contains(out, `no argument "last" here — try "nodes ?"`) ||
		!strings.Contains(out, `no argument "type" here — try "noise ?"`) {
		t.Errorf("foreign flags swallowed:\n%s", out)
	}
	// exit is quit's alias, table-driven like everything else.
	if !strings.Contains(out, "bye.") {
		t.Errorf("exit alias broken:\n%s", out)
	}
}

func TestPrintShowsProvenance(t *testing.T) {
	out := run(t, testDeps(t), "/radio/slot1/print")
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
		"frames relay=meshcore-433",
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
	out := run(t, deps, "status", "/relay meshcore-868 status")
	if strings.Count(out, "p50 -104 dBm (p90-p50 0.0 dB, 3s ago)") != 2 {
		t.Errorf("noise floor missing from status or status:\n%s", out)
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
	for _, p := range []struct{ dbm, spread float64 }{{-104, 3}, {-98, 0}} {
		deps.Sentinel.Process(context.Background(), bus.NoiseFloor{
			Relay: "meshcore-868", At: at, DBm: p.dbm, SpreadDB: p.spread})
	}
	deps.Sentinel.Process(context.Background(), bus.NoiseStarved{
		Relay: "meshcore-868", At: at, Aborted: 2})
	out := run(t, deps, "noise", "noise last=7d")
	if !strings.Contains(out, "current  p50 -104 dBm (p90-p50 0.0 dB, 3s ago)") {
		t.Errorf("noise lacks the live value:\n%s", out)
	}
	if !strings.Contains(out, "min -104.0") || !strings.Contains(out, "max -98.0") ||
		!strings.Contains(out, "p90-p50 1.5") || !strings.Contains(out, "starved 2") {
		t.Errorf("noise lacks the consolidated bucket:\n%s", out)
	}
	if bad := run(t, deps, "noise last=nope"); !strings.Contains(bad, "last= wants a duration") {
		t.Errorf("bad span accepted:\n%s", bad)
	}
}

func TestWatchValidatesItsRelayFilter(t *testing.T) {
	// A typo'd relay=must error like the query path does, not stream
	// nothing forever.
	deps := testDeps(t)
	out := run(t, deps, "frames watch relay=meshcor-868")
	if !strings.Contains(out, `error: no relay "meshcor-868"`) {
		t.Errorf("watch accepted an unknown relay:\n%s", out)
	}
}

func TestNoiseIsVisible(t *testing.T) {
	deps := testDeps(t)
	deps.Sentinel.Process(context.Background(), bus.FrameCorrupt{
		Relay: "meshcore-868", At: time.Now(), Err: "sx126x: crc mismatch"})
	out := run(t, deps, "journal", "/relay meshcore-868 status")
	if !strings.Contains(out, "noise") || !strings.Contains(out, "1 corrupt reception") {
		t.Errorf("corrupt receptions invisible:\n%s", out)
	}
}

func TestArchivedRelayStaysAddressable(t *testing.T) {
	// A relay removed from the configuration keeps its archive: the
	// journal queries accept its name, only the live stream refuses.
	deps := testDeps(t)
	ctx := context.Background()
	id := txn.New()
	deps.Sentinel.Process(ctx, bus.FrameJudged{
		Relay: "meshcore-433", Txn: id, At: time.Now(), Bytes: 20, RSSI: -90, SNR: 5,
		Verdict: "would-relay-flood", Type: "ADVERT", Route: "FLOOD"})
	deps.Sentinel.Process(ctx, bus.NoiseFloor{
		Relay: "meshcore-433", At: time.Now(), DBm: -101})

	out := run(t, deps, "frames relay=meshcore-433", "noise relay=meshcore-433")
	if !strings.Contains(out, "would-relay-flood") {
		t.Errorf("archived relay's frames unreachable:\n%s", out)
	}
	if !strings.Contains(out, "archived — relay not running") ||
		!strings.Contains(out, "avg(p50) -101.0") {
		t.Errorf("archived relay's noise history unreachable:\n%s", out)
	}
	if w := run(t, deps, "frames watch relay=meshcore-433"); !strings.Contains(w, `no relay "meshcore-433"`) {
		t.Errorf("watch accepted an archived relay:\n%s", w)
	}
	if bad := run(t, deps, "frames relay=nope"); !strings.Contains(bad,
		"running: meshcore-868; archived: meshcore-433") {
		t.Errorf("error should name both worlds:\n%s", bad)
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
	got := splitArgs(`node "FR91 🦝 Wanadoo" json`)
	want := []string{"node", "FR91 🦝 Wanadoo", "json"}
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

func TestTXModeIsShown(t *testing.T) {
	deps := testDeps(t)
	if out := run(t, deps, "status"); !strings.Contains(out, "tx dry") {
		t.Errorf("dry gate hidden:\n%s", out)
	}
	deps.Relays[0].TXMode = "shadow"
	if out := run(t, deps, "/relay meshcore-868 status"); !strings.Contains(out, "tx mode") ||
		!strings.Contains(out, "shadow") {
		t.Errorf("shadow gate hidden:\n%s", out)
	}
}

func TestOriginatedEmissionIsAddressable(t *testing.T) {
	// An advert has no reception behind it; the operator still reads
	// its txn in a log line and must be able to look it up.
	deps := testDeps(t)
	id := txn.New()
	deps.Sentinel.Process(context.Background(), bus.FrameSent{
		Relay: "meshcore-868", Txn: id, At: time.Now(), Kind: "advert-flood",
		Airtime: 1164 * time.Millisecond, PowerDBm: -5, Shadow: true,
	})
	out := run(t, deps, "txn "+id.Short())
	if !strings.Contains(out, "originated") || !strings.Contains(out, "advert-flood") ||
		!strings.Contains(out, "(shadow)") {
		t.Errorf("originated emission unreachable:\n%s", out)
	}
	if bad := run(t, deps, "txn ffffffff"); !strings.Contains(bad, "no transaction matching") {
		t.Errorf("an unknown prefix should still say so:\n%s", bad)
	}
}

func TestLifecycleHistoryIsReachable(t *testing.T) {
	deps := testDeps(t)
	at := time.Now()
	deps.Sentinel.Process(context.Background(), bus.RelayState{
		Relay: "meshcore-868", At: at, State: "error", Err: "radio gone",
	})
	deps.Sentinel.Process(context.Background(), bus.ObserverState{
		Observer: "community", At: at.Add(time.Second), State: "lost", Cause: "EOF",
	})

	out := run(t, deps, "states")
	for _, want := range []string{"relay", "meshcore-868", "radio gone", "observer", "community", "EOF"} {
		if !strings.Contains(out, want) {
			t.Errorf("states omitted %q:\n%s", want, out)
		}
	}
	latest := run(t, deps, "states last=1")
	if !strings.Contains(latest, "community") || strings.Contains(latest, "radio gone") {
		t.Errorf("last=1 did not keep only the newest transition:\n%s", latest)
	}
	jsonOut := run(t, deps, "states last=1 json")
	if !strings.Contains(jsonOut, `"kind":"observer"`) || !strings.Contains(jsonOut, `"cause":"EOF"`) {
		t.Errorf("states JSON = %s", jsonOut)
	}
}

func TestWatchRefusesTheJournalSelectors(t *testing.T) {
	// The live feed starts now: every word that names a slice of the
	// past belongs to the journal, and each is refused by name.
	for _, line := range []string{"frames watch last=5", "frames watch since=00:52",
		"frames watch around=abcd"} {
		out := run(t, testDeps(t), line)
		if !strings.Contains(out, "reads the journal") {
			t.Errorf("%q swallowed by the live feed:\n%s", line, out)
		}
	}
}

func TestNodesAdmitsAnUnmeasuredRSSI(t *testing.T) {
	// A node known only through a judgement whose reception was lost
	// has no signal measurement: 0 dBm would read a hundred decibels
	// too good.
	deps := testDeps(t)
	deps.Sentinel.Process(context.Background(), bus.FrameJudged{
		Relay: "meshcore-868", Txn: txn.New(), At: time.Now(),
		Verdict: "would-relay-flood",
		Type:    "ADVERT", Route: "FLOOD", Node: "Ghost", PubKey: "aabbccddeeff",
	})
	out := run(t, deps, "nodes")
	if !strings.Contains(out, "Ghost") {
		t.Fatalf("directory lost the node:\n%s", out)
	}
	if strings.Contains(out, "0 dBm") {
		t.Errorf("an unmeasured RSSI is rendered as 0 dBm:\n%s", out)
	}
}

func TestAdvertCommandIsAdminAndForwards(t *testing.T) {
	deps := testDeps(t)
	var got []bool
	deps.Relays[0].TriggerAdvert = func(flood bool) error {
		got = append(got, flood)
		return nil
	}

	// The transport decides: read-only sessions may not key the radio.
	if out := run(t, deps, "/relay/meshcore-868/advert"); !strings.Contains(out, "admin command") {
		t.Fatalf("read-only session was allowed to emit:\n%s", out)
	}
	if len(got) != 0 {
		t.Fatal("the refusal still reached the engine")
	}

	deps.Privilege = Admin
	out := run(t, deps, "/relay/meshcore-868/advert",
		"/relay/meshcore-868/advert flood", "/relay/meshcore-868/advert nope")
	if !strings.Contains(out, "zero-hop advert queued") ||
		!strings.Contains(out, "flood advert queued") {
		t.Errorf("confirmations missing:\n%s", out)
	}
	if !strings.Contains(out, `unknown argument "nope"`) {
		t.Errorf("a stray argument was swallowed:\n%s", out)
	}
	if len(got) != 2 || got[0] != false || got[1] != true {
		t.Fatalf("engine saw %v, want [false true]", got)
	}

	deps.Relays[0].TriggerAdvert = nil
	if out := run(t, deps, "/relay/meshcore-868/advert"); !strings.Contains(out, "gate is dry") {
		t.Errorf("a dry relay should refuse with its reason:\n%s", out)
	}
}

func TestADownRelaySaysWhyRatherThanWhatIsMissing(t *testing.T) {
	deps := testDeps(t)
	deps.Privilege = Admin // discover and advert key the radio
	deps.Relays[0].State = func() string { return relay.StateError }
	deps.Relays[0].Err = func() string {
		return `override scope "eu-868-narrow": cannot unmarshal !!str into []string`
	}
	// A relay that never configured has no scopes, no neighbourhood
	// and no scan — every command must name the cause, not the
	// consequence it happens to trip over.
	for _, cmd := range []string{"/relay/meshcore-868/scopes",
		"/relay/meshcore-868/advert",
		"/relay/meshcore-868/neighbours/discover",
		"/relay/meshcore-868/neighbours/print"} {
		out := run(t, deps, cmd)
		if !strings.Contains(out, "cannot unmarshal") {
			t.Errorf("%q said %q — the operator never learns why", cmd, strings.TrimSpace(out))
		}
	}
}

func TestARunningRelayStillReportsWhatItLacks(t *testing.T) {
	// The cause guard must not swallow the honest answer of a relay
	// that is up and simply keeps no neighbourhood.
	out := run(t, testDeps(t), "/relay/meshcore-868/neighbours/print")
	if !strings.Contains(out, "does not keep a neighbourhood") {
		t.Errorf("the drawer said %q", strings.TrimSpace(out))
	}
}

func TestDiscoverAsksAndReturns(t *testing.T) {
	deps := testDeps(t)
	deps.Privilege = Admin
	asked := 0
	found := make(chan Neighbour, 1)
	deps.Relays[0].Discover = func() (<-chan Neighbour, time.Time, error) {
		asked++
		return found, time.Now().Add(time.Minute), nil
	}

	// The default emits and hands the console back: the answers are
	// recorded whether or not anyone is watching for them.
	done := make(chan string, 1)
	go func() { done <- run(t, deps, "/relay/meshcore-868/neighbours/discover") }()
	select {
	case out := <-done:
		if !strings.Contains(out, "answers land in the neighbourhood") {
			t.Errorf("discover said %q", strings.TrimSpace(out))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("discover held the console — the scan runs without it")
	}
	if asked != 1 {
		t.Fatalf("the scan was asked for %d times", asked)
	}

	// watch stays and prints what lands.
	var key [32]byte
	key[0], key[1] = 0x88, 0x2f
	found <- Neighbour{PubKey: key, SNR: 12.25}
	close(found)
	if out := run(t, deps, "/relay/meshcore-868/neighbours/discover watch"); !strings.Contains(out, "882f") ||
		!strings.Contains(out, "12.2 dB") {
		t.Errorf("watch said %q", strings.TrimSpace(out))
	}
}

func TestWatchingDiscoverGivesTheConsoleBackOnALine(t *testing.T) {
	deps := testDeps(t)
	deps.Privilege = Admin
	// A window that never fills and never expires: only the operator's
	// line can end this, which is the point.
	deps.Relays[0].Discover = func() (<-chan Neighbour, time.Time, error) {
		return make(chan Neighbour), time.Now().Add(time.Hour), nil
	}

	done := make(chan string, 1)
	go func() { done <- run(t, deps, "/relay/meshcore-868/neighbours/discover watch", "status") }()
	select {
	case out := <-done:
		if !strings.Contains(out, "enter stops") {
			t.Error("the watch never said how to leave it")
		}
		if !strings.Contains(out, "the scan runs on") {
			t.Error("leaving the watch did not say the scan continues")
		}
		// The line that stopped it ran as a command, so the console is
		// genuinely back rather than merely unblocked.
		if !strings.Contains(out, "meshcore-868") {
			t.Errorf("the line after the watch never ran:\n%s", out)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a line did not take the console back")
	}
}

func TestNeighboursBorrowNamesFromTheJournal(t *testing.T) {
	deps := testDeps(t)
	var known, unknown [32]byte
	known[0], known[1] = 0x88, 0x2f
	unknown[0] = 0xAB
	deps.Relays[0].Neighbours = func() []Neighbour {
		return []Neighbour{
			// Heard by advert: the engine knows the name itself.
			{PubKey: known, Name: "quatre-vingt-huit", SNR: 12.25, Heard: time.Now()},
			// Only ever answered a scan: no name on that wire.
			{PubKey: unknown, SNR: 9, Heard: time.Now()},
		}
	}
	out := run(t, deps, "/relay/meshcore-868/neighbours/print")
	if !strings.Contains(out, "quatre-vingt-huit") {
		t.Errorf("the engine's own name never reached the table:\n%s", out)
	}
	// Nothing named the second one, so the row says so rather than
	// inventing one.
	if !strings.Contains(out, "—") {
		t.Errorf("a nameless neighbour got no placeholder:\n%s", out)
	}
}

func TestBothPathsEndALineTheSameWay(t *testing.T) {
	// The editor accepts CR, LF and CR LF; so must the plain reader,
	// or a peer's Enter is honoured on one path and swallowed on the
	// other with nothing to tell it which path it got.
	for _, term := range []string{"\r", "\n", "\r\n", "\r\x00"} {
		lines := make(chan string, 4)
		done := make(chan struct{})
		go readLines(strings.NewReader("status"+term+"quit"+term), lines, done)
		var got []string
		for l := range lines {
			if l != "" {
				got = append(got, l)
			}
		}
		close(done)
		if len(got) != 2 || got[0] != "status" || got[1] != "quit" {
			t.Errorf("terminator %q yielded %q", term, got)
		}
	}
}
