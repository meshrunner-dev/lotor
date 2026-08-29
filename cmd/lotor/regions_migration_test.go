package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"meshrunner.dev/lotor/internal/cli"
	"meshrunner.dev/lotor/internal/confdb"
	"meshrunner.dev/pkg/meshcore"
)

// The exact override shape the lab stored: the three scope keys inside
// the profile's override scope, beside keys that must not move.
const labRelayAttrs = `{"overrides":{"eu-868-narrow":{` +
	`"accept_scopes":["eu","europe","fr","fr-idf"],"accept_unscoped":true,` +
	`"default_scope":"fr-idf","node_name":"Raccoon City","tx_power_dbm":"6"}},` +
	`"profile":"eu-868-narrow","protocol":"meshcore","radio":"slot1"}`

func TestScopeAttrsLiftIntoTheRegionTable(t *testing.T) {
	healed, entries, meta, err := liftScopeAttrs(labRelayAttrs)
	if err != nil || meta == nil {
		t.Fatalf("lift: %v meta=%v", err, meta)
	}
	// Every accepted scope is a flood-allowed region, flat under the
	// wildcard, in list order; the default designation points at its
	// region; carrying plain floods leaves the wildcard's flags clear.
	names := make([]string, len(entries))
	for i, r := range entries {
		names[i] = r.Name
		if r.Parent != 0 || r.Flags != 0 {
			t.Errorf("row %+v — accepted scopes lift flat and flood-allowed", r)
		}
	}
	if len(names) != 4 || names[0] != "eu" || names[3] != "fr-idf" {
		t.Errorf("names = %v", names)
	}
	if meta.DefaultID != entries[3].ID || meta.HomeID != 0 ||
		meta.WildcardFlags != 0 || meta.NextID != 5 {
		t.Errorf("meta = %+v", meta)
	}
	// The three keys are gone; their neighbours stayed.
	var o map[string]any
	if err := json.Unmarshal([]byte(healed), &o); err != nil {
		t.Fatal(err)
	}
	overrides, ok := o["overrides"].(map[string]any)
	if !ok {
		t.Fatalf("overrides gone: %v", o)
	}
	kv, ok := overrides["eu-868-narrow"].(map[string]any)
	if !ok {
		t.Fatalf("override scope gone: %v", overrides)
	}
	for _, gone := range []string{"accept_scopes", "default_scope", "accept_unscoped"} {
		if _, held := kv[gone]; held {
			t.Errorf("%s survived the lift", gone)
		}
	}
	if kv["node_name"] != "Raccoon City" || kv["tx_power_dbm"] != "6" {
		t.Errorf("neighbour keys moved: %v", kv)
	}
}

func TestScopeLiftEdges(t *testing.T) {
	// A relay that never spoke of scopes lifts nothing — absent meta
	// is what tells "never configured" from "configured empty".
	if _, _, meta, err := liftScopeAttrs(`{"overrides":{"custom":{"node_name":"x"}}}`); err != nil || meta != nil {
		t.Errorf("untouched relay: meta=%+v err=%v", meta, err)
	}
	// The default scope missing from the accepted list is created, as
	// the reference auto-creates on designation; refusing was the old
	// rule and the migration must not enforce it against stored data.
	_, entries, meta, err := liftScopeAttrs(
		`{"overrides":{"custom":{"accept_scopes":["eu"],"default_scope":"lab"}}}`)
	if err != nil || len(entries) != 2 || entries[1].Name != "lab" || meta.DefaultID != entries[1].ID {
		t.Errorf("auto-create: entries=%+v meta=%+v err=%v", entries, meta, err)
	}
	// accept_unscoped false becomes the wildcard's deny-flood flag,
	// and the comma spelling of the list reads as the list.
	// The stored spelling is the ORIGINAL: "#fr" stays "#fr" — every
	// '#' is identity, and the model itself treats "fr" and "#fr" as
	// one region wherever it matters.
	_, entries, meta, err = liftScopeAttrs(
		`{"overrides":{"custom":{"accept_scopes":"eu, #fr","accept_unscoped":false}}}`)
	if err != nil || len(entries) != 2 || entries[1].Name != "#fr" || meta.WildcardFlags != 1 {
		t.Errorf("comma+deny: entries=%+v meta=%+v err=%v", entries, meta, err)
	}
}

func TestShapeSixMigratesAStore(t *testing.T) {
	ctx := context.Background()
	s, err := confdb.Open(ctx, confdb.Memory, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	seedRelay(ctx, t, s, "mc", labRelayAttrs)
	seedRelay(ctx, t, s, "quiet", `{"overrides":{},"protocol":"meshcore","radio":"r"}`)

	if err := s.Migrate(ctx, storeMigrations()); err != nil {
		t.Fatal(err)
	}
	rows, meta, ok, err := s.LoadRegions(ctx, "mc")
	if err != nil || !ok {
		t.Fatalf("regions after migration: ok=%v err=%v", ok, err)
	}
	if len(rows) != 4 || rows[3].Name != "fr-idf" || meta.DefaultID != rows[3].ID {
		t.Errorf("rows=%+v meta=%+v", rows, meta)
	}
	if _, _, ok, _ := s.LoadRegions(ctx, "quiet"); ok {
		t.Error("a relay without scope keys got a meta row")
	}
	// Migrating again is a no-op: the shape stamp holds.
	if err := s.Migrate(ctx, storeMigrations()); err != nil {
		t.Fatal(err)
	}
}

// seedRelay plants one relay object row as a past binary stored it.
func seedRelay(ctx context.Context, t *testing.T, s *confdb.Store, name, attrs string) {
	t.Helper()
	var o map[string]any
	if err := json.Unmarshal([]byte(attrs), &o); err != nil {
		t.Fatal(err)
	}
	if err := s.Replace(ctx, confdb.KindRelay, name, o, "test", "add", nil); err != nil {
		t.Fatal(err)
	}
}

func TestScopeLiftReadsTheSelectedProfileAlone(t *testing.T) {
	// The layering's contract survives the migration: only the
	// selected profile's overrides were ever in force, so only they
	// translate — an inactive profile's antagonistic policy must not
	// leak in, however its name sorts.
	attrs := `{"profile":"b","overrides":{` +
		`"a":{"accept_scopes":["evil","worse"],"default_scope":"evil","accept_unscoped":false},` +
		`"b":{"accept_scopes":["good"],"default_scope":"good","accept_unscoped":true}}}`
	healed, entries, meta, err := liftScopeAttrs(attrs)
	if err != nil || meta == nil {
		t.Fatalf("lift: %v meta=%v", err, meta)
	}
	if len(entries) != 1 || entries[0].Name != "good" ||
		meta.DefaultID != entries[0].ID || meta.WildcardFlags != 0 {
		t.Errorf("entries=%+v meta=%+v — the inactive profile leaked", entries, meta)
	}
	// The obsolete keys are stripped from EVERY scope.
	var o map[string]any
	if err := json.Unmarshal([]byte(healed), &o); err != nil {
		t.Fatal(err)
	}
	for _, sc := range []string{"a", "b"} {
		kv, ok := o["overrides"].(map[string]any)[sc].(map[string]any)
		if !ok {
			t.Fatalf("scope %s gone", sc)
		}
		for _, gone := range []string{"accept_scopes", "default_scope", "accept_unscoped"} {
			if _, held := kv[gone]; held {
				t.Errorf("%s.%s survived", sc, gone)
			}
		}
	}
}

func TestScopeLiftStripsInactiveProfilesEvenWithoutAnActivePolicy(t *testing.T) {
	// The active profile never spoke of scopes: no region table — the
	// running policy was the defaults — but the obsolete keys must
	// still leave the store, or the strict config door refuses the
	// relay at its next load.
	attrs := `{"profile":"b","overrides":{` +
		`"a":{"accept_scopes":["old"],"node_name":"x"},"b":{"node_name":"y"}}}`
	healed, entries, meta, err := liftScopeAttrs(attrs)
	if err != nil || meta != nil || entries != nil {
		t.Fatalf("lift: %v meta=%+v entries=%+v", err, meta, entries)
	}
	if strings.Contains(healed, "accept_scopes") {
		t.Error("the obsolete key survived in an inactive profile")
	}
	if !strings.Contains(healed, `"node_name":"x"`) {
		t.Error("a neighbour key was lost in the strip")
	}
}

func TestScopeLiftBoundsAndPolicies(t *testing.T) {
	// 33 accepted scopes cannot become a table the engine refuses to
	// attach: the migration fails before writing, backup in hand.
	names := make([]string, 33)
	for i := range names {
		names[i] = fmt.Sprintf("s%02d", i)
	}
	raw, err := json.Marshal(map[string]any{
		"overrides": map[string]any{"custom": map[string]any{"accept_scopes": names}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := liftScopeAttrs(string(raw)); err == nil ||
		!strings.Contains(err.Error(), "region table holds 32") {
		t.Errorf("33 scopes = %v", err)
	}
	// A truly private scope fails the migration whole, backup in
	// hand: erasing it silently would turn a stored policy into
	// unscoped emission.
	if _, _, _, err := liftScopeAttrs(
		`{"overrides":{"custom":{"accept_scopes":["eu","$secret"]}}}`); err == nil ||
		!strings.Contains(err.Error(), "private region") {
		t.Errorf("$ scope = %v, want the refusal", err)
	}
	// "#$secret" is NOT private: the '#' marks an auto region whose
	// bare name merely begins with '$' — MeshCore derives its key from
	// "#$secret" like any other. The class rides the first character,
	// so the stored name keeps its '#'.
	_, entries, meta, err := liftScopeAttrs(
		`{"overrides":{"custom":{"accept_scopes":["#fr","#$secret"],"default_scope":"#$secret"}}}`)
	if err != nil || len(entries) != 2 || entries[1].Name != "#$secret" {
		t.Fatalf("#$ lift: entries=%+v err=%v", entries, err)
	}
	if meta.DefaultID != entries[1].ID {
		t.Errorf("meta=%+v — the default lost its region", meta)
	}
	if entries[0].Name != "#fr" {
		t.Errorf("entries=%+v — the original spelling survives", entries)
	}
}

// recordingRegionDoor captures what reaches a relay's region door.
type recordingRegionDoor struct {
	lines []string
	armed bool
}

func TestOTARegionDoorGetsTheRawLine(t *testing.T) {
	door := &recordingRegionDoor{}
	m := &manager{infos: map[string]cli.RelayInfo{"mc": {
		Name: "mc",
		RegionLine: func(owner, line string) (string, bool, error) {
			door.lines = append(door.lines, line)
			return "ok", true, nil
		},
		RegionLoadArmed: func(owner string) bool { return door.armed },
	}}}

	// Not armed: a region line routes raw, anything else falls through
	// to the ordinary dispatch — which trims and answers as itself.
	if out := m.runOTA("mc", "p", "o", "  region put x"); out != "ok" {
		t.Fatalf("region line = %q", out)
	}
	if out := m.runOTA("mc", "p", "o", " eu F"); out != otaUnknown {
		t.Fatalf("stray load line = %q, want the ordinary dispatch", out)
	}

	// Armed: EVERY line belongs to the staging, its leading spaces
	// intact — they are the load format's whole meaning — and the
	// blank commit included.
	door.armed = true
	for _, line := range []string{"*^ F", " eu F", "  fr F", ""} {
		if out := m.runOTA("mc", "p", "o", line); out != "ok" {
			t.Fatalf("armed line %q = %q", line, out)
		}
	}
	want := []string{"  region put x", "*^ F", " eu F", "  fr F", ""}
	if len(door.lines) != len(want) {
		t.Fatalf("door saw %q", door.lines)
	}
	for i, l := range door.lines {
		if l != want[i] {
			t.Errorf("line %d = %q, want %q — normalisation crept in", i, l, want[i])
		}
	}
}

func TestOrphanRuntimeIsDroppedOnUpgrade(t *testing.T) {
	// A removal before the cascade existed left sessions and regions
	// behind; shape 9 sweeps them so a recreated name starts anew.
	ctx := context.Background()
	s, err := confdb.Open(ctx, confdb.Memory, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	seedRelay(ctx, t, s, "kept", `{"overrides":{},"protocol":"meshcore","radio":"r"}`)
	for _, relay := range []string{"kept", "gone"} {
		if err := s.ReplaceRegions(ctx, relay,
			[]confdb.RegionRow{{ID: 1, Name: "eu"}}, confdb.RegionsMeta{NextID: 2}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Migrate(ctx, storeMigrations()); err != nil {
		t.Fatal(err)
	}
	if _, _, ok, _ := s.LoadRegions(ctx, "gone"); ok {
		t.Error("the orphan's regions survived the sweep")
	}
	if _, _, ok, _ := s.LoadRegions(ctx, "kept"); !ok {
		t.Error("the living relay's regions were swept too")
	}
}

func TestScopeLiftKeepsEveryValidSpelling(t *testing.T) {
	// The review's counter-examples: "#" was a valid scope the old
	// validator accepted and must not become unscoped policy; "##x"
	// and "#x" are DIFFERENT identities that must both survive and
	// still restore; and the one-hash pairs collapse as ever.
	_, entries, meta, err := liftScopeAttrs(`{"overrides":{"custom":{` +
		`"accept_scopes":["#","##x","#x","fr","#fr"],"default_scope":"#"}}}`)
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	want := []string{"#", "##x", "#x", "fr"}
	if len(names) != len(want) {
		t.Fatalf("names = %v, want %v", names, want)
	}
	for i := range want {
		if names[i] != want[i] {
			t.Fatalf("names = %v, want %v", names, want)
		}
	}
	if meta.DefaultID != entries[0].ID {
		t.Errorf("meta = %+v — the '#' default lost its region", meta)
	}
	// And the produced table restores: distinct identities, no
	// collision after the model's own one-hash equivalence.
	rows := make([]meshcore.Region, len(entries))
	for i, e := range entries {
		rows[i] = meshcore.Region{ID: e.ID, Name: e.Name}
	}
	if _, err := meshcore.RestoreRegionMap(rows, 0, meta.NextID, 0, meta.DefaultID); err != nil {
		t.Errorf("the migrated table does not restore: %v", err)
	}
}

func TestOldImportRevisionsBecomeReadable(t *testing.T) {
	// Early imports journalled the raw object; the console showed
	// those rows as unreadable. Shape 11 wraps them into the delta
	// form — scrubbed on the way, like everything else was.
	ctx := context.Background()
	s, err := confdb.Open(ctx, confdb.Memory, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if err := s.Replace(ctx, confdb.KindRelay, "mc",
		map[string]any{"radio": "r"}, "t", "add", nil); err != nil {
		t.Fatal(err)
	}
	// Plant a pre-delta import row the way an old binary wrote it.
	if err := confdb.PlantRevision(ctx, s, "import",
		`{"overrides":{"custom":{"admin_password":"RACCOON-CANARY","node_name":"x"}}}`); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx, storeMigrations()); err != nil {
		t.Fatal(err)
	}
	revs, err := s.Revisions(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range revs {
		if r.Op != "import" {
			continue
		}
		if _, err := r.Changes(); err != nil {
			t.Errorf("import revision still unreadable: %v", err)
		}
		if strings.Contains(r.Change, "RACCOON-CANARY") {
			t.Error("the wrap left the secret in the clear")
		}
		if !strings.Contains(r.Change, `"object"`) {
			t.Errorf("import revision not in delta form: %s", r.Change)
		}
	}
}

// goodRadio is the one healthy radio the invalid-import matrix hangs
// its subjects from, so each case fails for its own reason alone.
const goodRadio = "radios:\n  r:\n    driver: sx126x-spi\n" +
	"    profile: rak6421-13300x-slot1\n    overrides:\n" +
	"      rak6421-13300x-slot1:\n        spi: /dev/spidev0.0\n"

// goodRelay is the same for a relay: valid, so an observer or sensor
// case is judged on itself.
const goodRelay = "relays:\n  mc:\n    protocol: meshcore\n    radio: r\n" +
	"    profile: eu-868-narrow\n"

func TestParkedObserversImportOnTheirShapeAlone(t *testing.T) {
	// Disabled means the configuration is kept and nothing runs — an
	// operator parks an observer precisely because it is not ready.
	// Judging a parked one on whether it could connect refuses files
	// the daemon boots happily, and makes its own export unimportable.
	// The lab's store is exactly this shape.
	dir := t.TempDir()
	yaml := filepath.Join(dir, "parked.yaml")
	if err := os.WriteFile(yaml, []byte(goodRadio+goodRelay+
		"mqtt:\n  parked:\n    disabled: true\n    profile: analyzer-eu\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	db := filepath.Join(dir, "config.db")
	if err := (&configImportCmd{Path: yaml, DB: db, Force: true}).Run(); err != nil {
		t.Fatalf("a parked observer was refused: %s", err)
	}
	// Its shape is still judged: a parked observer may be incomplete,
	// never incoherent.
	bad := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(bad, []byte(goodRadio+goodRelay+
		"mqtt:\n  parked:\n    disabled: true\n    profile: no-such-preset\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := (&configImportCmd{Path: bad, DB: filepath.Join(dir, "bad.db"),
		Force: true}).Run(); err == nil {
		t.Error("a parked observer naming no known profile imported")
	}
}

func TestImportRefusesEveryFamilyOfInvalidity(t *testing.T) {
	// The CFG-009 matrix: the preflight promises to refuse a file that
	// would not boot, and it must do so through the whole command
	// path — one case per family, each proving the existing base is
	// left exactly as it was. An import that half-lands is the one
	// outcome a configuration store may never have.
	for _, c := range []struct{ family, yaml string }{
		{"unknown protocol", goodRadio +
			"relays:\n  mc:\n    protocol: not-a-protocol\n    radio: r\n"},
		{"unknown driver",
			"radios:\n  r:\n    driver: not-a-driver\n" + goodRelay},
		{"unknown profile", goodRadio +
			"relays:\n  mc:\n    protocol: meshcore\n    radio: r\n" +
			"    profile: no-such-preset\n"},
		{"unknown attribute", goodRadio +
			"relays:\n  mc:\n    protocol: meshcore\n    radio: r\n" +
			"    profile: eu-868-narrow\n    overrides:\n" +
			"      eu-868-narrow:\n        not_an_attribute: 1\n"},
		{"attribute of the wrong type", goodRadio +
			"relays:\n  mc:\n    protocol: meshcore\n    radio: r\n" +
			"    profile: eu-868-narrow\n    overrides:\n" +
			"      eu-868-narrow:\n        tx_power_dbm: not-a-number\n"},
		{"inactive scope holding an unknown attribute", goodRadio +
			"relays:\n  mc:\n    protocol: meshcore\n    radio: r\n" +
			"    profile: eu-868-narrow\n    overrides:\n" +
			"      eu-433:\n        not_an_attribute: 1\n"},
		{"waveform outside the chip's envelope", goodRadio +
			"relays:\n  mc:\n    protocol: meshcore\n    radio: r\n" +
			"    profile: eu-868-narrow\n    overrides:\n" +
			"      eu-868-narrow:\n        frequency_hz: 2400000000\n"},
		{"dangling radio reference", goodRadio +
			"relays:\n  mc:\n    protocol: meshcore\n    radio: nowhere\n"},
		{"unknown sensor driver", goodRadio + goodRelay +
			"sensors:\n  part:\n    driver: not-a-sensor\n"},
		{"sensor read faster than the floor", goodRadio + goodRelay +
			"sensors:\n  part:\n    driver: bme280\n    sample_interval: 1ms\n"},
		{"observer with no broker", goodRadio + goodRelay +
			"mqtt:\n  obs:\n    profile: custom\n"},
		{"observer whose topic cannot be built", goodRadio + goodRelay +
			"mqtt:\n  obs:\n    profile: analyzer-eu\n"},
		{"observer whose relay does not exist", goodRadio + goodRelay +
			"mqtt:\n  obs:\n    profile: analyzer-eu\n    overrides:\n" +
			"      analyzer-eu:\n        iata: TLS\n        relay: nowhere\n"},
		{"unspellable instance name", goodRadio + goodRelay +
			"mqtt:\n  obs one:\n    disabled: true\n"},
	} {
		t.Run(c.family, func(t *testing.T) {
			dir := t.TempDir()
			yaml := filepath.Join(dir, "bad.yaml")
			if err := os.WriteFile(yaml, []byte(c.yaml), 0o600); err != nil {
				t.Fatal(err)
			}
			db := filepath.Join(dir, "config.db")
			if err := (&configImportCmd{Path: yaml, DB: db, Force: true}).Run(); err == nil {
				t.Fatalf("%s imported", c.family)
			}
			if _, err := os.Stat(db); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("the refused import touched the base: %v", err)
			}
		})
	}
}
