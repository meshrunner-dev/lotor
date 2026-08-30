package main

// The instance-name grammar, proved where it matters: at every door a
// name may arrive through, and in the store a looser binary left
// behind. A name the console cannot spell is a name no export can
// restore and no operator can reach — so the rule is one rule, and a
// legacy store is healed rather than left holding unreachable objects.

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"meshrunner.dev/lotor/internal/confdb"
	"meshrunner.dev/lotor/internal/config"
)

// hostileNames is the matrix the review asked for, each with the
// reason the console cannot carry it.
var hostileNames = []struct{ name, why string }{
	{"obs one", "space separates the words of a command"},
	{"obs/one", "slash walks the tree"},
	{`obs"one`, "the quote delimits a value"},
	{"obs=one", "equals opens a value"},
	{"obs\x1bone", "an escape types into the terminal"},
}

func TestEveryDoorRefusesTheSameNames(t *testing.T) {
	// The policy, stated once and asserted everywhere: import, the
	// whole-file preflight and the console's own creation agree, so
	// an object that exists is always one an export can restore.
	for _, c := range hostileNames {
		f := sampleFile()
		f.MQTT = map[string]config.MQTT{c.name: {Disabled: true}}
		if err := f.Validate(false); err == nil {
			t.Errorf("%s: Validate accepted %q", c.why, c.name)
		}

		m, b := replayManager(t, nil)
		out := adminConsole(t, m, b, "/mqtt add "+c.name+" url=tcp://127.0.0.1:1\n")
		if strings.Contains(out, "added — observer") {
			t.Errorf("%s: the console created %q", c.why, c.name)
		}
		if _, _, ok := m.Layers("mqtt", c.name); ok {
			t.Errorf("%s: %q reached the store through the console", c.why, c.name)
		}
	}
}

func TestImportRefusesUnspellableNamesWithoutTouchingTheBase(t *testing.T) {
	// The whole command path: a YAML whose objects are individually
	// valid but whose handle the console could never spell must not
	// land, and must not create the base on the way out.
	dir := t.TempDir()
	yaml := filepath.Join(dir, "spaced.yaml")
	if err := os.WriteFile(yaml, []byte(
		"radios:\n  r:\n    driver: sx126x-spi\n    profile: rak6421-13300x-slot1\n"+
			"relays:\n  mc:\n    protocol: meshcore\n    radio: r\n"+
			"    profile: eu-868-narrow\n"+
			"mqtt:\n  obs one:\n    disabled: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	db := filepath.Join(dir, "config.db")
	if err := (&configImportCmd{Path: yaml, DB: db, Force: true}).Run(); err == nil {
		t.Fatal("an observer the console cannot name imported")
	}
	if _, err := os.Stat(db); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the refused import touched the base: %v", err)
	}
}

func TestLegacyNamesAreHealedWithEverythingThatPointsAtThem(t *testing.T) {
	// A store a looser binary wrote: the names are unreachable, so
	// the migration renames them — and every reference and every
	// runtime table follows, because a dangling radio would refuse
	// the whole file at the next load and orphaned sessions would
	// silently rewind an admin's replay guard.
	ctx := context.Background()
	s, err := confdb.Open(ctx, confdb.Memory, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	plant := func(kind, name string, attrs map[string]any) {
		t.Helper()
		if err := s.Replace(ctx, kind, name, attrs, "test", "add", nil); err != nil {
			t.Fatal(err)
		}
	}
	plant(confdb.KindRadio, "slot 1", map[string]any{"driver": "sx126x-spi"})
	plant(confdb.KindRelay, "mc one", map[string]any{
		"protocol": "meshcore", "radio": "slot 1",
	})
	plant(confdb.KindMQTT, "obs one", map[string]any{"relay": "mc one"})
	// Two legacy names that canonicalise onto the same handle: the
	// second must land somewhere of its own, not overwrite the first.
	plant(confdb.KindMQTT, "obs/one", map[string]any{"relay": "mc one"})
	// Empty is invalid for an instance but canonical for a singleton:
	// filtering by kind, rather than by name, keeps that distinction.
	plant(confdb.KindSensor, "", map[string]any{"driver": "bme280"})
	if err := s.SaveACL(ctx, "mc one", confdb.ACLRow{
		PubKey: []byte{1, 2}, LastActive: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReplaceRegions(ctx, "mc one",
		[]confdb.RegionRow{{ID: 1, Name: "eu"}}, confdb.RegionsMeta{NextID: 2}); err != nil {
		t.Fatal(err)
	}

	if err := s.Migrate(ctx, storeMigrations()); err != nil {
		t.Fatal(err)
	}

	f, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Every handle is now one the console can spell — and the file
	// still passes the door that judges it.
	if err := f.Validate(false); err != nil {
		t.Fatalf("the healed store still fails the name grammar: %s", err)
	}
	relay, ok := f.Relays["mc-one"]
	if !ok {
		t.Fatalf("relays = %v — the relay was not healed", keysOfRelays(f.Relays))
	}
	if relay.Radio != "slot-1" {
		t.Errorf("relay points at radio %q — the reference did not follow", relay.Radio)
	}
	if _, ok := f.Radios["slot-1"]; !ok {
		t.Errorf("radios = %v", keysOfRadios(f.Radios))
	}
	if _, ok := f.Sensors["unnamed"]; !ok {
		t.Errorf("the empty legacy instance was not healed: sensors = %v", f.Sensors)
	}
	first, second := f.MQTT["obs-one"], f.MQTT["obs-one-2"]
	if len(f.MQTT) != 2 {
		t.Fatalf("observers = %d, want both healed apart", len(f.MQTT))
	}
	for what, mq := range map[string]config.MQTT{"obs-one": first, "obs-one-2": second} {
		// The observer's relay= is a layered attribute: it lives in
		// the override scope the store put it in, and the rename must
		// reach it there too.
		relayName, _ := mq.Layered.Overrides["custom"]["relay"].(string)
		if relayName != "mc-one" {
			t.Errorf("observer %s watches %q — the reference did not follow", what, relayName)
		}
	}
	// The runtime state arrived at the new name, whole.
	if rows, _ := s.LoadACL(ctx, "mc-one"); len(rows) != 1 {
		t.Errorf("sessions at the healed name = %d — the replay guard was rewound", len(rows))
	}
	if _, _, ok, _ := s.LoadRegions(ctx, "mc-one"); !ok {
		t.Error("the transport policy did not follow the relay")
	}
	if rows, _ := s.LoadACL(ctx, "mc one"); len(rows) != 0 {
		t.Error("sessions stayed behind at the old name")
	}
	// Migrating again changes nothing: the shape stamp holds and the
	// healed names are already grammatical.
	if err := s.Migrate(ctx, storeMigrations()); err != nil {
		t.Fatal(err)
	}
}

func keysOfRelays(m map[string]config.Relay) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func keysOfRadios(m map[string]config.Radio) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestHealedNamesSurviveTheObjectsTheyKey(t *testing.T) {
	// The rename is a rename, not a rewrite: the attributes an object
	// carried are the ones it still carries afterwards.
	ctx := context.Background()
	s, err := confdb.Open(ctx, confdb.Memory, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	before := map[string]any{
		"driver": "sx126x-spi",
		"overrides": map[string]any{
			"custom": map[string]any{"spi": "/dev/spidev0.0"},
		},
		"profile": "custom",
	}
	if err := s.Replace(ctx, confdb.KindRadio, "slot 1", before, "test", "add", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Migrate(ctx, storeMigrations()); err != nil {
		t.Fatal(err)
	}
	f, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := f.Radios["slot-1"]
	if !ok {
		t.Fatal("the radio did not survive its rename")
	}
	if got.Driver != "sx126x-spi" ||
		got.Layered.Overrides["custom"]["spi"] != "/dev/spidev0.0" {
		t.Errorf("the rename disturbed the object: %+v", got)
	}
}

func TestNameMigrationLeavesSingletonKeysAlone(t *testing.T) {
	// A singleton's empty name is not an invalid instance handle: it is
	// the canonical key every Replace and Remove uses. Shape 12 must
	// leave all four there, with no renamed row left to shadow a later
	// mutation when the store is opened again.
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "config.db")
	s, err := confdb.Open(ctx, path, 0)
	if err != nil {
		t.Fatal(err)
	}
	old := []struct {
		kind    string
		section any
	}{
		{confdb.KindSystem, config.System{Name: "old"}},
		{confdb.KindCLI, config.CLI{Listen: "127.0.0.1:2323"}},
		{confdb.KindSentinel, config.Sentinel{Journal: ":memory:", Retention: time.Hour}},
		{confdb.KindUpdate, config.Update{Channel: "dev"}},
		{confdb.KindWeb, config.Web{Listen: "127.0.0.1:8695"}},
	}
	for _, one := range old {
		if err := s.Replace(ctx, one.kind, "", one.section, "test", "set", nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Migrate(ctx, storeMigrations()); err != nil {
		t.Fatal(err)
	}
	for _, one := range old {
		if err := s.Remove(ctx, one.kind, "", "test"); err != nil {
			t.Fatalf("%s moved away from its empty key: %v", one.kind, err)
		}
	}
	empty, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if empty.System != nil || empty.CLI != nil || empty.Sentinel != nil ||
		empty.Update != nil || empty.Web != nil {
		t.Fatalf("a renamed singleton survived removal: %+v", empty)
	}

	newSystem := config.System{Name: "new"}
	newCLI := config.CLI{Listen: "127.0.0.1:2424"}
	newSentinel := config.Sentinel{Journal: ":memory:", Retention: 2 * time.Hour}
	newUpdate := config.Update{Channel: "beta"}
	for _, one := range []struct {
		kind    string
		section any
	}{
		{confdb.KindSystem, newSystem},
		{confdb.KindCLI, newCLI},
		{confdb.KindSentinel, newSentinel},
		{confdb.KindUpdate, newUpdate},
	} {
		if err := s.Replace(ctx, one.kind, "", one.section, "test", "set", nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = confdb.Open(ctx, path, shapeCeiling())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	got, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got.System == nil || *got.System != newSystem ||
		got.CLI == nil || got.CLI.Listen != newCLI.Listen ||
		got.Sentinel == nil || got.Sentinel.Journal != newSentinel.Journal ||
		got.Sentinel.Retention != newSentinel.Retention ||
		got.Update == nil || *got.Update != newUpdate {
		t.Fatalf("singleton mutations did not survive reopen: %+v", got)
	}
}

func TestSystemMutationReplyNeutralisesControls(t *testing.T) {
	// Banner and prompt already use the terminal renderer; the mutation
	// reply must not give the same stored name one raw opportunity in
	// between to execute an escape sequence.
	m, b := replayManager(t, nil)
	name := "lab\x1b[2J"
	out := adminConsole(t, m, b, "/system set64 name="+
		base64.StdEncoding.EncodeToString([]byte(name))+"\n")
	if strings.Contains(out, "\x1b[2J") {
		t.Fatalf("the system mutation reply emitted the stored ESC: %q", out)
	}
	if !strings.Contains(out, "this system is now lab?[2J") {
		t.Fatalf("the neutralised name is not visible in the reply: %q", out)
	}
}
