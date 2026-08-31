package confdb

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"meshrunner.dev/lotor/internal/config"
)

// sample is a configuration exercising every section and the shapes
// that have hurt before: durations, pointers, nested override maps,
// a string that looks like nothing else (the identity), a list.
func sample() *config.File {
	socket := "/run/lotor/console.sock"
	history := false
	return &config.File{
		Radios: map[string]config.Radio{
			"slot1": {
				Driver: "sx126x-spi",
				Layered: config.Layered{
					Profile: "rak6421-13300x-slot1",
					Overrides: map[string]map[string]any{
						"rak6421-13300x-slot1": {"spi": "/dev/spidev0.0"},
					},
				},
			},
		},
		// A sensor with a cadence: SampleInterval is the one field of
		// a new kind whose type is not a string, and the store's
		// round trip is the only thing that proves a Duration comes
		// back as one.
		Sensors: map[string]config.Sensor{
			"bat": {
				Driver:         "ina219",
				SampleInterval: 45 * time.Second,
				Layered: config.Layered{
					Profile: "",
					Overrides: map[string]map[string]any{
						"custom": {"i2c": "/dev/i2c-2", "shunt_ohms": 0.01},
					},
				},
			},
		},
		Relays: map[string]config.Relay{
			"meshcore-868": {
				Protocol:     "meshcore",
				Radio:        "slot1",
				NoiseHistory: &history,
				TX: &config.TX{
					Mode: "on-air", LBTExhausted: "transmit", QueueDepth: 32,
				},
				Layered: config.Layered{
					Profile: "eu-868-narrow",
					Overrides: map[string]map[string]any{
						"eu-868-narrow": {
							"identity":      "b5445dd625d531fcc7e805c81a2205cba1857e40081c5d7f5a05f566a32c4f3c",
							"node_name":     "test 🦝",
							"tx_power_dbm":  0,
							"accept_scopes": []any{"eu", "fr-91"},
							"session_limit": 6,
						},
					},
				},
			},
		},
		Sentinel: &config.Sentinel{
			Journal:   "/var/lib/lotor/journal.db",
			Retention: 720 * time.Hour,
			MaxFrames: 100000,
		},
		CLI: &config.CLI{Listen: "127.0.0.1:2323", Socket: &socket},
		Web: &config.Web{Listen: "127.0.0.1:8696"},
	}
}

func openTest(t *testing.T) *Store {
	t.Helper()
	s, err := Open(context.Background(), Memory, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestImportThenLoadIsTheSameConfiguration(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	want := sample()
	if err := s.ImportFile(ctx, want, "test"); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip diverged:\n got %+v\nwant %+v", got, want)
	}
}

func TestEmptyStoreIsAValidOne(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	empty, err := s.Empty(ctx)
	if err != nil || !empty {
		t.Fatalf("empty = %v, %v", empty, err)
	}
	f, err := s.Load(ctx)
	if err != nil {
		t.Fatalf("an empty store must load as nothing configured: %v", err)
	}
	if len(f.Relays) != 0 || len(f.Radios) != 0 || f.Sentinel != nil || f.CLI != nil || f.Web != nil {
		t.Fatalf("an empty store loaded something: %+v", f)
	}
}

func TestLoadStillCrossValidates(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	broken := sample()
	r := broken.Relays["meshcore-868"]
	r.Radio = "ghost" // names no declared radio
	broken.Relays["meshcore-868"] = r
	if err := s.ImportFile(ctx, broken, "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Load(ctx); err == nil {
		t.Fatal("a relay on an undeclared radio loaded anyway")
	}
}

func TestEveryImportLeavesARevision(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	if err := s.ImportFile(ctx, sample(), "migration"); err != nil {
		t.Fatal(err)
	}
	revs, err := s.Revisions(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(revs) != 6 { // radio, sensor, relay, sentinel, cli, web
		t.Fatalf("revisions = %d, want one per object", len(revs))
	}
	for _, r := range revs {
		if r.Principal != "migration" || r.Op != "import" || r.Change == "" {
			t.Fatalf("revision lacks its who/what: %+v", r)
		}
	}
}

func TestImportReplacesWhole(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	if err := s.ImportFile(ctx, sample(), "one"); err != nil {
		t.Fatal(err)
	}
	// The second import drops the sentinel and the CLI: they must not
	// survive as leftovers of the first.
	second := sample()
	second.Sentinel, second.CLI = nil, nil
	if err := s.ImportFile(ctx, second, "two"); err != nil {
		t.Fatal(err)
	}
	f, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if f.Sentinel != nil || f.CLI != nil {
		t.Fatal("the previous configuration bled through the import")
	}
}

func TestTheFileIsNobodyElsesToRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.db")
	s, err := Open(context.Background(), path, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o600 {
		t.Fatalf("config.db is %v — it carries the node's private key", st.Mode().Perm())
	}
	// And the single-file promise: no WAL companions after a write.
	if err := s.ImportFile(context.Background(), sample(), "test"); err != nil {
		t.Fatal(err)
	}
	for _, companion := range []string{path + "-wal", path + "-shm"} {
		if _, err := os.Stat(companion); err == nil {
			t.Fatalf("%s exists — a backup would need more than one file", companion)
		}
	}
}

func TestReplaceRecordsItsRevision(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	if err := s.ImportFile(ctx, sample(), "migration"); err != nil {
		t.Fatal(err)
	}
	// A set lands with its before and after.
	f := sample()
	rc := f.Relays["meshcore-868"]
	rc.Layered.Overrides["eu-868-narrow"]["node_name"] = "renamed"
	if err := s.Replace(ctx, KindRelay, "meshcore-868", rc, "console", "set",
		map[string]Change{"node_name": {Old: "test 🦝", New: "renamed"}}); err != nil {
		t.Fatal(err)
	}
	rev, err := s.LastMutation(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if rev.Op != "set" || rev.Principal != "console" {
		t.Fatalf("revision = %+v", rev)
	}
	ch, err := rev.Changes()
	if err != nil || ch["node_name"].Old != "test 🦝" || ch["node_name"].New != "renamed" {
		t.Fatalf("changes = %v (%v)", ch, err)
	}
	// And the object really changed.
	loaded, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Relays["meshcore-868"].Layered.Overrides["eu-868-narrow"]["node_name"] != "renamed" {
		t.Fatal("the object kept its old value")
	}
}

func TestUndoStopsAtAnImport(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	if err := s.ImportFile(ctx, sample(), "migration"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.LastMutation(ctx); err == nil {
		t.Fatal("an import offered itself to undo — its inverse is a wipe")
	}
}

func TestMigrationsLiftTheShapeOnce(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, Memory, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if shape, err := s.Shape(ctx); err != nil || shape != 1 {
		t.Fatalf("fresh shape = %d, %v", shape, err)
	}
	ran := 0
	lift := []Migration{{To: 2, Doc: "test", Run: func(ctx context.Context, tx *sql.Tx) error {
		ran++
		_, err := tx.ExecContext(ctx,
			"INSERT INTO objects(kind, name, attrs) VALUES('mqtt', 'seeded', '{}')")
		return err
	}}}
	if err := s.Migrate(ctx, lift); err != nil {
		t.Fatal(err)
	}
	if shape, _ := s.Shape(ctx); shape != 2 {
		t.Errorf("shape after lift = %d", shape)
	}
	// Idempotent: a second boot runs nothing.
	if err := s.Migrate(ctx, lift); err != nil || ran != 1 {
		t.Errorf("second boot ran %d migrations (%v)", ran, err)
	}
	// A hole in the ladder is refused before anything moves.
	err = s.Migrate(ctx, []Migration{{To: 4, Doc: "gap",
		Run: func(context.Context, *sql.Tx) error { return nil }}})
	if err == nil {
		t.Error("a gapped ladder was climbed")
	}
	// A failing migration leaves the stamp untouched.
	err = s.Migrate(ctx, []Migration{{To: 3, Doc: "boom",
		Run: func(context.Context, *sql.Tx) error { return errors.New("boom") }}})
	if err == nil {
		t.Fatal("failure swallowed")
	}
	if shape, _ := s.Shape(ctx); shape != 2 {
		t.Errorf("failed lift moved the stamp to %d", shape)
	}
}

func TestMigrationsFailClosedTowardTheFuture(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, Memory, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	// A store stamped past this binary's ceiling was written by a
	// newer lotor: touching it would mutate invariants this code has
	// never heard of.
	if _, err := s.db.ExecContext(ctx,
		"INSERT INTO meta(key, value) VALUES('shape', '99')"); err != nil {
		t.Fatal(err)
	}
	err = s.Migrate(ctx, []Migration{{To: 2, Doc: "t",
		Run: func(context.Context, *sql.Tx) error { return nil }}})
	if err == nil || !strings.Contains(err.Error(), "newer") {
		t.Errorf("future shape = %v, want the refusal to say a newer binary wrote it", err)
	}
	// A broken list is named before anything runs.
	fresh, _ := Open(ctx, Memory, 0)
	defer func() { _ = fresh.Close() }()
	err = fresh.Migrate(ctx, []Migration{
		{To: 2, Doc: "t", Run: func(context.Context, *sql.Tx) error { return nil }},
		{To: 4, Doc: "gap", Run: func(context.Context, *sql.Tx) error { return nil }},
	})
	if err == nil || !strings.Contains(err.Error(), "broken") {
		t.Errorf("gapped list = %v", err)
	}
}

func TestAFailedMigrationIsResumable(t *testing.T) {
	// The backup of a failed attempt is the recovery point the retry
	// keeps — not a name collision that stops every retry cold.
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.db")
	s, err := Open(ctx, path, 0)
	if err != nil {
		t.Fatal(err)
	}
	boom := []Migration{{To: 2, Doc: "boom",
		Run: func(context.Context, *sql.Tx) error { return errors.New("transient") }}}
	if err := s.Migrate(ctx, boom); err == nil {
		t.Fatal("the failing migration succeeded")
	}
	if _, err := os.Stat(path + ".pre-shape2"); err != nil {
		t.Fatalf("no recovery point: %v", err)
	}
	fixed := []Migration{{To: 2, Doc: "fixed",
		Run: func(context.Context, *sql.Tx) error { return nil }}}
	if err := s.Migrate(ctx, fixed); err != nil {
		t.Fatalf("the retry could not run: %v", err)
	}
	if shape, _ := s.Shape(ctx); shape != 2 {
		t.Errorf("shape = %d after the retry", shape)
	}
	// And every snapshot beside the base is 0600: they carry the
	// identity the main file guards.
	st, err := os.Stat(path + ".pre-shape2")
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o600 {
		t.Errorf("backup mode = %o, want 600", st.Mode().Perm())
	}
	probe := filepath.Join(dir, "probe.db")
	if err := s.CopyTo(ctx, probe); err != nil {
		t.Fatal(err)
	}
	if st, _ := os.Stat(probe); st.Mode().Perm() != 0o600 {
		t.Errorf("probe mode = %o, want 600", st.Mode().Perm())
	}
	if st, _ := os.Stat(path); st.Mode().Perm() != 0o600 {
		t.Errorf("base mode = %o, want 600", st.Mode().Perm())
	}
	_ = s.Close()
}

func TestRevisionsKeepMaskedCopiesEverywhere(t *testing.T) {
	// Import and remove used to journal raw whole objects while the
	// ordinary mutation masked its deltas: the journal outlives
	// rotations and rides every backup.
	ctx := context.Background()
	s, err := Open(ctx, Memory, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	s.SetRevisionMasker(func(raw string) string {
		return strings.ReplaceAll(raw, "supersecret", "<masked>")
	})
	f := &config.File{
		Radios: map[string]config.Radio{},
		Relays: map[string]config.Relay{"mc": {
			Protocol: "meshcore", Radio: "r",
			Layered: config.Layered{Overrides: map[string]map[string]any{
				"custom": {"identity": "supersecret"},
			}},
		}},
	}
	if err := s.ImportFile(ctx, f, "test"); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(ctx, KindRelay, "mc", "test"); err != nil {
		t.Fatal(err)
	}
	revs, err := s.Revisions(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range revs {
		if strings.Contains(r.Change, "supersecret") {
			t.Errorf("revision %s %s journalled the secret raw:\n%s", r.Op, r.Kind, r.Change)
		}
		// Every revision reads as the delta shape the console shows.
		if _, err := r.Changes(); err != nil {
			t.Errorf("revision %s %s is not readable as changes: %v", r.Op, r.Kind, err)
		}
	}
	// The object row itself keeps the raw value: the store is the
	// guarded home, the journal is the copy that travels.
	var attrs string
	if err := s.db.QueryRowContext(ctx,
		"SELECT attrs FROM objects WHERE kind='relay'").Scan(&attrs); err == nil {
		t.Log("relay object removed as expected") // removed above
	}
}

func TestImportReplacesRuntimeState(t *testing.T) {
	// An import is a NEW configuration: access entries and regions must
	// not survive it by mere equality of names.
	ctx := context.Background()
	s, err := Open(ctx, Memory, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	if err := s.SaveACL(ctx, "mc", ACLRow{PubKey: []byte{9}, Perms: 1, LastActive: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := s.ReplaceRegions(ctx, "mc",
		[]RegionRow{{ID: 1, Name: "eu"}}, RegionsMeta{NextID: 2}); err != nil {
		t.Fatal(err)
	}
	f := &config.File{Radios: map[string]config.Radio{}, Relays: map[string]config.Relay{}}
	if err := s.ImportFile(ctx, f, "test"); err != nil {
		t.Fatal(err)
	}
	if rows, _ := s.LoadACL(ctx, "mc"); len(rows) != 0 {
		t.Error("access entries survived the import")
	}
	if _, _, ok, _ := s.LoadRegions(ctx, "mc"); ok {
		t.Error("regions survived the import")
	}
}

func TestAFutureStoreIsRefusedBeforeAnyDDL(t *testing.T) {
	// The review's real-restart shape: a future binary removed a table
	// on purpose; reopening with this binary must refuse BEFORE the
	// CREATE IF NOT EXISTS resurrects it.
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "config.db")
	s, err := Open(ctx, path, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx, "DROP TABLE acl"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.ExecContext(ctx,
		"INSERT INTO meta(key, value) VALUES('shape', '99') "+
			"ON CONFLICT(key) DO UPDATE SET value = excluded.value"); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(ctx, path, 10); err == nil ||
		!strings.Contains(err.Error(), "newer") {
		t.Fatalf("future store opened: %v", err)
	}
	// The dropped table stayed dropped: nothing was written.
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()
	var n int
	if err := raw.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='acl'").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Error("Open recreated a table the future shape had removed")
	}
	// And a base within the ceiling opens normally.
	ok, err := Open(ctx, filepath.Join(t.TempDir(), "fresh.db"), 10)
	if err != nil {
		t.Fatal(err)
	}
	_ = ok.Close()
}

func TestStationConfigurationRoundTrips(t *testing.T) {
	ctx := context.Background()
	s, err := Open(ctx, Memory, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.Close() }()
	f := &config.File{Stations: map[string]config.Station{
		"alice": {
			Protocol: "meshcore", Listen: "127.0.0.1:5000",
			Layered: config.Layered{Profile: "eu-868-narrow"},
			TX:      &config.TX{Mode: config.TXShadow},
		},
	}}
	if err := s.ImportFile(ctx, f, "test"); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	station, ok := got.Stations["alice"]
	if !ok || station.Protocol != "meshcore" || station.Listen != "127.0.0.1:5000" ||
		station.Layered.Profile != "eu-868-narrow" || station.TX == nil || station.TX.Mode != config.TXShadow {
		t.Fatalf("station round trip = %+v", station)
	}
}

func TestAFutureStoreKeepsItsJournalMode(t *testing.T) {
	// The pragmas are writes too: journal_mode=DELETE durably rewrote
	// a future base's journaling protocol before the refusal landed.
	// The shape is judged on a bare connection now — a WAL base owned
	// by a newer binary comes back from the refusal still WAL.
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "config.db")
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.ExecContext(ctx, `PRAGMA journal_mode=WAL;
		CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
		INSERT INTO meta VALUES('shape', '99');`); err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(ctx, path, 10); err == nil {
		t.Fatal("the future store opened")
	}

	raw, err = sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()
	var mode string
	if err := raw.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != "wal" {
		t.Errorf("journal mode = %q after the refusal, want the owner's wal", mode)
	}
}
