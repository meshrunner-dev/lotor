package confdb

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
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
	}
}

func openTest(t *testing.T) *Store {
	t.Helper()
	s, err := Open(context.Background(), Memory)
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
	if len(f.Relays) != 0 || len(f.Radios) != 0 || f.Sentinel != nil || f.CLI != nil {
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
	if len(revs) != 4 { // radio, relay, sentinel, cli
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
	s, err := Open(context.Background(), path)
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
