package main

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	"meshrunner.dev/lotor/internal/confdb"
)

func TestACLDurabilityMigrationDropsGuestsAndConstrainsTheTable(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "config.db")
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
		`INSERT INTO meta(key, value) VALUES('shape', '12')`,
		`CREATE TABLE acl(
		   relay TEXT NOT NULL, pubkey BLOB NOT NULL, perms INTEGER NOT NULL,
		   last_timestamp INTEGER NOT NULL, out_path BLOB, out_path_len INTEGER,
		   learned TEXT, last_active TEXT NOT NULL,
		   granted INTEGER NOT NULL DEFAULT 0,
		   PRIMARY KEY(relay, pubkey))`,
		`INSERT INTO acl(relay, pubkey, perms, last_timestamp, last_active, granted)
		 VALUES('mc', x'01', 0, 10, '2026-08-30T00:00:00Z', 0),
		       ('mc', x'02', 1, 20, '2026-08-30T00:00:00Z', 1)`,
	} {
		if _, err := raw.ExecContext(ctx, stmt); err != nil {
			_ = raw.Close()
			t.Fatal(err)
		}
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}

	store, err := confdb.Open(ctx, path, shapeCeiling())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	if err := store.Migrate(ctx, storeMigrations()); err != nil {
		t.Fatal(err)
	}
	if shape, err := store.Shape(ctx); err != nil || shape != 13 {
		t.Fatalf("shape = %d, %v; want 13", shape, err)
	}
	rows, err := store.LoadACL(ctx, "mc")
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || len(rows[0].PubKey) != 1 || rows[0].PubKey[0] != 2 ||
		rows[0].Perms != 1 || !rows[0].Granted || rows[0].LastTimestamp != 20 {
		t.Fatalf("migration kept %+v, want only the read-only grant", rows)
	}
	if err := store.SaveACL(ctx, "mc", confdb.ACLRow{
		PubKey: []byte{3}, Perms: 0, LastActive: time.Now(),
	}); err == nil {
		t.Fatal("the migrated ACL accepted a new guest row")
	}
}
