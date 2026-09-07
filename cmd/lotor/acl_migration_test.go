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
	// Lifted all the way to the newest shape this binary writes.
	if shape, err := store.Shape(ctx); err != nil || shape != shapeCeiling() {
		t.Fatalf("shape = %d, %v; want %d", shape, err, shapeCeiling())
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

	// The room tables the later migration adds are usable on the
	// migrated store, round trip: the DDL a shipped migration pins and
	// the DDL a fresh store creates must agree, and only use proves it.
	author := [32]byte{7}
	if _, err := store.SaveRoomPost(ctx, "lobby", confdb.RoomPost{At: 1, Author: author, Text: "hi", Correlation: "c"}, 8); err != nil {
		t.Fatal(err)
	}
	posts, err := store.LoadRoomPosts(ctx, "lobby")
	if err != nil || len(posts) != 1 || posts[0].Author != author || posts[0].Text != "hi" || posts[0].At != 1 {
		t.Fatalf("room posts after migration = %+v, %v", posts, err)
	}
	if err := store.SaveRoomCursor(ctx, "lobby", confdb.RoomCursor{PubKey: author, SyncSince: 5}); err != nil {
		t.Fatal(err)
	}
	cursors, err := store.LoadRoomCursors(ctx, "lobby")
	if err != nil || len(cursors) != 1 || cursors[0].PubKey != author || cursors[0].SyncSince != 5 {
		t.Fatalf("room cursors after migration = %+v, %v", cursors, err)
	}
	if err := store.ForgetRoomCursor(ctx, "lobby", author); err != nil {
		t.Fatal(err)
	}
	if cursors, err = store.LoadRoomCursors(ctx, "lobby"); err != nil || len(cursors) != 0 {
		t.Fatalf("a forgotten cursor remains: %+v, %v", cursors, err)
	}
}
