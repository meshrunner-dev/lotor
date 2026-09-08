package main

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"meshrunner.dev/lotor/internal/confdb"
)

func TestRoomReceiptMigrationPreservesHistoryWithoutInventingClientTimes(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "config.db")
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = raw.Close() }()
	tx, err := raw.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	// Shape 15 is exercised as shipped: its history has room-clock
	// timestamps, with no record of the client's acceptance timestamp.
	if err := roomTablesMigration().Run(ctx, tx); err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{
		`CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
		`INSERT INTO meta(key, value) VALUES('shape', '15')`,
		`INSERT INTO room_posts(app, seq, at, author, text, corr)
		 VALUES('lobby', 1, 100, zeroblob(32), 'before', 'old-corr')`,
		`INSERT INTO room_cursors(app, pubkey, sync_since, updated)
		 VALUES('lobby', zeroblob(32), 99, '2026-09-08T00:00:00Z')`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	store, err := confdb.Open(ctx, path, shapeCeiling())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	// Open also installs fresh-store DDL. Remove just the new table so
	// this test proves migration 16 itself can install the old store's
	// missing table, rather than borrowing that work from Open.
	if _, err := raw.ExecContext(ctx, "DROP TABLE room_receipts"); err != nil {
		t.Fatal(err)
	}
	if err := store.Migrate(ctx, storeMigrations()); err != nil {
		t.Fatal(err)
	}
	if shape, err := store.Shape(ctx); err != nil || shape != shapeCeiling() {
		t.Fatalf("shape = %d, %v; want %d", shape, err, shapeCeiling())
	}
	posts, err := store.LoadRoomPosts(ctx, "lobby")
	if err != nil || len(posts) != 1 || posts[0].At != 100 || posts[0].Text != "before" {
		t.Fatalf("migration changed the history: %+v, %v", posts, err)
	}
	cursors, err := store.LoadRoomCursors(ctx, "lobby")
	if err != nil || len(cursors) != 1 || cursors[0].SyncSince != 99 {
		t.Fatalf("migration changed the cursors: %+v, %v", cursors, err)
	}
	if receipts, err := store.LoadRoomReceipts(ctx, "lobby"); err != nil || len(receipts) != 0 {
		t.Fatalf("migration invented client timestamps: %+v, %v", receipts, err)
	}
	author := [32]byte{1}
	if _, err := store.SaveRoomPost(ctx, "lobby", confdb.RoomPost{
		At: 101, Author: author, Text: "after", ClientTimestamp: 900,
	}, 1); err != nil {
		t.Fatal(err)
	}
	receipts, err := store.LoadRoomReceipts(ctx, "lobby")
	if err != nil || len(receipts) != 1 || receipts[0].Author != author || receipts[0].ClientTimestamp != 900 {
		t.Fatalf("migrated store receipt = %+v, %v", receipts, err)
	}
}
