package main

import (
	"database/sql"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"meshrunner.dev/lotor/internal/confdb"
	"meshrunner.dev/lotor/internal/meshcorehost"
	mesh "meshrunner.dev/pkg/meshcore"
)

// A second connection installs a failing trigger without changing the
// production adapter: DELETE succeeds, then the newcomer's INSERT fails.
func membershipStore(t *testing.T) (*confdb.Store, *aclStore, *sql.DB) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.db")
	db, err := confdb.Open(t.Context(), path, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	inject, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inject.Close() })
	m := &manager{store: db}
	store, ok := m.applicationSessions("lobby").(*aclStore)
	if !ok {
		t.Fatal("the application adapter is not an ACL store")
	}
	return db, store, inject
}

func TestRoomReplacementKeepsEveryDurableFieldOnFailure(t *testing.T) {
	db, store, inject := membershipStore(t)
	ctx := t.Context()
	table := meshcorehost.NewTable(store, 1)
	table.Spare = func(_, old *meshcorehost.Client) bool { return old.IsAdmin() }
	victim := &meshcorehost.Client{
		PubKey: [mesh.PubKeySize]byte{1}, Perms: mesh.PermReadWrite,
		LastTimestamp: 100, LastActive: time.Now(),
	}
	if err := table.Put(victim); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveRoomCursor(ctx, "lobby", confdb.RoomCursor{PubKey: victim.PubKey, SyncSince: 90}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.SaveRoomPost(ctx, "lobby", confdb.RoomPost{
		At: 90, Author: victim.PubKey, Text: "kept", ClientTimestamp: 99,
	}, 10); err != nil {
		t.Fatal(err)
	}
	// The same key's state in another room must survive both outcomes.
	if err := db.SaveRoomCursor(ctx, "other", confdb.RoomCursor{PubKey: victim.PubKey, SyncSince: 80}); err != nil {
		t.Fatal(err)
	}
	before, err := store.LoadSessions()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := inject.ExecContext(ctx,
		"CREATE TRIGGER refuse_acl_inserts BEFORE INSERT ON acl BEGIN SELECT RAISE(ABORT, 'write refused'); END"); err != nil {
		t.Fatal(err)
	}
	newcomer := &meshcorehost.Client{
		PubKey: [mesh.PubKeySize]byte{2}, Perms: mesh.PermReadWrite,
		LastTimestamp: 101, LastActive: time.Now(),
	}
	if _, _, err := table.PutEvicting(newcomer); err == nil {
		t.Fatal("replacement ignored the rejected insert")
	}
	if table.Live(victim.PubKey) != victim || table.Live(newcomer.PubKey) != nil {
		t.Fatal("a refused replacement changed the live members")
	}
	after, err := store.LoadSessions()
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatalf("a refused replacement changed durable members: %+v, %v", after, err)
	}
	cursors, err := db.LoadRoomCursors(ctx, "lobby")
	if err != nil || len(cursors) != 1 || cursors[0].SyncSince != 90 {
		t.Fatalf("a refused replacement changed the cursor: %+v, %v", cursors, err)
	}
	receipts, err := db.LoadRoomReceipts(ctx, "lobby")
	if err != nil || len(receipts) != 1 || receipts[0].ClientTimestamp != 99 {
		t.Fatalf("a refused replacement changed the receipt: %+v, %v", receipts, err)
	}
	if _, err := inject.ExecContext(ctx, "DROP TRIGGER refuse_acl_inserts"); err != nil {
		t.Fatal(err)
	}
	if _, evicted, err := table.PutEvicting(newcomer); err != nil || !evicted {
		t.Fatalf("replacement after repair = %v, %v", evicted, err)
	}
	after, err = store.LoadSessions()
	if err != nil || len(after) != 1 || after[0].PubKey != newcomer.PubKey {
		t.Fatalf("successful replacement left the wrong durable member: %+v, %v", after, err)
	}
	cursors, err = db.LoadRoomCursors(ctx, "lobby")
	if err != nil || len(cursors) != 0 {
		t.Fatalf("successful eviction retained its cursor: %+v, %v", cursors, err)
	}
	receipts, err = db.LoadRoomReceipts(ctx, "lobby")
	if err != nil || len(receipts) != 0 {
		t.Fatalf("successful eviction retained its receipt: %+v, %v", receipts, err)
	}
	posts, err := db.LoadRoomPosts(ctx, "lobby")
	if err != nil || len(posts) != 1 || posts[0].Text != "kept" {
		t.Fatalf("eviction changed room history: %+v, %v", posts, err)
	}
	cursors, err = db.LoadRoomCursors(ctx, "other")
	if err != nil || len(cursors) != 1 || cursors[0].SyncSince != 80 {
		t.Fatalf("eviction touched another room: %+v, %v", cursors, err)
	}
}

func TestGuestReplacementForgetsItsCursorWithoutSavingAccess(t *testing.T) {
	db, store, _ := membershipStore(t)
	table := meshcorehost.NewTable(store, 1)
	table.ForgetGuestState = true
	victim := &meshcorehost.Client{PubKey: [mesh.PubKeySize]byte{1}}
	if err := table.Put(victim); err != nil {
		t.Fatal(err)
	}
	if err := db.SaveRoomCursor(t.Context(), "lobby", confdb.RoomCursor{PubKey: victim.PubKey, SyncSince: 90}); err != nil {
		t.Fatal(err)
	}
	if _, evicted, err := table.PutEvicting(&meshcorehost.Client{PubKey: [mesh.PubKeySize]byte{2}}); err != nil || !evicted {
		t.Fatalf("guest replacement = %v, %v", evicted, err)
	}
	if cursors, err := db.LoadRoomCursors(t.Context(), "lobby"); err != nil || len(cursors) != 0 {
		t.Fatalf("guest replacement retained its cursor: %+v, %v", cursors, err)
	}
	if rows, err := store.LoadSessions(); err != nil || len(rows) != 0 {
		t.Fatalf("guest replacement created durable access: %+v, %v", rows, err)
	}
}
