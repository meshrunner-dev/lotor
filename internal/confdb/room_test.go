package confdb

import (
	"context"
	"testing"

	"meshrunner.dev/lotor/internal/config"
)

func TestRoomPostsKeepTheRingAndCursorsTheirMember(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	var alice [32]byte
	alice[0] = 0xA1
	for i, text := range []string{"one", "two", "three", "four"} {
		seq, err := s.SaveRoomPost(ctx, "lobby", RoomPost{At: uint32(100 + i), Author: alice, Text: text, Correlation: "c"}, 3)
		if err != nil || seq != int64(i+1) {
			t.Fatalf("save %q: seq %d, %v", text, seq, err)
		}
	}
	posts, err := s.LoadRoomPosts(ctx, "lobby")
	if err != nil {
		t.Fatal(err)
	}
	// Four saved, three kept: the oldest gave way, the order held.
	if len(posts) != 3 || posts[0].Text != "two" || posts[2].Text != "four" || posts[2].Seq != 4 ||
		posts[0].Author != alice || posts[0].At != 101 {
		t.Fatalf("history = %+v", posts)
	}
	// Another room's posts are another room's.
	if other, _ := s.LoadRoomPosts(ctx, "annex"); len(other) != 0 {
		t.Errorf("annex holds lobby's posts: %+v", other)
	}
	// Cursors: written, rewritten, read back per member.
	if err := s.SaveRoomCursor(ctx, "lobby", RoomCursor{PubKey: alice, SyncSince: 101}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveRoomCursor(ctx, "lobby", RoomCursor{PubKey: alice, SyncSince: 103}); err != nil {
		t.Fatal(err)
	}
	cursors, err := s.LoadRoomCursors(ctx, "lobby")
	if err != nil || len(cursors) != 1 || cursors[0].SyncSince != 103 || cursors[0].PubKey != alice {
		t.Fatalf("cursors = %+v, %v", cursors, err)
	}
	if err := s.ClearRoomHistory(ctx, "lobby"); err != nil {
		t.Fatal(err)
	}
	if posts, _ := s.LoadRoomPosts(ctx, "lobby"); len(posts) != 0 {
		t.Errorf("history survived the clear: %+v", posts)
	}
}

func TestRemovingAnApplicationTakesItsMemoryWithIt(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	f := &config.File{Applications: map[string]config.Application{
		"lobby": {Protocol: "meshcore", Type: "meshcore-room"},
	}}
	if err := s.ImportFile(ctx, f, "test"); err != nil {
		t.Fatal(err)
	}
	var bob [32]byte
	bob[0] = 0xB0
	if _, err := s.SaveRoomPost(ctx, "lobby", RoomPost{At: 1, Author: bob, Text: "hi"}, 0); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveRoomCursor(ctx, "lobby", RoomCursor{PubKey: bob, SyncSince: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveACL(ctx, ApplicationOwner("lobby"), ACLRow{PubKey: bob[:], Perms: 3}); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(ctx, KindApplication, "lobby", "test"); err != nil {
		t.Fatal(err)
	}
	posts, _ := s.LoadRoomPosts(ctx, "lobby")
	cursors, _ := s.LoadRoomCursors(ctx, "lobby")
	acl, _ := s.LoadACL(ctx, ApplicationOwner("lobby"))
	if len(posts) != 0 || len(cursors) != 0 || len(acl) != 0 {
		t.Fatalf("the removal left %d posts, %d cursors, %d members", len(posts), len(cursors), len(acl))
	}
}
