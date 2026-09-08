package confdb

import (
	"context"
	"path/filepath"
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
	// A shortened memory prunes to the newest keep; a keep of zero
	// prunes nothing.
	if err := s.PruneRoomPosts(ctx, "lobby", 0); err != nil {
		t.Fatal(err)
	}
	if posts, _ := s.LoadRoomPosts(ctx, "lobby"); len(posts) != 3 {
		t.Errorf("a keep of zero pruned: %+v", posts)
	}
	if err := s.PruneRoomPosts(ctx, "lobby", 1); err != nil {
		t.Fatal(err)
	}
	if posts, _ := s.LoadRoomPosts(ctx, "lobby"); len(posts) != 1 || posts[0].Text != "four" {
		t.Errorf("pruned history = %+v, want the newest one", posts)
	}
	if err := s.ForgetRoomCursor(ctx, "lobby", alice); err != nil {
		t.Fatal(err)
	}
	if cursors, _ := s.LoadRoomCursors(ctx, "lobby"); len(cursors) != 0 {
		t.Errorf("a forgotten cursor remains: %+v", cursors)
	}
	if receipts, err := s.LoadRoomReceipts(ctx, "lobby"); err != nil || len(receipts) != 0 {
		t.Fatalf("posts without client timestamps made receipts: %+v, %v", receipts, err)
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
	if _, err := s.SaveRoomPost(ctx, "lobby", RoomPost{At: 1, Author: bob, Text: "hi", ClientTimestamp: 42}, 0); err != nil {
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
	receipts, _ := s.LoadRoomReceipts(ctx, "lobby")
	acl, _ := s.LoadACL(ctx, ApplicationOwner("lobby"))
	if len(posts) != 0 || len(cursors) != 0 || len(receipts) != 0 || len(acl) != 0 {
		t.Fatalf("the removal left %d posts, %d cursors, %d receipts, %d members", len(posts), len(cursors), len(receipts), len(acl))
	}
}

func TestRoomReceiptsSurvivePruningAndRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "config.db")
	s, err := Open(ctx, path, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if s != nil {
			_ = s.Close()
		}
	})
	alice, bob := [32]byte{1}, [32]byte{2}
	for i, author := range [][32]byte{alice, bob} {
		if _, err := s.SaveRoomPost(ctx, "lobby", RoomPost{
			At: uint32(100 + i), Author: author, Text: "kept", ClientTimestamp: uint32(500 + i),
		}, 1); err != nil {
			t.Fatal(err)
		}
	}
	// The post ring already lost Alice's post, but her receipt is the
	// proof her retry still needs after a restart.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = Open(ctx, path, 0)
	if err != nil {
		t.Fatal(err)
	}
	posts, err := s.LoadRoomPosts(ctx, "lobby")
	if err != nil || len(posts) != 1 || posts[0].Author != bob {
		t.Fatalf("history after restart = %+v, %v", posts, err)
	}
	receipts, err := s.LoadRoomReceipts(ctx, "lobby")
	if err != nil || len(receipts) != 2 {
		t.Fatalf("receipts after pruning and restart = %+v, %v", receipts, err)
	}
	for _, receipt := range receipts {
		want := uint32(500)
		if receipt.Author == bob {
			want++
		}
		if receipt.ClientTimestamp != want {
			t.Fatalf("receipt = %+v, want timestamp %d", receipt, want)
		}
	}
	// A post without a client request does not replace a receipt with
	// a fabricated zero timestamp.
	if _, err := s.SaveRoomPost(ctx, "lobby", RoomPost{At: 102, Author: alice, Text: "local"}, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.PruneRoomPosts(ctx, "lobby", 1); err != nil {
		t.Fatal(err)
	}
	if err := s.ForgetRoomReceipt(ctx, "lobby", bob); err != nil {
		t.Fatal(err)
	}
	receipts, err = s.LoadRoomReceipts(ctx, "lobby")
	if err != nil || len(receipts) != 1 || receipts[0] != (RoomReceipt{Author: alice, ClientTimestamp: 500}) {
		t.Fatalf("the remaining receipt = %+v, %v", receipts, err)
	}
	if other, err := s.LoadRoomReceipts(ctx, "annex"); err != nil || len(other) != 0 {
		t.Fatalf("another room inherited receipts: %+v, %v", other, err)
	}
}

func TestRoomPostAndReceiptCommitTogether(t *testing.T) {
	for _, failAt := range []struct{ name, trigger string }{
		{"post", "BEFORE INSERT ON room_posts"},
		{"receipt", "BEFORE INSERT ON room_receipts"},
		{"pruning", "BEFORE DELETE ON room_posts"},
	} {
		t.Run(failAt.name, func(t *testing.T) {
			s := openTest(t)
			ctx := context.Background()
			author := [32]byte{1}
			if _, err := s.SaveRoomPost(ctx, "lobby", RoomPost{
				At: 100, Author: author, Text: "accepted", ClientTimestamp: 200,
			}, 1); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.ExecContext(ctx, "CREATE TRIGGER refuse_room_write "+failAt.trigger+
				" BEGIN SELECT RAISE(ABORT, 'write refused'); END"); err != nil {
				t.Fatal(err)
			}
			if _, err := s.SaveRoomPost(ctx, "lobby", RoomPost{
				At: 101, Author: author, Text: "refused", ClientTimestamp: 201,
			}, 1); err == nil {
				t.Fatal("the injected write failure was ignored")
			}
			posts, err := s.LoadRoomPosts(ctx, "lobby")
			if err != nil || len(posts) != 1 || posts[0].Text != "accepted" || posts[0].Seq != 1 {
				t.Fatalf("a refused write changed the history: %+v, %v", posts, err)
			}
			receipts, err := s.LoadRoomReceipts(ctx, "lobby")
			if err != nil || len(receipts) != 1 || receipts[0].ClientTimestamp != 200 {
				t.Fatalf("a refused post earned a receipt: %+v, %v", receipts, err)
			}
		})
	}
}

func TestImportDiscardsRoomReceipts(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	for _, app := range []string{"lobby", "annex"} {
		if _, err := s.SaveRoomPost(ctx, app, RoomPost{
			At: 1, Author: [32]byte{1}, Text: "hi", ClientTimestamp: 42,
		}, 1); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.ImportFile(ctx, &config.File{}, "test"); err != nil {
		t.Fatal(err)
	}
	for _, app := range []string{"lobby", "annex"} {
		if receipts, err := s.LoadRoomReceipts(ctx, app); err != nil || len(receipts) != 0 {
			t.Fatalf("import kept %s receipts: %+v, %v", app, receipts, err)
		}
	}
}
