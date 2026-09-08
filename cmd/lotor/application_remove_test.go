package main

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"meshrunner.dev/lotor/internal/application"
	"meshrunner.dev/lotor/internal/confdb"
)

// applicationWithFinalCursor replaces a real room's running loop with
// a deterministic final flush. The manager still owns the real config,
// service and lifecycle, so Remove must join this writer before its
// store cascade and must rebuild the room if the cascade is refused.
func applicationWithFinalCursor(t *testing.T, m *manager) <-chan error {
	// Keep the setup in the public creation path: rebuilding after a
	// failed removal must resolve and build exactly this stored config.
	t.Helper()
	if _, err := m.Create(context.Background(), confdb.KindApplication, "lobby",
		map[string]string{
			"protocol": "meshcore", "type": "meshcore-room", "profile": "eu-868-narrow",
			"identity": "new", "node_name": "Lobby", "guest_password": "welcome",
		}, "test"); err != nil {
		t.Fatal(err)
	}
	flushed := make(chan error, 1)
	m.mu.Lock()
	h := m.applications["lobby"]
	m.stopHost(applicationHosting, "lobby", &h.managedHost)
	m.runHost(m.ctx, applicationHosting, "lobby", &h.managedHost, func(ctx context.Context) error {
		<-ctx.Done()
		err := m.store.SaveRoomCursor(context.WithoutCancel(ctx), "lobby", confdb.RoomCursor{
			PubKey: [32]byte{1}, SyncSince: 100,
		})
		flushed <- err
		return err
	}, nil)
	m.mu.Unlock()
	return flushed
}

func TestRemovingApplicationJoinsItsFinalFlushBeforeDeletingState(t *testing.T) {
	m := lifecycleManager(t)
	flushed := applicationWithFinalCursor(t, m)
	if _, err := m.Remove(context.Background(), confdb.KindApplication, "lobby", "test"); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-flushed:
		if err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("removal returned without joining the application's final write")
	}
	cursors, err := m.store.LoadRoomCursors(context.Background(), "lobby")
	if err != nil || len(cursors) != 0 {
		t.Fatalf("the stopped application recreated orphan cursors: %+v, %v", cursors, err)
	}
	if _, exists := m.file.Applications["lobby"]; exists || len(m.ApplicationInfos()) != 0 {
		t.Fatal("the removed application remains configured or visible")
	}
}

func TestFailedApplicationRemovalRestartsItsOriginalConfiguration(t *testing.T) {
	m := lifecycleManager(t)
	flushed := applicationWithFinalCursor(t, m)
	before, err := cloneFile(m.file)
	if err != nil {
		t.Fatal(err)
	}
	previous := m.applications["lobby"].service
	identity := previous.Info().PublicKey
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.Remove(ctx, confdb.KindApplication, "lobby", "test"); !errors.Is(err, context.Canceled) {
		t.Fatalf("remove with cancelled storage context = %v", err)
	}
	select {
	case err := <-flushed:
		if err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("the failed removal left the previous application loop running")
	}
	stored, err := m.store.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(m.file.Applications, before.Applications) ||
		!reflect.DeepEqual(stored.Applications, before.Applications) {
		t.Fatal("a refused removal changed the application's configuration")
	}
	current := m.applications["lobby"]
	if current == nil || current.service == nil || current.service == previous {
		t.Fatal("the stopped application was not rebuilt after the refused removal")
	}
	deadline := time.Now().Add(time.Second)
	for {
		info := current.service.Info()
		if info.State == application.StateRunning {
			if info.PublicKey != identity {
				t.Fatal("the restarted application lost its identity")
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the application did not restart: %+v", info)
		}
		time.Sleep(time.Millisecond)
	}
	cursors, err := m.store.LoadRoomCursors(context.Background(), "lobby")
	if err != nil || len(cursors) != 1 || cursors[0].SyncSince != 100 {
		t.Fatalf("a refused removal lost the final cursor: %+v, %v", cursors, err)
	}
}
