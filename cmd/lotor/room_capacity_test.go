package main

import (
	"context"
	"crypto/rand"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"meshrunner.dev/lotor/internal/confdb"
	"meshrunner.dev/lotor/internal/meshcorehost"
	mesh "meshrunner.dev/pkg/meshcore"
)

func TestRoomCapacityReductionRefusesToLoseAdministrators(t *testing.T) {
	m := lifecycleManager(t)
	ctx := context.Background()
	if _, err := m.Create(ctx, confdb.KindApplication, "lobby", map[string]string{
		"protocol": "meshcore", "type": "meshcore-room", "profile": "eu-868-narrow",
		"identity": "new", "node_name": "Lobby", "max_members": "3",
	}, "test"); err != nil {
		t.Fatal(err)
	}
	store := m.applicationSessions("lobby")
	for i := byte(1); i <= 2; i++ {
		id, err := mesh.NewLocalIdentity(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.SaveSession(meshcorehost.PersistedSession{
			PubKey: id.PubKey, Perms: mesh.PermAdmin,
			LastTimestamp: uint32(i), LastActive: time.Now().Add(-time.Hour),
		}); err != nil {
			t.Fatal(err)
		}
	}
	before, err := m.store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	previous := m.applications["lobby"].service
	if _, err := m.Mutate(ctx, confdb.KindApplication, "lobby", map[string]string{"max_members": "1"}, nil, "test"); err == nil || !strings.Contains(err.Error(), "2 existing administrators") {
		t.Fatalf("unsafe capacity edit = %v", err)
	}
	after, err := m.store.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(after.Applications, before.Applications) ||
		!reflect.DeepEqual(m.file.Applications, before.Applications) || m.applications["lobby"].service != previous {
		t.Fatal("a refused capacity edit changed configuration or restarted the room")
	}
	// Capacity may still be reduced to the protected population. A
	// fresher non-admin must not displace either admin during reload.
	if err := store.SaveSession(meshcorehost.PersistedSession{
		PubKey: [mesh.PubKeySize]byte{3}, Perms: mesh.PermReadWrite, LastActive: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.Mutate(ctx, confdb.KindApplication, "lobby", map[string]string{"max_members": "2"}, nil, "test"); err != nil {
		t.Fatal(err)
	}
	current := m.applications["lobby"].service
	if current == nil || current == previous {
		t.Fatal("the valid capacity edit did not rebuild the room")
	}
	if info := current.Info(); info.Summary["members"] != "2" {
		t.Fatalf("capacity reduction did not restore the protected population: %+v", info)
	}
}

func TestRoomCapacityEditRechecksAfterJoiningWriters(t *testing.T) {
	for _, tc := range []struct {
		name     string
		capacity string
		cancel   bool
	}{
		{name: "admin admitted during preflight", capacity: "1"},
		{name: "configuration commit refused", capacity: "2", cancel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := lifecycleManager(t)
			ctx := context.Background()
			if _, err := m.Create(ctx, confdb.KindApplication, "lobby", map[string]string{
				"protocol": "meshcore", "type": "meshcore-room", "profile": "eu-868-narrow",
				"identity": "new", "node_name": "Lobby", "max_members": "3",
			}, "test"); err != nil {
				t.Fatal(err)
			}
			store := m.applicationSessions("lobby")
			admins := make([]meshcorehost.PersistedSession, 2)
			for i := range admins {
				id, err := mesh.NewLocalIdentity(rand.Reader)
				if err != nil {
					t.Fatal(err)
				}
				admins[i] = meshcorehost.PersistedSession{PubKey: id.PubKey, Perms: mesh.PermAdmin,
					LastTimestamp: uint32(i + 1), LastActive: time.Now()}
			}
			if err := store.SaveSession(admins[0]); err != nil {
				t.Fatal(err)
			}
			// Complete a login at the end of the old owner's turn. It
			// wasn't in the first preflight's snapshot, but is durable
			// before Run exits and the manager can commit the reduction.
			flushed := make(chan error, 1)
			m.mu.Lock()
			h := m.applications["lobby"]
			m.stopHost(applicationHosting, "lobby", &h.managedHost)
			m.runHost(m.ctx, applicationHosting, "lobby", &h.managedHost, func(ctx context.Context) error {
				<-ctx.Done()
				err := store.SaveSession(admins[1])
				flushed <- err
				return err
			}, nil)
			m.mu.Unlock()
			previous := h.service
			before, err := m.store.Load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			requestCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			if tc.cancel {
				cancel()
			}
			_, err = m.Mutate(requestCtx, confdb.KindApplication, "lobby",
				map[string]string{"max_members": tc.capacity}, nil, "test")
			if tc.cancel {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled commit = %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), "2 existing administrators") {
				t.Fatalf("the newly admitted administrator did not prevent the reduction: %v", err)
			}
			select {
			case err := <-flushed:
				if err != nil {
					t.Fatal(err)
				}
			default:
				t.Fatal("capacity edit returned without joining the old owner's final write")
			}
			after, err := m.store.Load(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(after.Applications, before.Applications) ||
				!reflect.DeepEqual(m.file.Applications, before.Applications) {
				t.Fatal("a refused capacity edit changed configuration")
			}
			current := m.applications["lobby"].service
			if current == nil || current == previous || current.Info().Summary["members"] != "2" {
				t.Fatal("a refused capacity edit did not rebuild the retained room with both administrators")
			}
		})
	}
}
