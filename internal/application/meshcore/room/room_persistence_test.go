package room

import (
	"context"
	"errors"
	"testing"
	"time"

	"meshrunner.dev/lotor/internal/application"
	"meshrunner.dev/lotor/internal/confdb"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/meshcorehost"

	mesh "meshrunner.dev/pkg/meshcore"
)

type roomSessions map[[mesh.PubKeySize]byte]meshcorehost.PersistedSession

func (s roomSessions) LoadSessions() ([]meshcorehost.PersistedSession, error) {
	out := make([]meshcorehost.PersistedSession, 0, len(s))
	for _, entry := range s {
		out = append(out, entry)
	}
	return out, nil
}

func (s roomSessions) SaveSession(entry meshcorehost.PersistedSession) error {
	s[entry.PubKey] = entry
	return nil
}

func (s roomSessions) ForgetSession(key [mesh.PubKeySize]byte) error {
	delete(s, key)
	return nil
}

func (s roomSessions) ReplaceSession(key [mesh.PubKeySize]byte, entry *meshcorehost.PersistedSession) error {
	delete(s, key)
	if entry != nil {
		s[entry.PubKey] = *entry
	}
	return nil
}

func persistentRoomBuilder(t *testing.T, db *confdb.Store) func() *service {
	t.Helper()
	cfg := baseConfig()
	cfg["guest_password"] = "welcome"
	cfg["history"] = 1
	cfg["advert_local_interval"], cfg["advert_flood_interval"] = "0s", "0s"
	spec := application.Spec{Name: "lobby", Config: cfg, Store: db, Sessions: roomSessions{},
		TX: application.TXPolicy{Mode: config.TXShadow}}
	return func() *service {
		t.Helper()
		built, err := build(spec)
		if err != nil {
			t.Fatal(err)
		}
		svc, ok := built.(*service)
		if !ok {
			t.Fatalf("build returned a %T", built)
		}
		svc.delays.response, svc.delays.ack = 0, 0
		return svc
	}
}

func TestAKeptPostRetrySurvivesRebuildAndHistoryPruning(t *testing.T) {
	for _, prune := range []bool{false, true} {
		t.Run(map[bool]string{false: "retained", true: "pruned"}[prune], func(t *testing.T) {
			db := memoryStore(t)
			makeRoom := persistentRoomBuilder(t, db)
			first := makeRoom()
			alice, bob := newClient(t, first), newClient(t, first)
			login(t, first, alice, "welcome", 0)
			login(t, first, bob, "welcome", 0)
			stamp := time.Now()
			if ack, _ := sendPost(t, first, alice, "only once", stamp); ack == nil {
				t.Fatal("initial post rejected")
			}
			want := "only once"
			if prune {
				want = "newer post"
				if ack, _ := sendPost(t, first, bob, want, stamp.Add(time.Second)); ack == nil {
					t.Fatal("replacement post rejected")
				}
			}
			before, err := db.LoadRoomPosts(t.Context(), "lobby")
			if err != nil || len(before) != 1 {
				t.Fatalf("initial history = %+v, %v", before, err)
			}
			rebuilt := makeRoom()
			ack, plain := sendPostAttempt(t, rebuilt, alice, "only once", stamp, 1)
			if ack == nil {
				t.Fatal("retry rejected after rebuild")
			}
			wantACK, err := mesh.BuildCommandAck(plain, alice.id.PubKey[:])
			if err != nil || string(ack.Payload) != string(wantACK.Payload) {
				t.Fatalf("retry ACK does not acknowledge the retry plaintext: %v", err)
			}
			posts, err := db.LoadRoomPosts(t.Context(), "lobby")
			if err != nil || len(posts) != 1 || posts[0] != before[0] || posts[0].Text != want {
				t.Fatalf("retry rewrote history: %+v, %v", posts, err)
			}
			if rebuilt.posted != 0 {
				t.Fatal("retry counted as a new post")
			}
		})
	}
}

type refusingRoomStore struct{ *confdb.Store }

func (refusingRoomStore) SaveRoomPost(context.Context, string, confdb.RoomPost, int) (int64, error) {
	return 0, errors.New("disk write refused")
}

func TestARefusedPostRetryIsStoredAfterRebuild(t *testing.T) {
	db := memoryStore(t)
	makeRoom := persistentRoomBuilder(t, db)
	first := makeRoom()
	alice := newClient(t, first)
	login(t, first, alice, "welcome", 0)
	stamp := time.Now()
	if ack, _ := sendPost(t, first, alice, "kept", stamp); ack == nil {
		t.Fatal("initial post rejected")
	}
	first.store = refusingRoomStore{db}
	stamp = stamp.Add(time.Second)
	if ack, _ := sendPost(t, first, alice, "retry me", stamp); ack != nil {
		t.Fatal("failed write earned an ACK")
	}
	// Its replay guard was persisted, but only the older post earned a
	// receipt. Rebuilding must judge this retry afresh and keep it.
	rebuilt := makeRoom()
	if ack, _ := sendPostAttempt(t, rebuilt, alice, "retry me", stamp, 1); ack == nil {
		t.Fatal("retry rejected after the store recovered")
	}
	posts, err := db.LoadRoomPosts(t.Context(), "lobby")
	if err != nil || len(posts) != 1 || posts[0].Text != "retry me" || rebuilt.posted != 1 {
		t.Fatalf("retry acknowledged without storing the refused post: %+v, %v", posts, err)
	}
}
