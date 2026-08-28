package meshcore

import (
	"testing"
	"time"

	"meshrunner.dev/pkg/meshcore"
)

// memStore is a SessionStore backed by a map, standing in for confdb.
type memStore struct {
	rows map[[meshcore.PubKeySize]byte]PersistedSession
}

func newMemStore() *memStore {
	return &memStore{rows: map[[meshcore.PubKeySize]byte]PersistedSession{}}
}
func (m *memStore) LoadSessions() ([]PersistedSession, error) {
	out := make([]PersistedSession, 0, len(m.rows))
	for _, p := range m.rows {
		out = append(out, p)
	}
	return out, nil
}
func (m *memStore) SaveSession(p PersistedSession) error            { m.rows[p.PubKey] = p; return nil }
func (m *memStore) ForgetSession(k [meshcore.PubKeySize]byte) error { delete(m.rows, k); return nil }

func TestSessionsSurviveABounce(t *testing.T) {
	store := newMemStore()

	// First engine: a companion logs in, teaches a route, advances.
	a := newACL(store)
	var key [meshcore.PubKeySize]byte
	for i := range key {
		key[i] = byte(i + 1)
	}
	c := &client{pubKey: key, secret: []byte("shared"), perms: permAdmin,
		lastTimestamp: 1000, lastActive: time.Now()}
	c.out = &outPath{pathLen: 2, path: []byte{0xaa, 0xbb}, learned: time.Now()}
	a.put(c)
	c.lastTimestamp = 1005
	a.save(c)

	// The store kept it.
	if len(store.rows) != 1 {
		t.Fatalf("store holds %d sessions", len(store.rows))
	}

	// Second engine: a fresh table loads from the same store, the
	// secret recomputed — here a stub that proves it was asked.
	b := newACL(store)
	asked := false
	b.load(func(pubKey []byte) ([]byte, error) {
		asked = true
		return []byte("recomputed"), nil
	})
	got := b.get(key[:])
	if got == nil {
		t.Fatal("the session did not survive the bounce")
	}
	if !asked {
		t.Error("the secret was read from disk, not recomputed")
	}
	if got.lastTimestamp != 1005 {
		t.Errorf("replay guard reset to %d", got.lastTimestamp)
	}
	if !got.isAdmin() {
		t.Error("the admin role was lost")
	}
	if got.out == nil || got.out.pathLen != 2 {
		t.Errorf("the route home was lost: %+v", got.out)
	}

	// The distinction that bit in flight: a zero-hop adjacent client
	// — empty path, still a route — must not reload as flood.
	var adj [meshcore.PubKeySize]byte
	adj[0] = 0x77
	ac := &client{pubKey: adj, secret: []byte("s"), perms: permGuest,
		lastTimestamp: 1, lastActive: time.Now()}
	ac.out = &outPath{pathLen: 0, path: nil, learned: time.Now()} // adjacent
	a.put(ac)
	e := newACL(store)
	e.load(func([]byte) ([]byte, error) { return []byte("x"), nil })
	got2 := e.get(adj[:])
	if got2 == nil || got2.out == nil {
		t.Fatalf("adjacent session reloaded as flood: %+v", got2)
	}
	if got2.out.pathLen != 0 {
		t.Errorf("adjacent route grew hops: %d", got2.out.pathLen)
	}
	if string(got.secret) != "recomputed" {
		t.Errorf("secret = %q, want the recomputed one", got.secret)
	}

	// A session gone stale on disk is not restored, and is forgotten.
	stale := PersistedSession{Perms: permGuest, LastActive: time.Now().Add(-2 * sessionIdle)}
	stale.PubKey[0] = 0xff
	store.SaveSession(stale)
	d := newACL(store)
	d.load(func([]byte) ([]byte, error) { return []byte("x"), nil })
	if d.get(stale.PubKey[:]) != nil {
		t.Error("a stale session was restored")
	}
	if _, held := store.rows[stale.PubKey]; held {
		t.Error("the stale session was not forgotten from the store")
	}
}
