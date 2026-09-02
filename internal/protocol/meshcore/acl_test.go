package meshcore

import (
	"errors"
	"testing"
	"time"

	"meshrunner.dev/pkg/meshcore"
)

// memStore is a SessionStore backed by a map, standing in for confdb.
// Only durable access entries should ever be handed to it.
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

func TestAccessEntriesSurviveABounce(t *testing.T) {
	store := newMemStore()

	// First engine: an administrator has an access entry, teaches a
	// route and advances its replay guard.
	a := newACL(store)
	var key [meshcore.PubKeySize]byte
	for i := range key {
		key[i] = byte(i + 1)
	}
	c := &client{PubKey: key, Secret: []byte("shared"), Perms: permAdmin,
		LastTimestamp: 1000, LastActive: time.Now()}
	c.Out = &outPath{PathLen: 2, Path: []byte{0xaa, 0xbb}, Learned: time.Now()}
	a.Put(c)
	c.LastTimestamp = 1005
	a.Save(c)

	// The store kept it.
	if len(store.rows) != 1 {
		t.Fatalf("store holds %d sessions", len(store.rows))
	}

	// Second engine: a fresh table loads from the same store, the
	// secret recomputed — here a stub that proves it was asked.
	b := newACL(store)
	asked := false
	b.Load(func(pubKey []byte) ([]byte, error) {
		asked = true
		return []byte("recomputed"), nil
	}, func() rateLimiter { return rateLimiter{Max: 8, Window: time.Minute} })
	got := b.Get(key[:])
	if got == nil {
		t.Fatal("the session did not survive the bounce")
	}
	if !asked {
		t.Error("the secret was read from disk, not recomputed")
	}
	if got.LastTimestamp != 1005 {
		t.Errorf("replay guard reset to %d", got.LastTimestamp)
	}
	if !got.IsAdmin() {
		t.Error("the admin role was lost")
	}
	if got.Out == nil || got.Out.PathLen != 2 {
		t.Errorf("the route home was lost: %+v", got.Out)
	}
	if sessions := b.Sessions(); len(sessions) != 0 {
		t.Fatalf("restored access appeared as live traffic: %+v", sessions)
	}

	// The distinction that bit in flight: a zero-hop adjacent
	// read-only principal — empty path, still a route — must not reload
	// as flood.
	var adj [meshcore.PubKeySize]byte
	adj[0] = 0x77
	ac := &client{PubKey: adj, Secret: []byte("s"), Perms: permReadOnly, Granted: true,
		LastTimestamp: 1, LastActive: time.Now()}
	ac.Out = &outPath{PathLen: 0, Path: nil, Learned: time.Now()} // adjacent
	a.Put(ac)
	e := newACL(store)
	e.Load(func([]byte) ([]byte, error) { return []byte("x"), nil },
		func() rateLimiter { return rateLimiter{Max: 8, Window: time.Minute} })
	got2 := e.Get(adj[:])
	if got2 == nil || got2.Out == nil {
		t.Fatalf("adjacent session reloaded as flood: %+v", got2)
	}
	if got2.Out.PathLen != 0 {
		t.Errorf("adjacent route grew hops: %d", got2.Out.PathLen)
	}
	if string(got.Secret) != "recomputed" {
		t.Errorf("secret = %q, want the recomputed one", got.Secret)
	}

	// A legacy guest row is not an access entry. Loading cleans it up
	// instead of resurrecting its ephemeral session.
	legacy := PersistedSession{Perms: permGuest, LastActive: time.Now()}
	legacy.PubKey[0] = 0xff
	store.SaveSession(legacy)
	d := newACL(store)
	d.Load(func([]byte) ([]byte, error) { return []byte("x"), nil },
		func() rateLimiter { return rateLimiter{Max: 8, Window: time.Minute} })
	if d.Get(legacy.PubKey[:]) != nil {
		t.Error("a legacy guest was restored into the access list")
	}
	if _, held := store.rows[legacy.PubKey]; held {
		t.Error("the legacy guest was not removed from the store")
	}
}

func TestGuestSessionIsMemoryOnly(t *testing.T) {
	// A guest exists in the live session table, but neither SQLite nor
	// the access-list views may learn it. A restart therefore requires
	// a fresh login.
	store := newMemStore()
	e, dev, sub, peer := txRig(t, "on-air")
	e.p.GuestAccess, e.p.GuestPassword = guestPassword, "raccoon"
	e.AttachSessions(store)
	runEngine(t, e, dev)
	frame, secret := login(t, e.id, peer, nowTS(100), "raccoon", false)
	dev.frames <- frame
	if sent := awaitSent(t, sub); sent.Kind != "login-resp" {
		t.Fatalf("sent = %+v", sent)
	}
	<-dev.sent
	if len(store.rows) != 0 {
		t.Fatalf("guest login persisted %d ACL rows", len(store.rows))
	}
	access, err := e.AccessList()
	if err != nil {
		t.Fatal(err)
	}
	if len(access) != 0 {
		t.Fatalf("guest appeared in access list: %+v", access)
	}
	sessions, err := e.ClientSessions()
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 1 || sessions[0].PubKey != peer.PubKey ||
		sessions[0].Perms&permRoleMask != permGuest {
		t.Fatalf("guest missing from sessions: %+v", sessions)
	}

	restarted := newACL(store)
	if err := restarted.Load(func([]byte) ([]byte, error) { return secret, nil },
		func() rateLimiter { return rateLimiter{Max: 8, Window: time.Minute} }); err != nil {
		t.Fatal(err)
	}
	if restarted.Get(peer.PubKey[:]) != nil {
		t.Fatal("guest session survived a restart")
	}
}

func TestClosingASessionPreservesOnlyDurableAccess(t *testing.T) {
	store := newMemStore()
	a := newACL(store)
	now := time.Now()

	guest := &client{PubKey: aclKey(0x11), Perms: permGuest, Active: true, LastActive: now}
	if err := a.Put(guest); err != nil {
		t.Fatal(err)
	}
	if err := a.CloseSession(guest.PubKey); err != nil {
		t.Fatal(err)
	}
	if a.Get(guest.PubKey[:]) != nil {
		t.Fatal("closing a guest left its memory-only session behind")
	}

	granted := &client{
		PubKey: aclKey(0x22), Secret: []byte("shared"), Perms: permReadOnly,
		Granted: true, Active: true, LastTimestamp: 42, LastActive: now,
	}
	granted.Out = &outPath{PathLen: 1, Path: []byte{0xaa}, Learned: now}
	if err := a.Put(granted); err != nil {
		t.Fatal(err)
	}
	if err := a.CloseSession(granted.PubKey); err != nil {
		t.Fatal(err)
	}
	kept := a.Get(granted.PubKey[:])
	if kept == nil || !kept.HasAccess() || kept.Active || !kept.Closed {
		t.Fatalf("close damaged the durable role or left it live: %+v", kept)
	}
	if kept.Out != nil {
		t.Fatal("close kept a route belonging to the old conversation")
	}
	if kept.LastTimestamp != 42 {
		t.Fatalf("close reset replay guard to %d", kept.LastTimestamp)
	}
	if len(a.Entries()) != 1 || len(a.Sessions()) != 0 {
		t.Fatalf("views after close: access=%+v sessions=%+v", a.Entries(), a.Sessions())
	}
	if len(a.Matching(granted.PubKey[0])) != 0 {
		t.Fatal("ordinary authenticated traffic reopened an explicitly closed session")
	}
	persisted := store.rows[granted.PubKey]
	if !persisted.Granted || persisted.Perms != permReadOnly || persisted.HasOut {
		t.Fatalf("persisted ACL after close = %+v", persisted)
	}
	if err := a.CloseSession(granted.PubKey); !errors.Is(err, ErrNoSuchSession) {
		t.Fatalf("closing an already closed session = %v", err)
	}
}

func TestAClosedACLSessionReopensOnlyByLogin(t *testing.T) {
	e, dev, sub, peer := txRig(t, "on-air")
	if err := e.AttachSessions(newMemStore()); err != nil {
		t.Fatal(err)
	}
	runEngine(t, e, dev)
	if err := e.Grant(peer.PubKey[:], permReadOnly); err != nil {
		t.Fatal(err)
	}

	frame, _ := login(t, e.id, peer, nowTS(200), "", false)
	dev.frames <- frame
	if sent := awaitSent(t, sub); sent.Kind != "login-resp" {
		t.Fatalf("first login = %+v", sent)
	}
	<-dev.sent
	if err := e.CloseSession(peer.PubKey[:]); err != nil {
		t.Fatal(err)
	}
	if sessions, err := e.ClientSessions(); err != nil || len(sessions) != 0 {
		t.Fatalf("closed session still visible: %+v, %v", sessions, err)
	}
	if access, err := e.AccessList(); err != nil || len(access) != 1 || access[0].Perms != permReadOnly {
		t.Fatalf("close revoked durable access: %+v, %v", access, err)
	}

	// The durable role makes the blank-password recheck possible, and
	// that explicit login is what opens a new conversation.
	frame, _ = login(t, e.id, peer, nowTS(201), "", false)
	dev.frames <- frame
	if sent := awaitSent(t, sub); sent.Kind != "login-resp" {
		t.Fatalf("relogin = %+v", sent)
	}
	<-dev.sent
	if sessions, err := e.ClientSessions(); err != nil || len(sessions) != 1 {
		t.Fatalf("fresh login did not reopen the session: %+v, %v", sessions, err)
	}
}

func TestAGrantOutlivesIdleAndIsReachable(t *testing.T) {
	// setperm's contract: a granted admin stays one whether or not it
	// is talking, and can be reached before it ever logs in — the
	// secret is computed at the grant.
	e, dev, sub, peer := txRig(t, "on-air")
	store := newMemStore()
	e.AttachSessions(store)
	runEngine(t, e, dev)

	if err := e.Grant(peer.PubKey[:], permAdmin); err != nil {
		t.Fatalf("grant: %v", err)
	}
	// It persisted as a grant.
	if p, ok := store.rows[peer.PubKey]; !ok || !p.Granted || p.Perms&permRoleMask != permAdmin {
		t.Errorf("grant not persisted: %+v", p)
	}
	// The access list shows it.
	list, err := e.AccessList()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || !list[0].Admin || !list[0].Granted {
		t.Fatalf("access list = %+v", list)
	}
	// A blank login proves the grant already carries the derived secret:
	// no site password is revealed, yet the key is reachable immediately.
	frame, _ := login(t, e.id, peer, nowTS(350), "", false)
	dev.frames <- frame
	if sent := awaitSent(t, sub); sent.Kind != "login-resp" {
		t.Fatalf("blank login was not answered: %+v", sent)
	}
	<-dev.sent
	if sessions, err := e.ClientSessions(); err != nil || len(sessions) != 1 {
		t.Fatalf("grant did not become a live session: %+v, %v", sessions, err)
	}

	// Revoke removes it, from table and store: a guest role is the
	// reference's word for removal. It also closes the live session the
	// blank login just established.
	if err := e.Grant(peer.PubKey[:], permGuest); err != nil {
		t.Fatal(err)
	}
	if _, held := store.rows[peer.PubKey]; held {
		t.Error("the revoke left the grant in the store")
	}
	if sessions, err := e.ClientSessions(); err != nil || len(sessions) != 0 {
		t.Fatalf("the revoke left a live session: %+v, %v", sessions, err)
	}
}

func TestAReadOnlyGrantIsNotAnAdmin(t *testing.T) {
	// setperm 1 and 2 are the reference's read-only and read-write —
	// stored as what they are, shown as what they are, and admin only
	// at exactly three. Flattening them into admin was the bug.
	e, dev, sub, peer := txRig(t, "on-air")
	e.AttachSessions(newMemStore())
	runEngine(t, e, dev)

	if err := e.Grant(peer.PubKey[:], PermReadOnly); err != nil {
		t.Fatal(err)
	}
	list, err := e.AccessList()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].Admin || list[0].Perms != PermReadOnly {
		t.Fatalf("read-only grant = %+v", list)
	}
	if RoleName(list[0].Perms) != "read-only" {
		t.Fatalf("role named %q", RoleName(list[0].Perms))
	}
	sessions, err := e.ClientSessions()
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 0 {
		t.Fatalf("a silent grant appeared as a live session: %+v", sessions)
	}

	// The granted key needs no shared password: its blank login keeps
	// the role and turns the authorisation into a live session.
	frame, secret := login(t, e.id, peer, nowTS(400), "", false)
	dev.frames <- frame
	if sent := awaitSent(t, sub); sent.Kind != "login-resp" {
		t.Fatalf("blank login was not answered: %+v", sent)
	}
	_, body := openReply(t, <-dev.sent, secret)
	if len(body) < 4 || body[3]&permRoleMask != permReadOnly {
		t.Fatalf("blank login lost the read-only grant: % x", body)
	}
	sessions, err = e.ClientSessions()
	if err != nil {
		t.Fatal(err)
	}
	if len(sessions) != 1 || sessions[0].Perms&permRoleMask != permReadOnly {
		t.Fatalf("read-only principal absent from live sessions: %+v", sessions)
	}
}

func TestARemovalMayNameItsEntryByPrefix(t *testing.T) {
	// applyPermissions' split: destroying looks the entry up, so a
	// prefix serves; creating writes one, so the whole key must be
	// said. And a prefix that names nobody is an error, not a shrug.
	e, dev, _, peer := txRig(t, "on-air")
	e.AttachSessions(newMemStore())
	runEngine(t, e, dev)

	if err := e.Grant(peer.PubKey[:4], PermAdmin); err == nil {
		t.Fatal("a grant by prefix was accepted")
	}
	if err := e.Grant(peer.PubKey[:], PermAdmin); err != nil {
		t.Fatal(err)
	}
	if err := e.Grant(peer.PubKey[:4], PermGuest); err != nil {
		t.Fatalf("removal by prefix refused: %v", err)
	}
	if rows, err := e.AccessList(); err != nil || len(rows) != 0 {
		t.Fatalf("the entry survived its prefix removal: %+v, %v", rows, err)
	}
	if err := e.Grant(peer.PubKey[:4], PermGuest); !errors.Is(err, ErrNoSuchEntry) {
		t.Errorf("removing nobody answered %v", err)
	}
}
