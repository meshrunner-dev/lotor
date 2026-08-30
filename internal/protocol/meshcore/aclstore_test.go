package meshcore

// The session table's durability contract, by invariant rather than
// by line: what a failing store must refuse, what a full table must
// protect, and what a replayed login must leave untouched. A session
// that lives only in RAM is one a restart forgets — its replay guard
// with it — so every one of these is a security property, not a
// bookkeeping preference.

import (
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/correlation"
)

// fakeStore is an access store whose three operations fail on demand,
// separately: load, save and forget each carry a different promise.
type fakeStore struct {
	rows                        []PersistedSession
	loadErr, saveErr, forgetErr error
	loads, saves, forgets       int
	saved                       map[[meshcore.PubKeySize]byte]PersistedSession
	forgotten                   []([meshcore.PubKeySize]byte)
}

func newFakeStore() *fakeStore {
	return &fakeStore{saved: map[[meshcore.PubKeySize]byte]PersistedSession{}}
}

func (s *fakeStore) LoadSessions() ([]PersistedSession, error) {
	s.loads++
	if s.loadErr != nil {
		return nil, s.loadErr
	}
	return s.rows, nil
}

func (s *fakeStore) SaveSession(p PersistedSession) error {
	s.saves++
	if s.saveErr != nil {
		return s.saveErr
	}
	s.saved[p.PubKey] = p
	return nil
}

func (s *fakeStore) ForgetSession(k [meshcore.PubKeySize]byte) error {
	s.forgets++
	if s.forgetErr != nil {
		return s.forgetErr
	}
	delete(s.saved, k)
	s.forgotten = append(s.forgotten, k)
	return nil
}

// key builds a distinct public key from one byte.
func aclKey(b byte) [meshcore.PubKeySize]byte {
	var k [meshcore.PubKeySize]byte
	for i := range k {
		k[i] = b
	}
	return k
}

func TestUnreadableStoreRefusesTheRelay(t *testing.T) {
	// Coming up with an empty table would rewind every admin's replay
	// guard to zero: a recent capture of a login and the command
	// after it would replay straight through.
	e, _ := identifiedEngine(t)
	store := newFakeStore()
	store.loadErr = errors.New("disk on fire")
	err := e.AttachSessions(store)
	if err == nil {
		t.Fatal("an unreadable access store was accepted")
	}
	if len(e.acl.by) != 0 {
		t.Error("a refused load left sessions behind")
	}
}

func TestLoadPurgesLegacyGuestsAndKeepsAccess(t *testing.T) {
	e, _ := identifiedEngine(t)
	store := newFakeStore()
	// Real keys: restoring a session derives its shared secret, and a
	// key that is not a curve point is one load rightly skips.
	peer := func() [meshcore.PubKeySize]byte {
		t.Helper()
		id, err := meshcore.NewLocalIdentity(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		return id.PubKey
	}
	// One grant, listed last, behind a full table's worth of legacy
	// guest rows: guests must be deleted, not restored into ACL slots.
	for range maxClients {
		store.rows = append(store.rows, PersistedSession{
			PubKey: peer(), Perms: permGuest, LastActive: time.Now(),
		})
	}
	granted := peer()
	store.rows = append(store.rows, PersistedSession{
		PubKey: granted, Perms: permAdmin, Granted: true, LastActive: time.Now(),
	})
	if err := e.AttachSessions(store); err != nil {
		t.Fatal(err)
	}
	if len(e.acl.by) != 1 {
		t.Errorf("restored %d access entries, want 1", len(e.acl.by))
	}
	if _, kept := e.acl.by[granted]; !kept {
		t.Error("the grant was crowded out by legacy guests")
	}
	if store.forgets != maxClients {
		t.Errorf("cleaned %d legacy guests, want %d", store.forgets, maxClients)
	}
}

func TestAccessEntriesAreNeverEvicted(t *testing.T) {
	e, _ := identifiedEngine(t)
	store := newFakeStore()
	if err := e.AttachSessions(store); err != nil {
		t.Fatal(err)
	}
	// One granted read-only entry, old and quiet, then a table's worth
	// of fresh guests: a run of logins must not unseat any durable
	// authorisation, not only administrators.
	admin := aclKey(0xAA)
	if err := e.acl.put(&client{
		pubKey: admin, perms: permReadOnly, granted: true,
		lastActive: time.Now().Add(-time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	for i := range maxClients - 1 {
		if err := e.acl.put(&client{
			pubKey: aclKey(byte(i)), perms: permGuest, lastActive: time.Now(),
		}); err != nil {
			t.Fatalf("guest %d refused: %v", i, err)
		}
	}
	// The table is full. One more guest evicts a guest, never the admin.
	if err := e.acl.put(&client{
		pubKey: aclKey(0xBB), perms: permGuest, lastActive: time.Now(),
	}); err != nil {
		t.Fatalf("a guest could not take a guest's place: %v", err)
	}
	if _, kept := e.acl.by[admin]; !kept {
		t.Fatal("a guest login evicted the read-only access entry")
	}
	if _, gone := store.saved[admin]; !gone {
		t.Error("the read-only access entry was deleted from the store")
	}
	if len(e.acl.by) != maxClients {
		t.Errorf("table holds %d, want %d", len(e.acl.by), maxClients)
	}
}

func TestAFullTableOfAccessEntriesRefusesTheLogin(t *testing.T) {
	e, _ := identifiedEngine(t)
	if err := e.AttachSessions(newFakeStore()); err != nil {
		t.Fatal(err)
	}
	for i := range maxClients {
		if err := e.acl.put(&client{
			pubKey: aclKey(byte(i)), perms: permReadOnly, granted: true, lastActive: time.Now(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	err := e.acl.put(&client{pubKey: aclKey(0xCC), perms: permGuest, lastActive: time.Now()})
	if !errors.Is(err, errSessionsFull) {
		t.Fatalf("a full table of grants answered %v", err)
	}
	if len(e.acl.by) != maxClients {
		t.Errorf("the refusal changed the table: %d entries", len(e.acl.by))
	}
	if _, leaked := e.acl.by[aclKey(0xCC)]; leaked {
		t.Error("the refused session was installed anyway")
	}
}

func TestARefusedSaveInstallsNothing(t *testing.T) {
	e, _ := identifiedEngine(t)
	store := newFakeStore()
	if err := e.AttachSessions(store); err != nil {
		t.Fatal(err)
	}
	store.saveErr = errors.New("disk full")
	k := aclKey(0x11)
	if err := e.acl.put(&client{pubKey: k, perms: permAdmin, lastActive: time.Now()}); err == nil {
		t.Fatal("a session that never reached the store was installed")
	}
	if _, live := e.acl.by[k]; live {
		t.Error("the table holds a session the store refused")
	}
}

func TestARefusedForgetKeepsTheEntry(t *testing.T) {
	// Answering "revoked" to a revocation the next restart undoes is
	// the one answer worse than refusing it.
	e, _ := identifiedEngine(t)
	store := newFakeStore()
	if err := e.AttachSessions(store); err != nil {
		t.Fatal(err)
	}
	k := aclKey(0x22)
	if err := e.acl.put(&client{pubKey: k, perms: permAdmin, granted: true, lastActive: time.Now()}); err != nil {
		t.Fatal(err)
	}
	store.forgetErr = errors.New("disk read-only")
	if err := e.acl.remove(k); err == nil {
		t.Fatal("a revocation that did not persist reported success")
	}
	if _, gone := e.acl.by[k]; !gone {
		t.Error("the entry was dropped from the table anyway — the two would disagree on restart")
	}
}

func TestGuestActivityNeverTouchesTheAccessStore(t *testing.T) {
	e, _ := identifiedEngine(t)
	store := newFakeStore()
	if err := e.AttachSessions(store); err != nil {
		t.Fatal(err)
	}
	k := aclKey(0x2A)
	c := &client{pubKey: k, perms: permGuest, lastActive: time.Now()}
	if err := e.acl.put(c); err != nil {
		t.Fatal(err)
	}
	if err := e.acl.advance(c, 100, time.Now()); err != nil {
		t.Fatal(err)
	}
	c.out = &outPath{pathLen: 1, path: []byte{0x42}, learned: time.Now()}
	if err := e.acl.save(c); err != nil {
		t.Fatal(err)
	}
	if store.saves != 0 || store.forgets != 0 || len(store.saved) != 0 {
		t.Fatalf("guest activity touched the access store: saves=%d forgets=%d rows=%d",
			store.saves, store.forgets, len(store.saved))
	}
}

func TestGuestDemotionMustRemoveTheAccessEntry(t *testing.T) {
	e, _ := identifiedEngine(t)
	store := newFakeStore()
	if err := e.AttachSessions(store); err != nil {
		t.Fatal(err)
	}
	k := aclKey(0x2B)
	admin := &client{
		pubKey: k, perms: permAdmin, lastTimestamp: 10, lastActive: time.Now(),
	}
	if err := e.acl.put(admin); err != nil {
		t.Fatal(err)
	}
	guest := *admin
	guest.perms = permGuest
	guest.granted = false

	store.forgetErr = errors.New("disk read-only")
	if err := e.acl.put(&guest); err == nil {
		t.Fatal("a demotion whose ACL deletion failed was installed")
	}
	if live := e.acl.by[k]; live == nil || !live.isAdmin() {
		t.Fatalf("failed demotion changed the live role: %+v", live)
	}
	if _, kept := store.saved[k]; !kept {
		t.Fatal("failed demotion deleted the durable role")
	}

	store.forgetErr = nil
	if err := e.acl.put(&guest); err != nil {
		t.Fatal(err)
	}
	if live := e.acl.by[k]; live == nil || live.hasAccess() {
		t.Fatalf("successful demotion did not install a guest session: %+v", live)
	}
	if _, kept := store.saved[k]; kept {
		t.Fatal("successful demotion left the ACL entry durable")
	}
}

func TestLegacyGuestCleanupMustSucceed(t *testing.T) {
	e, _ := identifiedEngine(t)
	store := newFakeStore()
	store.rows = []PersistedSession{{
		PubKey: aclKey(0x2C), Perms: permGuest, LastActive: time.Now(),
	}}
	store.forgetErr = errors.New("disk read-only")
	if err := e.AttachSessions(store); err == nil {
		t.Fatal("a legacy guest that could not be removed was accepted")
	}
	if len(e.acl.by) != 0 {
		t.Fatal("a failed cleanup restored the legacy guest")
	}
}

func TestTheReplayGuardIsDurableBeforeItCounts(t *testing.T) {
	e, _ := identifiedEngine(t)
	store := newFakeStore()
	if err := e.AttachSessions(store); err != nil {
		t.Fatal(err)
	}
	k := aclKey(0x33)
	c := &client{pubKey: k, perms: permAdmin, lastTimestamp: 100, lastActive: time.Now()}
	if err := e.acl.put(c); err != nil {
		t.Fatal(err)
	}
	store.saveErr = errors.New("disk stalled")
	if err := e.acl.advance(c, 200, time.Now()); err == nil {
		t.Fatal("the guard advanced past a store that refused it")
	}
	if c.lastTimestamp != 100 {
		t.Errorf("the guard moved to %d on a refused save — the request must look unserved",
			c.lastTimestamp)
	}
	store.saveErr = nil
	if err := e.acl.advance(c, 200, time.Now()); err != nil {
		t.Fatal(err)
	}
	if got := store.saved[k].LastTimestamp; got != 200 {
		t.Errorf("the store holds guard %d, want 200", got)
	}
}

func TestACommandIsNotRunWhenItsGuardCannotPersist(t *testing.T) {
	// The exposure this closes: the mutation landed, then the proof it
	// had landed was allowed to fail, so a recording applied it twice.
	e, _, _, peer := txRig(t, "on-air")
	store := newFakeStore()
	if err := e.AttachSessions(store); err != nil {
		t.Fatal(err)
	}
	secret, err := e.id.SharedSecret(peer.PubKey[:])
	if err != nil {
		t.Fatal(err)
	}
	c := &client{
		pubKey: peer.PubKey, secret: secret, perms: permAdmin, granted: true,
		lastTimestamp: nowTS(0), lastActive: time.Now(),
		asks: rateLimiter{max: 6, window: time.Minute},
	}
	if err := e.acl.put(c); err != nil {
		t.Fatal(err)
	}
	ran := 0
	e.AttachCommands(func(string, []byte) string { ran++; return "OK" })

	store.saveErr = errors.New("disk stalled")
	plain := meshcore.BuildTextPlaintext(time.Unix(int64(nowTS(10)), 0), meshcore.TxtTypePlain, "set tx 6")
	pkt, err := meshcore.BuildDatagram(meshcore.PayloadTypeTxtMsg,
		e.id.PubKey[:meshcore.PathHashSize], peer.PubKey[:meshcore.PathHashSize], secret, plain)
	if err != nil {
		t.Fatal(err)
	}
	pkt.Header = meshcore.MakeHeader(meshcore.RouteDirect,
		meshcore.PayloadTypeTxtMsg, meshcore.PayloadVer1)
	// Through the verdict, as production does: it is what admits the
	// subtype and hands the decode on.
	rx := rxOf(e, pkt)
	if rx.opened == nil || rx.opened.text == nil {
		t.Fatal("the verdict did not admit the command")
	}
	e.runCommand(rx, rx.id)
	if ran != 0 {
		t.Fatal("the command ran on a replay guard that never reached the disk")
	}
	if c.lastTimestamp != nowTS(0) {
		t.Errorf("the guard moved to %d despite the refusal", c.lastTimestamp)
	}
	// The disk recovers and the same line is served, once.
	store.saveErr = nil
	e.runCommand(rxOf(e, pkt), rx.id)
	if ran != 1 {
		t.Errorf("the recovered command ran %d times, want 1", ran)
	}
}

func TestAReplayedLoginLeavesTheSessionAlone(t *testing.T) {
	// An old guest login, captured before its key was promoted, is
	// correctly refused as a replay — and must not demote the admin on
	// its way to that refusal.
	e, _, _, peer := txRig(t, "on-air")
	e.p.GuestAccess, e.p.GuestPassword = guestPassword, "raccoon"
	e.p.AdminPassword = "badger"
	if err := e.AttachSessions(newFakeStore()); err != nil {
		t.Fatal(err)
	}
	secret, err := e.id.SharedSecret(peer.PubKey[:])
	if err != nil {
		t.Fatal(err)
	}
	c := &client{
		pubKey: peer.PubKey, secret: secret, perms: permAdmin, granted: true,
		lastTimestamp: nowTS(500), lastActive: time.Now(),
	}
	if err := e.acl.put(c); err != nil {
		t.Fatal(err)
	}
	before := *c

	got := e.admitLogin(peer.PubKey[:], secret, "raccoon", nowTS(400), false, correlation.New())
	if got != nil {
		t.Fatal("a replayed login was admitted")
	}
	live := e.acl.get(peer.PubKey[:])
	if live == nil {
		t.Fatal("the replay retired the session")
	}
	if live.perms != before.perms || !live.granted {
		t.Errorf("the replay rewrote the session: perms %#x granted %v, want %#x true",
			live.perms, live.granted, before.perms)
	}
	if live.lastTimestamp != before.lastTimestamp {
		t.Errorf("the replay moved the guard to %d", live.lastTimestamp)
	}
	// A fresh guest login on the same key still demotes, as the
	// reference's every-password-rewrites-the-role rule says.
	if got := e.admitLogin(peer.PubKey[:], secret, "raccoon", nowTS(600), false, correlation.New()); got == nil {
		t.Fatal("a fresh guest login was refused")
	} else if got.perms&permRoleMask != permGuest || got.granted {
		t.Errorf("a fresh guest login kept %#x granted=%v", got.perms, got.granted)
	}
}

func TestIdentitylessEngineNeedsNoStore(t *testing.T) {
	// No identity, no recomputable secret, no session to restore: the
	// attachment is a no-op rather than a refusal.
	e, _ := testEngine(t)
	store := newFakeStore()
	store.loadErr = errors.New("would have failed")
	if err := e.AttachSessions(store); err != nil {
		t.Errorf("an engine with no identity refused a store it never reads: %v", err)
	}
	if store.loads != 0 {
		t.Error("the store was read without an identity to derive secrets with")
	}
}

func TestARefusedGrantLeavesTheLiveSessionAlone(t *testing.T) {
	// The residual the remediation review found: the grant promoted
	// the live session first and asked the disk after, so a refused
	// write left the principal administering the node until the next
	// restart while the operator read "the grant would not persist".
	e, _ := identifiedEngine(t)
	store := newFakeStore()
	if err := e.AttachSessions(store); err != nil {
		t.Fatal(err)
	}
	peer, err := meshcore.NewLocalIdentity(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	// An entry that already exists — the case the first round missed.
	secret, err := e.id.SharedSecret(peer.PubKey[:])
	if err != nil {
		t.Fatal(err)
	}
	guest := &client{
		pubKey: peer.PubKey, secret: secret, perms: permReadOnly,
		lastTimestamp: 42, lastActive: time.Now(),
	}
	if err := e.acl.put(guest); err != nil {
		t.Fatal(err)
	}
	before := *e.acl.by[peer.PubKey]

	store.saveErr = errors.New("disk full")
	o := &aclOrder{perms: permAdmin, prefixLen: meshcore.PubKeySize, done: newAck()}
	copy(o.pubKey[:], peer.PubKey[:])
	if err := e.applyGrant(o); err == nil {
		t.Fatal("a grant the store refused reported success")
	}
	live := e.acl.by[peer.PubKey]
	if live == nil {
		t.Fatal("the refused grant retired the session")
	}
	if live.perms != before.perms || live.granted != before.granted {
		t.Errorf("the refused grant promoted the live session: perms %#x granted %v, want %#x %v",
			live.perms, live.granted, before.perms, before.granted)
	}
	if live.lastTimestamp != before.lastTimestamp {
		t.Errorf("the refused grant moved the replay guard to %d", live.lastTimestamp)
	}
	// The disk recovers and the same grant lands, once and wholly.
	store.saveErr = nil
	if err := e.applyGrant(o); err != nil {
		t.Fatal(err)
	}
	if live = e.acl.by[peer.PubKey]; !live.isAdmin() || !live.granted {
		t.Errorf("the recovered grant did not land: %+v", live)
	}
	if p := store.saved[peer.PubKey]; p.Perms&permRoleMask != permAdmin || !p.Granted {
		t.Errorf("the store holds %+v", p)
	}
}

func TestAdmittingIntoAFullGuestTableKeepsGuestsOffDisk(t *testing.T) {
	// A full live table may swap one guest for another, but neither
	// side of that in-memory eviction belongs in the access store.
	fill := func(t *testing.T) (*engine, *fakeStore) {
		t.Helper()
		e, _ := identifiedEngine(t)
		store := newFakeStore()
		if err := e.AttachSessions(store); err != nil {
			t.Fatal(err)
		}
		for i := range maxClients {
			if err := e.acl.put(&client{
				pubKey: aclKey(byte(i)), perms: permGuest,
				lastActive: time.Now().Add(time.Duration(i) * time.Minute),
			}); err != nil {
				t.Fatal(err)
			}
		}
		return e, store
	}

	e, store := fill(t)
	victim := aclKey(0) // the least recently active
	newcomer := aclKey(0xEE)
	if err := e.acl.put(&client{
		pubKey: newcomer, perms: permGuest, lastActive: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	if store.saves != 0 || store.forgets != 0 || len(store.saved) != 0 {
		t.Errorf("guest eviction touched the access store: saves=%d forgets=%d rows=%d",
			store.saves, store.forgets, len(store.saved))
	}
	if _, gone := e.acl.by[victim]; gone {
		t.Error("the least-recent guest was not evicted")
	}
	if _, in := e.acl.by[newcomer]; !in {
		t.Error("the newcomer was not admitted")
	}
}
