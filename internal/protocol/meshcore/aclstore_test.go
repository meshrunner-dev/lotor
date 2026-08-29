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

	"meshrunner.dev/lotor/internal/txn"
)

// fakeStore is a session store whose three operations fail on demand,
// separately: the audit's own shape, since load, save and forget each
// carry a different promise.
type fakeStore struct {
	rows                        []PersistedSession
	loadErr, saveErr, forgetErr error
	// swapErr fails the atomic replacement alone, which is neither a
	// plain save nor a plain forget.
	swapErr               error
	loads, saves, forgets int
	swaps                 int
	saved                 map[[meshcore.PubKeySize]byte]PersistedSession
	forgotten             []([meshcore.PubKeySize]byte)
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

// ReplaceSession is the one durable step a full table's admission
// really is: the newcomer in, the victim out, both or neither.
func (s *fakeStore) ReplaceSession(add PersistedSession, drop [meshcore.PubKeySize]byte) error {
	s.swaps++
	if s.swapErr != nil {
		return s.swapErr
	}
	if s.saveErr != nil {
		return s.saveErr
	}
	delete(s.saved, drop)
	s.saved[add.PubKey] = add
	s.forgotten = append(s.forgotten, drop)
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
		t.Fatal("an unreadable session store was accepted")
	}
	if len(e.acl.by) != 0 {
		t.Error("a refused load left sessions behind")
	}
}

func TestLoadKeepsGrantsWhenTheStoreOverflows(t *testing.T) {
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
	// One grant, listed last, behind a full table's worth of guests:
	// the places must not be spent on whoever the store lists first.
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
	if len(e.acl.by) != maxClients {
		t.Errorf("restored %d sessions, want %d", len(e.acl.by), maxClients)
	}
	if _, kept := e.acl.by[granted]; !kept {
		t.Error("the grant was crowded out by plain logins")
	}
}

func TestGrantsAndAdminsAreNeverEvicted(t *testing.T) {
	e, _ := identifiedEngine(t)
	store := newFakeStore()
	if err := e.AttachSessions(store); err != nil {
		t.Fatal(err)
	}
	// One granted admin, old and quiet, then a table's worth of fresh
	// guests: the reference hunts its victim among non-admins alone,
	// or a run of logins unseats the node's only administrator.
	admin := aclKey(0xAA)
	if err := e.acl.put(&client{
		pubKey: admin, perms: permAdmin, granted: true,
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
		t.Fatal("a guest login evicted the granted admin")
	}
	if _, gone := store.saved[admin]; !gone {
		t.Error("the granted admin was deleted from the store")
	}
	if len(e.acl.by) != maxClients {
		t.Errorf("table holds %d, want %d", len(e.acl.by), maxClients)
	}
}

func TestAFullTableOfGrantsRefusesTheLogin(t *testing.T) {
	e, _ := identifiedEngine(t)
	if err := e.AttachSessions(newFakeStore()); err != nil {
		t.Fatal(err)
	}
	for i := range maxClients {
		if err := e.acl.put(&client{
			pubKey: aclKey(byte(i)), perms: permAdmin, granted: true, lastActive: time.Now(),
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

	got := e.admitLogin(peer.PubKey[:], secret, "raccoon", nowTS(400), false, txn.New())
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
	if got := e.admitLogin(peer.PubKey[:], secret, "raccoon", nowTS(600), false, txn.New()); got == nil {
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

func TestAdmittingIntoAFullTableIsOneDurableStep(t *testing.T) {
	// Two writes left the store holding thirty-three sessions for a
	// table of thirty-two, and nothing said which a restart would
	// drop: an evicted session could come back while the newcomer,
	// replay guard and all, went missing instead.
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

	// The swap succeeds: the two sets agree exactly, and it took one
	// durable step rather than two.
	e, store := fill(t)
	victim := aclKey(0) // the least recently active
	newcomer := aclKey(0xEE)
	if err := e.acl.put(&client{
		pubKey: newcomer, perms: permGuest, lastActive: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	if store.swaps != 1 {
		t.Errorf("the admission took %d swaps, want one", store.swaps)
	}
	assertSetsAgree(t, e, store)
	if _, gone := store.saved[victim]; gone {
		t.Error("the victim is still persisted while the table dropped it")
	}
	if _, in := store.saved[newcomer]; !in {
		t.Error("the newcomer never reached the store")
	}

	// The swap fails: neither side moves, and the login is refused.
	e, store = fill(t)
	store.swapErr = errors.New("disk full")
	err := e.acl.put(&client{
		pubKey: aclKey(0xFF), perms: permGuest, lastActive: time.Now().Add(time.Hour),
	})
	if err == nil {
		t.Fatal("an admission the store refused reported success")
	}
	if _, leaked := e.acl.by[aclKey(0xFF)]; leaked {
		t.Error("the refused newcomer was installed")
	}
	if _, gone := e.acl.by[aclKey(0)]; !gone {
		t.Error("the victim was evicted for an admission that never persisted")
	}
	assertSetsAgree(t, e, store)
}

// assertSetsAgree proves the live table and the store hold exactly the
// same sessions — the property two separate writes could not keep.
func assertSetsAgree(t *testing.T, e *engine, store *fakeStore) {
	t.Helper()
	if len(e.acl.by) != len(store.saved) {
		t.Fatalf("table holds %d sessions, store holds %d", len(e.acl.by), len(store.saved))
	}
	for k := range e.acl.by {
		if _, ok := store.saved[k]; !ok {
			t.Errorf("session %x lives only in the table", k[:4])
		}
	}
}
