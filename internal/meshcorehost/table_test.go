package meshcorehost

import (
	"errors"
	"testing"
	"time"

	"meshrunner.dev/pkg/meshcore"
)

// memStore is a SessionStore that remembers, and can be told to refuse.
type memStore struct {
	rows    map[[meshcore.PubKeySize]byte]PersistedSession
	refuse  error
	saves   int
	forgets int
}

func newMemStore() *memStore {
	return &memStore{rows: map[[meshcore.PubKeySize]byte]PersistedSession{}}
}

func (s *memStore) LoadSessions() ([]PersistedSession, error) {
	out := make([]PersistedSession, 0, len(s.rows))
	for _, r := range s.rows {
		out = append(out, r)
	}
	return out, nil
}

func (s *memStore) SaveSession(p PersistedSession) error {
	if s.refuse != nil {
		return s.refuse
	}
	s.saves++
	s.rows[p.PubKey] = p
	return nil
}

func (s *memStore) ForgetSession(k [meshcore.PubKeySize]byte) error {
	if s.refuse != nil {
		return s.refuse
	}
	s.forgets++
	delete(s.rows, k)
	return nil
}

func key(b byte) [meshcore.PubKeySize]byte {
	var k [meshcore.PubKeySize]byte
	k[0], k[1] = b, b
	return k
}

// The repeater's doors, for the tests: one admin word, one guest word.
func doors(word string) (byte, bool) {
	switch word {
	case "admin":
		return meshcore.PermAdmin, true
	case "guest":
		return meshcore.PermGuest, true
	}
	return 0, false
}

func TestAdmitComposesOnACandidate(t *testing.T) {
	k := key(0x11)
	live := &Client{PubKey: k, Perms: meshcore.PermAdmin, Granted: true, LastTimestamp: 100, Closed: true}
	// A wrong word touches nothing, and says which refusal it earned.
	if c, why := Admit(live, k[:], []byte("s"), "nope", 200, doors); c != nil || why != RefusedWord {
		t.Fatalf("wrong word = %+v, %q", c, why)
	}
	if !live.Closed || live.LastTimestamp != 100 {
		t.Fatal("a refused word touched the live session")
	}
	// A replay is judged after the role, so an old guest login cannot
	// demote the admin it replays against.
	if c, why := Admit(live, k[:], []byte("s"), "guest", 50, doors); c != nil || why != RefusedReplay {
		t.Fatalf("replay = %+v, %q", c, why)
	}
	if live.Perms != meshcore.PermAdmin || !live.Granted {
		t.Fatal("a replayed guest login demoted the live admin")
	}
	// A blank word from a known key rechecks: role and grant kept,
	// the session reopened.
	c, why := Admit(live, k[:], []byte("s"), "", 200, doors)
	if why != "" || c.Perms != meshcore.PermAdmin || !c.Granted || c.Closed {
		t.Fatalf("recheck = %+v, %q", c, why)
	}
	// A blank word from a stranger opens nothing.
	if c, why := Admit(nil, k[:], []byte("s"), "", 200, doors); c != nil || why != RefusedWord {
		t.Fatalf("blank stranger = %+v, %q", c, why)
	}
	// The guest word rewrites the role and drops the grant — the
	// reference rewrites the bits on every password login.
	c, why = Admit(live, k[:], []byte("s2"), "guest", 300, doors)
	if why != "" || c.Perms != meshcore.PermGuest || c.Granted || string(c.Secret) != "s2" {
		t.Fatalf("guest login = %+v, %q", c, why)
	}
	// The route a client taught survives its next login.
	live.Out = &OutPath{PathLen: 1, Path: []byte{0xAA}}
	if c, _ := Admit(live, k[:], []byte("s"), "admin", 400, doors); c.Out == nil || c.Out.Path[0] != 0xAA {
		t.Fatal("a login lost the taught route")
	}
}

func TestSkewedReadsARecording(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	if Skewed(uint32(now.Unix()), now) || Skewed(uint32(now.Add(-LoginMaxSkew/2).Unix()), now) {
		t.Error("a fresh login read as skewed")
	}
	if !Skewed(uint32(now.Add(-2*LoginMaxSkew).Unix()), now) || !Skewed(uint32(now.Add(2*LoginMaxSkew).Unix()), now) {
		t.Error("a stale or future login passed")
	}
}

func TestTheTablePersistsAccessAndForgetsGuests(t *testing.T) {
	store := newMemStore()
	tb := NewTable(store, 2)
	admin := &Client{PubKey: key(0x01), Perms: meshcore.PermAdmin, Active: true}
	guest := &Client{PubKey: key(0x02), Perms: meshcore.PermGuest, Active: true, LastActive: time.Now()}
	if err := tb.Put(admin); err != nil || store.saves != 1 {
		t.Fatalf("admin put: %v, saves %d", err, store.saves)
	}
	if err := tb.Put(guest); err != nil || store.saves != 1 {
		t.Fatalf("guest put: %v, saves %d — a guest must never reach the store", err, store.saves)
	}
	// Full of one guest and one admin: a newcomer evicts the guest,
	// never the admin.
	victim, evicting, room := tb.Evictable()
	if !room || !evicting || victim != guest.PubKey {
		t.Fatalf("evictable = %x %v %v", victim[:2], evicting, room)
	}
	if err := tb.Put(&Client{PubKey: key(0x03)}); err != nil || tb.Live(guest.PubKey) != nil {
		t.Fatalf("newcomer: %v, guest kept %v", err, tb.Live(guest.PubKey) != nil)
	}
	// Two durable entries fill the table for good.
	if err := tb.Put(&Client{PubKey: key(0x03), Perms: meshcore.PermReadOnly}); err != nil {
		t.Fatal(err)
	}
	if err := tb.Put(&Client{PubKey: key(0x04)}); !errors.Is(err, ErrSessionsFull) {
		t.Fatalf("a full table of access entries admitted a login: %v", err)
	}
	// A store that refuses leaves the table as it found it.
	store.refuse = errors.New("disk")
	before := tb.Live(admin.PubKey).LastTimestamp
	if err := tb.Advance(tb.Live(admin.PubKey), 99, time.Now()); err == nil || tb.Live(admin.PubKey).LastTimestamp != before {
		t.Fatalf("a refused save moved the replay guard: %v", err)
	}
	if err := tb.Remove(admin.PubKey); err == nil || tb.Live(admin.PubKey) == nil {
		t.Fatalf("a refused forget removed the entry: %v", err)
	}
	store.refuse = nil
	// Load rebuilds access entries only, with fresh budgets and secrets.
	store.rows[key(0x09)] = PersistedSession{PubKey: key(0x09), Perms: meshcore.PermGuest}
	fresh := NewTable(store, 8)
	err := fresh.Load(func([]byte) ([]byte, error) { return []byte("sec"), nil },
		func() RateLimiter { return RateLimiter{Max: 3, Window: time.Minute} })
	if err != nil {
		t.Fatal(err)
	}
	if _, legacy := store.rows[key(0x09)]; legacy || fresh.Live(key(0x09)) != nil {
		t.Error("a legacy guest row was restored or kept")
	}
	if c := fresh.Live(admin.PubKey); c == nil || string(c.Secret) != "sec" || c.Asks.Max != 3 || c.Active {
		t.Fatalf("restored admin = %+v", c)
	}
}

func TestExpiryRetiresGuestsAndSleepsPrincipals(t *testing.T) {
	tb := NewTable(nil, 8)
	now := time.Now()
	tb.By[key(0x01)] = &Client{PubKey: key(0x01), Perms: meshcore.PermAdmin, Active: true, LastActive: now.Add(-2 * SessionIdle)}
	tb.By[key(0x02)] = &Client{PubKey: key(0x02), Active: true, LastActive: now.Add(-2 * SessionIdle)}
	tb.By[key(0x03)] = &Client{PubKey: key(0x03), Active: true, LastActive: now.Add(-time.Minute)}
	if wait, ok := tb.NextExpiry(now, SessionIdle); !ok || wait != 0 {
		t.Fatalf("next expiry = %v %v, want due now", wait, ok)
	}
	if !tb.Expire(now, SessionIdle) {
		t.Fatal("nothing expired")
	}
	if c := tb.Live(key(0x01)); c == nil || c.Active {
		t.Error("the idle admin was removed or left active")
	}
	if tb.Live(key(0x02)) != nil {
		t.Error("the idle guest survived")
	}
	if c := tb.Live(key(0x03)); c == nil || !c.Active {
		t.Error("a fresh session was retired")
	}
	if wait, ok := tb.NextExpiry(now, SessionIdle); !ok || wait <= 0 {
		t.Errorf("next expiry after the sweep = %v %v", wait, ok)
	}
}

func TestGrantRemovesOnGuestAndPromotesDurably(t *testing.T) {
	store := newMemStore()
	tb := NewTable(store, 8)
	secret := func([]byte) ([]byte, error) { return []byte("sec"), nil }
	asks := RateLimiter{Max: 6, Window: time.Minute}
	k := key(0x42)
	_, wasRemoval, c, err := tb.Grant(k, meshcore.PubKeySize, meshcore.PermAdmin, secret, asks, time.Now())
	if err != nil || wasRemoval || c == nil || !c.Granted || !c.IsAdmin() || string(c.Secret) != "sec" {
		t.Fatalf("grant = %+v %v %v", c, wasRemoval, err)
	}
	if _, saved := store.rows[k]; !saved {
		t.Fatal("the grant never reached the store")
	}
	// A prefix names it for removal; an unknown prefix names nobody.
	if _, _, _, err := tb.Grant(key(0x99), 2, meshcore.PermGuest, secret, asks, time.Now()); !errors.Is(err, ErrNoSuchEntry) {
		t.Errorf("unknown prefix = %v", err)
	}
	removed, wasRemoval, _, err := tb.Grant(k, 2, meshcore.PermGuest, secret, asks, time.Now())
	if err != nil || !wasRemoval || removed != k || tb.Live(k) != nil {
		t.Fatalf("revoke = %x %v %v", removed[:2], wasRemoval, err)
	}
	// A store that refuses installs nothing, and says so in its words.
	store.refuse = errors.New("disk")
	if _, _, _, err := tb.Grant(k, meshcore.PubKeySize, meshcore.PermReadWrite, secret, asks, time.Now()); err == nil || tb.Live(k) != nil {
		t.Fatalf("a refused grant installed: %v", err)
	}
}
