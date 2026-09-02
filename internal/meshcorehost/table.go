package meshcorehost

// The client table: who has logged in, what each may still ask, and
// which of them the store remembers.

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"meshrunner.dev/pkg/meshcore"
)

// SessionIdle retires a live session nobody has used. The secret it
// holds is derived from two long-term keys, so nothing is lost by
// deriving it again — and a table of live credentials should not
// outlive the conversations that made it.
const SessionIdle = time.Hour

// PersistedSession is one durable access entry as a store keeps it —
// the fields that must outlive a restart, the shared secret excluded
// because it is recomputed from the node identity and the peer key.
// A guest is a live session, never a PersistedSession.
type PersistedSession struct {
	PubKey        [meshcore.PubKeySize]byte
	Perms         byte
	Granted       bool
	LastTimestamp uint32
	// HasOut says the client taught a route, told apart from the path
	// bytes: a zero-hop route is adjacent — empty path, still present
	// — which is not the same as no route at all, where answers flood.
	HasOut     bool
	OutPath    []byte
	OutPathLen uint8
	Learned    time.Time
	LastActive time.Time
}

// SessionStore persists the access entries in the client table. The
// name follows the table's historical API; guests never cross this
// door. An access entry — an admin-password promotion included —
// survives a bounce with its replay guard. Nil keeps every entry in
// memory, the posture for a node with no store behind it.
type SessionStore interface {
	LoadSessions() ([]PersistedSession, error)
	SaveSession(s PersistedSession) error
	ForgetSession(pubKey [meshcore.PubKeySize]byte) error
}

// OutPath is a route home a client taught us, so an answer can travel
// straight there instead of costing the whole mesh a flood.
type OutPath struct {
	// PathLen is the encoded descriptor, Path the bytes it describes.
	PathLen uint8
	Path    []byte
	Learned time.Time
}

// Client is one logged-in companion.
type Client struct {
	PubKey [meshcore.PubKeySize]byte
	Secret []byte
	Perms  byte
	// Out is the way back to this client, when it has told us one. A
	// nil pointer means it has not: that is the reference's
	// OUT_PATH_UNKNOWN, and it is not the same as a route of zero
	// hops, which says the client is adjacent. The route lives and
	// dies with the session — a companion that has to log in again
	// will teach us its path again, and one that moved would have
	// taught us a stale route otherwise.
	Out *OutPath
	// LastTimestamp is the newest request instant this client has
	// signed: anything at or before it is a replay.
	LastTimestamp uint32
	LastActive    time.Time
	// Asks bounds what this session may make us emit.
	Asks RateLimiter
	// Active says this principal has authenticated traffic in this
	// process lifetime. A restored or newly granted access entry is
	// authorised, but it is not a live session until it speaks.
	Active bool
	// Closed is the operator's explicit session boundary. Unlike an
	// idle session, a closed durable principal stays in the table, but
	// its ordinary authenticated traffic is ignored until a fresh
	// login opens it again. It is deliberately not persisted: closing
	// a session is not revoking the authorisation behind it.
	Closed bool
	// Granted marks a permission set explicitly, by an admin, distinct
	// from an access entry that the admin password created. Both are
	// durable; the bit explains how the role was earned.
	Granted bool
}

// IsAdmin reports the admin role — exactly three, never "non-zero".
func (c *Client) IsAdmin() bool { return meshcore.Role(c.Perms) == meshcore.PermAdmin }

// HasAccess says whether this client belongs to the durable access
// list. Guest is the reference's zero/deleted role: it names a live
// session only and must never reach the store.
func (c *Client) HasAccess() bool { return meshcore.Role(c.Perms) != meshcore.PermGuest }

// Persisted is one client in the shape the store keeps.
func (c *Client) Persisted() PersistedSession {
	p := PersistedSession{
		PubKey: c.PubKey, Perms: c.Perms, Granted: c.Granted,
		LastTimestamp: c.LastTimestamp, LastActive: c.LastActive,
	}
	if c.Out != nil {
		p.HasOut = true
		p.OutPath = c.Out.Path
		p.OutPathLen = c.Out.PathLen
		p.Learned = c.Out.Learned
	}
	return p
}

// ACLEntry is one durable authorisation, as the console shows the
// access list: who, what role, whether it was granted explicitly or
// earned with the admin password, and how fresh.
type ACLEntry struct {
	PubKey [meshcore.PubKeySize]byte
	// Perms is the byte as stored — the reference's vocabulary, the
	// role in the low two bits.
	Perms      byte
	Admin      bool
	Granted    bool
	LastActive time.Time
}

// ClientSession is one logged-in companion, as an operator may see
// it: who, how it is reached, and how fresh the conversation is. No
// secret leaves the owner's goroutine with it.
type ClientSession struct {
	PubKey [meshcore.PubKeySize]byte
	// Perms carries the reference's role byte so a read-only or
	// read-write principal is not mislabeled as a guest in the
	// session view.
	Perms byte
	// Path is the route home the client taught us, one hash byte per
	// hop; HasPath false means it has not, and answers flood. The two
	// are distinct on purpose: a zero-hop path says the client is
	// adjacent, which is not the same as not knowing.
	Path        []byte
	HasPath     bool
	PathLearned time.Time
	LastActive  time.Time
}

// ErrSessionsFull refuses a new session when every one of the table's
// places is held by an entry that outranks a login.
var ErrSessionsFull = errors.New("the session table is full of durable access entries")

// ErrNoSuchEntry says a removal named nobody the table holds.
var ErrNoSuchEntry = errors.New("no such entry")

// ErrNoSuchSession says a close named no currently active session.
var ErrNoSuchSession = errors.New("no such active session")

// Table holds the live sessions, and belongs to its owner's goroutine
// alone — which is why there is no mutex here: nothing outside judges
// a frame, and a lock would only have protected the map while the
// sessions it points at were written through anyway.
//
// Guests live in memory only. Non-guest roles are access entries and
// cross a restart through the store, including an admin role earned
// with the admin password.
type Table struct {
	// By is the table, keyed by full public key. It is exposed because
	// the owner walks it — for expiry, for delivery — and a wrapper per
	// walk would only hide that the walk happens on the owner's turn.
	By       map[[meshcore.PubKeySize]byte]*Client
	store    SessionStore // nil keeps the table in memory only
	capacity int
}

// NewTable makes an empty table of the given capacity — the
// reference's MAX_CLIENTS, which each role sets to its own figure.
func NewTable(store SessionStore, capacity int) *Table {
	return &Table{By: map[[meshcore.PubKeySize]byte]*Client{}, store: store, capacity: capacity}
}

// SetStore installs the persistence door after construction — what an
// owner does when the store arrives later than the table.
func (t *Table) SetStore(store SessionStore) { t.store = store }

// Save mirrors one durable access entry to the store. Guests stop
// here: their replay guards and routes belong to the live session and
// disappear with it. The refusal is the caller's to judge, and the
// judgement differs by what was being written: a route learned may be
// lost to disk trouble and cost only a flood, while an administrator's
// replay guard that never reached disk is a command the next restart
// would let through a second time.
func (t *Table) Save(c *Client) error {
	if t.store == nil || !c.HasAccess() {
		return nil
	}
	return t.store.SaveSession(c.Persisted())
}

// Forget drops one session from the store.
func (t *Table) Forget(k [meshcore.PubKeySize]byte) error {
	if t.store == nil {
		return nil
	}
	return t.store.ForgetSession(k)
}

// Advance moves a session's replay guard. For an access entry it makes
// the move durable before the caller acts; for a guest the guard is
// deliberately RAM-only with the session. A store that refuses an
// access entry leaves the guard exactly where it was and the caller
// must serve nothing: an executed command whose timestamp only ever
// lived in RAM is one the next restart accepts again from a recording.
func (t *Table) Advance(c *Client, ts uint32, now time.Time) error {
	wasTS, wasActive := c.LastTimestamp, c.LastActive
	c.LastTimestamp, c.LastActive = ts, now
	if err := t.Save(c); err != nil {
		c.LastTimestamp, c.LastActive = wasTS, wasActive
		return err
	}
	return nil
}

// Load rebuilds the durable access list from the store, the secret
// recomputed per entry. Legacy guest rows are deleted instead of
// restored: loading one would turn an ephemeral session back into an
// access entry. secret returns the shared key for a peer, or an error
// a nil identity would raise; asks hands each restored client a fresh
// rate budget.
//
// A store that cannot be read is an error, never an empty table: the
// entries it holds carry the replay guards of every admin, and
// starting without them silently rewinds those clocks to zero.
func (t *Table) Load(secret func(pubKey []byte) ([]byte, error), asks func() RateLimiter) error {
	if t.store == nil {
		return nil
	}
	rows, err := t.store.LoadSessions()
	if err != nil {
		return err
	}
	for _, p := range rows {
		if meshcore.Role(p.Perms) == meshcore.PermGuest {
			if err := t.Forget(p.PubKey); err != nil {
				return fmt.Errorf("remove legacy guest %x: %w", p.PubKey[:6], err)
			}
			continue
		}
		if len(t.By) >= t.capacity {
			continue
		}
		sec, err := secret(p.PubKey[:])
		if err != nil {
			continue
		}
		c := &Client{
			PubKey: p.PubKey, Secret: sec, Perms: p.Perms, Granted: p.Granted,
			LastTimestamp: p.LastTimestamp, LastActive: p.LastActive,
			Asks: asks(),
		}
		if p.HasOut {
			c.Out = &OutPath{PathLen: p.OutPathLen, Path: p.OutPath, Learned: p.Learned}
		}
		t.By[p.PubKey] = c
	}
	return nil
}

// Put adds or refreshes a client, making room first when the table is
// full. A non-guest reaches the store before the live table moves. A
// guest never reaches it; when a guest login demotes a durable entry,
// that entry is forgotten before the guest session replaces it.
func (t *Table) Put(c *Client) error {
	var victim [meshcore.PubKeySize]byte
	evicting := false
	old, known := t.By[c.PubKey]
	if !known {
		var room bool
		if victim, evicting, room = t.Evictable(); !room {
			return ErrSessionsFull
		}
	}
	switch {
	case c.HasAccess():
		if err := t.Save(c); err != nil {
			return err
		}
	case known && old.HasAccess():
		if err := t.Forget(c.PubKey); err != nil {
			return err
		}
	}
	if evicting {
		delete(t.By, victim)
	}
	t.By[c.PubKey] = c
	return nil
}

// Evictable names the session that would make room for a new one.
// room is false when the table is full and every place is held by a
// durable access entry. The reference skips non-guest permissions
// when it hunts for its victim (ClientACL::putClient), because a run
// of fresh guest logins must not unseat an authorised principal.
// evicting is false when there was a free place to begin with.
func (t *Table) Evictable() (victim [meshcore.PubKeySize]byte, evicting, room bool) {
	if len(t.By) < t.capacity {
		return victim, false, true
	}
	var when time.Time
	for k, v := range t.By {
		if v.HasAccess() {
			continue
		}
		if !room || v.LastActive.Before(when) {
			victim, when, room = k, v.LastActive, true
		}
	}
	return victim, room, room
}

// MatchPrefix finds the lowest-keyed entry a prefix names; found is
// false when it names none.
func (t *Table) MatchPrefix(prefix []byte) (k [meshcore.PubKeySize]byte, found bool) {
	for key := range t.By {
		if len(prefix) > len(key) || !bytes.HasPrefix(key[:], prefix) {
			continue
		}
		if !found || bytes.Compare(key[:], k[:]) < 0 {
			k, found = key, true
		}
	}
	return k, found
}

// Remove drops one entry from the table and the store. A store that
// refuses keeps the entry: answering "revoked" to a revocation the
// next restart undoes is the one answer worse than refusing it.
func (t *Table) Remove(k [meshcore.PubKeySize]byte) error {
	if err := t.Forget(k); err != nil {
		return err
	}
	delete(t.By, k)
	return nil
}

// CloseSession ends the live conversation under k. A guest owns no
// durable state, so closing it removes it outright. An access
// principal keeps its role and replay guard, while its route is
// cleared and the explicit closed marker makes it log in again before
// ordinary authenticated traffic is accepted. The route change reaches
// the store before the live table moves, so a failed close is no close.
func (t *Table) CloseSession(k [meshcore.PubKeySize]byte) error {
	c := t.Live(k)
	if c == nil || !c.Active {
		return ErrNoSuchSession
	}
	if !c.HasAccess() {
		delete(t.By, k)
		return nil
	}
	candidate := *c
	candidate.Active = false
	candidate.Closed = true
	candidate.Out = nil
	if err := t.Save(&candidate); err != nil {
		return err
	}
	t.By[k] = &candidate
	return nil
}

// Entries is the durable access list as the console reads it. Guests
// are sessions only and therefore absent by construction.
func (t *Table) Entries() []ACLEntry {
	out := make([]ACLEntry, 0, len(t.By))
	for k, c := range t.By {
		if !c.HasAccess() {
			continue
		}
		out = append(out, ACLEntry{
			PubKey: k, Perms: c.Perms, Admin: c.IsAdmin(),
			Granted: c.Granted, LastActive: c.LastActive,
		})
	}
	return out
}

// Sessions renders the active table. Expiry is performed by the owner
// before publication, never by this read.
func (t *Table) Sessions() []ClientSession {
	out := make([]ClientSession, 0, len(t.By))
	for _, c := range t.By {
		if !c.Active {
			continue
		}
		row := ClientSession{PubKey: c.PubKey, Perms: c.Perms, LastActive: c.LastActive}
		if c.Out != nil {
			row.HasPath = true
			row.Path = append([]byte(nil), c.Out.Path...)
			row.PathLearned = c.Out.Learned
		}
		out = append(out, row)
	}
	return out
}

// Get returns a client by full public key. Time never mutates the
// table from a lookup: the owner's clock retires sessions explicitly.
func (t *Table) Get(pubKey []byte) *Client {
	var k [meshcore.PubKeySize]byte
	copy(k[:], pubKey)
	return t.Live(k)
}

// Live returns the client under k. Expiration belongs to the owner,
// before frame judgement, so this lookup has no hidden side effect.
func (t *Table) Live(k [meshcore.PubKeySize]byte) *Client { return t.By[k] }

// Matching returns every session whose key starts with the given hash
// — the reference's searchPeersByHash. A one-byte hash collides often;
// the MAC decides which session actually sent the packet.
func (t *Table) Matching(hash byte) []*Client {
	var out []*Client
	for k := range t.By {
		if k[0] != hash {
			continue
		}
		if c := t.Live(k); c != nil && !c.Closed {
			out = append(out, c)
		}
	}
	return out
}

// NextExpiry names the earliest active session deadline under idle.
func (t *Table) NextExpiry(now time.Time, idle time.Duration) (time.Duration, bool) {
	var wait time.Duration
	set := false
	for _, c := range t.By {
		if !c.Active {
			continue
		}
		candidate := max(time.Duration(0), c.LastActive.Add(idle).Sub(now))
		if !set || candidate < wait {
			wait, set = candidate, true
		}
	}
	return wait, set
}

// Expire applies the idle deadline under the owner's turn. Guests
// disappear with their derived credential; durable principals merely
// leave the live-session view and remain authorised. changed says
// whether a view should be republished.
func (t *Table) Expire(now time.Time, idle time.Duration) bool {
	changed := false
	for k, c := range t.By {
		if !c.Active || now.Before(c.LastActive.Add(idle)) {
			continue
		}
		if c.HasAccess() {
			c.Active = false
		} else {
			delete(t.By, k)
		}
		changed = true
	}
	return changed
}

// Grant applies one permission change on the owner's turn. A guest
// role is not a grant: setting it, like the reference's setperm,
// removes the entry the prefix names — the first prefix match, as its
// getClient answers. Any other role is composed on a candidate — the
// live entry when there is one, a fresh one otherwise — and installed
// only once it is durable: promoting the live session first meant a
// disk that refused the grant left the principal administering the
// node until the next restart, while the operator read "the grant
// would not persist". secret seals the candidate to the peer, so a
// granted admin can be reached before it has ever logged in; asks is
// the budget a fresh principal starts with.
func (t *Table) Grant(key [meshcore.PubKeySize]byte, prefixLen int, perms byte,
	secret func(pubKey []byte) ([]byte, error), asks RateLimiter, now time.Time,
) (removed [meshcore.PubKeySize]byte, wasRemoval bool, c *Client, err error) {
	if meshcore.Role(perms) == meshcore.PermGuest {
		k, found := t.MatchPrefix(key[:prefixLen])
		if !found {
			return removed, true, nil, ErrNoSuchEntry
		}
		if err := t.Remove(k); err != nil {
			return removed, true, nil, fmt.Errorf("the revocation would not persist: %w", err)
		}
		return k, true, nil, nil
	}
	sec, err := secret(key[:])
	if err != nil {
		return removed, false, nil, err
	}
	var candidate Client
	if live := t.Live(key); live != nil {
		candidate = *live
	} else {
		candidate.PubKey = key
		candidate.Asks = asks
	}
	candidate.Secret = sec
	// The whole byte, as the reference stores it — upper bits are
	// future capability flags, and flattening them here would erase
	// what a companion asked for.
	candidate.Perms = perms
	candidate.Granted = true
	if candidate.LastActive.IsZero() {
		candidate.LastActive = now
	}
	if err := t.Put(&candidate); err != nil {
		return removed, false, nil, fmt.Errorf("the grant would not persist: %w", err)
	}
	return removed, false, &candidate, nil
}
