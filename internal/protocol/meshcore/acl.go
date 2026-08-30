package meshcore

// The session table: who is logged in, and what each of them may
// still ask before the node stops answering.

import (
	"bytes"
	"errors"
	"fmt"
	"time"

	"meshrunner.dev/pkg/meshcore"
)

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
// memory, the posture for a relay with no store behind it.
type SessionStore interface {
	LoadSessions() ([]PersistedSession, error)
	SaveSession(s PersistedSession) error
	ForgetSession(pubKey [meshcore.PubKeySize]byte) error
}

// outPath is a route home a client taught us, so an answer can travel
// straight there instead of costing the whole mesh a flood.
type outPath struct {
	// pathLen is the encoded descriptor, path the bytes it describes.
	pathLen uint8
	path    []byte
	learned time.Time
}

// client is one logged-in companion.
type client struct {
	pubKey [meshcore.PubKeySize]byte
	secret []byte
	perms  byte
	// out is the way back to this client, when it has told us one. A
	// nil pointer means it has not: that is the reference's
	// OUT_PATH_UNKNOWN, and it is not the same as a route of zero
	// hops, which says the client is adjacent. The route lives and
	// dies with the session — a companion that has to log in again
	// will teach us its path again, and one that moved would have
	// taught us a stale route otherwise.
	out *outPath
	// lastTimestamp is the newest request instant this client has
	// signed: anything at or before it is a replay.
	lastTimestamp uint32
	lastActive    time.Time
	// asks bounds what this session may make us emit.
	asks rateLimiter
	// active says this principal has authenticated traffic in this
	// process lifetime. A restored or newly granted access entry is
	// authorised, but it is not a live session until it speaks.
	active bool
	// granted marks a permission set explicitly, by an admin, distinct
	// from an access entry that the admin password created. Both are
	// durable; the bit explains how the role was earned.
	granted bool
}

// isAdmin reports the role; always false here, kept because the wire
// carries the field and a reader should see why it is what it is.
func (c *client) isAdmin() bool { return c.perms&permRoleMask == permAdmin }

// acl holds the live sessions, and belongs to the engine's goroutine
// alone — like the dedup table and unlike the neighbourhood, which the
// console reads. That is why there is no mutex here: nothing outside
// judges a frame, and a lock would only have protected the map while
// the sessions it points at were written through anyway.
//
// Guests live in memory only. Non-guest roles are access entries and
// cross a restart through store, including an admin role earned with
// the admin password.
type acl struct {
	by    map[[meshcore.PubKeySize]byte]*client
	store SessionStore // nil keeps the table in memory only
}

func newACL(store SessionStore) *acl {
	return &acl{by: map[[meshcore.PubKeySize]byte]*client{}, store: store}
}

// persisted is one client in the shape the store keeps.
func (c *client) persisted() PersistedSession {
	p := PersistedSession{
		PubKey: c.pubKey, Perms: c.perms, Granted: c.granted,
		LastTimestamp: c.lastTimestamp, LastActive: c.lastActive,
	}
	if c.out != nil {
		p.HasOut = true
		p.OutPath = c.out.path
		p.OutPathLen = c.out.pathLen
		p.Learned = c.out.learned
	}
	return p
}

// hasAccess says whether this client belongs to the durable access
// list. Guest is the reference's zero/deleted role: it names a live
// session only and must never reach the store.
func (c *client) hasAccess() bool { return c.perms&permRoleMask != permGuest }

// errSessionsFull refuses a new session when every one of the
// table's places is held by an entry that outranks a login.
var errSessionsFull = errors.New("the session table is full of durable access entries")

// save mirrors one durable access entry to the store. Guests stop
// here: their replay guards and routes belong to the live session and
// disappear with it. The refusal is the caller's to judge, and the
// judgement differs by what was being written: a route learned may be
// lost to disk trouble and cost only a flood, while an administrator's
// replay guard that never reached disk is a command the next restart
// would let through a second time.
func (a *acl) save(c *client) error {
	if a.store == nil || !c.hasAccess() {
		return nil
	}
	return a.store.SaveSession(c.persisted())
}

// forget drops one session from the store.
func (a *acl) forget(k [meshcore.PubKeySize]byte) error {
	if a.store == nil {
		return nil
	}
	return a.store.ForgetSession(k)
}

// advance moves a session's replay guard. For an access entry it makes
// the move durable before the caller acts; for a guest the guard is
// deliberately RAM-only with the session. A store that refuses an
// access entry leaves the guard exactly where it was and the caller
// must serve nothing: an executed command whose timestamp only ever
// lived in RAM is one the next restart accepts again from a recording.
func (a *acl) advance(c *client, ts uint32, now time.Time) error {
	wasTS, wasActive := c.lastTimestamp, c.lastActive
	c.lastTimestamp, c.lastActive = ts, now
	if err := a.save(c); err != nil {
		c.lastTimestamp, c.lastActive = wasTS, wasActive
		return err
	}
	return nil
}

// load rebuilds the durable access list from the store, the secret
// recomputed per entry. Legacy guest rows are deleted instead of
// restored: loading one would turn an ephemeral session back into an
// ACL entry. secret returns the shared key for a peer, or an error a
// nil identity would raise; asks hands each restored client a fresh
// rate budget.
//
// A store that cannot be read is an error, never an empty table: the
// entries it holds carry the replay guards of every admin, and
// starting without them silently rewinds those clocks to zero.
func (a *acl) load(secret func(pubKey []byte) ([]byte, error), asks func() rateLimiter) error {
	if a.store == nil {
		return nil
	}
	rows, err := a.store.LoadSessions()
	if err != nil {
		return err
	}
	for _, p := range rows {
		if p.Perms&permRoleMask == permGuest {
			if err := a.forget(p.PubKey); err != nil {
				return fmt.Errorf("remove legacy guest %x: %w", p.PubKey[:6], err)
			}
			continue
		}
		if len(a.by) >= maxClients {
			continue
		}
		sec, err := secret(p.PubKey[:])
		if err != nil {
			continue
		}
		c := &client{
			pubKey: p.PubKey, secret: sec, perms: p.Perms, granted: p.Granted,
			lastTimestamp: p.LastTimestamp, lastActive: p.LastActive,
			asks: asks(),
		}
		if p.HasOut {
			c.out = &outPath{pathLen: p.OutPathLen, path: p.OutPath, learned: p.Learned}
		}
		a.by[p.PubKey] = c
	}
	return nil
}

// put adds or refreshes a client, making room first when the table is
// full. A non-guest reaches the store before the live table moves. A
// guest never reaches it; when a guest login demotes a durable entry,
// that entry is forgotten before the guest session replaces it.
func (a *acl) put(c *client) error {
	var victim [meshcore.PubKeySize]byte
	evicting := false
	old, known := a.by[c.pubKey]
	if !known {
		var room bool
		if victim, evicting, room = a.evictable(); !room {
			return errSessionsFull
		}
	}
	switch {
	case c.hasAccess():
		if err := a.save(c); err != nil {
			return err
		}
	case known && old.hasAccess():
		if err := a.forget(c.pubKey); err != nil {
			return err
		}
	}
	if evicting {
		delete(a.by, victim)
	}
	a.by[c.pubKey] = c
	return nil
}

// evictable names the session that would make room for a new one.
// room is false when the table is full and every place is held by a
// durable access entry. The reference skips non-guest permissions
// when it hunts for its victim (ClientACL::putClient), because a run
// of fresh guest logins must not unseat an authorised principal.
// evicting is false when there was a free place to begin with.
func (a *acl) evictable() (victim [meshcore.PubKeySize]byte, evicting, room bool) {
	if len(a.by) < maxClients {
		return victim, false, true
	}
	var when time.Time
	for k, v := range a.by {
		if v.hasAccess() {
			continue
		}
		if !room || v.lastActive.Before(when) {
			victim, when, room = k, v.lastActive, true
		}
	}
	return victim, room, room
}

// matchPrefix finds the lowest-keyed entry a prefix names; found is
// false when it names none.
func (a *acl) matchPrefix(prefix []byte) (k [meshcore.PubKeySize]byte, found bool) {
	for key := range a.by {
		if len(prefix) > len(key) {
			continue
		}
		hit := true
		for i := range prefix {
			if key[i] != prefix[i] {
				hit = false
				break
			}
		}
		if !hit {
			continue
		}
		if !found || bytes.Compare(key[:], k[:]) < 0 {
			k, found = key, true
		}
	}
	return k, found
}

// remove drops one entry from the table and the store. A store that
// refuses keeps the entry: answering "revoked" to a revocation the
// next restart undoes is the one answer worse than refusing it.
func (a *acl) remove(k [meshcore.PubKeySize]byte) error {
	if err := a.forget(k); err != nil {
		return err
	}
	delete(a.by, k)
	return nil
}

// entries is the durable access list as the console reads it. Guests
// are sessions only and therefore absent by construction.
func (a *acl) entries() []ACLEntry {
	out := make([]ACLEntry, 0, len(a.by))
	for k, c := range a.by {
		if !c.hasAccess() {
			continue
		}
		out = append(out, ACLEntry{
			PubKey: k, Perms: c.perms, Admin: c.isAdmin(),
			Granted: c.granted, LastActive: c.lastActive,
		})
	}
	return out
}

// get returns a live session by full public key; one nobody has used
// within sessionIdle is retired rather than returned.
func (a *acl) get(pubKey []byte) *client {
	var k [meshcore.PubKeySize]byte
	copy(k[:], pubKey)
	return a.live(k)
}

// live returns the client under k. An idle guest session is retired;
// a durable access entry remains authorised independently of current
// activity.
func (a *acl) live(k [meshcore.PubKeySize]byte) *client {
	c, ok := a.by[k]
	if !ok {
		return nil
	}
	if !c.hasAccess() && time.Since(c.lastActive) > sessionIdle {
		delete(a.by, k)
		return nil
	}
	return c
}

// matching returns every session whose key starts with the given hash
// — the reference's searchPeersByHash. A one-byte hash collides often;
// the MAC decides which session actually sent the packet.
func (a *acl) matching(hash byte) []*client {
	var out []*client
	for k := range a.by {
		if k[0] != hash {
			continue
		}
		if c := a.live(k); c != nil {
			out = append(out, c)
		}
	}
	return out
}
