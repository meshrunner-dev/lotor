package meshcore

// The session table: who is logged in, and what each of them may
// still ask before the node stops answering.

import (
	"bytes"
	"time"

	"meshrunner.dev/pkg/meshcore"
)

// PersistedSession is one session as a store keeps it — the fields
// that must outlive a restart, the shared secret excluded because it
// is recomputed from the node identity and the peer key.
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

// SessionStore persists the session table, so a companion — an admin
// above all — is not asked to log in again on every bounce, and its
// replay guard is not reset to zero. Nil keeps the table in memory,
// the posture for a relay with no store behind it.
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
	// granted marks a permission set explicitly, by an admin — the
	// reference's persisted contact, distinct from a login that a
	// password opened. It survives idle: a granted admin is meant to
	// stay one whether or not it happens to be talking right now.
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
// Sessions live in memory only. A restart asks every companion to log
// in again, which is the honest posture for a credential nothing here
// persists.
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

// save mirrors one session to the store; a store that refuses does
// not fail the session — the reference keeps a companion whose flash
// write failed, and losing it over disk trouble would be worse than
// forgetting it on the next restart.
func (a *acl) save(c *client) {
	if a.store != nil {
		_ = a.store.SaveSession(c.persisted())
	}
}

// forget drops one session from the store.
func (a *acl) forget(k [meshcore.PubKeySize]byte) {
	if a.store != nil {
		_ = a.store.ForgetSession(k)
	}
}

// load rebuilds the table from the store, the secret recomputed per
// session and the too-stale left behind. secret returns the shared
// key for a peer, or an error a nil identity would raise; asks hands
// each restored session a fresh rate budget — the zero-value limiter
// grants nothing, which is exactly the silent no-answer a restored
// session must not wake up into.
func (a *acl) load(secret func(pubKey []byte) ([]byte, error), asks func() rateLimiter) {
	if a.store == nil {
		return
	}
	rows, err := a.store.LoadSessions()
	if err != nil {
		return
	}
	for _, p := range rows {
		if !p.Granted && time.Since(p.LastActive) > sessionIdle {
			a.forget(p.PubKey)
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
}

// put adds or refreshes a session, evicting the least recently active
// one when the table is full.
func (a *acl) put(c *client) {
	if _, known := a.by[c.pubKey]; !known {
		a.evict()
	}
	a.by[c.pubKey] = c
	a.save(c)
}

// evict drops the least recently active session when the table is
// full, forgetting it from the store too — an evicted session must
// not resurrect on restart.
func (a *acl) evict() {
	if len(a.by) < maxClients {
		return
	}
	var oldest [meshcore.PubKeySize]byte
	var when time.Time
	first := true
	for k, v := range a.by {
		if first || v.lastActive.Before(when) {
			oldest, when, first = k, v.lastActive, false
		}
	}
	delete(a.by, oldest)
	a.forget(oldest)
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

// remove drops one entry from the table and the store.
func (a *acl) remove(k [meshcore.PubKeySize]byte) {
	delete(a.by, k)
	a.forget(k)
}

// entries is the access list as the console reads it: grants first
// by their nature, then whoever is merely logged in.
func (a *acl) entries() []ACLEntry {
	out := make([]ACLEntry, 0, len(a.by))
	for k, c := range a.by {
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

// live returns the session under k, dropping it when it has gone
// quiet. The caller holds mu.
func (a *acl) live(k [meshcore.PubKeySize]byte) *client {
	c, ok := a.by[k]
	if !ok {
		return nil
	}
	if !c.granted && time.Since(c.lastActive) > sessionIdle {
		delete(a.by, k)
		a.forget(k)
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
