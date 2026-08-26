package meshcore

// The session table: who is logged in, and what each of them may
// still ask before the node stops answering.

import (
	"time"

	"meshrunner.dev/pkg/meshcore"
)

// client is one logged-in companion.
type client struct {
	pubKey [meshcore.PubKeySize]byte
	secret []byte
	perms  byte
	// lastTimestamp is the newest request instant this client has
	// signed: anything at or before it is a replay.
	lastTimestamp uint32
	lastActive    time.Time
	// asks bounds what this session may make us emit.
	asks rateLimiter
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
	by map[[meshcore.PubKeySize]byte]*client
}

func newACL() *acl {
	return &acl{by: map[[meshcore.PubKeySize]byte]*client{}}
}

// put adds or refreshes a session, evicting the least recently active
// one when the table is full.
func (a *acl) put(c *client) {
	if _, known := a.by[c.pubKey]; !known {
		evictOldest(a.by, maxClients, func(v *client) time.Time { return v.lastActive })
	}
	a.by[c.pubKey] = c
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
	if time.Since(c.lastActive) > sessionIdle {
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
