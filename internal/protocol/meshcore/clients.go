package meshcore

// The console's window onto the session table. The table belongs to
// the pipeline's goroutine and carries live credentials, so nothing
// outside reads it directly: a snapshot is an order like any other,
// served on the pipeline's own turn — and unlike every other order it
// asks for no emission, so a dry gate serves it too.

import (
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"
)

// ClientSession is one logged-in companion, as an operator may see
// it: who, how it is reached, and how fresh the conversation is. No
// secret leaves the pipeline with it.
type ClientSession struct {
	PubKey [meshcore.PubKeySize]byte
	Admin  bool
	// Path is the route home the client taught us, one hash byte per
	// hop; HasPath false means it has not, and answers flood. The two
	// are distinct on purpose: a zero-hop path says the client is
	// adjacent, which is not the same as not knowing.
	Path        []byte
	HasPath     bool
	PathLearned time.Time
	LastActive  time.Time
}

// sessionsOrder asks the pipeline for a snapshot of the client table.
type sessionsOrder struct {
	reply chan []ClientSession
}

// aclOrder carries a permission change into the pipeline's goroutine,
// which owns the table. The perms byte is the reference's own: the
// role in the low two bits, a guest role meaning removal — setperm's
// contract, where a guest is not persisted.
type aclOrder struct {
	pubKey [meshcore.PubKeySize]byte
	perms  byte
	reply  chan error
}

// Grant records a permission byte for a public key — the role in its
// low bits, zero taking the entry away. The full key is required: a
// permission set to a prefix could name the wrong node.
func (e *engine) Grant(pubKey []byte, perms byte) error {
	if e.id == nil {
		return errors.New("this relay has no identity — it grants nothing")
	}
	if len(pubKey) != meshcore.PubKeySize {
		return fmt.Errorf("a permission needs the whole %d-byte key", meshcore.PubKeySize)
	}
	o := &aclOrder{perms: perms, reply: make(chan error, 1)}
	copy(o.pubKey[:], pubKey)
	select {
	case e.aclAsk <- o:
	default:
		return errors.New("a permission change is already pending")
	}
	e.wakeReceiver()
	select {
	case err := <-o.reply:
		return err
	case <-time.After(askWait):
		return errors.New("the relay never picked the permission change up")
	}
}

// ACLEntry is one grant or session, as the console shows the access
// list: who, what role, whether it was granted or merely logged in,
// and how fresh.
type ACLEntry struct {
	PubKey [meshcore.PubKeySize]byte
	// Perms is the byte as stored — the reference's vocabulary, the
	// role in the low two bits.
	Perms      byte
	Admin      bool
	Granted    bool
	LastActive time.Time
}

// AccessList reports the grants and live sessions — any goroutine.
func (e *engine) AccessList() ([]ACLEntry, error) {
	o := &aclListOrder{reply: make(chan []ACLEntry, 1)}
	select {
	case e.aclListAsk <- o:
	default:
		return nil, errors.New("an access-list snapshot is already pending")
	}
	e.wakeReceiver()
	select {
	case rows := <-o.reply:
		return rows, nil
	case <-time.After(askWait):
		return nil, errors.New("the relay never picked the access-list snapshot up")
	}
}

// aclListOrder asks the pipeline for the access list.
type aclListOrder struct {
	reply chan []ACLEntry
}

// RoleName names the role a permission byte carries — the reference's
// four, by the low two bits. The one place the words exist.
func RoleName(perms byte) string {
	switch perms & permRoleMask {
	case permAdmin:
		return "admin"
	case permReadWrite:
		return "read-write"
	case permReadOnly:
		return "read-only"
	default:
		return "guest"
	}
}

// drainACLAsk serves a pending grant or revoke, on the pipeline's
// turn. The secret is computed here so a granted admin can be reached
// before it has ever logged in — the reference calcs it at setperm.
func (e *engine) drainACLAsk() {
	select {
	case o := <-e.aclAsk:
		o.reply <- e.applyGrant(o)
	default:
	}
	select {
	case o := <-e.aclListAsk:
		o.reply <- e.acl.entries()
	default:
	}
}

// applyGrant carries out one grant or revoke on the table the
// pipeline owns.
func (e *engine) applyGrant(o *aclOrder) error {
	if o.perms&permRoleMask == permGuest {
		// A guest role is not a grant; setting it, like the reference,
		// removes the entry entirely.
		e.acl.remove(o.pubKey)
		e.log.Info("permission revoked", zap.String("pubkey", shortKey(o.pubKey[:])))
		return nil
	}
	secret, err := e.id.SharedSecret(o.pubKey[:])
	if err != nil {
		return err
	}
	c := e.acl.get(o.pubKey[:])
	if c == nil {
		c = &client{asks: rateLimiter{max: e.p.SessionLimit, window: sessionLimitWindow}}
		c.pubKey = o.pubKey
	}
	c.secret = secret
	// The whole byte, as the reference stores it — upper bits are
	// future capability flags, and flattening them here would erase
	// what a companion asked for.
	c.perms = o.perms
	c.granted = true
	if c.lastActive.IsZero() {
		c.lastActive = time.Now()
	}
	e.acl.put(c)
	e.log.Info("permission granted",
		zap.String("pubkey", shortKey(o.pubKey[:])), zap.Bool("admin", c.isAdmin()))
	return nil
}

// ClientSessions reports the logged-in companions — any goroutine.
func (e *engine) ClientSessions() ([]ClientSession, error) {
	o := &sessionsOrder{reply: make(chan []ClientSession, 1)}
	select {
	case e.sessionsAsk <- o:
	default:
		return nil, errors.New("a session snapshot is already pending")
	}
	e.wakeReceiver()
	select {
	case rows := <-o.reply:
		return rows, nil
	case <-time.After(askWait):
		return nil, errors.New("the relay never picked the session snapshot up" +
			" — see \"status\" for what it is doing")
	}
}

// drainSessionsAsk serves a pending snapshot, on the pipeline's turn.
func (e *engine) drainSessionsAsk(now time.Time) {
	select {
	case o := <-e.sessionsAsk:
		o.reply <- e.acl.sessions(now)
	default:
	}
}

// sessions renders the table for the snapshot. Idle entries are
// skipped rather than retired: a read must not change what it reads.
func (a *acl) sessions(now time.Time) []ClientSession {
	out := make([]ClientSession, 0, len(a.by))
	for _, c := range a.by {
		if now.Sub(c.lastActive) > sessionIdle {
			continue
		}
		row := ClientSession{
			PubKey:     c.pubKey,
			Admin:      c.isAdmin(),
			LastActive: c.lastActive,
		}
		if c.out != nil {
			row.HasPath = true
			row.Path = append([]byte(nil), c.out.path...)
			row.PathLearned = c.out.learned
		}
		out = append(out, row)
	}
	return out
}
