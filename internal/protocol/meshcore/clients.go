package meshcore

// The console's window onto the session table. The table belongs to
// the pipeline's goroutine and carries live credentials, so nothing
// outside reads it directly: a snapshot is an order like any other,
// served on the pipeline's own turn — and unlike every other order it
// asks for no emission, so a dry gate serves it too.

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
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
	// prefixLen says how much of pubKey was given — a removal may
	// name its entry by prefix.
	prefixLen int
	perms     byte
	// done arbitrates the deadline: a permission change whose author
	// was told it never happened must not happen afterwards.
	done *ack
}

// ErrNoSuchEntry says a removal named nobody the table holds.
var ErrNoSuchEntry = errors.New("no such entry")

// Grant records a permission byte for a public key — the role in its
// low bits, zero taking the entry away. Granting requires the whole
// key, because a permission set to a prefix could name the wrong
// node; removal accepts a prefix, exactly as the reference's
// applyPermissions does — what is being destroyed is looked up, not
// created.
func (e *engine) Grant(pubKey []byte, perms byte) error {
	if e.id == nil {
		return errors.New("this relay has no identity — it grants nothing")
	}
	removing := perms&permRoleMask == permGuest
	if !removing && len(pubKey) != meshcore.PubKeySize {
		return fmt.Errorf("a permission needs the whole %d-byte key", meshcore.PubKeySize)
	}
	if removing && (len(pubKey) == 0 || len(pubKey) > meshcore.PubKeySize) {
		return fmt.Errorf("a removal needs 1..%d key bytes", meshcore.PubKeySize)
	}
	o := &aclOrder{perms: perms, prefixLen: len(pubKey), done: newAck()}
	copy(o.pubKey[:], pubKey)
	select {
	case e.aclAsk <- o:
	default:
		return errors.New("a permission change is already pending")
	}
	e.wakeReceiver("operator-order")
	return o.done.wait("permission change")
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
	e.wakeReceiver("operator-order")
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

// accessListBody frames the access list the way the reference's
// REQ_TYPE_GET_ACCESS_LIST answers it: seven bytes per entry — a
// six-byte key prefix and the permission byte — guests skipped like
// the reference skips them, the list cut where the reply would
// outgrow the packet. Sorted by key so two asks read alike.
func (e *engine) accessListBody(bodyMax int) []byte {
	const entrySize = 6 + 1
	// bodyMax is what the route this answer will travel can actually
	// carry, resolved by the caller: a list sized on the raw payload
	// was composed whole and then refused — twenty-five entries were
	// enough — and one sized on the direct budget was refused again
	// whenever the question had arrived flooded, since a path return
	// pays for the path it came by. Either way the asker got nothing
	// and its replay guard was already spent.
	rows := e.acl.entries()
	sort.Slice(rows, func(i, j int) bool {
		return bytes.Compare(rows[i].PubKey[:], rows[j].PubKey[:]) < 0
	})
	body := make([]byte, 0, min(len(rows), bodyMax/entrySize)*entrySize)
	for _, r := range rows {
		if r.Perms&permRoleMask == permGuest {
			continue // the reference's "skip deleted (or guest) entries"
		}
		if len(body)+entrySize > bodyMax {
			break
		}
		body = append(body, r.PubKey[:6]...)
		body = append(body, r.Perms)
	}
	return body
}

// The role words, spelled once — RoleName and RoleByte are the two
// directions of the same dictionary.
const (
	RoleAdmin     = "admin"
	RoleReadWrite = "read-write"
	RoleReadOnly  = "read-only"
	RoleGuest     = "guest"
)

// RoleByte is RoleName backwards: the byte a role's word means, for
// the channels that speak words. ok is false for a word no role
// carries.
func RoleByte(name string) (byte, bool) {
	switch name {
	case RoleAdmin:
		return permAdmin, true
	case RoleReadWrite:
		return permReadWrite, true
	case RoleReadOnly:
		return permReadOnly, true
	case RoleGuest:
		return permGuest, true
	}
	return 0, false
}

// RoleName names the role a permission byte carries — the reference's
// four, by the low two bits. The one place the words exist.
func RoleName(perms byte) string {
	switch perms & permRoleMask {
	case permAdmin:
		return RoleAdmin
	case permReadWrite:
		return RoleReadWrite
	case permReadOnly:
		return RoleReadOnly
	default:
		return RoleGuest
	}
}

// drainACLAsk serves a pending grant or revoke, on the pipeline's
// turn. The secret is computed here so a granted admin can be reached
// before it has ever logged in — the reference calcs it at setperm.
func (e *engine) drainACLAsk() {
	select {
	case o := <-e.aclAsk:
		// Claimed before it is applied: a change whose author already
		// gave up must leave the table alone, not land behind their
		// back on the strength of a deadline they saw expire.
		if !o.done.claim() {
			break
		}
		if err := e.applyGrant(o); err != nil {
			o.done.refused(err)
		} else {
			o.done.taken()
		}
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
		// removes the entry entirely — the first prefix match, as its
		// getClient answers.
		k, found := e.acl.matchPrefix(o.pubKey[:o.prefixLen])
		if !found {
			return ErrNoSuchEntry
		}
		if err := e.acl.remove(k); err != nil {
			return fmt.Errorf("the revocation would not persist: %w", err)
		}
		e.log.Info("permission revoked", zap.String("pubkey", shortKey(k[:])))
		return nil
	}
	secret, err := e.id.SharedSecret(o.pubKey[:])
	if err != nil {
		return err
	}
	// Composed on a candidate and installed only once it is durable —
	// admitLogin's discipline, for the same reason. Promoting the live
	// session first meant a disk that refused the grant left the
	// principal administering the node until the next restart, while
	// the operator read "the grant would not persist".
	var c client
	if live := e.acl.get(o.pubKey[:]); live != nil {
		c = *live
	} else {
		c.pubKey = o.pubKey
		c.asks = rateLimiter{max: e.p.SessionLimit, window: sessionLimitWindow}
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
	if err := e.acl.put(&c); err != nil {
		return fmt.Errorf("the grant would not persist: %w", err)
	}
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
	e.wakeReceiver("operator-order")
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
