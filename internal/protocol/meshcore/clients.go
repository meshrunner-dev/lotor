package meshcore

// The console's window onto the client table. The mutable table and
// its credentials belong to the pipeline goroutine; readers receive a
// complete immutable edition containing no secret. A display read is
// therefore never an order and never interrupts radio reception.

import (
	"bytes"
	"errors"
	"fmt"
	"sort"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/bus"
)

// ClientSession is one logged-in companion, as an operator may see
// it: who, how it is reached, and how fresh the conversation is. No
// secret leaves the pipeline with it.
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

// ClientSnapshot is one coherent outside edition of both client
// surfaces. Generation increases whenever the pipeline publishes a
// changed access, activity, route or session membership state.
type ClientSnapshot struct {
	Generation uint64
	Access     []ACLEntry
	Sessions   []ClientSession
}

// sessionCloseOrder asks the pipeline to end one live conversation
// without necessarily taking back the durable role behind it.
type sessionCloseOrder struct {
	pubKey [meshcore.PubKeySize]byte
	done   *ack
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

// ErrNoSuchSession says a close named no currently active session.
var ErrNoSuchSession = errors.New("no such active session")

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
	e.wakeReceiver("acl-change")
	return o.done.wait("permission change")
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

// AccessList reports durable non-guest authorisations — any goroutine.
func (e *engine) AccessList() ([]ACLEntry, error) {
	return e.Clients().Access, nil
}

// accessListBody frames the access list the way the reference's
// REQ_TYPE_GET_ACCESS_LIST answers it: seven bytes per entry — a
// six-byte key prefix and the permission byte — the list cut where
// the reply would outgrow the packet. entries has already excluded
// guests. Sorted by key so two asks read alike.
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
		e.publishClientView(time.Now(), true)
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
	e.publishClientView(time.Now(), true)
	e.log.Info("permission granted",
		zap.String("pubkey", shortKey(o.pubKey[:])), zap.Bool("admin", c.isAdmin()))
	return nil
}

// ClientSessions reports the logged-in companions — any goroutine.
func (e *engine) ClientSessions() ([]ClientSession, error) {
	return e.Clients().Sessions, nil
}

// CloseSession ends one live over-the-air conversation. A durable ACL
// entry remains authorised and may log in again; a guest disappears
// entirely. The whole key is required because closing the wrong live
// principal is no more recoverable than granting the wrong one.
func (e *engine) CloseSession(pubKey []byte) error {
	if len(pubKey) != meshcore.PubKeySize {
		return fmt.Errorf("a session close needs the whole %d-byte key", meshcore.PubKeySize)
	}
	o := &sessionCloseOrder{done: newAck()}
	copy(o.pubKey[:], pubKey)
	select {
	case e.sessionCloseAsk <- o:
	default:
		return errors.New("a session close is already pending")
	}
	e.wakeReceiver("session-close")
	return o.done.wait("session close")
}

// drainSessionCloseAsk serves a pending close on the pipeline's turn.
func (e *engine) drainSessionCloseAsk() {
	select {
	case o := <-e.sessionCloseAsk:
		if !o.done.claim() {
			break
		}
		if err := e.acl.closeSession(o.pubKey); err != nil {
			if !errors.Is(err, ErrNoSuchSession) {
				err = fmt.Errorf("the session close would not persist: %w", err)
			}
			o.done.refused(err)
		} else {
			e.publishClientView(time.Now(), true)
			e.log.Info("session closed", zap.String("pubkey", shortKey(o.pubKey[:])))
			o.done.taken()
		}
	default:
	}
}

// sessions renders the active table. Expiry is performed by the
// engine clock before publication, never by this read.
func (a *acl) sessions() []ClientSession {
	out := make([]ClientSession, 0, len(a.by))
	for _, c := range a.by {
		if !c.active {
			continue
		}
		row := ClientSession{
			PubKey:     c.pubKey,
			Perms:      c.perms,
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

// Clients returns a detached copy of the latest coherent client
// edition — any goroutine. Path bytes are copied again because the
// caller is free to retain and edit what it receives.
func (e *engine) Clients() ClientSnapshot {
	v := e.clientView.Load()
	if v == nil {
		return ClientSnapshot{}
	}
	return ClientSnapshot{
		Generation: v.Generation,
		Access:     append([]ACLEntry(nil), v.Access...),
		Sessions:   cloneClientSessions(v.Sessions),
	}
}

func cloneClientSessions(in []ClientSession) []ClientSession {
	out := make([]ClientSession, len(in))
	copy(out, in)
	for i := range out {
		out[i].Path = append([]byte(nil), out[i].Path...)
	}
	return out
}

// publishClientView installs one complete immutable edition. notify
// wakes views only after Run-time changes; construction and store load
// establish their initial edition before any reader can subscribe.
func (e *engine) publishClientView(at time.Time, notify bool) {
	e.clientGeneration++
	v := &ClientSnapshot{
		Generation: e.clientGeneration,
		Access:     e.acl.entries(),
		Sessions:   e.acl.sessions(),
	}
	sort.Slice(v.Access, func(i, j int) bool {
		return bytes.Compare(v.Access[i].PubKey[:], v.Access[j].PubKey[:]) < 0
	})
	sort.Slice(v.Sessions, func(i, j int) bool {
		return bytes.Compare(v.Sessions[i].PubKey[:], v.Sessions[j].PubKey[:]) < 0
	})
	e.clientView.Store(v)
	if !notify || e.bus == nil {
		return
	}
	if at.IsZero() {
		at = time.Now()
	}
	e.bus.Publish(bus.SessionsChanged{
		Relay: e.relay, At: at, Generation: v.Generation,
	})
}

// advanceClient moves the replay clock and publishes the resulting
// activity only after a durable entry's store accepted it.
func (e *engine) advanceClient(c *client, ts uint32, now time.Time) error {
	wasActive := c.active
	c.active = true
	if err := e.acl.advance(c, ts, now); err != nil {
		c.active = wasActive
		return err
	}
	e.publishClientView(now, true)
	return nil
}

// clientSessionWake names the earliest active session deadline.
func (e *engine) clientSessionWake(now time.Time) (time.Duration, bool) {
	var wait time.Duration
	set := false
	for _, c := range e.acl.by {
		if !c.active {
			continue
		}
		candidate := max(time.Duration(0), c.lastActive.Add(sessionIdle).Sub(now))
		if !set || candidate < wait {
			wait, set = candidate, true
		}
	}
	return wait, set
}

// expireClientSessions applies the deadline under pipeline ownership.
// Guests disappear with their derived credential; durable principals
// merely leave the live-session view and remain authorised.
func (e *engine) expireClientSessions(now time.Time) bool {
	changed := false
	for k, c := range e.acl.by {
		if !c.active || now.Before(c.lastActive.Add(sessionIdle)) {
			continue
		}
		if c.hasAccess() {
			c.active = false
		} else {
			delete(e.acl.by, k)
		}
		changed = true
	}
	if changed {
		e.publishClientView(now, true)
	}
	return changed
}
