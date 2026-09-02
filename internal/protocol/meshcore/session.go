package meshcore

import (
	"crypto/subtle"
	"encoding/hex"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/correlation"
	"meshrunner.dev/lotor/internal/meshcorehost"
)

// Client sessions, the reference repeater's shape. A companion logs in
// with a password or a durable key grant and may then ask authenticated
// questions. Admin sessions also carry the reference's CLI over the
// air. Guest is the one ephemeral role; every non-guest role belongs
// to the durable access list.
const (
	// The reference's ClientACL roles — PERM_ACL_* in ClientACL.h —
	// living in the low two bits of the permission byte. They are wire
	// units, so the library owns them; these names keep this package's
	// spelling. Guest is not persisted: setting it is how an entry is
	// removed. Admin is admin at exactly three, never at "non-zero".
	permRoleMask  = meshcore.PermRoleMask
	permGuest     = meshcore.PermGuest
	permReadOnly  = meshcore.PermReadOnly
	permReadWrite = meshcore.PermReadWrite
	permAdmin     = meshcore.PermAdmin

	// PermRoleMask and the Perm* roles are the same ladder for the
	// callers that speak grants — one vocabulary, defined once, so an
	// edit that changes a role changes it everywhere or nowhere.
	PermRoleMask  = permRoleMask
	PermGuest     = permGuest
	PermReadOnly  = permReadOnly
	PermReadWrite = permReadWrite
	PermAdmin     = permAdmin

	// firmwareVerLevel tells a companion which reply fields to expect;
	// 2 is the level whose shapes this engine answers with.
	firmwareVerLevel = 2

	// TelemChannelSelf is the LPP channel a node's own readings ride.
	// Exported because the daemon numbers its parts' channels after
	// it, and two constants for one number drift apart in silence.
	TelemChannelSelf = 1

	// maxClients bounds the session table; the least recently active
	// session makes room for a new one.
	maxClients = 32

	// A logged-in client's budget — charged only on the answers that
	// flood, which the reference does not bound and we do: a client
	// that never taught a route home makes every answer cross the
	// whole mesh, and one poller in a loop would spend everyone's
	// airtime. An answer down a taught route costs one directed
	// emission and is never charged, exactly as freely as the
	// reference serves it. session_limit moves the figure.
	sessionLimitMax    = 6
	sessionLimitWindow = time.Minute

	// sessionIdle and loginMaxSkew are the kernel's clocks, spelled
	// the way this engine always has.
	sessionIdle  = meshcorehost.SessionIdle
	loginMaxSkew = meshcorehost.LoginMaxSkew
)

// respondLogin answers a password attempt. Unlike the other anonymous
// questions this one is served whatever the inbound route — a
// companion that has not found a path yet floods it.
func (e *engine) respondLogin(rx *reception, senderPub, secret, plain []byte, origin correlation.ID) {
	pkt := rx.pkt
	// Named permissively or not at all: an access mode nobody resolved
	// is a door nobody opened. The admin word is its own door, though:
	// a site that administers from the field without letting guests
	// read anything is an ordinary posture, and gating logins on the
	// guest mode alone would lock its owner out.
	// A known key may always perform the reference's blank-password
	// recheck. That is what makes an explicit read-only grant useful
	// without sharing either configured password; the configured doors
	// only decide whether an unknown key may create a session.
	known := e.acl.Get(senderPub) != nil
	if !known && e.p.AdminPassword == "" &&
		e.p.GuestAccess != guestPassword && e.p.GuestAccess != guestOpen {
		e.responseSuppressed(origin, "login", "access-closed")
		return
	}
	// No limiter here, deliberately — the reference has none either.
	// A failed attempt is answered with silence, so a guesser earns
	// no emission and pays the airtime of every try; charging logins
	// was bounding nothing but the honest companion, whose retry
	// burst after a restart locked it out for minutes.
	ts, password, err := meshcore.AnonPassword(plain)
	if err != nil {
		e.responseSuppressed(origin, "login", "malformed")
		return
	}
	// A session that does not survive a restart is a session an
	// attacker can resurrect by replaying the login that made it,
	// rolling its replay clock back to the capture. Nothing in the
	// packet says how old it is, so our own clock does: a login
	// stamped far from now is a recording, not a request. The window
	// is generous — a companion's clock is its own — but finite,
	// which is the part the reference's RTC-less nodes cannot afford.
	if meshcorehost.Skewed(ts, time.Now()) {
		e.log.Debug("login refused: stale or future timestamp",
			zap.String("corr", origin.Short()), zap.Uint32("timestamp", ts))
		return
	}
	c := e.admitLogin(senderPub, secret, password, ts, origin)
	if c == nil {
		return
	}
	// Built before the session moves: a failure here would otherwise
	// leave the client logged in at a timestamp it never heard back
	// from, and its retry refused as a replay.
	body, err := loginReply(c)
	if err != nil {
		e.log.Warn("login reply abandoned", zap.String("corr", origin.Short()), zap.Error(err))
		return
	}

	c.LastTimestamp, c.LastActive = ts, time.Now()
	c.Asks = rateLimiter{Max: e.p.SessionLimit, Window: sessionLimitWindow}
	c.Active = true
	// The session is only real once the table takes it. A non-guest
	// login reaches the access store first; a guest deliberately stays
	// in RAM and must log in again after a restart.
	if err := e.acl.Put(c); err != nil {
		e.log.Warn("login refused: the session table would not take it",
			zap.String("corr", origin.Short()),
			zap.String("pubkey", shortKey(c.PubKey[:])), zap.Error(err))
		return
	}
	e.publishClientView(c.LastActive, true)

	role := "guest"
	if c.IsAdmin() {
		role = "admin"
	}
	e.log.Info(role+" logged in", zap.String("corr", origin.Short()),
		zap.String("pubkey", shortKey(c.PubKey[:])))
	// A login reply echoes no tag: the reference puts its own clock in
	// that position, so the frame's timestamp is the clock and the
	// body is what follows it.
	clock, rest, err := meshcore.UnframeAdmin(body)
	if err != nil {
		return
	}
	e.reply(pkt, meshcorehost.Answer{
		DestHash: c.PubKey[:meshcore.PathHashSize], Secret: c.Secret,
		Tag: clock, Body: rest, Scope: e.replyScope(rx), Out: c.Out,
	}, "login-resp", origin)
}

// admitLogin resolves a password attempt into a session, or nil when
// it earns silence — the kernel's judgement, behind this node's two
// doors: the admin word, checked first, because with an open guest
// door every password admits someone and the roles must not depend on
// which arm of a switch ran first; then the guest door, open or by
// its own word.
func (e *engine) admitLogin(senderPub, secret []byte, password string,
	ts uint32, origin correlation.ID,
) *client {
	doors := func(word string) (byte, bool) {
		switch {
		case e.p.AdminPassword != "" &&
			subtle.ConstantTimeCompare([]byte(word), []byte(e.p.AdminPassword)) == 1:
			return permAdmin, true
		case e.p.GuestAccess == guestOpen ||
			subtle.ConstantTimeCompare([]byte(word), []byte(e.p.GuestPassword)) == 1:
			return permGuest, true
		}
		return 0, false
	}
	c, refusal := meshcorehost.Admit(e.acl.Get(senderPub), senderPub, secret, password, ts, doors)
	switch refusal {
	case meshcorehost.RefusedWord:
		e.log.Debug("login refused", zap.String("corr", origin.Short()))
	case meshcorehost.RefusedReplay:
		e.log.Debug("login replay refused", zap.String("corr", origin.Short()))
	case meshcorehost.RefusedSkew:
		// Judged before admission, in respondLogin; named here so the
		// switch says every refusal the kernel can pronounce.
		e.log.Debug("login refused: stale or future timestamp", zap.String("corr", origin.Short()))
	}
	return c
}

// loginReply composes what the reference sends back, at the reply
// level this engine answers at.
func loginReply(c *client) ([]byte, error) {
	return meshcorehost.LoginReply(c, firmwareVerLevel, time.Now())
}

// reqVerdict judges an authenticated request: ours to read only when a
// live session's MAC verifies over it.
func (e *engine) reqVerdict(rx *reception) (verdict, why string, handled bool) {
	c, plain := e.openReq(rx.pkt)
	if c == nil {
		return "", "", false // not ours, or no session: route it on
	}
	// The MAC sweep this took is kept for the answer.
	rx.opened = &opened{session: c, secret: c.Secret, plain: plain}
	return verdictRequest, "authenticated request", true
}

// openReq finds the session that sent a REQ and returns its decrypted
// content. The source hash narrows the candidates; the MAC decides.
func (e *engine) openReq(pkt *meshcore.Packet) (*client, []byte) {
	return e.acl.OpenSession(e.id, pkt.Payload)
}

// respondRequest serves one authenticated request.
func (e *engine) respondRequest(rx *reception, origin correlation.ID) {
	if rx.opened == nil || rx.opened.session == nil {
		return
	}
	pkt, c, plain := rx.pkt, rx.opened.session, rx.opened.plain
	ts, args, err := meshcore.UnframeAdmin(plain)
	if err != nil {
		e.responseSuppressed(origin, "session", "malformed")
		return
	}
	if ts <= c.LastTimestamp {
		e.log.Debug("request replay refused", zap.String("corr", origin.Short()))
		return
	}
	// The budget charges only the answers that flood — the reference
	// limits nothing here, and what justifies a bound at all is the
	// amplification: a client that never taught a route home makes
	// every answer cross the whole mesh. One that did costs a single
	// directed emission, and flows as freely as the reference lets it.
	if c.Out == nil && !c.Asks.Allow(time.Now()) {
		e.log.Debug("session rate-limited — flood answers", zap.String("corr", origin.Short()))
		e.dropRateLimited(origin)
		return
	}
	// The guard moves before the answer is served, and durably: a
	// request answered on a timestamp that never reached the disk is
	// one a recording replays after the next restart. A question we
	// do not answer still moves it — the keep-alive exists for
	// exactly that, and retiring the companion that sends one instead
	// of polling would be perverse.
	if err := e.advanceClient(c, ts, time.Now()); err != nil {
		e.storeRefused(origin, "request", err)
		return
	}
	// The budget is the route's, resolved before a byte is composed:
	// a question that arrived flooded is answered inside a path
	// return that pays for the path it came by, and that envelope is
	// far smaller than a direct response. Sizing on the direct budget
	// and discovering the route afterwards produced answers that
	// could not be sealed — with the replay guard already spent, so
	// the client's identical retry was refused as a replay.
	log := e.log.With(zap.String("corr", origin.Short()))
	body, answered := e.answerRequest(c, args, e.answerBudget(pkt), log)
	if !answered {
		request := "empty"
		if len(args) > 0 {
			request = reqName(args[0])
		}
		e.responseSuppressed(origin, request, "unsupported-or-forbidden")
		return // nothing to say, but the session lives on
	}
	if len(args) > 0 {
		log.Debug("session request answered",
			zap.String("request", reqName(args[0])), zap.Int("body_bytes", len(body)))
	}

	// Every response is tagged with the asker's own timestamp, so a
	// companion can match answers to questions.
	e.reply(pkt, meshcorehost.Answer{
		DestHash: c.PubKey[:meshcore.PathHashSize], Secret: c.Secret,
		Tag: ts, Body: body, Scope: e.replyScope(rx), Out: c.Out,
	}, "req-resp", origin)
}

// answerRequest builds the body of an authenticated answer. answered
// is false for a question this node does not serve — which is not the
// same as an answer that happens to be empty: a node with no sensor
// still owes the asker a reply saying so.
func (e *engine) answerRequest(c *client, args []byte, budget int,
	log *zap.Logger,
) (body []byte, answered bool) {
	switch args[0] {
	case meshcore.ReqGetStatus:
		return e.statusBody(), true
	case meshcore.ReqGetAccessList:
		// Admin only, both reserved bytes zero — the reference's
		// exact gate; anyone else earns the silence a question this
		// node does not serve earns.
		if !c.IsAdmin() || len(args) < 3 || args[1] != 0 || args[2] != 0 {
			return nil, false
		}
		return e.accessListBody(budget), true
	case meshcore.ReqGetTelemetry:
		// The first reserved byte is an inverse permission mask, and
		// a guest is forced past it to the base readings alone — the
		// reference's gate, plumbed before the sensors exist so that
		// work lands on a correct contract.
		mask := byte(0xFF)
		if len(args) >= 2 {
			mask = ^args[1]
		}
		if c.Perms&permRoleMask == permGuest {
			mask = 0
		}
		return e.telemetryBodyLogged(log, mask, budget), true
	case meshcore.ReqGetNeighbours:
		b := e.neighboursBody(args, budget)
		return b, b != nil
	case meshcore.ReqGetOwnerInfo:
		// Three lines, the reference's own: the firmware version,
		// the node's name, then whatever the owner wrote — which
		// carries its own newlines, and is last so they cannot be
		// mistaken for a fourth field.
		return withinBudget(
			e.firmware+"\n"+e.p.NodeName+"\n"+e.p.OwnerInfo, budget), true
	case meshcore.ReqKeepAlive:
		// The reference answers nothing here either, and the session's
		// clock has already moved on the request that carried it.
		return nil, false
	default:
		return nil, false
	}
}

// shortKey is a public key's readable prefix.
func shortKey(k []byte) string {
	return hex.EncodeToString(k[:min(6, len(k))])
}

// queueLen reports the outbound backlog for the status answer.
func (e *engine) queueLen() int {
	if e.queue == nil {
		return 0
	}
	return len(e.queue.entries)
}

// dropRateLimited counts a refusal that never became a packet.
func (e *engine) dropRateLimited(origin correlation.ID) {
	e.bus.Publish(bus.TxDropped{
		Relay: e.relay, Correlation: origin, At: time.Now(), Reason: reasonRateLimited,
		Kind: "answer",
	})
}

// storeRefused reports work abandoned because the replay guard could
// not be made durable. Serving it anyway would leave the mesh with an
// answer this node cannot promise not to give twice.
func (e *engine) storeRefused(origin correlation.ID, what string, err error) {
	e.log.Warn("access store refused the replay guard — "+what+" not served",
		zap.String("corr", origin.Short()), zap.Error(err))
	e.bus.Publish(bus.TxDropped{
		// Keep the recorded reason stable for existing journal and
		// metrics consumers; only the log prose adopts the clearer name.
		Relay: e.relay, Correlation: origin, At: time.Now(), Reason: "session-store", Kind: "answer",
	})
}
