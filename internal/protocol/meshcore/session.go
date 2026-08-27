package meshcore

import (
	"crypto/subtle"
	"encoding/hex"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/txn"
	"meshrunner.dev/lotor/internal/version"
)

// Client sessions, the reference repeater's shape. A companion logs in
// with a password and may then ask a handful of authenticated
// questions: how the node is doing, what it hears, who it is.
//
// Only the guest role exists here. The reference also serves an admin
// role — settings, access lists, a whole CLI over the air — and this
// daemon deliberately declines that: administration goes through the
// local console socket, where the operating system's own permissions
// are the authentication, not a password crossing a shared band.
const (
	permGuest    = 0
	permAdmin    = 3
	permRoleMask = 3

	// firmwareVerLevel tells a companion which reply fields to expect;
	// 2 is the level whose shapes this engine answers with.
	firmwareVerLevel = 2

	// telemChannelSelf is the LPP channel a node's own readings ride.
	telemChannelSelf = 1

	// maxClients bounds the session table; the least recently active
	// session makes room for a new one.
	maxClients = 32

	// A logged-in guest gets its own budget, which the reference does
	// without. Two reasons to keep it. A client is answered directly
	// only once it has taught us a route home, and until then — for a
	// companion that never sends one, always — every answer floods and
	// one poller in a loop would spend the whole mesh's airtime. And
	// even a direct answer costs an emission at every repeater along
	// the way, so unbounded questioning is never free. Generous enough
	// for a status page and a neighbourhood query in the same breath,
	// and session_limit moves it for a site that wants otherwise.
	sessionLimitMax    = 6
	sessionLimitWindow = time.Minute

	// sessionIdle retires a session nobody has used. The secret it
	// holds is derived from two long-term keys, so nothing is lost by
	// deriving it again — and a table of live credentials should not
	// outlive the conversations that made it.
	sessionIdle = time.Hour

	// Login attempts are bounded on their own — separate from the
	// anonymous questions, so a password guesser cannot starve the
	// name lookups, and slower than the reference, which bounds them
	// not at all.
	loginLimitMax    = 4
	loginLimitWindow = 3 * time.Minute

	// loginMaxSkew bounds how far a login's own timestamp may sit from
	// ours before we read it as a recording rather than a request.
	loginMaxSkew = 24 * time.Hour
)

// respondLogin answers a password attempt. Unlike the other anonymous
// questions this one is served whatever the inbound route — a
// companion that has not found a path yet floods it.
func (e *engine) respondLogin(rx *reception, senderPub, secret, plain []byte, origin txn.ID) {
	pkt := rx.pkt
	// Named permissively or not at all: an access mode nobody resolved
	// is a door nobody opened.
	if e.p.GuestAccess != guestPassword && e.p.GuestAccess != guestOpen {
		return
	}
	// Charged before the password is even read: a limiter that only
	// sees successes bounds the honest client and lets the guesser
	// run free — the exact inverse of what it is for.
	if !e.limits.login.allow(time.Now()) {
		e.log.Debug("login rate-limited", zap.String("txn", origin.Short()))
		e.dropRateLimited(origin)
		return
	}
	ts, password, err := meshcore.AnonPassword(plain)
	if err != nil {
		return
	}
	// A session that does not survive a restart is a session an
	// attacker can resurrect by replaying the login that made it,
	// rolling its replay clock back to the capture. Nothing in the
	// packet says how old it is, so our own clock does: a login
	// stamped far from now is a recording, not a request. The window
	// is generous — a companion's clock is its own — but finite,
	// which is the part the reference's RTC-less nodes cannot afford.
	if skew := time.Since(time.Unix(int64(ts), 0)); skew > loginMaxSkew || skew < -loginMaxSkew {
		e.log.Debug("login refused: stale or future timestamp",
			zap.String("txn", origin.Short()), zap.Duration("skew", skew))
		return
	}
	c := e.acl.get(senderPub)
	switch {
	case password == "" && c != nil:
		// A blank password re-checks an existing session.
	case e.p.GuestAccess == guestOpen ||
		subtle.ConstantTimeCompare([]byte(password), []byte(e.p.GuestPassword)) == 1:
		if c == nil {
			c = &client{perms: permGuest}
			copy(c.pubKey[:], senderPub)
		}
		c.secret = secret
	default:
		e.log.Debug("login refused", zap.String("txn", origin.Short()))
		return
	}
	if ts <= c.lastTimestamp {
		e.log.Debug("login replay refused", zap.String("txn", origin.Short()))
		return
	}
	// Built before the session moves: a failure here would otherwise
	// leave the client logged in at a timestamp it never heard back
	// from, and its retry refused as a replay.
	body, err := loginReply(c)
	if err != nil {
		e.log.Warn("login reply abandoned", zap.Error(err))
		return
	}

	c.lastTimestamp, c.lastActive = ts, time.Now()
	c.asks = rateLimiter{max: e.p.SessionLimit, window: sessionLimitWindow}
	e.acl.put(c)

	e.log.Info("guest logged in", zap.String("txn", origin.Short()),
		zap.String("pubkey", shortKey(c.pubKey[:])))
	// A login reply echoes no tag: the reference puts its own clock in
	// that position, so the frame's timestamp is the clock and the
	// body is what follows it.
	clock, rest, err := meshcore.UnframeAdmin(body)
	if err != nil {
		return
	}
	e.reply(pkt, answer{
		destHash: c.pubKey[:meshcore.PathHashSize], secret: c.secret,
		tag: clock, body: rest, kind: "login-resp",
		scope: e.replyScope(rx), out: c.out,
	}, origin)
}

// loginReply composes what the reference sends back: our clock, the
// verdict, its legacy keep-alive hint, the role, the permissions, a
// random blob so two logins never hash alike, and the reply level we
// answer at.
func loginReply(c *client) ([]byte, error) {
	return meshcore.FrameLoginReply(meshcore.LoginReply{
		Clock:         uint32(time.Now().Unix()),
		Result:        meshcore.LoginOK,
		KeepAlive:     0, // legacy hint, in units of sixteen seconds
		IsAdmin:       c.isAdmin(),
		Permissions:   c.perms,
		FirmwareLevel: firmwareVerLevel,
	})
}

// reqVerdict judges an authenticated request: ours to read only when a
// live session's MAC verifies over it.
func (e *engine) reqVerdict(rx *reception) (verdict, why string, handled bool) {
	c, plain := e.openReq(rx.pkt)
	if c == nil {
		return "", "", false // not ours, or no session: route it on
	}
	// The MAC sweep this took is kept for the answer.
	rx.opened = &opened{session: c, secret: c.secret, plain: plain}
	return verdictRequest, "authenticated request", true
}

// openReq finds the session that sent a REQ and returns its decrypted
// content. The source hash narrows the candidates; the MAC decides.
func (e *engine) openReq(pkt *meshcore.Packet) (*client, []byte) {
	d, err := meshcore.ParseDatagram(pkt.Payload)
	if err != nil || e.id == nil || d.DestHash[0] != e.id.PubKey[0] {
		return nil, nil
	}
	for _, c := range e.acl.matching(d.SrcHash[0]) {
		if plain, err := d.Open(c.secret); err == nil && len(plain) >= 5 {
			return c, plain
		}
	}
	return nil, nil
}

// respondRequest serves one authenticated request.
func (e *engine) respondRequest(rx *reception, origin txn.ID) {
	if rx.opened == nil || rx.opened.session == nil {
		return
	}
	pkt, c, plain := rx.pkt, rx.opened.session, rx.opened.plain
	ts, args, err := meshcore.UnframeAdmin(plain)
	if err != nil {
		return
	}
	if ts <= c.lastTimestamp {
		e.log.Debug("request replay refused", zap.String("txn", origin.Short()))
		return
	}
	// A live session still costs the mesh something, whether the
	// answer floods or walks a route home. Charged before the answer
	// is built, like every other limiter here.
	if !c.asks.allow(time.Now()) {
		e.log.Debug("session rate-limited", zap.String("txn", origin.Short()))
		e.dropRateLimited(origin)
		return
	}
	body, answered := e.answerRequest(args)
	// A question we do not answer still proves the client is there:
	// the keep-alive exists for exactly that, and retiring the
	// companion that sends one instead of polling would be perverse.
	c.lastTimestamp, c.lastActive = ts, time.Now()
	if !answered {
		return // nothing to say, but the session lives on
	}

	// Every response is tagged with the asker's own timestamp, so a
	// companion can match answers to questions.
	e.reply(pkt, answer{
		destHash: c.pubKey[:meshcore.PathHashSize], secret: c.secret,
		tag: ts, body: body, kind: "req-resp",
		scope: e.replyScope(rx), out: c.out,
	}, origin)
}

// answerRequest builds the body of an authenticated answer. answered
// is false for a question this node does not serve — which is not the
// same as an answer that happens to be empty: a node with no sensor
// still owes the asker a reply saying so.
func (e *engine) answerRequest(args []byte) (body []byte, answered bool) {
	switch args[0] {
	case meshcore.ReqGetStatus:
		return e.statusBody(), true
	case meshcore.ReqGetTelemetry:
		return e.telemetryBody(), true
	case meshcore.ReqGetNeighbours:
		b := e.neighboursBody(args)
		return b, b != nil
	case meshcore.ReqGetOwnerInfo:
		return []byte("lotor " + version.Version + "\n" + e.p.NodeName + "\n" + e.p.OwnerInfo), true
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
func (e *engine) dropRateLimited(origin txn.ID) {
	e.bus.Publish(bus.TxDropped{
		Relay: e.relay, Txn: origin, At: time.Now(), Reason: "rate-limited",
	})
}
