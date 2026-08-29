package meshcore

import (
	"errors"
	"strings"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/txn"
)

// Anonymous requests, the reference repeater's shape: a stranger who
// knows only our public key — a neighbourhood scan gave it to them —
// may ask a few harmless questions before any login exists. The
// request rides an envelope sealed to our key — the asker's own
// public key travels in the clear, so the content is private and the
// asker's identity is not; only the holder of our
// private key can read it, and only the asker can read the answer.
const (
	// One fixed-window limiter across every anonymous answer — the
	// reference's anon_limiter(4, 180): at most four replies per three
	// minutes, whatever the mix, tested before anything is built.
	anonLimitMax    = 4
	anonLimitWindow = 3 * time.Minute

	// serverResponseDelay is the reference's fixed pause before a
	// server-style reply (SERVER_RESPONSE_DELAY) — the asker's radio
	// needs a beat to turn around after transmitting.
	serverResponseDelay = 300 * time.Millisecond
)

// anonVerdict judges an ANON_REQ. handled is false when the request is
// not ours to read — wrong destination hash, a MAC that does not
// verify — and the packet must flow through plain routing like any
// other traffic, exactly as the reference forwards what it could not
// decrypt. A request we could read is consumed here whatever its type:
// the reference marks it do-not-retransmit the moment decryption
// succeeds.
func (e *engine) anonVerdict(rx *reception) (verdict, why string, handled bool) {
	d, err := meshcore.ParseAnonDatagram(rx.pkt.Payload)
	if err != nil {
		// The reference releases an incomplete anon packet unrouted.
		return verdictIgnored, "anon request too short", true
	}
	if e.id == nil || d.DestHash[0] != e.id.PubKey[0] {
		return "", "", false
	}
	secret, err := e.id.SharedSecret(d.SenderPub)
	if err != nil {
		return "", "", false
	}
	plain, err := d.Open(secret)
	if err != nil {
		return "", "", false // a failed MAC routes on, unread
	}
	// What this cost — one scalar multiplication and a MAC sweep — is
	// kept for the answer rather than paid again.
	req, err := meshcore.ParseAnonRequest(plain)
	if errors.Is(err, meshcore.ErrIsLogin) {
		rx.opened = &opened{sender: d.SenderPub, secret: secret, plain: plain}
		return verdictAnon, "login request", true
	}
	if err != nil {
		return verdictIgnored, "anon request truncated", true
	}
	rx.opened = &opened{sender: d.SenderPub, secret: secret, plain: plain, req: req}
	switch req.Kind {
	case meshcore.AnonReqOwner:
		return verdictAnon, "owner request — the name behind the key", true
	case meshcore.AnonReqScopes:
		return verdictAnon, "scopes request", true
	case meshcore.AnonReqClock:
		return verdictAnon, "clock request", true
	default:
		return verdictAnon, "unknown anonymous request", true
	}
}

// respondAnon answers the anonymous questions a stranger may ask —
// owner (the name behind the key), clock, scopes — each only when the
// request came direct, the reference's own gating, and all behind one
// shared limiter. Logins were consumed by the judgement and stay
// unanswered.

// anonReplyClockLen is the clock FrameAnonReply writes ahead of the
// text it carries.
const anonReplyClockLen = 4

// answerBudget is how many body bytes a reply to this reception may
// carry, on whichever of the two shapes it will travel home: a
// flooded question is answered with the path it came by, and pays for
// it. Both figures come from the codec, so what this node composes is
// what the codec can seal.
func (e *engine) answerBudget(inbound *meshcore.Packet) int {
	if inbound.IsRouteFlood() {
		return meshcore.PathReturnBodyBudget(len(inbound.Path))
	}
	return meshcore.ResponseBodyBudget()
}

// withinBudget cuts a text answer at the last whole line that fits.
// A line half-sent reads at the far end as a shorter answer rather
// than a truncated one, which is the worse of the two.
func withinBudget(text string, budget int) []byte {
	if len(text) <= budget {
		return []byte(text)
	}
	cut := text[:max(0, budget)]
	if i := strings.LastIndexByte(cut, '\n'); i >= 0 {
		cut = cut[:i]
	}
	return []byte(cut)
}

// joinWithin joins names with the wire's comma, stopping at the last
// whole one that fits. A truncated name would read at the far end as
// a scope nobody carries.
func joinWithin(names []string, budget int) string {
	var b strings.Builder
	for _, n := range names {
		next := len(n)
		if b.Len() > 0 {
			next++ // the separator this name would need
		}
		if b.Len()+next > budget {
			break
		}
		if b.Len() > 0 {
			b.WriteByte(',')
		}
		b.WriteString(n)
	}
	return b.String()
}

func (e *engine) respondAnon(rx *reception, origin txn.ID) {
	if rx.opened == nil {
		return
	}
	pkt, sender, secret := rx.pkt, rx.opened.sender, rx.opened.secret
	req := rx.opened.req
	if req == nil {
		// A password: the login path, which the reference accepts by
		// flood as well as direct — a stranger logging in from across
		// the mesh has no path to us yet.
		e.respondLogin(rx, sender, secret, rx.opened.plain, origin)
		return
	}
	if !pkt.IsRouteDirect() {
		return // the reference gates every other anonymous answer on direct
	}
	// What each question gets. The clock the reply opens with is
	// common to all three; owner adds the name, scopes the
	// comma-joined list the reference's exportNamesTo produces.
	var text string
	switch req.Kind {
	case meshcore.AnonReqOwner:
		text = e.p.NodeName + "\n" + e.p.OwnerInfo
	case meshcore.AnonReqClock:
		// The clock alone is the whole answer.
	case meshcore.AnonReqScopes:
		// Cut at the last whole name that fits, the way the reference
		// bounds its own export: a list composed past the packet is a
		// question left unanswered, and half a scope name at the far
		// end is a scope nobody can derive a key for.
		text = joinWithin(e.regions.served(), e.answerBudget(pkt)-anonReplyClockLen)
	default:
		return // a question nobody defined stays unanswered
	}
	if !e.limits.anon.allow(time.Now()) {
		e.log.Debug("anonymous reply rate-limited", zap.String("txn", origin.Short()))
		e.bus.Publish(bus.TxDropped{
			Relay: e.relay, Txn: origin, At: time.Now(), Reason: reasonRateLimited,
		})
		return
	}

	// The reply the reference composes: the asker's timestamp echoed
	// as a tag, our clock for an easy sync, then the answer's text.
	e.reply(pkt, answer{
		destHash: sender[:meshcore.PathHashSize],
		secret:   secret,
		tag:      req.Timestamp,
		body:     meshcore.FrameAnonReply(uint32(time.Now().Unix()), text),
		scope:    e.replyScope(rx),
		supplied: true,
		pathLen:  req.PathLen,
		path:     req.Path,
		kind:     "anon-resp",
	}, origin)
}
