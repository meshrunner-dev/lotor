package meshcore

import (
	"errors"
	"strings"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/correlation"
	"meshrunner.dev/lotor/internal/meshcorehost"
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
	a, short, ok := meshcorehost.OpenAnon(e.id, rx.pkt.Payload)
	if short {
		// The reference releases an incomplete anon packet unrouted.
		return verdictIgnored, "anon request too short", true
	}
	if !ok {
		return "", "", false
	}
	// What this cost — one scalar multiplication and a MAC sweep — is
	// kept for the answer rather than paid again.
	req, err := meshcore.ParseAnonRequest(a.Plain)
	if errors.Is(err, meshcore.ErrIsLogin) {
		rx.opened = &opened{sender: a.Sender, secret: a.Secret, plain: a.Plain}
		return verdictAnon, "login request", true
	}
	if err != nil {
		return verdictIgnored, "anon request truncated", true
	}
	rx.opened = &opened{sender: a.Sender, secret: a.Secret, plain: a.Plain, req: req}
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

func (e *engine) respondAnon(rx *reception, origin correlation.ID) {
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
		e.responseSuppressed(origin, "anonymous", "not-direct",
			zap.Stringer("route", pkt.Route()), zap.Uint8("kind", req.Kind))
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
		// The reference's own export rule, skip-and-continue: a name
		// that will not fit is left out and the walk keeps looking for
		// shorter ones, where a break at the first misfit answered a
		// shorter list than the reference does. The +1 mirrors its
		// buffer arithmetic, which reserves the terminator our wire
		// never carries.
		text = e.regions.m.ExportNames(meshcore.RegionDenyFlood, false,
			e.answerBudget(pkt)-anonReplyClockLen+1)
	default:
		e.responseSuppressed(origin, "anonymous", "unsupported-request",
			zap.Uint8("kind", req.Kind))
		return // a question nobody defined stays unanswered
	}
	if !e.limits.anon.Allow(time.Now()) {
		e.log.Debug("anonymous reply rate-limited", zap.String("corr", origin.Short()))
		e.bus.Publish(bus.TxDropped{
			Relay: e.relay, Correlation: origin, At: time.Now(), Reason: reasonRateLimited,
			Kind: "anon-reply",
		})
		return
	}

	// The reply the reference composes: the asker's timestamp echoed
	// as a tag, our clock for an easy sync, then the answer's text.
	e.reply(pkt, meshcorehost.Answer{
		DestHash: sender[:meshcore.PathHashSize],
		Secret:   secret,
		Tag:      req.Timestamp,
		Body:     meshcore.FrameAnonReply(uint32(time.Now().Unix()), text),
		Scope:    e.replyScope(rx),
		Supplied: true,
		PathLen:  req.PathLen,
		Path:     req.Path,
	}, "anon-resp", origin)
}
