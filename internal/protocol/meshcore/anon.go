package meshcore

import (
	"encoding/binary"
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
	// The request types the reference dispatches (MyMesh.cpp); a
	// first byte of zero or a printable is a login attempt instead.
	// The scopes request; the reference names this one REGIONS, the
	// word this codebase reserves for a radio band.
	anonReqTypeScopes = 0x01
	anonReqTypeOwner  = 0x02
	anonReqTypeBasic  = 0x03

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
	if len(plain) < 5 {
		return verdictIgnored, "anon request truncated", true
	}
	// What this cost — one scalar multiplication and a MAC sweep — is
	// kept for the answer rather than paid again.
	rx.opened = &opened{sender: d.SenderPub, secret: secret, plain: plain}
	switch t := plain[4]; {
	case t == anonReqTypeOwner:
		return verdictAnon, "owner request — the name behind the key", true
	case t == anonReqTypeScopes:
		return verdictAnon, "scopes request", true
	case t == anonReqTypeBasic:
		return verdictAnon, "clock request", true
	case t == 0 || t >= ' ':
		return verdictAnon, "login request", true
	default:
		return verdictAnon, "unknown anonymous request", true
	}
}

// replyPath decodes the return path a request supplies: the
// reference's path descriptor — hop count in the low six bits, hash
// width in the top two — followed by that many bytes of path.
func replyPath(body []byte) (pathLen uint8, path []byte, ok bool) {
	if len(body) < 1 || !meshcore.ValidPathLen(body[0]) {
		return 0, nil, false
	}
	n := int(body[0]&63) * (int(body[0]>>6) + 1)
	if len(body) < 1+n {
		return 0, nil, false
	}
	return body[0], append([]byte(nil), body[1:1+n]...), true
}

// respondAnon answers the anonymous questions a stranger may ask —
// owner (the name behind the key), clock, scopes — each only when the
// request came direct, the reference's own gating, and all behind one
// shared limiter. Logins were consumed by the judgement and stay
// unanswered.

func (e *engine) respondAnon(rx *reception, origin txn.ID) {
	if rx.opened == nil {
		return
	}
	pkt, sender, secret, plain := rx.pkt, rx.opened.sender, rx.opened.secret, rx.opened.plain
	if t := plain[4]; t == 0 || t >= ' ' {
		// A password: the login path, which the reference accepts by
		// flood as well as direct — a stranger logging in from across
		// the mesh has no path to us yet.
		e.respondLogin(rx, sender, secret, plain, origin)
		return
	}
	if !pkt.IsRouteDirect() {
		return // the reference gates every other anonymous answer on direct
	}
	// What each question gets. The clock prefix below is common to all
	// three; owner adds the name, scopes the comma-joined list the
	// reference's exportNamesTo produces.
	var text string
	switch plain[4] {
	case anonReqTypeOwner:
		text = e.p.NodeName + "\n" + e.p.OwnerInfo
	case anonReqTypeBasic:
		// The clock alone is the whole answer.
	case anonReqTypeScopes:
		text = strings.Join(e.scopes.served(), ",")
	default:
		return // logins and the unknown stay unanswered
	}
	// The body supplies the return path. A bad encoding is refused
	// before the limiter — it costs nothing and is not an answer.
	pathLen, path, ok := replyPath(plain[5:])
	if !ok {
		return
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
	body := binary.LittleEndian.AppendUint32(nil, uint32(time.Now().Unix()))
	body = append(body, []byte(text)...)
	e.reply(pkt, answer{
		destHash: sender[:meshcore.PathHashSize],
		secret:   secret,
		tag:      binary.LittleEndian.Uint32(plain[0:4]),
		body:     body,
		scope:    e.replyScope(rx),
		supplied: true,
		pathLen:  pathLen,
		path:     path,
		kind:     "anon-resp",
	}, origin)
}
