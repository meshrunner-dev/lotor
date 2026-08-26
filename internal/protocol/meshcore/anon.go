package meshcore

import (
	"encoding/binary"
	"strings"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/txn"
)

// Anonymous requests, the reference repeater's shape: a stranger who
// knows only our public key — a neighbourhood scan gave it to them —
// may ask a few harmless questions before any login exists. The
// request rides an ephemeral-keyed envelope; only the holder of our
// private key can read it, and only the asker can read the answer.
const (
	// The request types the reference dispatches (MyMesh.cpp); a
	// first byte of zero or a printable is a login attempt instead.
	anonReqTypeRegions = 0x01
	anonReqTypeOwner   = 0x02
	anonReqTypeBasic   = 0x03

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
func (e *engine) anonVerdict(pkt *meshcore.Packet) (verdict, why string, handled bool) {
	d, err := meshcore.ParseAnonDatagram(pkt.Payload)
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
	switch t := plain[4]; {
	case t == anonReqTypeOwner:
		return verdictAnon, "owner request — the name behind the key", true
	case t == anonReqTypeRegions:
		return verdictAnon, "regions request", true
	case t == anonReqTypeBasic:
		return verdictAnon, "clock request", true
	case t == 0 || t >= ' ':
		return verdictAnon, "login request", true
	default:
		return verdictAnon, "unknown anonymous request", true
	}
}

// placeholderRegions is what the regions request gets until transport
// scoping exists here: named placeholders, so a companion's region
// browser shows something honest to point at rather than an error.
var placeholderRegions = []string{"lotor-1", "lotor-2"}

// respondAnon answers the anonymous questions a stranger may ask —
// owner (the name behind the key), clock, regions — each only when the
// request came direct, the reference's own gating, and all behind one
// shared limiter. Logins were consumed by the judgement and stay
// unanswered.
// openAnon parses and decrypts an ANON_REQ addressed to us; nil when
// it is not ours to read or too short to mean anything.
func (e *engine) openAnon(pkt *meshcore.Packet) (sender, secret, plain []byte) {
	d, err := meshcore.ParseAnonDatagram(pkt.Payload)
	if err != nil {
		return nil, nil, nil
	}
	secret, err = e.id.SharedSecret(d.SenderPub)
	if err != nil {
		return nil, nil, nil
	}
	plain, err = d.Open(secret)
	if err != nil || len(plain) < 5 {
		return nil, nil, nil
	}
	return d.SenderPub, secret, plain
}

func (e *engine) respondAnon(dev radio.Device, pkt *meshcore.Packet, origin txn.ID) {
	sender, secret, plain := e.openAnon(pkt)
	if plain == nil {
		return
	}
	if t := plain[4]; t == 0 || t >= ' ' {
		// A password: the login path, which the reference accepts by
		// flood as well as direct — a stranger logging in from across
		// the mesh has no path to us yet.
		e.respondLogin(pkt, sender, secret, plain, origin)
		return
	}
	if !pkt.IsRouteDirect() {
		return // the reference gates every other anonymous answer on direct
	}
	// What each question gets. The clock prefix below is common to all
	// three; owner adds the name, regions the comma-joined list the
	// reference's exportNamesTo produces.
	var text string
	switch plain[4] {
	case anonReqTypeOwner:
		text = e.p.NodeName + "\n" + e.p.OwnerInfo
	case anonReqTypeBasic:
		// The clock alone is the whole answer.
	case anonReqTypeRegions:
		text = strings.Join(placeholderRegions, ",")
	default:
		return // logins and the unknown stay unanswered
	}
	// The body supplies the return path: {len}{hashes}. Reject a bad
	// encoding before the limiter — it costs nothing and is not an
	// answer.
	body := plain[5:]
	if len(body) < 1 {
		return
	}
	plen := int(body[0])
	if plen > 63 || len(body) < 1+plen {
		return
	}
	if !e.anonLimit.allow(time.Now()) {
		e.log.Debug("anonymous reply rate-limited", zap.String("txn", origin.Short()))
		e.bus.Publish(bus.TxDropped{
			Relay: e.relay, Txn: origin, At: time.Now(), Reason: reasonRateLimited,
		})
		return
	}

	// The reply the reference composes: the asker's timestamp echoed
	// as a tag, our clock for an easy sync, then the answer's text.
	ts := binary.LittleEndian.Uint32(plain[0:4])
	reply := binary.LittleEndian.AppendUint32(nil, uint32(time.Now().Unix()))
	reply = append(reply, []byte(text)...)
	resp, err := meshcore.BuildResponse(
		sender[:meshcore.PathHashSize], e.id.PubKey[:meshcore.PathHashSize],
		secret, ts, reply)
	if err != nil {
		e.log.Warn("anonymous reply build failed", zap.Error(err))
		return
	}
	resp.Header = meshcore.MakeHeader(meshcore.RouteDirect,
		meshcore.PayloadTypeResponse, meshcore.PayloadVer1)
	if plen > 0 {
		resp.Path = append([]byte(nil), body[1:1+plen]...)
		resp.SetPathHashSizeAndCount(1, plen)
	}
	e.enqueueAfter(resp, "anon-resp", origin, prioDirect, serverResponseDelay)
	_ = dev
}
