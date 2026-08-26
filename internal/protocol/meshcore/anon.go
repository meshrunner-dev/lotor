package meshcore

import (
	"encoding/binary"
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
		return verdictAnon, "regions request — none configured here", true
	case t == anonReqTypeBasic:
		return verdictAnon, "clock request", true
	case t == 0 || t >= ' ':
		return verdictAnon, "login attempt — no admin over RF here", true
	default:
		return verdictAnon, "unknown anonymous request", true
	}
}

// respondAnon answers the anonymous requests a stranger may ask. Only
// the owner request is served today — the name every companion's
// "request name" is waiting for — and only when the request came
// direct, the reference's own gating. Everything else was consumed by
// the judgement and stays unanswered.
func (e *engine) respondAnon(dev radio.Device, pkt *meshcore.Packet, origin txn.ID) {
	d, err := meshcore.ParseAnonDatagram(pkt.Payload)
	if err != nil {
		return
	}
	secret, err := e.id.SharedSecret(d.SenderPub)
	if err != nil {
		return
	}
	plain, err := d.Open(secret)
	if err != nil || len(plain) < 5 {
		return
	}
	if plain[4] != anonReqTypeOwner || !pkt.IsRouteDirect() {
		return
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
			Relay: e.relay, Txn: origin, At: time.Now(), Reason: "rate-limited",
		})
		return
	}

	// The reply the reference composes: the asker's timestamp echoed
	// as a tag, our clock for an easy sync, then "name\nowner".
	ts := binary.LittleEndian.Uint32(plain[0:4])
	reply := binary.LittleEndian.AppendUint32(nil, uint32(time.Now().Unix()))
	reply = append(reply, []byte(e.p.NodeName+"\n"+e.p.OwnerInfo)...)
	resp, err := meshcore.BuildResponse(
		d.SenderPub[:meshcore.PathHashSize], e.id.PubKey[:meshcore.PathHashSize],
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
