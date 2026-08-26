package meshcore

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
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

	// The authenticated request types a client may send.
	reqTypeGetStatus     = 0x01
	reqTypeKeepAlive     = 0x02
	reqTypeGetTelemetry  = 0x03
	reqTypeGetNeighbours = 0x06
	reqTypeGetOwnerInfo  = 0x07

	// respLoginOK heads a successful login reply.
	respLoginOK = 0
	// firmwareVerLevel tells a companion which reply fields to expect;
	// 2 is the level whose shapes this engine answers with.
	firmwareVerLevel = 2

	// telemChannelSelf is the LPP channel a node's own readings ride.
	telemChannelSelf = 1

	// maxClients bounds the session table; the least recently active
	// session makes room for a new one.
	maxClients = 32

	// Login attempts are bounded on their own — separate from the
	// anonymous questions, so a password guesser cannot starve the
	// name lookups, and slower than the reference, which bounds them
	// not at all.
	loginLimitMax    = 4
	loginLimitWindow = 3 * time.Minute
)

// client is one logged-in companion.
type client struct {
	pubKey [meshcore.PubKeySize]byte
	secret []byte
	perms  byte
	// lastTimestamp is the newest request instant this client has
	// signed: anything at or before it is a replay.
	lastTimestamp uint32
	lastActive    time.Time
}

// isAdmin reports the role; always false here, kept because the wire
// carries the field and a reader should see why it is what it is.
func (c *client) isAdmin() bool { return c.perms&permRoleMask == permAdmin }

// clientACL holds the live sessions. The engine's goroutine writes it;
// the console reads it, so it is held under a mutex. Sessions live in
// memory only — a restart asks every companion to log in again, which
// is the honest posture for a credential we never persist.
type acl struct {
	mu sync.Mutex
	by map[[meshcore.PubKeySize]byte]*client
}

func newACL() *acl {
	return &acl{by: map[[meshcore.PubKeySize]byte]*client{}}
}

// put adds or refreshes a session, evicting the least recently active
// one when the table is full.
func (a *acl) put(c *client) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, known := a.by[c.pubKey]; !known && len(a.by) >= maxClients {
		var oldest [meshcore.PubKeySize]byte
		first := true
		for k, v := range a.by {
			if first || v.lastActive.Before(a.by[oldest].lastActive) {
				oldest, first = k, false
			}
		}
		delete(a.by, oldest)
	}
	a.by[c.pubKey] = c
}

// get returns a session by full public key.
func (a *acl) get(pubKey []byte) *client {
	a.mu.Lock()
	defer a.mu.Unlock()
	var k [meshcore.PubKeySize]byte
	copy(k[:], pubKey)
	return a.by[k]
}

// matching returns every session whose key starts with the given hash
// — the reference's searchPeersByHash. A one-byte hash collides often;
// the MAC decides which session actually sent the packet.
func (a *acl) matching(hash byte) []*client {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []*client
	for k, c := range a.by {
		if k[0] == hash {
			out = append(out, c)
		}
	}
	return out
}

// handleLogin answers a password attempt. Unlike the other anonymous
// questions this one is served whatever the inbound route — a
// companion that has not found a path yet floods it.
func (e *engine) respondLogin(pkt *meshcore.Packet, senderPub, secret, plain []byte, origin txn.ID) {
	if e.p.GuestPassword == "" {
		return // no credential configured: the door does not exist
	}
	// Charged before the password is even read: a limiter that only
	// sees successes bounds the honest client and lets the guesser
	// run free — the exact inverse of what it is for.
	if !e.loginLimit.allow(time.Now()) {
		e.log.Debug("login rate-limited", zap.String("txn", origin.Short()))
		e.dropRateLimited(origin)
		return
	}
	ts := binary.LittleEndian.Uint32(plain[:4])
	password := cString(plain[4:])

	c := e.acl.get(senderPub)
	switch {
	case password == "" && c != nil:
		// A blank password re-checks an existing session.
	case subtle.ConstantTimeCompare([]byte(password), []byte(e.p.GuestPassword)) == 1:
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
		e.log.Warn("login replay refused", zap.String("txn", origin.Short()))
		return
	}
	c.lastTimestamp, c.lastActive = ts, time.Now()
	e.acl.put(c)

	// The reply the reference composes: our clock, the verdict, a
	// legacy keep-alive hint, the role, the permissions, a random blob
	// for hash uniqueness, and the reply level we answer at.
	body := make([]byte, 13)
	binary.LittleEndian.PutUint32(body, uint32(time.Now().Unix()))
	body[4] = respLoginOK
	body[5] = 0 // legacy: recommended keep-alive interval, secs/16
	if c.isAdmin() {
		body[6] = 1
	}
	body[7] = c.perms
	if _, err := rand.Read(body[8:12]); err != nil {
		return
	}
	body[12] = firmwareVerLevel

	e.log.Info("guest logged in", zap.String("txn", origin.Short()),
		zap.String("pubkey", shortKey(c.pubKey[:])))
	e.replyToClient(pkt, c, body, "login-resp", origin)
}

// reqVerdict judges an authenticated request: ours to read only when a
// live session's MAC verifies over it.
func (e *engine) reqVerdict(pkt *meshcore.Packet) (verdict, why string, handled bool) {
	if c, _ := e.openReq(pkt); c == nil {
		return "", "", false // not ours, or no session: route it on
	}
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
func (e *engine) respondRequest(pkt *meshcore.Packet, origin txn.ID) {
	c, plain := e.openReq(pkt)
	if c == nil {
		return
	}
	ts := binary.LittleEndian.Uint32(plain[:4])
	if ts <= c.lastTimestamp {
		e.log.Warn("request replay refused", zap.String("txn", origin.Short()))
		return
	}
	body, answered := e.answerRequest(plain[4:])
	if !answered {
		return // a question this node does not serve
	}
	c.lastTimestamp, c.lastActive = ts, time.Now()

	// Every response is tagged with the asker's own timestamp, so a
	// companion can match answers to questions.
	reply := binary.LittleEndian.AppendUint32(nil, ts)
	e.replyToClient(pkt, c, append(reply, body...), "req-resp", origin)
}

// answerRequest builds the body of an authenticated answer. answered
// is false for a question this node does not serve — which is not the
// same as an answer that happens to be empty: a node with no sensor
// still owes the asker a reply saying so.
func (e *engine) answerRequest(args []byte) (body []byte, answered bool) {
	switch args[0] {
	case reqTypeGetStatus:
		return e.statusBody(), true
	case reqTypeGetTelemetry:
		return e.telemetryBody(), true
	case reqTypeGetNeighbours:
		b := e.neighboursBody(args)
		return b, b != nil
	case reqTypeGetOwnerInfo:
		return []byte("lotor " + version.Version + "\n" + e.p.NodeName + "\n" + e.p.OwnerInfo), true
	case reqTypeKeepAlive:
		// The reference answers nothing here either, and the session's
		// clock has already moved on the request that carried it.
		return nil, false
	default:
		return nil, false
	}
}

// statusBody packs the repeater statistics a companion's status page
// reads — the reference's RepeaterStats, field for field, in its own
// little-endian order.
func (e *engine) statusBody() []byte {
	s := e.stats.snapshot()
	nf := int16(0)
	if e.floor != nil {
		if f, ok := e.floor(); ok {
			nf = int16(f.DBm)
		}
	}
	out := make([]byte, 0, 56)
	u16 := func(v uint16) { out = binary.LittleEndian.AppendUint16(out, v) }
	u32 := func(v uint32) { out = binary.LittleEndian.AppendUint32(out, v) }

	u16(0) // battery millivolts: mains powered, and a lie would be worse
	u16(uint16(e.queueLen()))
	u16(uint16(nf))
	u16(uint16(int16(s.LastRSSI)))
	u32(s.RecvFlood + s.RecvDirect)
	u32(s.SentFlood + s.SentDirect)
	u32(uint32(s.TxAirtime / time.Second))
	u32(uint32(time.Since(e.started) / time.Second))
	u32(s.SentFlood)
	u32(s.SentDirect)
	u32(s.RecvFlood)
	u32(s.RecvDirect)
	u16(0) // error events: none tracked as a bitfield here
	u16(uint16(int16(s.LastSNR * 4)))
	u16(uint16(s.DirectDups))
	u16(uint16(s.FloodDups))
	u32(uint32(s.RxAirtime / time.Second))
	u32(s.RecvErrors)
	return out
}

// telemetryBody reports what this node can honestly measure about
// itself, in the Cayenne encoding companions expect.
func (e *engine) telemetryBody() []byte {
	enc := meshcore.NewLPPEncoder()
	if c, ok := hostTemperature(); ok {
		_ = enc.Add(meshcore.LPPReading{
			Channel: telemChannelSelf, Type: meshcore.LPPTemperature, Value: c,
		})
	}
	return enc.Bytes()
}

// neighboursBody answers the neighbourhood query: the total known, how
// many are returned, then each one's key prefix, how long ago it was
// heard, and the SNR it was heard at.
func (e *engine) neighboursBody(args []byte) []byte {
	if len(args) < 7 || args[1] != 0 {
		return nil // only version 0 exists
	}
	count := int(args[2])
	offset := int(binary.LittleEndian.Uint16(args[3:5]))
	orderBy := args[5]
	prefixLen := min(int(args[6]), meshcore.PubKeySize)

	all := e.neighbours.snapshot() // newest heard first
	switch orderBy {
	case 1: // oldest to newest
		sort.SliceStable(all, func(i, j int) bool { return all[i].Heard.Before(all[j].Heard) })
	case 2: // strongest to weakest
		sort.SliceStable(all, func(i, j int) bool { return all[i].SNR > all[j].SNR })
	case 3: // weakest to strongest
		sort.SliceStable(all, func(i, j int) bool { return all[i].SNR < all[j].SNR })
	}

	// The reference bounds its results buffer; so does this, and the
	// count it reports is what actually fits.
	const maxResults = 130
	entry := prefixLen + 5
	var rows []byte
	returned := 0
	now := time.Now()
	for i := offset; i < len(all) && returned < count; i++ {
		if len(rows)+entry > maxResults {
			break
		}
		n := all[i]
		rows = append(rows, n.PubKey[:prefixLen]...)
		rows = binary.LittleEndian.AppendUint32(rows, uint32(now.Sub(n.Heard)/time.Second))
		rows = append(rows, byte(int8(n.SNR*4)))
		returned++
	}
	out := binary.LittleEndian.AppendUint16(nil, uint16(len(all)))
	out = binary.LittleEndian.AppendUint16(out, uint16(returned))
	return append(out, rows...)
}

// replyToClient routes one answer back the way the reference chooses:
// a flooded question earns a path return — telling the asker how to
// reach us directly next time, with the answer riding along — and a
// direct question is answered direct when a path is known, flooded
// when none is.
func (e *engine) replyToClient(inbound *meshcore.Packet, c *client, body []byte, kind string, origin txn.ID) {
	destHash := c.pubKey[:meshcore.PathHashSize]
	srcHash := e.id.PubKey[:meshcore.PathHashSize]

	if inbound.IsRouteFlood() {
		path, err := meshcore.BuildPathReturn(destHash, srcHash, c.secret,
			inbound.PathLen, inbound.Path, byte(meshcore.PayloadTypeResponse), body)
		if err != nil {
			e.log.Warn("path return build failed", zap.Error(err))
			return
		}
		e.enqueueAfter(path, kind, origin, prioPathReturn, serverResponseDelay)
		return
	}

	resp, err := meshcore.BuildResponse(destHash, srcHash, c.secret,
		binary.LittleEndian.Uint32(body[:4]), body[4:])
	if err != nil {
		e.log.Warn("response build failed", zap.Error(err))
		return
	}
	// A direct question arrives with its path already spent — every
	// hop consumed its own entry on the way — so an empty path says
	// nothing about how far the asker is. Adjacent or five hops out,
	// they look identical here, and the only route that reaches both
	// is a flood. The reference makes the same call, and buys its way
	// out of it by remembering an out-path per client, which this
	// engine does not learn yet.
	resp.Header = meshcore.MakeHeader(meshcore.RouteFlood,
		meshcore.PayloadTypeResponse, meshcore.PayloadVer1)
	e.enqueueAfter(resp, kind, origin, prioFloodReply, serverResponseDelay)
}

// cString reads up to the terminator, the form every password and name
// crosses the air in.
func cString(b []byte) string {
	for i, c := range b {
		if c == 0 {
			return string(b[:i])
		}
	}
	return string(b)
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

// hostTemperature reads the host's own thermal sensor, the one figure
// a mains-powered relay can honestly report about itself. Absent on
// hosts without one, which is not a fault.
func hostTemperature() (float64, bool) {
	raw, err := os.ReadFile("/sys/class/thermal/thermal_zone0/temp")
	if err != nil {
		return 0, false
	}
	milli, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		return 0, false
	}
	return float64(milli) / 1000, true
}
