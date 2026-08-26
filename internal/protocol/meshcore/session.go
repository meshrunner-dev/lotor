package meshcore

import (
	"encoding/binary"
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
)

// Guest sessions, the reference repeater's shape — with one deliberate
// cut: there is no admin over RF here. The local console socket is the
// administration surface; the radio offers read-only guest access, and
// the admin password simply does not exist. What a guest may ask is
// exactly what the reference grants its guests: status, base
// telemetry, the neighbourhood, the owner line.
const (
	permGuest = 0x02 // PERM_ACL_GUEST

	respServerLoginOK = 0

	reqTypeGetStatus    = 0x01
	reqTypeKeepAlive    = 0x02
	reqTypeGetTelemetry = 0x03
	reqTypeGetAccessLst = 0x05
	reqTypeGetNeighbrs  = 0x06
	reqTypeGetOwnerInfo = 0x07

	// firmwareVerLevel is the protocol feature level the login reply
	// advertises; 2 is where GET_OWNER_INFO appeared.
	firmwareVerLevel = 2

	// maxClients bounds the ACL; the least recently active guest makes
	// room. Nothing persists — the reference deliberately spares its
	// flash for guests too, and a guest just logs in again.
	maxClients = 32

	// The login limiter is ours, not the reference's: its login path
	// stands behind no limiter at all, which leaves password guessing
	// bounded only by airtime. Same fixed window as the others.
	loginLimitMax    = 4
	loginLimitWindow = 3 * time.Minute
)

// client is one logged-in guest.
type client struct {
	pubKey       [32]byte
	secret       []byte
	lastStamp    uint32 // replay floor: every request must beat it
	lastActivity time.Time
	outPath      []byte // the path a flood login walked, reversed by the client
	outPathKnown bool
}

// acl is the client table. The engine's goroutine owns all writes.
type acl struct {
	mu sync.Mutex
	by map[byte][]*client // keyed by the 1-byte path hash; collisions share a bucket
	n  int
}

func newACL() *acl { return &acl{by: map[byte][]*client{}} }

// find returns the clients whose hash matches; the MAC decides which,
// if any, is the real sender.
func (a *acl) find(hash byte) []*client {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]*client(nil), a.by[hash]...)
}

// put adds or refreshes a client, evicting the least recently active
// when full.
func (a *acl) put(c *client) {
	a.mu.Lock()
	defer a.mu.Unlock()
	bucket := a.by[c.pubKey[0]]
	for i, old := range bucket {
		if old.pubKey == c.pubKey {
			bucket[i] = c
			return
		}
	}
	if a.n >= maxClients {
		a.evictOldest()
	}
	a.by[c.pubKey[0]] = append(a.by[c.pubKey[0]], c)
	a.n++
}

// evictOldest drops the least recently active client; the caller holds
// the lock.
func (a *acl) evictOldest() {
	var oldestKey byte
	oldestIdx := -1
	var oldestAt time.Time
	for k, bucket := range a.by {
		for i, c := range bucket {
			if oldestIdx < 0 || c.lastActivity.Before(oldestAt) {
				oldestKey, oldestIdx, oldestAt = k, i, c.lastActivity
			}
		}
	}
	if oldestIdx >= 0 {
		b := a.by[oldestKey]
		a.by[oldestKey] = append(b[:oldestIdx], b[oldestIdx+1:]...)
		a.n--
	}
}

// respondLogin serves a password carried by an ANON_REQ. Only the
// guest password opens anything; a wrong password — the admin's
// included, deliberately — is silence, exactly like the reference's
// failed login.
func (e *engine) respondLogin(pkt *meshcore.Packet, sender, secret, plain []byte, origin txn.ID) {
	if e.p.GuestPassword == "" {
		return // no guests configured: the door does not exist
	}
	password := string(trimNul(plain[4:]))
	senderStamp := binary.LittleEndian.Uint32(plain[0:4])

	c := e.admitLogin(sender, secret, password, senderStamp, origin)
	if c == nil {
		return
	}
	c.lastStamp = senderStamp
	c.lastActivity = time.Now()
	if pkt.IsRouteFlood() {
		c.outPathKnown = false // the client must rediscover its path
	}
	e.acl.put(c)

	// The reference's login reply: our clock, OK, a legacy zero, the
	// admin flag (never, here), the permissions, a random blob for
	// hash uniqueness, and the protocol feature level.
	reply := binary.LittleEndian.AppendUint32(nil, uint32(time.Now().Unix()))
	reply = append(reply, respServerLoginOK, 0, 0, permGuest)
	var blob [4]byte
	timeBlob(blob[:])
	reply = append(reply, blob[:]...)
	reply = append(reply, firmwareVerLevel)
	e.replyToClient(pkt, c, reply, origin, "login-resp")
}

// admitLogin decides whether a password opens (or refreshes) a
// session; nil is silence, the fate of every wrong password.
func (e *engine) admitLogin(sender, secret []byte, password string, senderStamp uint32, origin txn.ID) *client {
	if password == "" {
		// A blank password is an "am I still known?" probe.
		return e.clientFor(sender)
	}
	if password != e.p.GuestPassword {
		return nil
	}
	if !e.loginLimit.allow(time.Now()) {
		e.log.Debug("login rate-limited", zap.String("txn", origin.Short()))
		e.bus.Publish(bus.TxDropped{
			Relay: e.relay, Txn: origin, At: time.Now(), Reason: reasonRateLimited,
		})
		return nil
	}
	c := e.clientFor(sender)
	if c == nil {
		c = &client{secret: secret}
		copy(c.pubKey[:], sender)
	}
	if senderStamp <= c.lastStamp {
		e.log.Debug("login replay refused", zap.String("txn", origin.Short()))
		return nil
	}
	return c
}

// clientFor finds the sender's client entry by full key.
func (e *engine) clientFor(pubKey []byte) *client {
	for _, c := range e.acl.find(pubKey[0]) {
		if string(c.pubKey[:]) == string(pubKey) {
			return c
		}
	}
	return nil
}

// reqVerdict judges a REQ: ours when the destination hash is ours and
// a logged-in client's MAC verifies; anything else flows through plain
// routing, unread.
func (e *engine) reqVerdict(pkt *meshcore.Packet) (verdict string, handled bool) {
	if e.id == nil || e.acl == nil {
		return "", false
	}
	d, err := meshcore.ParseDatagram(pkt.Payload)
	if err != nil {
		return "", false
	}
	if d.DestHash[0] != e.id.PubKey[0] {
		return "", false
	}
	for _, c := range e.acl.find(d.SrcHash[0]) {
		if _, err := d.Open(c.secret); err == nil {
			return verdictPeerReq, true
		}
	}
	return "", false
}

// respondRequest serves a REQ from a logged-in guest: found by source
// hash, proven by the MAC, replay-floored by its timestamp.
func (e *engine) respondRequest(pkt *meshcore.Packet, origin txn.ID) {
	d, err := meshcore.ParseDatagram(pkt.Payload)
	if err != nil {
		return
	}
	for _, c := range e.acl.find(d.SrcHash[0]) {
		plain, err := d.Open(c.secret)
		if err != nil {
			continue // not this client: the MAC said no
		}
		if len(plain) < 5 {
			return
		}
		stamp := binary.LittleEndian.Uint32(plain[0:4])
		if stamp <= c.lastStamp {
			e.log.Debug("request replay refused", zap.String("txn", origin.Short()))
			return
		}
		body := e.answer(stamp, plain[4], plain[5:])
		if body == nil {
			return // unknown or refused request: silence
		}
		c.lastStamp = stamp
		c.lastActivity = time.Now()
		e.replyToClient(pkt, c, body, origin, "req-resp")
		return
	}
}

// answer builds one reply body — the sender's timestamp reflected as a
// tag, then the answer — or nil for anything a guest may not ask.
func (e *engine) answer(stamp uint32, reqType byte, args []byte) []byte {
	reply := binary.LittleEndian.AppendUint32(nil, stamp)
	switch reqType {
	case reqTypeGetStatus:
		return append(reply, e.statusBlob()...)
	case reqTypeKeepAlive:
		return reply
	case reqTypeGetTelemetry:
		return append(reply, e.telemetryBlob()...)
	case reqTypeGetNeighbrs:
		body := e.neighboursBlob(args)
		if body == nil {
			return nil
		}
		return append(reply, body...)
	case reqTypeGetOwnerInfo:
		info := Version + "\n" + e.p.NodeName + "\n" + e.p.OwnerInfo
		return append(reply, info...)
	case reqTypeGetAccessLst:
		return nil // admin's question; there are no admins over RF here
	default:
		return nil
	}
}

// replyToClient routes a sealed reply the reference's way: a flooded
// question earns a PATH return that both teaches the way back and
// carries the answer; a direct one is answered direct along the known
// out path, or flooded when none is known yet.
func (e *engine) replyToClient(pkt *meshcore.Packet, c *client, body []byte, origin txn.ID, kind string) {
	if pkt.IsRouteFlood() {
		resp, err := meshcore.BuildPathReturn(
			c.pubKey[:meshcore.PathHashSize], e.id.PubKey[:meshcore.PathHashSize],
			c.secret, pkt.PathLen, pkt.Path,
			byte(meshcore.PayloadTypeResponse), body)
		if err != nil {
			e.log.Warn("path return build failed", zap.Error(err))
			return
		}
		resp.Header = meshcore.MakeHeader(meshcore.RouteFlood,
			meshcore.PayloadTypePath, meshcore.PayloadVer1)
		e.seen.witness(resp.Hash(), origin, time.Now())
		e.enqueueAfter(resp, kind, origin, prioPathReturn, serverResponseDelay)
		return
	}
	resp, err := meshcore.BuildDatagram(meshcore.PayloadTypeResponse,
		c.pubKey[:meshcore.PathHashSize], e.id.PubKey[:meshcore.PathHashSize],
		c.secret, body)
	if err != nil {
		e.log.Warn("reply build failed", zap.Error(err))
		return
	}
	if c.outPathKnown {
		resp.Header = meshcore.MakeHeader(meshcore.RouteDirect,
			meshcore.PayloadTypeResponse, meshcore.PayloadVer1)
		resp.Path = append([]byte(nil), c.outPath...)
		resp.SetPathHashSizeAndCount(1, len(c.outPath))
	} else {
		resp.Header = meshcore.MakeHeader(meshcore.RouteFlood,
			meshcore.PayloadTypeResponse, meshcore.PayloadVer1)
		e.seen.witness(resp.Hash(), origin, time.Now())
	}
	e.enqueueAfter(resp, kind, origin, prioDirect, serverResponseDelay)
}

// statusBlob is the reference's RepeaterStats, packed little-endian:
// the wire layout every companion status page reads.
func (e *engine) statusBlob() []byte {
	s := e.stats.snapshot()
	used, _, _ := e.Duty()
	out := make([]byte, 0, 56)
	le16 := func(v uint16) { out = binary.LittleEndian.AppendUint16(out, v) }
	le32 := func(v uint32) { out = binary.LittleEndian.AppendUint32(out, v) }

	le16(0)                                 // batt_milli_volts: no battery on this host
	le16(uint16(len(e.queue.entries)))      // curr_tx_queue_len
	le16(uint16(int16(e.lastFloor())))      // noise_floor
	le16(uint16(int16(s.LastRSSI)))         // last_rssi
	le32(s.RecvFlood + s.RecvDirect)        // n_packets_recv
	le32(s.SentFlood + s.SentDirect)        // n_packets_sent
	le32(uint32(s.TxAirtime / time.Second)) // total_air_time_secs
	le32(uint32(time.Since(e.started) / time.Second))
	le32(s.SentFlood)                       // n_sent_flood
	le32(s.SentDirect)                      // n_sent_direct
	le32(s.RecvFlood)                       // n_recv_flood
	le32(s.RecvDirect)                      // n_recv_direct
	le16(0)                                 // err_events
	le16(uint16(int16(s.LastSNR * 4)))      // last_snr ×4
	le16(uint16(s.DirectDups))              // n_direct_dups
	le16(uint16(s.FloodDups))               // n_flood_dups
	le32(uint32(s.RxAirtime / time.Second)) // total_rx_air_time_secs
	le32(s.RecvErrors)                      // n_recv_errors
	_ = used
	return out
}

// telemetryBlob is the base sensor set, Cayenne-encoded: what the
// reference grants guests — no permission mask games, just channel 1.
// This host has no battery; the CPU temperature stands in when the
// platform offers one.
func (e *engine) telemetryBlob() []byte {
	enc := meshcore.NewLPPEncoder()
	if t, ok := hostTemperature(); ok {
		_ = enc.Add(meshcore.LPPReading{Channel: 1, Type: meshcore.LPPTemperature, Value: t})
	}
	_ = enc.Add(meshcore.LPPReading{Channel: 1, Type: meshcore.LPPUnixTime,
		Value: float64(time.Now().Unix())})
	return enc.Bytes()
}

// hostTemperature reads the platform's first thermal zone, best
// effort: absent on hosts without one, and no reason to fail anything.
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

// neighboursBlob answers GET_NEIGHBOURS version 0: total, returned,
// then per neighbour the key prefix, seconds since heard, and the SNR
// quarter-dB byte.
func (e *engine) neighboursBlob(args []byte) []byte {
	if len(args) < 6 || args[0] != 0 {
		return nil // only request version 0 exists
	}
	count := int(args[1])
	offset := int(binary.LittleEndian.Uint16(args[2:4]))
	orderBy := args[4]
	prefixLen := min(int(args[5]), 32)

	rows := e.neighbours.snapshot() // newest first — order 0
	switch orderBy {
	case 1:
		sort.Slice(rows, func(i, j int) bool { return rows[i].Heard.Before(rows[j].Heard) })
	case 2:
		sort.Slice(rows, func(i, j int) bool { return rows[i].SNR > rows[j].SNR })
	case 3:
		sort.Slice(rows, func(i, j int) bool { return rows[i].SNR < rows[j].SNR })
	}

	out := make([]byte, 4, 4+128)
	binary.LittleEndian.PutUint16(out[0:2], uint16(len(rows)))
	returned := 0
	const resultsCap = 130
	body := make([]byte, 0, resultsCap)
	for i := 0; i < count && i+offset < len(rows); i++ {
		entry := prefixLen + 4 + 1
		if len(body)+entry > resultsCap {
			break
		}
		n := rows[i+offset]
		body = append(body, n.PubKey[:prefixLen]...)
		body = binary.LittleEndian.AppendUint32(body, uint32(time.Since(n.Heard)/time.Second))
		body = append(body, byte(int8(n.SNR*4)))
		returned++
	}
	binary.LittleEndian.PutUint16(out[2:4], uint16(returned))
	return append(out, body...)
}

// Version is what GET_OWNER_INFO reports as the firmware line; the
// daemon sets it at start-up.
var Version = "lotor"

// Neighbours reports the direct neighbourhood, newest first; nil when
// the pipeline never armed. Any goroutine.
func (e *engine) Neighbours() []Neighbour {
	if e.neighbours == nil {
		return nil
	}
	return e.neighbours.snapshot()
}

// trimNul cuts a C string at its terminator.
func trimNul(b []byte) []byte {
	for i, c := range b {
		if c == 0 {
			return b[:i]
		}
	}
	return b
}

// timeBlob fills b with hash-uniqueness bytes derived from the clock —
// the role the reference gives four random bytes.
func timeBlob(b []byte) {
	binary.LittleEndian.PutUint32(b, uint32(time.Now().UnixNano()))
}

// lastFloor is the noise floor the status reports, dBm; zero when
// unmeasured.
func (e *engine) lastFloor() float64 {
	if e.floor == nil {
		return 0
	}
	if nf, ok := e.floor(); ok {
		return nf.DBm
	}
	return 0
}
