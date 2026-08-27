package meshcore

import (
	"fmt"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/txn"
)

// Discovery answering, the reference repeater's shape: at most four
// responses per fixed two-minute window, and the response spreads
// over a jitter four times wider than a relay's — every repeater in
// range answers the same request, and without the spread they would
// all answer at once.
const (
	discoverLimitMax    = 4
	discoverLimitWindow = 2 * time.Minute
	discoverDelayFactor = 4 * floodDelayFactor
)

// rateLimiter is a fixed window: so many events from the window's
// first, then denial until it expires. The reference's RateLimiter,
// ported shape and quirk alike — the guard every anonymous answer
// stands behind, so a request flood cannot turn the node into an
// amplifier. Refused work costs the price of its reception, nothing
// more: no packet is built, nothing reaches the queue.
type rateLimiter struct {
	max    int
	window time.Duration
	start  time.Time
	count  int
}

// allow consumes one slot; the window opens on the first event after
// the previous one expired.
func (r *rateLimiter) allow(now time.Time) bool {
	if r.max <= 0 {
		// A budget nobody set grants nothing. These are the only
		// defence against being made an amplifier, so the zero value
		// refuses rather than permits.
		return false
	}
	if now.Before(r.start.Add(r.window)) {
		r.count++
		return r.count <= r.max
	}
	r.start, r.count = now, 1
	return true
}

// limits are the budgets for the work a stranger can ask of this
// node — one set, built with the engine, so none of them can sit at a
// zero value while a packet is already being judged.
type limits struct {
	discover rateLimiter
	anon     rateLimiter
	login    rateLimiter
}

func newLimits() limits {
	return limits{
		discover: rateLimiter{max: discoverLimitMax, window: discoverLimitWindow},
		anon:     rateLimiter{max: anonLimitMax, window: anonLimitWindow},
		login:    rateLimiter{max: loginLimitMax, window: loginLimitWindow},
	}
}

// controlVerdict judges a direct CONTROL packet. The reference admits
// only the zero-hop, high-bit subset (Mesh::onRecvPacket: "just
// zero-hop control packets allowed"); anything else is released.
func (e *engine) controlVerdict(rx *reception) (string, string) {
	pkt := rx.pkt
	if pkt.PathHashCount() != 0 || len(pkt.Payload) == 0 || pkt.Payload[0]&0x80 == 0 {
		return verdictIgnored, "control outside the zero-hop subset"
	}
	if v, why, handled := e.sweepAnswer(rx); handled {
		return v, why
	}
	if req, err := meshcore.ParseDiscoverReq(pkt); err == nil {
		return verdictDiscover, fmt.Sprintf("filter %#02x tag %08x", byte(req.Filter), req.Tag)
	}
	return verdictZeroHop, ""
}

// respondDiscover answers a neighbourhood scan: presence, our key,
// and the SNR we heard the request at — the scanner's direct measure
// of the inbound link. The name is not in this packet by design; it
// travels in the signed advert, which is why a discoverable repeater
// also announces itself.
func (e *engine) respondDiscover(dev radio.Device, pkt *meshcore.Packet, origin txn.ID, snr float64) {
	req, err := meshcore.ParseDiscoverReq(pkt)
	if err != nil {
		return // the verdict parsed it once already
	}
	if !req.Filter.Includes(meshcore.AdvTypeRepeater) {
		return // the scan is not looking for repeaters
	}
	if req.Since != 0 && uint32(e.discoverySince.Unix()) < req.Since {
		return // nothing about us changed since the scanner last looked
	}
	if !e.limits.discover.allow(time.Now()) {
		// Debug, not Warn: the volume here is attacker-controlled.
		e.log.Debug("discovery response rate-limited", zap.String("txn", origin.Short()))
		e.bus.Publish(bus.TxDropped{
			Relay: e.relay, Txn: origin, At: time.Now(), Reason: reasonRateLimited,
		})
		return
	}
	resp, err := meshcore.BuildDiscoverResp(meshcore.DiscoverResp{
		NodeType: meshcore.AdvTypeRepeater,
		SNR:      snr,
		Tag:      req.Tag,
		PubKey:   e.id.PubKey[:],
	}, req.PrefixOnly)
	if err != nil {
		e.log.Warn("discovery response build failed", zap.Error(err))
		return
	}
	e.enqueue(dev, resp, "discover-resp", origin, prioDirect, discoverDelayFactor)
}
