package meshcore

import (
	"fmt"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/correlation"
	"meshrunner.dev/lotor/internal/radio"
)

// Discovery answering, the reference repeater's shape: at most four
// responses per fixed two-minute window, and the response spreads
// over a jitter four times wider than a relay's — every repeater in
// range answers the same request, and without the spread they would
// all answer at once.
const (
	discoverLimitMax    = 4
	discoverLimitWindow = 2 * time.Minute
	discoverDelayWiden  = 4
)

// limits are the budgets for the work a stranger can ask of this
// node — one set, built with the engine, so none of them can sit at a
// zero value while a packet is already being judged.
type limits struct {
	discover rateLimiter
	anon     rateLimiter
}

func newLimits() limits {
	return limits{
		discover: rateLimiter{Max: discoverLimitMax, Window: discoverLimitWindow},
		anon:     rateLimiter{Max: anonLimitMax, Window: anonLimitWindow},
	}
}

// controlVerdict judges a direct CONTROL packet of the high-bit
// subset — its caller admits no other. The reference answers those
// only at zero hops and releases the rest (Mesh::onRecvPacket: "just
// zero-hop control packets allowed"): a subset packet that walked a
// path is nobody's to carry onward.
func (e *engine) controlVerdict(rx *reception) (string, string) {
	pkt := rx.pkt
	if pkt.PathHashCount() != 0 {
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
func (e *engine) respondDiscover(dev radio.Device, pkt *meshcore.Packet, origin correlation.ID, snr float64) {
	req, err := meshcore.ParseDiscoverReq(pkt)
	if err != nil {
		return // the verdict parsed it once already
	}
	if !req.Filter.Includes(meshcore.AdvTypeRepeater) {
		e.responseSuppressed(origin, "discover", "filter-miss",
			zap.Uint8("filter", uint8(req.Filter)))
		return // the scan is not looking for repeaters
	}
	if req.Since != 0 && uint32(e.discoverySince.Unix()) < req.Since {
		e.responseSuppressed(origin, "discover", "unchanged-since",
			zap.Uint32("since", req.Since),
			zap.Time("last_change", e.discoverySince))
		return // nothing about us changed since the scanner last looked
	}
	if !e.limits.discover.Allow(time.Now()) {
		// Debug, not Warn: the volume here is attacker-controlled.
		e.log.Debug("discovery response rate-limited", zap.String("corr", origin.Short()))
		e.bus.Publish(bus.TxDropped{
			Relay: e.relay, Correlation: origin, At: time.Now(), Reason: reasonRateLimited,
			Kind: "discover-response",
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
		e.log.Warn("discovery response build failed",
			zap.String("corr", origin.Short()), zap.Error(err))
		return
	}
	e.enqueue(dev, resp, "discover-resp", origin, prioDirect, discoverDelayWiden*e.p.txDelayFactor())
}
