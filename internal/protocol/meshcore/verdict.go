package meshcore

import (
	"fmt"

	"meshrunner.dev/pkg/meshcore"
)

// Verdicts. The vocabulary is the dry run's contract: each "would-…"
// names the action a transmitting relay will take under the same
// judgement, each "would-drop-…" names the reference gate that stops
// it. Reference: Mesh::onRecvPacket / Mesh::routeRecvPacket.
const (
	verdictRelayFlood     = "would-relay-flood"
	verdictRelayDirect    = "would-relay-direct"         // our hash heads the path: the reference relays and consumes it
	verdictRelayTrace     = "would-relay-trace"          // our hash is the trace's next target hop
	verdictDropFloodType  = "would-drop-flood-type"      // the reference never re-floods this payload type
	verdictDropFloodShort = "would-drop-flood-truncated" // the payload does not hold the envelope its type declares
	verdictDropBadAdvert  = "would-drop-invalid-advert"  // flood advert whose signature fails
	verdictDropPathFull   = "would-drop-flood-path-full" // appending our hash would exceed the path
	verdictDropFloodHops  = "would-drop-flood-hops"      // the flood travelled past its hop limit
	verdictDropLoop       = "would-drop-flood-loop"      // our hash already rides the path: we relayed this already
	verdictDropScoped     = "would-drop-flood-scoped"    // transport-scoped flood into a scope this relay denies
	verdictSelfAdvert     = "self-advert"                // our own advert echoing back
	verdictSameRadio      = "heard-on-our-own-radio"     // a binding sharing this antenna: relaying reaches nobody new
	verdictCommand        = "administration"             // a logged-in admin's command line
	verdictZeroHop        = "heard-zero-hop"             // direct, empty path: addressed to whoever hears it
	verdictNotAddressed   = "direct-not-addressed"       // the path's next hop is not us (or no identity exists)
	verdictDiscover       = "discover-request"           // a zero-hop neighbourhood scan asking who hears it
	verdictAnon           = "anon-request"               // a question sealed to our key, asker named in the clear
	verdictDiscoverAnswer = "discover-answer"            // a neighbour answering a scan this node sent
	verdictScopeAnswer    = "scopes-answer"              // a neighbour telling us what it carries, for our own question
	verdictRequest        = "authenticated-request"      // a question from a client whose session we hold
	verdictClientPath     = "client-route-home"          // a client teaching us how to reach it directly
	verdictTraceTransit   = "trace-transit"              // trace walking its target path, next hop unjudgeable
	verdictTraceNotUs     = "trace-not-addressed"        // trace walking its target path, next hop is not us
	verdictTraceArrived   = "trace-arrived"              // trace consumed its whole target path
	verdictBadVersion     = "unsupported-version"        // the reference dispatcher rejects it at parse
	verdictIgnored        = "ignored"
	verdictMalformed      = "malformed"
	verdictDuplicate      = "duplicate"
)

// maxPathBytes is the reference's path capacity (MAX_PATH_SIZE), and
// maxPathHashes the most the 6-bit count field can name.
const (
	maxPathBytes  = 64
	maxPathHashes = 63
)

// Flood hop limits, the reference repeater's active defaults. A flood
// that already carries this many hashes is not forwarded again:
// adverts stop far earlier than traffic, because every node emits them
// and the mesh would otherwise drown in stale announcements.
//
// The unscoped limit is the reference's third knob, and it applies to
// plain floods alone. It ships at the same value as the general one,
// but lowering it is how an operator throttles the traffic that
// belongs to no scope without touching the traffic that does.
const (
	referenceFloodMaxHops         = 64
	referenceFloodMaxUnscopedHops = 64
	referenceFloodMaxAdvertHops   = 8
)

// floodHopCaps resolves the configured limits, falling back to the
// reference defaults — an unconfigured engine judges like the
// reference rather than dropping everything.
func (e *engine) floodHopCaps() (maxHops, maxUnscopedHops, maxAdvertHops int) {
	maxHops = e.p.FloodMaxHops
	maxUnscopedHops = e.p.FloodMaxUnscopedHops
	maxAdvertHops = e.p.FloodMaxAdvertHops
	if maxHops <= 0 {
		maxHops = referenceFloodMaxHops
	}
	if maxUnscopedHops <= 0 {
		maxUnscopedHops = referenceFloodMaxUnscopedHops
	}
	if maxAdvertHops <= 0 {
		maxAdvertHops = referenceFloodMaxAdvertHops
	}
	return maxHops, maxUnscopedHops, maxAdvertHops
}

// floodRoutable holds the payload types Mesh::onRecvPacket hands to
// routeRecvPacket. RAW_CUSTOM, MULTIPART, TRACE and CONTROL are
// deliberately absent — the reference never re-floods them.
var floodRoutable = map[meshcore.PayloadType]bool{
	meshcore.PayloadTypeAck:      true,
	meshcore.PayloadTypePath:     true,
	meshcore.PayloadTypeReq:      true,
	meshcore.PayloadTypeResponse: true,
	meshcore.PayloadTypeTxtMsg:   true,
	meshcore.PayloadTypeAnonReq:  true,
	meshcore.PayloadTypeGrpData:  true,
	meshcore.PayloadTypeGrpTxt:   true,
	meshcore.PayloadTypeAdvert:   true,
}

// unsupportedVersion reports a payload version this engine does not
// read. The reference refuses one in its dispatcher, before any
// application decoding at all — which is where the refusal has to be:
// a version we say we do not understand must not name a neighbour,
// take a place in the duplicate table, or be held for a score delay
// on the strength of fields we just admitted we cannot read.
func unsupportedVersion(pkt *meshcore.Packet) bool {
	return pkt.PayloadVer() > meshcore.PayloadVer1
}

// floodEnvelopeIntact reports whether a flood's payload actually
// holds the envelope its type declares. The reference reaches
// routeRecvPacket only past its own length gate — an incomplete ACK
// or data packet is released, never carried — so judging on the type
// alone had this node spreading through its own neighbourhood frames
// that every reference repeater stops. The shapes are the codec's:
// asking the parsers is what keeps the two from drifting.
func floodEnvelopeIntact(pkt *meshcore.Packet) bool {
	var err error
	switch pkt.PayloadType() {
	case meshcore.PayloadTypeAck:
		_, err = meshcore.ParseAck(pkt.Payload)
	case meshcore.PayloadTypePath, meshcore.PayloadTypeReq,
		meshcore.PayloadTypeResponse, meshcore.PayloadTypeTxtMsg:
		_, err = meshcore.ParseDatagram(pkt.Payload)
	case meshcore.PayloadTypeAnonReq:
		_, err = meshcore.ParseAnonDatagram(pkt.Payload)
	case meshcore.PayloadTypeGrpData, meshcore.PayloadTypeGrpTxt:
		_, err = meshcore.ParseGroupDatagram(pkt.Payload)
	default:
		// ADVERT stands on its signature, which the verdict already
		// checked; nothing else reaches here.
	}
	return err == nil
}

// highBitControl names the CONTROL subset the reference singles out:
// the first payload byte with its high bit set (Mesh::onRecvPacket).
// Those are answered when they arrive zero-hop and released
// otherwise. A CONTROL without that bit is ordinary directed traffic
// and continues through the normal routing, which is what keeps this
// relay transparent to a control type it does not itself speak.
func highBitControl(pkt *meshcore.Packet) bool {
	return pkt.PayloadType() == meshcore.PayloadTypeControl &&
		len(pkt.Payload) > 0 && pkt.Payload[0]&0x80 != 0
}

// verdict states what a transmitting relay would do with the packet,
// walking the reference's gates in the reference's order. advertOK
// reports the advert signature check when the type is ADVERT;
// selfAdvert that the advert is our own echo.
// addressedToUs judges the requests a stranger or a client sends to
// this node's own key — but only among the packets the reference lets
// reach its payload switch: a direct packet still walking a path is
// somebody else's hop to carry, never ours to open. handled is false
// for everything the ordinary routing should decide.
func (e *engine) addressedToUs(rx *reception) (verdict, why string, handled bool) {
	pkt := rx.pkt
	if !pkt.IsRouteFlood() && pkt.PathHashCount() != 0 {
		return "", "", false
	}
	switch pkt.PayloadType() {
	case meshcore.PayloadTypeAnonReq:
		return e.anonVerdict(rx)
	case meshcore.PayloadTypeReq:
		// Only a live session's MAC can claim an authenticated one.
		return e.reqVerdict(rx)
	case meshcore.PayloadTypeTxtMsg:
		// A logged-in admin's command line; anything else routes on.
		return e.cmdVerdict(rx)
	case meshcore.PayloadTypePath:
		// A client teaching us its route home; anything else routes
		// on.
		return e.pathVerdict(rx)
	case meshcore.PayloadTypeResponse:
		// An answer to a question this node asked; anything else
		// routes on.
		return e.scopeAnswer(rx)
	default:
		return "", "", false
	}
}

func (e *engine) verdict(rx *reception) (string, string) {
	pkt := rx.pkt
	if unsupportedVersion(pkt) {
		return verdictBadVersion, ""
	}
	// Requests addressed to us are examined before any routing: the
	// reference decrypts first and forwards only what it could not
	// read.
	if v, why, handled := e.addressedToUs(rx); handled {
		return v, why
	}
	switch {
	case rx.selfAdvert:
		// The reference releases its own adverts before any routing.
		return verdictSelfAdvert, ""
	case pkt.IsRouteFlood():
		v, why := e.floodVerdict(rx, rx.advertOK)
		if scope, _ := e.regionOf(rx); scope != "" && scope != wildcardRegion {
			if why == "" {
				why = "scope " + scope
			} else {
				why += " (scope " + scope + ")"
			}
		}
		return v, why
	case pkt.IsRouteDirect():
		if pkt.PayloadType() == meshcore.PayloadTypeTrace {
			return e.traceVerdict(pkt)
		}
		if highBitControl(pkt) {
			return e.controlVerdict(rx)
		}
		if pkt.PathHashCount() == 0 {
			return verdictZeroHop, ""
		}
		// The reference relays a direct packet only when its own hash
		// heads the path; consuming it is the transmit path's future.
		if e.id != nil && e.id.HashMatches(pkt.Path[:min(pkt.PathHashSize(), len(pkt.Path))]) {
			return verdictRelayDirect, ""
		}
		return verdictNotAddressed, ""
	default:
		return verdictIgnored, ""
	}
}

func (e *engine) floodVerdict(rx *reception, advertOK bool) (string, string) {
	pkt := rx.pkt
	// This flood already left through our antenna: a binding on this
	// controller emitted it, so every node that would hear the relay
	// heard the original, and re-flooding spends the shared duty
	// ledger twice to reach nobody new. Only floods are judged here.
	// A direct packet still travels: a path naming us asks this node
	// to carry it, whoever sent it. No reference gate says any of
	// this, the reference never having two identities on one radio.
	if rx.frame.Binding != "" {
		return verdictSameRadio, rx.frame.Binding + " shares our antenna"
	}
	// A plain flood is governed by the wildcard. A transport flood is
	// carried only when one flood-allowed named region verifies its
	// code: the wildcard is the absence of a region, never a fallback
	// for an unknown one.
	if _, carried := e.regionOf(rx); !carried {
		return verdictDropScoped, "unknown transport code or flood denied"
	}
	t := pkt.PayloadType()
	if !floodRoutable[t] {
		return verdictDropFloodType, ""
	}
	if t == meshcore.PayloadTypeAdvert && !advertOK {
		return verdictDropBadAdvert, ""
	}
	if !floodEnvelopeIntact(pkt) {
		return verdictDropFloodShort, "the payload is shorter than its own envelope"
	}
	// Both halves of what appending our hash requires: the bytes must
	// fit, and the count must stay inside its 6-bit field. Judging on
	// the byte length alone promised a relay the append would refuse.
	if next := pkt.PathHashCount() + 1; next > maxPathHashes || next*pkt.PathHashSize() > maxPathBytes {
		return verdictDropPathFull, ""
	}
	// The distance gate the reference applies through
	// allowPacketForward → isFloodHopLimitExceeded, in the same place:
	// after the capacity check, before the loop scan.
	hops := pkt.PathHashCount()
	maxHops, maxUnscopedHops, maxAdvertHops := e.floodHopCaps()
	if hops >= maxHops {
		return verdictDropFloodHops, fmt.Sprintf("%d hops, limit %d", hops, maxHops)
	}
	if !pkt.HasTransportCodes() && hops >= maxUnscopedHops {
		return verdictDropFloodHops,
			fmt.Sprintf("%d unscoped hops, limit %d", hops, maxUnscopedHops)
	}
	if t == meshcore.PayloadTypeAdvert && hops >= maxAdvertHops {
		return verdictDropFloodHops, fmt.Sprintf("%d advert hops, limit %d", hops, maxAdvertHops)
	}
	// The repeater's orbit gate: count how many times our hash
	// already rides the path. At narrow widths a match may be another
	// node's collision — one in 256 per hop at one byte — so each
	// mode tolerates a per-width number of apparent visits before the
	// refusal, exactly the reference's isLooped and its tables.
	if e.id != nil && e.p.loopDetect() != loopOff {
		size := pkt.PathHashSize()
		limit := loopMaxima[e.p.loopDetect()][min(size, 3)]
		n := 0
		for i := 0; i+size <= len(pkt.Path); i += size {
			if e.id.HashMatches(pkt.Path[i : i+size]) {
				n++
			}
		}
		if n >= limit {
			return verdictDropLoop, fmt.Sprintf(
				"our hash rides the path %d times — %s tolerates %d", n, e.p.loopDetect(), limit-1)
		}
	}
	return verdictRelayFlood, ""
}

// The orbit gate's vocabulary and thresholds. loopMaxima[mode][w] is
// how many appearances of our own hash refuse the relay at hash width
// w (index 3 also serves the wire's fourth width, which the reference
// tables never covered: at that width a match is no collision).
const (
	loopOff      = "off"
	loopMinimal  = "minimal"
	loopModerate = "moderate"
	loopStrict   = "strict"

	// defaultPathHashWidth is what this node's own floods declare
	// when path_hash_mode is unset — two bytes, one step wider than
	// the reference ships, chosen for meshes dense enough that a
	// one-byte path reads as visited by everyone.
	defaultPathHashWidth = 2
)

var loopMaxima = map[string][4]int{
	loopMinimal:  {0, 4, 2, 1},
	loopModerate: {0, 2, 1, 1},
	loopStrict:   {0, 1, 1, 1},
}

// traceVerdict walks a direct TRACE the reference's way: the payload
// carries the target path (tag, auth, flags, then hashes of
// 1<<(flags&3) bytes each) and the packet's own path accumulates one
// SNR byte per hop walked.
func (e *engine) traceVerdict(pkt *meshcore.Packet) (string, string) {
	tr, err := meshcore.ParseTrace(pkt)
	if err != nil {
		return verdictIgnored, "trace payload too short"
	}
	// The route is a list of node hashes, one per planned hop; the
	// walked path is one SNR byte per hop already taken. The hop we
	// are being asked about is therefore at the walked length.
	walked := len(tr.SNRx4)
	offset := walked * tr.HashWidth
	total := len(tr.Route) / tr.HashWidth
	why := fmt.Sprintf("hop %d of %d", walked, total)
	switch {
	case offset >= len(tr.Route):
		return verdictTraceArrived, fmt.Sprintf("walked %d hops", walked)
	case e.id == nil:
		return verdictTraceTransit, why
	case e.id.HashMatches(tr.Route[offset:min(offset+tr.HashWidth, len(tr.Route))]):
		return verdictRelayTrace, why
	default:
		return verdictTraceNotUs, why
	}
}
