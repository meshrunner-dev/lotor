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
	verdictRelayFlood    = "would-relay-flood"
	verdictRelayDirect   = "would-relay-direct"         // our hash heads the path: the reference relays and consumes it
	verdictRelayTrace    = "would-relay-trace"          // our hash is the trace's next target hop
	verdictDropFloodType = "would-drop-flood-type"      // the reference never re-floods this payload type
	verdictDropBadAdvert = "would-drop-invalid-advert"  // flood advert whose signature fails
	verdictDropPathFull  = "would-drop-flood-path-full" // appending our hash would exceed the path
	verdictDropFloodHops = "would-drop-flood-hops"      // the flood travelled past its hop limit
	verdictDropLoop      = "would-drop-flood-loop"      // our hash already rides the path: we relayed this already
	verdictDropScoped    = "would-drop-flood-scoped"    // transport-scoped flood, and no scoping exists here
	verdictSelfAdvert    = "self-advert"                // our own advert echoing back
	verdictZeroHop       = "heard-zero-hop"             // direct, empty path: addressed to whoever hears it
	verdictNotAddressed  = "direct-not-addressed"       // the path's next hop is not us (or no identity exists)
	verdictDiscover      = "discover-request"           // a zero-hop neighbourhood scan asking who hears it
	verdictAnon          = "anon-request"               // an ephemeral-keyed question addressed to our key
	verdictRequest       = "authenticated-request"      // a question from a client whose session we hold
	verdictPeerReq       = "peer-request"               // a logged-in guest's question, proven by its MAC
	verdictTraceTransit  = "trace-transit"              // trace walking its target path, next hop unjudgeable
	verdictTraceNotUs    = "trace-not-addressed"        // trace walking its target path, next hop is not us
	verdictTraceArrived  = "trace-arrived"              // trace consumed its whole target path
	verdictBadVersion    = "unsupported-version"        // the reference dispatcher rejects it at parse
	verdictIgnored       = "ignored"
	verdictMalformed     = "malformed"
	verdictDuplicate     = "duplicate"
)

// maxPathBytes is the reference's path capacity (MAX_PATH_SIZE), and
// maxPathHashes the most the 6-bit count field can name.
const (
	maxPathBytes  = 64
	maxPathHashes = 63
)

// Flood hop limits, the reference repeater's active defaults
// (flood_max, flood_max_advert). A flood that already carries this
// many hashes is not forwarded again: adverts stop far earlier than
// traffic, because every node emits them and the mesh would otherwise
// drown in stale announcements. The reference's third knob,
// flood_max_unscoped, distinguishes plain from transport-scoped
// floods; both ship at the same value and this engine has no scoping
// concept, so it is folded into referenceFloodMaxHops.
const (
	referenceFloodMaxHops       = 64
	referenceFloodMaxAdvertHops = 8
)

// floodHopCaps resolves the configured limits, falling back to the
// reference defaults — an unconfigured engine judges like the
// reference rather than dropping everything.
func (e *engine) floodHopCaps() (maxHops, maxAdvertHops int) {
	maxHops, maxAdvertHops = e.p.FloodMaxHops, e.p.FloodMaxAdvertHops
	if maxHops <= 0 {
		maxHops = referenceFloodMaxHops
	}
	if maxAdvertHops <= 0 {
		maxAdvertHops = referenceFloodMaxAdvertHops
	}
	return maxHops, maxAdvertHops
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

// verdict states what a transmitting relay would do with the packet,
// walking the reference's gates in the reference's order. advertOK
// reports the advert signature check when the type is ADVERT;
// selfAdvert that the advert is our own echo.
// addressedToUs judges the requests a stranger or a client sends to
// this node's own key — but only among the packets the reference lets
// reach its payload switch: a direct packet still walking a path is
// somebody else's hop to carry, never ours to open. handled is false
// for everything the ordinary routing should decide.
func (e *engine) addressedToUs(pkt *meshcore.Packet) (verdict, why string, handled bool) {
	if !pkt.IsRouteFlood() && pkt.PathHashCount() != 0 {
		return "", "", false
	}
	switch pkt.PayloadType() {
	case meshcore.PayloadTypeAnonReq:
		return e.anonVerdict(pkt)
	case meshcore.PayloadTypeReq:
		// Only a live session's MAC can claim an authenticated one.
		return e.reqVerdict(pkt)
	default:
		return "", "", false
	}
}

func (e *engine) verdict(pkt *meshcore.Packet, advertOK, selfAdvert bool) (string, string) {
	if pkt.PayloadVer() > meshcore.PayloadVer1 {
		return verdictBadVersion, ""
	}
	// Requests addressed to us are examined before any routing: the
	// reference decrypts first and forwards only what it could not
	// read.
	if v, why, handled := e.addressedToUs(pkt); handled {
		return v, why
	}
	switch {
	case selfAdvert:
		// The reference releases its own adverts before any routing.
		return verdictSelfAdvert, ""
	case pkt.IsRouteFlood():
		return e.floodVerdict(pkt, advertOK)
	case pkt.IsRouteDirect():
		if pkt.PayloadType() == meshcore.PayloadTypeTrace {
			return e.traceVerdict(pkt)
		}
		if pkt.PayloadType() == meshcore.PayloadTypeControl {
			return e.controlVerdict(pkt)
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

func (e *engine) floodVerdict(pkt *meshcore.Packet, advertOK bool) (string, string) {
	// The reference refuses to forward a scoped flood whose transport
	// code it does not know (allowPacketForward: "unknown transport
	// code"); with no region map at all, that is every one of them.
	if pkt.Route() == meshcore.RouteTransportFlood {
		return verdictDropScoped, "no transport scoping here"
	}
	t := pkt.PayloadType()
	if !floodRoutable[t] {
		return verdictDropFloodType, ""
	}
	if t == meshcore.PayloadTypeAdvert && !advertOK {
		return verdictDropBadAdvert, ""
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
	maxHops, maxAdvertHops := e.floodHopCaps()
	if hops >= maxHops {
		return verdictDropFloodHops, fmt.Sprintf("%d hops, limit %d", hops, maxHops)
	}
	if t == meshcore.PayloadTypeAdvert && hops >= maxAdvertHops {
		return verdictDropFloodHops, fmt.Sprintf("%d advert hops, limit %d", hops, maxAdvertHops)
	}
	// The repeater's loop gate: our hash already in the path means we
	// relayed this packet once — flooding it again would orbit.
	if e.id != nil {
		size := pkt.PathHashSize()
		for i := 0; i+size <= len(pkt.Path); i += size {
			if e.id.HashMatches(pkt.Path[i : i+size]) {
				return verdictDropLoop, ""
			}
		}
	}
	return verdictRelayFlood, ""
}

// traceVerdict walks a direct TRACE the reference's way: the payload
// carries the target path (tag, auth, flags, then hashes of
// 1<<(flags&3) bytes each) and the packet's own path accumulates one
// SNR byte per hop walked.
func (e *engine) traceVerdict(pkt *meshcore.Packet) (string, string) {
	const traceHeader = 9 // tag(4) + auth(4) + flags(1)
	if len(pkt.Payload) < traceHeader {
		return verdictIgnored, "trace payload too short"
	}
	shift := uint(pkt.Payload[8] & 0x03)
	size := 1 << shift
	targetBytes := len(pkt.Payload) - traceHeader
	walked := len(pkt.Path)
	offset := walked << shift
	total := targetBytes >> shift
	why := fmt.Sprintf("hop %d of %d", walked, total)
	switch {
	case offset >= targetBytes:
		return verdictTraceArrived, fmt.Sprintf("walked %d hops", walked)
	case e.id == nil:
		return verdictTraceTransit, why
	case e.id.HashMatches(pkt.Payload[traceHeader+offset : min(traceHeader+offset+size, len(pkt.Payload))]):
		return verdictRelayTrace, why
	default:
		return verdictTraceNotUs, why
	}
}
