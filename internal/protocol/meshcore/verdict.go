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
	verdictDropLoop      = "would-drop-flood-loop"      // our hash already rides the path: we relayed this already
	verdictSelfAdvert    = "self-advert"                // our own advert echoing back
	verdictZeroHop       = "heard-zero-hop"             // direct, empty path: addressed to whoever hears it
	verdictNotAddressed  = "direct-not-addressed"       // the path's next hop is not us (or no identity exists)
	verdictTraceTransit  = "trace-transit"              // trace walking its target path, next hop unjudgeable
	verdictTraceNotUs    = "trace-not-addressed"        // trace walking its target path, next hop is not us
	verdictTraceArrived  = "trace-arrived"              // trace consumed its whole target path
	verdictBadVersion    = "unsupported-version"        // the reference dispatcher rejects it at parse
	verdictIgnored       = "ignored"
	verdictMalformed     = "malformed"
	verdictDuplicate     = "duplicate"
)

// maxPathBytes is the reference's path capacity (MAX_PATH_SIZE).
const maxPathBytes = 64

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
func (e *engine) verdict(pkt *meshcore.Packet, advertOK, selfAdvert bool) (string, string) {
	if pkt.PayloadVer() > meshcore.PayloadVer1 {
		return verdictBadVersion, ""
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
	t := pkt.PayloadType()
	if !floodRoutable[t] {
		return verdictDropFloodType, ""
	}
	if t == meshcore.PayloadTypeAdvert && !advertOK {
		return verdictDropBadAdvert, ""
	}
	if (pkt.PathHashCount()+1)*pkt.PathHashSize() > maxPathBytes {
		return verdictDropPathFull, ""
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
