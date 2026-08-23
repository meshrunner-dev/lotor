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
	verdictDropFloodType = "would-drop-flood-type"      // the reference never re-floods this payload type
	verdictDropBadAdvert = "would-drop-invalid-advert"  // flood advert whose signature fails
	verdictDropPathFull  = "would-drop-flood-path-full" // appending our hash would exceed the path
	verdictZeroHop       = "heard-zero-hop"             // direct, empty path: addressed to whoever hears it
	verdictNotAddressed  = "direct-not-addressed"       // direct routing needs a node identity; without one, never ours
	verdictTraceTransit  = "trace-transit"              // trace still walking its target path
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
// reports the advert signature check when the type is ADVERT.
func (e *engine) verdict(pkt *meshcore.Packet, advertOK bool) (string, string) {
	if pkt.PayloadVer() > meshcore.PayloadVer1 {
		return verdictBadVersion, ""
	}
	switch {
	case pkt.IsRouteFlood():
		return floodVerdict(pkt, advertOK)
	case pkt.IsRouteDirect():
		if pkt.PayloadType() == meshcore.PayloadTypeTrace {
			return traceVerdict(pkt)
		}
		if pkt.PathHashCount() == 0 {
			return verdictZeroHop, ""
		}
		// The reference relays a direct packet only when its own hash
		// heads the path. A relay with no identity heads no path.
		return verdictNotAddressed, ""
	default:
		return verdictIgnored, ""
	}
}

func floodVerdict(pkt *meshcore.Packet, advertOK bool) (string, string) {
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
	return verdictRelayFlood, ""
}

// traceVerdict walks a direct TRACE the reference's way: the payload
// carries the target path (tag, auth, flags, then hashes of
// 1<<(flags&3) bytes each) and the packet's own path accumulates one
// SNR byte per hop walked.
func traceVerdict(pkt *meshcore.Packet) (string, string) {
	const traceHeader = 9 // tag(4) + auth(4) + flags(1)
	if len(pkt.Payload) < traceHeader {
		return verdictIgnored, "trace payload too short"
	}
	shift := uint(pkt.Payload[8] & 0x03)
	targetBytes := len(pkt.Payload) - traceHeader
	walked := len(pkt.Path)
	offset := walked << shift
	total := targetBytes >> shift
	if offset >= targetBytes {
		return verdictTraceArrived, fmt.Sprintf("walked %d hops", walked)
	}
	return verdictTraceTransit, fmt.Sprintf("hop %d of %d", walked, total)
}
