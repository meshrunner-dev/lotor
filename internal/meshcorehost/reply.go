package meshcorehost

// How an answer travels home — the reference's chooseReplyRoute, in
// its order, composed here and emitted by the owner.

import (
	"meshrunner.dev/pkg/meshcore"
)

// The priorities a reply is queued at, the reference's ladder: routed
// traffic first, a flooded reply behind it, a path return last of the
// three. Lower serves first.
const (
	PrioDirect     = 0
	PrioFloodReply = 1
	PrioPathReturn = 2
)

// Answer is one reply looking for its way home: what to say, to whom,
// sealed under what, and any return path the question itself carried.
type Answer struct {
	DestHash []byte // the asker, at path-hash width
	Secret   []byte // what seals the content to them alone
	Tag      uint32 // the timestamp the asker matches answers by
	Body     []byte // the content, after the tag
	// Scope is the transport scope this answer travels under; a zero
	// key travels plain.
	Scope meshcore.TransportKey
	// Supplied says the question carried its own route home, and
	// PathLen/Path are it. A supplied path of zero hops is not the
	// same as no path at all: the first names the asker as adjacent
	// and earns a zero-hop answer, the second says nothing and earns
	// a flood. The reference draws exactly that line, by whether its
	// handler set reply_path_len at all.
	Supplied bool
	PathLen  uint8
	Path     []byte
	// Out is the route the asker taught us in an earlier PATH, when
	// there is one. Consulted only after a supplied path: a route
	// carried by this very question is fresher than one remembered
	// from an earlier exchange.
	Out *OutPath
}

// routeHome picks the direct route this answer should take, or nil
// when it has none and must flood.
func (a Answer) routeHome() *OutPath {
	if a.Supplied {
		return &OutPath{PathLen: a.PathLen, Path: a.Path}
	}
	return a.Out
}

// ComposeReply builds an answer's packet and routes it the way the
// reference chooses. Four routes, in the order it tries them. A
// flooded question earns a path return: the answer travels inside a
// packet whose whole purpose is to teach the asker how to reach us
// directly next time. A question that carried its own return path is
// answered along it. Failing that, the route the asker taught us in
// an earlier PATH. And only when none of those exists, a flood —
// because a direct question arrives with its path already spent,
// every hop having consumed its own entry, so an empty path says
// nothing about how far the asker is, and a flood is the one thing
// that reaches both the adjacent and the distant.
//
// The scope is stamped last, once the payload is final: the code is
// computed over it. Every reply inherits the hash width the asker's
// mesh uses: a narrower one collides more often for the repeaters
// carrying it. source names the route for the journal.
func ComposeReply(inbound *meshcore.Packet, a Answer, srcHash []byte,
) (pkt *meshcore.Packet, priority int, source string, err error) {
	framed := meshcore.FrameAdmin(a.Tag, a.Body)
	if inbound.IsRouteFlood() {
		pkt, err = meshcore.BuildPathReturn(a.DestHash, srcHash, a.Secret,
			inbound.PathLen, inbound.Path, byte(meshcore.PayloadTypeResponse), framed)
		if err != nil {
			return nil, 0, "", err
		}
		pkt.SetPathHashSizeAndCount(inbound.PathHashSize(), 0)
		a.Scope.Scope(pkt)
		return pkt, PrioPathReturn, "path-return", nil
	}
	pkt, err = meshcore.BuildResponse(a.DestHash, srcHash, a.Secret, a.Tag, a.Body)
	if err != nil {
		return nil, 0, "", err
	}
	if home := a.routeHome(); home != nil {
		source = "learned"
		if a.Supplied {
			source = "supplied"
		}
		priority = RouteDirect(pkt, home, a.Scope)
		return pkt, priority, source, nil
	}
	priority = RouteFlood(pkt, inbound, a.Scope)
	return pkt, priority, "flood", nil
}

// RouteDirect sends a composed packet straight down a taught route and
// reports the priority it earns.
func RouteDirect(pkt *meshcore.Packet, home *OutPath, scope meshcore.TransportKey) int {
	pkt.Header = meshcore.MakeHeader(meshcore.RouteDirect, pkt.PayloadType(), meshcore.PayloadVer1)
	pkt.Path, pkt.PathLen = home.Path, home.PathLen
	scope.Scope(pkt)
	return PrioDirect
}

// RouteFlood floods a composed packet at the asker's hash width and
// reports the priority it earns.
func RouteFlood(pkt, inbound *meshcore.Packet, scope meshcore.TransportKey) int {
	pkt.Header = meshcore.MakeHeader(meshcore.RouteFlood, pkt.PayloadType(), meshcore.PayloadVer1)
	pkt.SetPathHashSizeAndCount(inbound.PathHashSize(), 0)
	scope.Scope(pkt)
	return PrioFloodReply
}

// RouteHome routes an already-composed packet — an ACK, a text reply —
// down the client's taught route when there is one, flooded otherwise,
// and names the route for the journal.
func RouteHome(pkt, inbound *meshcore.Packet, out *OutPath, scope meshcore.TransportKey) (priority int, source string) {
	if out != nil {
		return RouteDirect(pkt, out, scope), "learned"
	}
	return RouteFlood(pkt, inbound, scope), "flood"
}
