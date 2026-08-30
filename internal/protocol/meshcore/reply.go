package meshcore

import (
	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/txn"
)

// logReplyRoute records the routing decision once the response packet has
// its final route. Packet enqueue and radio details are logged separately.
func (e *engine) logReplyRoute(pkt *meshcore.Packet, origin txn.ID,
	kind, source string, priority int,
) {
	if !e.log.Core().Enabled(zap.DebugLevel) {
		return
	}
	e.log.Debug("reply route selected",
		zap.String("txn", origin.Short()), zap.String("kind", kind),
		zap.String("route_source", source), zap.Stringer("route", pkt.Route()),
		zap.Int("hops", pkt.PathHashCount()), zap.Int("priority", priority),
		zap.Bool("scoped", pkt.HasTransportCodes()))
}

// responseSuppressed makes an intentional silence visible at debug without
// promoting mesh traffic or attacker-controlled input to operator alerts.
func (e *engine) responseSuppressed(origin txn.ID, request, reason string,
	fields ...zap.Field,
) {
	if !e.log.Core().Enabled(zap.DebugLevel) {
		return
	}
	base := make([]zap.Field, 0, len(fields)+3)
	base = append(base, zap.String("txn", origin.Short()),
		zap.String("request", request), zap.String("reason", reason))
	base = append(base, fields...)
	e.log.Debug("response suppressed", base...)
}

// routeHome picks the direct route this answer should take, or nil
// when it has none and must flood.
func (a answer) routeHome() *outPath {
	if a.supplied {
		return &outPath{pathLen: a.pathLen, path: a.path}
	}
	return a.out
}

// answer is one reply looking for its way home: what to say, to whom,
// sealed under what, and any return path the question itself carried.
type answer struct {
	destHash []byte // the asker, at path-hash width
	secret   []byte // what seals the content to them alone
	tag      uint32 // the timestamp the asker matches answers by
	body     []byte // the content, after the tag
	// scope is the transport scope this answer travels under; a zero
	// key travels plain.
	scope meshcore.TransportKey
	// supplied says the question carried its own route home, and
	// pathLen/path are it. A supplied path of zero hops is not the
	// same as no path at all: the first names the asker as adjacent
	// and earns a zero-hop answer, the second says nothing and earns
	// a flood. The reference draws exactly that line, by whether its
	// handler set reply_path_len at all.
	supplied bool
	pathLen  uint8
	path     []byte
	// out is the route the asker taught us in an earlier PATH, when
	// there is one. Consulted only after a supplied path: a route
	// carried by this very question is fresher than one remembered
	// from an earlier exchange.
	out  *outPath
	kind string // what the journal calls this emission
}

// reply routes one answer the way the reference chooses (its
// chooseReplyRoute), and is the only place in this engine that does.
//
// Four routes, in the order the reference tries them. A flooded
// question earns a path return: the answer travels inside a packet
// whose whole purpose is to teach the asker how to reach us directly
// next time. A question that carried its own return path is answered
// along it. Failing that, the route the asker taught us in an earlier
// PATH. And only when none of those exists, a flood — because a direct
// question arrives with its path already spent, every hop having
// consumed its own entry, so an empty path says nothing about how far
// the asker is, and a flood is the one thing that reaches both the
// adjacent and the distant.
//
// The scope is stamped last, once the payload is final: the code is
// computed over it. Every reply inherits the hash width the asker's
// mesh uses: a
// narrower one collides more often for the repeaters carrying it.
func (e *engine) reply(inbound *meshcore.Packet, a answer, origin txn.ID) {
	srcHash := e.id.PubKey[:meshcore.PathHashSize]
	framed := meshcore.FrameAdmin(a.tag, a.body)

	if inbound.IsRouteFlood() {
		pkt, err := meshcore.BuildPathReturn(a.destHash, srcHash, a.secret,
			inbound.PathLen, inbound.Path, byte(meshcore.PayloadTypeResponse), framed)
		if err != nil {
			// An answer nobody can compose is an answer nobody
			// receives, and for an authenticated question the replay
			// guard is already spent — so the refusal is counted
			// against its transaction rather than logged and lost.
			e.abandonKind(origin, "malformed", "answer",
				"reply too large to compose ("+a.kind+")", err)
			return
		}
		pkt.SetPathHashSizeAndCount(inbound.PathHashSize(), 0)
		a.scope.Scope(pkt)
		e.logReplyRoute(pkt, origin, a.kind, "path-return", prioPathReturn)
		e.enqueueAfter(pkt, a.kind, origin, prioPathReturn, serverResponseDelay)
		return
	}

	pkt, err := meshcore.BuildResponse(a.destHash, srcHash, a.secret, a.tag, a.body)
	if err != nil {
		e.abandonKind(origin, "malformed", "answer",
			"reply too large to compose ("+a.kind+")", err)
		return
	}
	if home := a.routeHome(); home != nil {
		pkt.Header = meshcore.MakeHeader(meshcore.RouteDirect,
			meshcore.PayloadTypeResponse, meshcore.PayloadVer1)
		pkt.Path, pkt.PathLen = home.path, home.pathLen
		a.scope.Scope(pkt)
		source := "learned"
		if a.supplied {
			source = "supplied"
		}
		e.logReplyRoute(pkt, origin, a.kind, source, prioDirect)
		e.enqueueAfter(pkt, a.kind, origin, prioDirect, serverResponseDelay)
		return
	}
	pkt.Header = meshcore.MakeHeader(meshcore.RouteFlood,
		meshcore.PayloadTypeResponse, meshcore.PayloadVer1)
	pkt.SetPathHashSizeAndCount(inbound.PathHashSize(), 0)
	a.scope.Scope(pkt)
	e.logReplyRoute(pkt, origin, a.kind, "flood", prioFloodReply)
	e.enqueueAfter(pkt, a.kind, origin, prioFloodReply, serverResponseDelay)
}
