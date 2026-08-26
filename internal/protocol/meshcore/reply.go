package meshcore

import (
	"encoding/binary"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/txn"
)

// answer is one reply looking for its way home: what to say, to whom,
// sealed under what, and any return path the question itself carried.
type answer struct {
	destHash []byte // the asker, at path-hash width
	secret   []byte // what seals the content to them alone
	tag      uint32 // the timestamp the asker matches answers by
	body     []byte // the content, after the tag
	// supplied says the question carried its own route home, and
	// pathLen/path are it. A supplied path of zero hops is not the
	// same as no path at all: the first names the asker as adjacent
	// and earns a zero-hop answer, the second says nothing and earns
	// a flood. The reference draws exactly that line, by whether its
	// handler set reply_path_len at all.
	supplied bool
	pathLen  uint8
	path     []byte
	kind     string // what the journal calls this emission
}

// reply routes one answer the way the reference chooses (its
// chooseReplyRoute), and is the only place in this engine that does.
//
// A flooded question earns a path return: the answer travels inside a
// packet whose whole purpose is to teach the asker how to reach us
// directly next time. A question that carried its own return path is
// answered along it. Anything else floods — a direct question arrives
// with its path already spent, every hop having consumed its own
// entry, so an empty path says nothing about how far the asker is,
// and only a flood reaches both the adjacent and the distant. The
// reference buys its way out of that last case with a stored
// out-path, which this engine does not learn yet.
//
// Every reply inherits the hash width the asker's mesh uses: a
// narrower one collides more often for the repeaters carrying it.
func (e *engine) reply(inbound *meshcore.Packet, a answer, origin txn.ID) {
	srcHash := e.id.PubKey[:meshcore.PathHashSize]
	framed := binary.LittleEndian.AppendUint32(nil, a.tag)
	framed = append(framed, a.body...)

	if inbound.IsRouteFlood() {
		pkt, err := meshcore.BuildPathReturn(a.destHash, srcHash, a.secret,
			inbound.PathLen, inbound.Path, byte(meshcore.PayloadTypeResponse), framed)
		if err != nil {
			e.log.Warn("path return build failed", zap.String("kind", a.kind), zap.Error(err))
			return
		}
		pkt.SetPathHashSizeAndCount(inbound.PathHashSize(), 0)
		e.enqueueAfter(pkt, a.kind, origin, prioPathReturn, serverResponseDelay)
		return
	}

	pkt, err := meshcore.BuildResponse(a.destHash, srcHash, a.secret, a.tag, a.body)
	if err != nil {
		e.log.Warn("response build failed", zap.String("kind", a.kind), zap.Error(err))
		return
	}
	if a.supplied {
		pkt.Header = meshcore.MakeHeader(meshcore.RouteDirect,
			meshcore.PayloadTypeResponse, meshcore.PayloadVer1)
		pkt.Path, pkt.PathLen = a.path, a.pathLen
		e.enqueueAfter(pkt, a.kind, origin, prioDirect, serverResponseDelay)
		return
	}
	pkt.Header = meshcore.MakeHeader(meshcore.RouteFlood,
		meshcore.PayloadTypeResponse, meshcore.PayloadVer1)
	pkt.SetPathHashSizeAndCount(inbound.PathHashSize(), 0)
	e.enqueueAfter(pkt, a.kind, origin, prioFloodReply, serverResponseDelay)
}
