package meshcore

import (
	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/correlation"
	"meshrunner.dev/lotor/internal/meshcorehost"
)

// logReplyRoute records the routing decision once the response packet has
// its final route. Packet enqueue and radio details are logged separately.
func (e *engine) logReplyRoute(pkt *meshcore.Packet, origin correlation.ID,
	kind, source string, priority int,
) {
	if !e.log.Core().Enabled(zap.DebugLevel) {
		return
	}
	e.log.Debug("reply route selected",
		zap.String("corr", origin.Short()), zap.String("kind", kind),
		zap.String("route_source", source), zap.Stringer("route", pkt.Route()),
		zap.Int("hops", pkt.PathHashCount()), zap.Int("priority", priority),
		zap.Bool("scoped", pkt.HasTransportCodes()))
}

// responseSuppressed makes an intentional silence visible at debug without
// promoting mesh traffic or attacker-controlled input to operator alerts.
func (e *engine) responseSuppressed(origin correlation.ID, request, reason string,
	fields ...zap.Field,
) {
	if !e.log.Core().Enabled(zap.DebugLevel) {
		return
	}
	base := make([]zap.Field, 0, len(fields)+3)
	base = append(base, zap.String("corr", origin.Short()),
		zap.String("request", request), zap.String("reason", reason))
	base = append(base, fields...)
	e.log.Debug("response suppressed", base...)
}

// reply routes one answer the way the reference chooses — the
// kernel's ComposeReply — then queues it at the priority the route
// earns, after the server's fixed pause. kind is what the journal
// calls this emission.
func (e *engine) reply(inbound *meshcore.Packet, a meshcorehost.Answer, kind string, origin correlation.ID) {
	pkt, priority, source, err := meshcorehost.ComposeReply(inbound, a, e.id.PubKey[:meshcore.PathHashSize])
	if err != nil {
		// An answer nobody can compose is an answer nobody receives,
		// and for an authenticated question the replay guard is
		// already spent — so the refusal is counted against its
		// correlation rather than logged and lost.
		e.abandonKind(origin, "malformed", "answer", "reply too large to compose ("+kind+")", err)
		return
	}
	e.logReplyRoute(pkt, origin, kind, source, priority)
	e.enqueueAfter(pkt, kind, origin, priority, serverResponseDelay)
}
