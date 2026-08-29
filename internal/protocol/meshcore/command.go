package meshcore

// Administration over the air: a logged-in admin sends a TXT_MSG
// carrying a command line, this node runs it and answers with the
// output. The wire contract is the reference's — the same words its
// companions already send, because the clients are the ecosystem's
// apps and their buttons speak CommonCLI — while what the words do
// here goes through this daemon's own mutation door, so a change
// made from the air is journalled exactly like one made at the
// console.

import (
	"strings"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/logging"
	"meshrunner.dev/lotor/internal/txn"
)

// commandMaxReply bounds one answer: the reference's reply buffer is
// 160-odd bytes, and a longer line would be truncated on arrival
// anyway. Ours says so instead of pretending.
const commandMaxReply = 150

// cmdVerdict judges a text message: ours to run only when a live
// admin session's MAC verifies over it. A guest's text is not an
// error — it is simply not a command, and routes on like any other
// traffic this node is not the destination of.
func (e *engine) cmdVerdict(rx *reception) (verdict, why string, handled bool) {
	c, plain := e.openText(rx.pkt)
	if c == nil {
		return "", "", false
	}
	if !c.isAdmin() {
		return "", "", false
	}
	rx.opened = &opened{session: c, secret: c.secret, plain: plain}
	return verdictCommand, "administration from a logged-in admin", true
}

// openText finds the session that sent a TXT_MSG and returns its
// decrypted content — openReq's shape, for the other payload type.
func (e *engine) openText(pkt *meshcore.Packet) (*client, []byte) {
	d, err := meshcore.ParseDatagram(pkt.Payload)
	if err != nil || e.id == nil || d.DestHash[0] != e.id.PubKey[0] {
		return nil, nil
	}
	for _, c := range e.acl.matching(d.SrcHash[0]) {
		if plain, err := d.Open(c.secret); err == nil && len(plain) >= 5 {
			return c, plain
		}
	}
	return nil, nil
}

// runCommand serves one administration line: the replay guard first,
// then the reference's retry semantics — a repeat of the newest
// timestamp is the client asking again for an answer it missed, and
// re-running the command would apply a mutation twice.
func (e *engine) runCommand(rx *reception, origin txn.ID) {
	if rx.opened == nil || rx.opened.session == nil || e.commands == nil {
		return
	}
	pkt, c, plain := rx.pkt, rx.opened.session, rx.opened.plain
	text, err := meshcore.ParseTextPlaintext(plain)
	if err != nil {
		return
	}
	ts := uint32(text.Timestamp.Unix())
	if ts < c.lastTimestamp {
		e.log.Debug("command replay refused", zap.String("txn", origin.Short()))
		return
	}
	retry := ts == c.lastTimestamp
	if !c.asks.allow(time.Now()) {
		e.log.Debug("command rate-limited", zap.String("txn", origin.Short()))
		e.dropRateLimited(origin)
		return
	}

	line := strings.TrimSpace(text.Text)
	var out string
	if retry {
		// The reference answers a retry with an empty reply rather
		// than running the line again: the command already took.
		e.log.Info("command retried — not run again",
			zap.String("txn", origin.Short()), zap.String("command", line))
	} else {
		out = e.commands(line, c.pubKey[:])
		e.log.Info("command from the air",
			zap.String("txn", origin.Short()),
			zap.String("pubkey", shortKey(c.pubKey[:])),
			zap.String("command", line))
		logging.Trace(e.log, "command answered", zap.String("reply", out))
	}
	c.lastTimestamp, c.lastActive = ts, time.Now()
	e.acl.save(c)
	if out == "" {
		return
	}
	if len(out) > commandMaxReply {
		out = out[:commandMaxReply-1] + "…"
	}
	// The answer carries this node's own clock, never the asker's:
	// two messages an instant apart must not hash alike, and the
	// reference bumps its clock for exactly that reason.
	replyAt := time.Now()
	if uint32(replyAt.Unix()) == ts {
		replyAt = replyAt.Add(time.Second)
	}
	e.replyText(pkt, c, meshcore.BuildTextPlaintext(replyAt, meshcore.TxtTypeCLIData, out), origin)
}

// replyText sends one command's output back as a text message, down
// the route the admin taught when there is one.
func (e *engine) replyText(inbound *meshcore.Packet, c *client, plain []byte, origin txn.ID) {
	pkt, err := meshcore.BuildDatagram(meshcore.PayloadTypeTxtMsg,
		c.pubKey[:meshcore.PathHashSize], e.id.PubKey[:meshcore.PathHashSize],
		c.secret, plain)
	if err != nil {
		e.log.Warn("command reply build failed", zap.Error(err))
		return
	}
	scope := e.replyScope(&reception{pkt: inbound})
	if c.out != nil {
		pkt.Header = meshcore.MakeHeader(meshcore.RouteDirect,
			meshcore.PayloadTypeTxtMsg, meshcore.PayloadVer1)
		pkt.Path, pkt.PathLen = c.out.path, c.out.pathLen
		scope.Scope(pkt)
		e.enqueueAfter(pkt, "cmd-resp", origin, prioDirect, serverResponseDelay)
		return
	}
	pkt.Header = meshcore.MakeHeader(meshcore.RouteFlood,
		meshcore.PayloadTypeTxtMsg, meshcore.PayloadVer1)
	pkt.SetPathHashSizeAndCount(inbound.PathHashSize(), 0)
	scope.Scope(pkt)
	e.enqueueAfter(pkt, "cmd-resp", origin, prioFloodReply, serverResponseDelay)
}
