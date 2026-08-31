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
	"errors"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/correlation"
)

// commandMaxReply bounds one answer: the reference's reply buffer is
// 160-odd bytes, and a longer line would be truncated on arrival
// anyway. Ours says so instead of pretending.
const commandMaxReply = 150

// commandSubtype says whether a text's wire type asks for a command,
// matching the reference's own gate (its simple_repeater refuses
// everything else with "unsupported text type received").
func commandSubtype(t uint8) bool {
	switch t {
	case meshcore.TxtTypePlain, meshcore.TxtTypeCLIData, meshcore.TxtTypeCLICommand:
		return true
	}
	return false
}

// cmdVerdict judges a text message: ours to run only when a live
// admin session's MAC verifies over it, and only when the wire type
// is one the reference runs. A guest's text is not an error — it is
// simply not a command, and routes on like any other traffic this
// node is not the destination of.
//
// Three subtypes are commands, the reference's own set: the explicit
// TXT_TYPE_CLI_COMMAND, the older CLI_DATA that its own header calls
// "a CLI command -or- reply", and PLAIN, which its companions used
// before either existed. Accepting only the explicit one would refuse
// every companion in the field, which is the interoperability this
// daemon exists to keep; the MAC is what says an order is authentic,
// and the subtype only says which dialect asked.
func (e *engine) cmdVerdict(rx *reception) (verdict, why string, handled bool) {
	c, plain := e.openText(rx.pkt)
	if c == nil {
		return "", "", false
	}
	if !c.isAdmin() {
		return "", "", false
	}
	text, err := meshcore.ParseTextPlaintext(plain)
	if err != nil {
		return "", "", false
	}
	if !commandSubtype(text.Type) {
		// Authenticated, addressed here, and not a command: it is the
		// admin's own traffic, judged like anyone else's.
		return "", "", false
	}
	// The decode is kept: running it twice would let the verdict and
	// the action disagree about what was said.
	rx.opened = &opened{session: c, secret: c.secret, plain: plain, text: text}
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
func (e *engine) runCommand(rx *reception, origin correlation.ID) {
	if rx.opened == nil || rx.opened.session == nil || rx.opened.text == nil || e.commands == nil {
		return
	}
	pkt, c, text := rx.pkt, rx.opened.session, rx.opened.text
	ts := uint32(text.Timestamp.Unix())
	if ts < c.lastTimestamp {
		e.log.Debug("command replay refused", zap.String("corr", origin.Short()))
		return
	}
	retry := ts == c.lastTimestamp
	// Charged only when the answer would flood, like every session
	// answer: an admin at the end of a taught route is the person
	// this node exists to obey, not an amplification risk.
	if c.out == nil && !c.asks.allow(time.Now()) {
		e.log.Debug("command rate-limited — flood answers", zap.String("corr", origin.Short()))
		e.dropRateLimited(origin)
		return
	}

	// The line crosses whole, leading spaces included: a modal region
	// load encodes each entry's parent purely in its indentation, and
	// a trim here silently reduced every dump to orphan lines that a
	// blank commit then persisted as an empty table. Whoever runs the
	// line trims for its own dispatch AFTER the region door has had
	// the raw bytes.
	line := text.Text
	// The companion's optional pairing prefix — two characters and a
	// bar — is stripped before the words and reflected at the head of
	// the answer: it is how the app matches replies to the commands
	// it sent, and a reply without it reads as a timeout over there.
	tag := ""
	if len(line) > 4 && line[2] == '|' {
		tag, line = line[:3], line[3:]
	}
	// The guard moves before the line runs, and durably: a mutation
	// executed on a timestamp that never reached the disk is one a
	// recording applies a second time after the next restart. Ordering
	// it after the command was the whole exposure — the effect landed,
	// then the proof it had landed was allowed to fail.
	if err := e.advanceClient(c, ts, time.Now()); err != nil {
		e.storeRefused(origin, "command", err)
		return
	}
	// Acknowledged before the retry is judged, as the reference does:
	// the ACK is what tells a legacy client its line arrived, and a
	// retry that earns neither ACK nor reply leaves it asking for
	// ever. The library knows which subtypes are owed one and what
	// they hash; ErrNoAck is the ordinary answer for the rest.
	if ack, err := meshcore.BuildCommandAck(rx.opened.plain, c.pubKey[:]); err == nil {
		e.ackText(pkt, c, ack, origin)
	} else if !errors.Is(err, meshcore.ErrNoAck) {
		e.log.Warn("command ack build failed", zap.String("corr", origin.Short()), zap.Error(err))
	}
	var out string
	if retry {
		// The reference answers a retry with an empty reply rather
		// than running the line again: the command already took.
		e.log.Debug("command retried — not run again",
			zap.String("corr", origin.Short()), zap.String("command", safeCommandLine(line)))
	} else {
		out = e.commands(line, c.pubKey[:])
		e.log.Info("command from the air",
			zap.String("corr", origin.Short()),
			zap.String("pubkey", shortKey(c.pubKey[:])),
			zap.String("command", safeCommandLine(line)))
		e.log.Debug("command answered",
			zap.String("corr", origin.Short()), zap.String("reply", safeCommandReply(line, out)))
	}
	if out == "" {
		return
	}
	out = tag + out
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

// ackText sends the legacy text acknowledgement the reference sends
// for a plain-subtype command line, down the same route its answer
// would take.
func (e *engine) ackText(inbound *meshcore.Packet, c *client, pkt *meshcore.Packet, origin correlation.ID) {
	scope := e.replyScope(&reception{pkt: inbound})
	if c.out != nil {
		pkt.Header = meshcore.MakeHeader(meshcore.RouteDirect,
			meshcore.PayloadTypeAck, meshcore.PayloadVer1)
		pkt.Path, pkt.PathLen = c.out.path, c.out.pathLen
		scope.Scope(pkt)
		e.logReplyRoute(pkt, origin, "cmd-ack", "learned", prioDirect)
		e.enqueueAfter(pkt, "cmd-ack", origin, prioDirect, serverResponseDelay)
		return
	}
	pkt.Header = meshcore.MakeHeader(meshcore.RouteFlood,
		meshcore.PayloadTypeAck, meshcore.PayloadVer1)
	pkt.SetPathHashSizeAndCount(inbound.PathHashSize(), 0)
	scope.Scope(pkt)
	e.logReplyRoute(pkt, origin, "cmd-ack", "flood", prioFloodReply)
	e.enqueueAfter(pkt, "cmd-ack", origin, prioFloodReply, serverResponseDelay)
}

// replyText sends one command's output back as a text message, down
// the route the admin taught when there is one.
func (e *engine) replyText(inbound *meshcore.Packet, c *client, plain []byte, origin correlation.ID) {
	pkt, err := meshcore.BuildDatagram(meshcore.PayloadTypeTxtMsg,
		c.pubKey[:meshcore.PathHashSize], e.id.PubKey[:meshcore.PathHashSize],
		c.secret, plain)
	if err != nil {
		e.log.Warn("command reply build failed", zap.String("corr", origin.Short()), zap.Error(err))
		return
	}
	scope := e.replyScope(&reception{pkt: inbound})
	if c.out != nil {
		pkt.Header = meshcore.MakeHeader(meshcore.RouteDirect,
			meshcore.PayloadTypeTxtMsg, meshcore.PayloadVer1)
		pkt.Path, pkt.PathLen = c.out.path, c.out.pathLen
		scope.Scope(pkt)
		e.logReplyRoute(pkt, origin, "cmd-resp", "learned", prioDirect)
		e.enqueueAfter(pkt, "cmd-resp", origin, prioDirect, serverResponseDelay)
		return
	}
	pkt.Header = meshcore.MakeHeader(meshcore.RouteFlood,
		meshcore.PayloadTypeTxtMsg, meshcore.PayloadVer1)
	pkt.SetPathHashSizeAndCount(inbound.PathHashSize(), 0)
	scope.Scope(pkt)
	e.logReplyRoute(pkt, origin, "cmd-resp", "flood", prioFloodReply)
	e.enqueueAfter(pkt, "cmd-resp", origin, prioFloodReply, serverResponseDelay)
}

// commandTailSafe are the verbs whose whole line may enter the
// journal: their tails carry names and figures, never credentials.
// Everything else logs its verb and subject alone — a set's value is
// a password whenever the setting is one, and an unknown command's
// tail is whatever the sender typed, canaries included. The store's
// revision journal keeps the (masked) values; the log does not need
// them twice.
// verbGet is the read verb, named once for the reply mask.
const verbGetSetting = "get"

var commandTailSafe = map[string]bool{
	"region": true, verbGetSetting: true, "advert": true, "advert.zerohop": true,
	"setperm": true, "discover.neighbors": true, "neighbor.remove": true,
	"ver": true, "clock": true, "time": true,
}

// safeCommandLine renders one command line for the journal. A set
// keeps its setting's name — that is the diagnosis — and loses its
// value; an unknown verb loses everything past itself, because its
// "subject" is just the first word of a tail nobody vetted.
func safeCommandLine(line string) string {
	fields := strings.Fields(line)
	switch {
	case len(fields) == 0:
		return ""
	case commandTailSafe[fields[0]]:
		return strings.Join(fields, " ")
	case fields[0] == "set" && len(fields) >= 2:
		return "set " + fields[1] + " …"
	case len(fields) == 1:
		return fields[0]
	default:
		return fields[0] + " …"
	}
}

// safeCommandReply renders one answer for the trace. A get's answer
// IS the setting's value — secret whenever the setting is — and an
// unknown line's echo is its tail again; both trace as their size.
func safeCommandReply(line, reply string) string {
	fields := strings.Fields(line)
	if len(fields) > 0 && fields[0] != verbGetSetting && commandTailSafe[fields[0]] {
		return reply
	}
	return fmt.Sprintf("(%d bytes)", len(reply))
}
