package room

// The room's members and what they say to it: logins judged by the
// shared kernel behind the room's own doors, posts acknowledged and
// kept, keep-alives answered with what is left to read, routes learned
// from the PATH returns clients send. Every handler runs on the RF
// goroutine and takes the service mutex: the push clock and the
// console read the same tables.

import (
	"context"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/correlation"
	"meshrunner.dev/lotor/internal/logging"
	"meshrunner.dev/lotor/internal/meshcorehost"
	"meshrunner.dev/lotor/internal/origin"
	"meshrunner.dev/lotor/internal/radio"

	mesh "meshrunner.dev/pkg/meshcore"
)

const (
	// The reference room server's constants, by name.
	maxPostText         = 151
	serverResponseDelay = 300 * time.Millisecond
	textAckDelay        = 200 * time.Millisecond
	pushNotifyDelay     = 2 * time.Second
	sessionLimitMax     = 6
	sessionLimitWindow  = time.Minute
	firmwareVerLevel    = 1

	// How long what the room composes is still worth the air. An
	// answer's asker waits four seconds plus two per hop for a direct
	// ACK, twelve for a flooded one, then re-asks: past half a minute
	// a queued answer only delays the fresh one behind it. An advert
	// that has waited a minute is older than the local clock's period.
	answerLife = 30 * time.Second
	advertLife = time.Minute
)

// routeOf is how the pipeline's neutral tally names the way a packet
// was composed to travel. Four lines rather than a shared helper on
// purpose: they turn a MeshCore header into a label origin defines,
// and neither package may learn about the other.
func routeOf(pkt *mesh.Packet) origin.Route {
	switch {
	case pkt.IsRouteFlood():
		return origin.RouteFlood
	case pkt.IsRouteDirect():
		return origin.RouteDirect
	default:
		return origin.RouteUnclassified
	}
}

// member is the room's own state about one client, beside what the
// kernel's table holds: how far it has read and the push in flight.
type member struct {
	syncSince   uint32
	pendingAck  uint32
	pushAt      uint32
	ackDeadline time.Time
	failures    uint8
	cursorDirty bool
	// lastKept is the timestamp of the newest post this member had
	// accepted and kept: the one a retry may claim an ACK for. The
	// replay guard moves on every attempt, kept or refused, so it
	// cannot tell the two apart; this can.
	lastKept uint32
}

func (s *service) member(key [mesh.PubKeySize]byte) *member {
	m := s.members[key]
	if m == nil {
		m = &member{}
		s.members[key] = m
	}
	return m
}

// processRF judges one frame on the RF goroutine: what is sealed to
// this room is answered, everything else is the mesh's business. The
// reference keeps every handler behind its seen ring, and so does the
// room: the copies a flood sends back through several repeaters, and a
// recording replayed on the air, hash like their original and act no
// more — no second ACK, no second write, no cursor pulled back twice.
func (s *service) processRF(ctx context.Context, frame radio.Frame) {
	pkt, err := mesh.ParsePacket(frame.Payload)
	if err != nil {
		return
	}
	s.mu.Lock()
	duplicate := s.seen.Witness(pkt.Hash())
	if duplicate {
		s.duplicates++
	}
	s.mu.Unlock()
	if duplicate {
		logging.Trace(s.log, "duplicate frame ignored", zap.String("corr", frame.Correlation.Short()),
			zap.Stringer("type", pkt.PayloadType()))
		return
	}
	switch pkt.PayloadType() {
	case mesh.PayloadTypeAnonReq:
		s.handleLogin(ctx, pkt, frame.Correlation)
	case mesh.PayloadTypeTxtMsg:
		s.handleText(ctx, pkt, frame.Correlation)
	case mesh.PayloadTypeReq:
		s.handleRequest(ctx, pkt, frame.Correlation)
	case mesh.PayloadTypePath:
		s.handlePath(pkt, frame.Correlation)
	case mesh.PayloadTypeAck:
		s.handleAck(pkt.Payload, frame.Correlation)
	case mesh.PayloadTypeMultipart:
		if crc, _, err := mesh.ParseMultiAck(pkt.Payload); err == nil {
			s.ackReceived(crc, frame.Correlation)
		}
	default:
	}
}

// doors is the room's word-to-role map, the reference's: the admin
// word earns admin, the room word earns a member who may post,
// allow_read_only admits any other word as a guest who may only read.
// An empty word opens nothing here — the recheck a known key makes
// with a blank password never reaches the doors.
func (s *service) doors(word string) (byte, bool) {
	switch {
	case s.p.AdminPassword != "" && subtle.ConstantTimeCompare([]byte(word), []byte(s.p.AdminPassword)) == 1:
		return mesh.PermAdmin, true
	case s.p.GuestPassword != "" && subtle.ConstantTimeCompare([]byte(word), []byte(s.p.GuestPassword)) == 1:
		return mesh.PermReadWrite, true
	case s.p.AllowReadOnly:
		return mesh.PermGuest, true
	}
	return 0, false
}

// handleLogin answers an ANON_REQ sealed to the room. The kernel
// judges the attempt on a candidate; the room applies the cursor the
// client offered and resets its push state, then answers the
// reference's 13 bytes the way the question came — a path return when
// it flooded, so the client learns the way here.
func (s *service) handleLogin(ctx context.Context, pkt *mesh.Packet, corr correlation.ID) {
	// A stranger pays for the key agreement out of a budget; a member
	// the room knows never does. Judged before the ECDH, on the key
	// the envelope carries in the clear, so a refused attempt costs
	// the reception and nothing more.
	if sender, ok := meshcorehost.AnonSender(s.id, pkt.Payload); !ok {
		return
	} else if !s.admitStranger(sender) {
		s.log.Debug("login rate-limited: unknown key", zap.String("corr", corr.Short()))
		return
	}
	a, _, ok := meshcorehost.OpenAnon(s.id, pkt.Payload)
	if !ok {
		return
	}
	login, err := mesh.ParseRoomLogin(a.Plain)
	if err != nil {
		return
	}
	now := time.Now()
	if meshcorehost.Skewed(login.Timestamp, now) {
		s.log.Debug("login refused: stale or future timestamp", zap.String("corr", corr.Short()))
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	c, refusal := meshcorehost.Admit(s.table.Get(a.Sender), a.Sender, a.Secret, login.Password,
		login.Timestamp, s.doors)
	if c == nil {
		s.log.Debug("login refused", zap.String("corr", corr.Short()), zap.String("why", string(refusal)))
		return
	}
	c.LastTimestamp, c.LastActive, c.Active = login.Timestamp, now, true
	c.Asks = meshcorehost.RateLimiter{Max: sessionLimitMax, Window: sessionLimitWindow}
	if pkt.IsRouteFlood() {
		// The reference rediscovers the route home after a flooded
		// login: whatever it knew, the client is somewhere else now.
		c.Out = nil
	}
	victim, evicted, err := s.table.PutEvicting(c)
	if err != nil {
		s.log.Warn("login refused: the member table would not take it",
			zap.String("corr", corr.Short()), zap.Error(err))
		return
	}
	if evicted {
		s.evictedLocked(ctx, victim, corr)
	}
	m := s.member(c.PubKey)
	// Applied on every accepted login, the blank recheck included —
	// the reference leaves a rechecking member's cursor where it was,
	// which is the sharp edge that strands a returning admin.
	m.syncSince, m.pendingAck, m.failures, m.cursorDirty = login.SyncSince, 0, 0, true
	s.nextPush = now.Add(pushNotifyDelay)
	body, err := meshcorehost.LoginReply(c, firmwareVerLevel, now, true)
	if err != nil {
		return
	}
	clock, rest, err := mesh.UnframeAdmin(body)
	if err != nil {
		return
	}
	s.log.Info(meshcorehost.RoleName(c.Perms)+" logged in", zap.String("corr", corr.Short()),
		zap.String("pubkey", hex.EncodeToString(c.PubKey[:6])), zap.Uint32("since", login.SyncSince))
	s.replyLocked(ctx, pkt, meshcorehost.Answer{
		DestHash: c.PubKey[:mesh.PathHashSize], Secret: c.Secret, Tag: clock, Body: rest, Out: c.Out,
	}, "login-resp", corr)
}

// admitStranger charges a login from a key the room does not know to
// the strangers' budget, and reports whether it may proceed.
func (s *service) admitStranger(sender []byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.table.Get(sender) != nil {
		return true
	}
	if s.strangers.Allow(time.Now()) {
		return true
	}
	s.limited++
	return false
}

// evictedLocked lets go of the room's own memory of a member the table
// unseated: its cursor, in RAM and — best effort — in the store, a
// stale row costing nothing but a few bytes until a login rewrites it.
func (s *service) evictedLocked(ctx context.Context, victim [mesh.PubKeySize]byte, corr correlation.ID) {
	delete(s.members, victim)
	if s.store != nil {
		forgetCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), storeWait)
		defer cancel()
		if err := s.store.ForgetRoomCursor(forgetCtx, s.name, victim[:]); err != nil {
			s.log.Warn("an evicted member's cursor did not leave the store", zap.Error(err))
		}
	}
	s.log.Info("member evicted to make room", zap.String("corr", corr.Short()),
		zap.String("pubkey", hex.EncodeToString(victim[:6])))
}

// handleText takes a member's post — or an admin's command line, which
// this cut does not yet serve. The replay guard is the reference's: an
// older timestamp is a recording, an equal one a retry that earns its
// ACK again and nothing else. A guest may read and never post, and
// earns silence for trying.
func (s *service) handleText(ctx context.Context, pkt *mesh.Packet, corr correlation.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, plain := s.table.OpenSession(s.id, pkt.Payload)
	if c == nil {
		return
	}
	text, err := mesh.ParseTextPlaintext(plain)
	if err != nil || (text.Type != mesh.TxtTypePlain && text.Type != mesh.TxtTypeCLIData) {
		return
	}
	ts := uint32(text.Timestamp.Unix())
	if ts < c.LastTimestamp {
		s.log.Debug("post replay refused", zap.String("corr", corr.Short()))
		return
	}
	// A retry is the newest timestamp asked again — and only when the
	// room kept what it carried: the guard moved on a refused attempt
	// too, and a retry of a refusal must be judged afresh, or the room
	// would acknowledge a post it never had.
	m := s.member(c.PubKey)
	retry := ts == c.LastTimestamp && m.lastKept == ts
	now := time.Now()
	if err := s.table.Advance(c, ts, now); err != nil {
		s.log.Warn("the member store refused the replay guard — post not taken",
			zap.String("corr", corr.Short()), zap.Error(err))
		return
	}
	c.Active = true
	m.failures = 0
	if text.Type == mesh.TxtTypeCLIData {
		if c.IsAdmin() {
			s.log.Debug("admin command line not served yet", zap.String("corr", corr.Short()))
		}
		return // the reference acknowledges no CLI text; the reply is the acknowledgement
	}
	if mesh.Role(c.Perms) == mesh.PermGuest {
		s.log.Debug("post refused: guest", zap.String("corr", corr.Short()))
		return
	}
	s.acceptPostLocked(ctx, pkt, c, m, plain, text.Text, ts, retry, corr)
}

// acceptPostLocked keeps a member's post and acknowledges it — a retry
// earns the acknowledgement again and nothing else. The reference
// truncates silently at 151 characters while allowing its clients 160;
// a post that is not what its author wrote is not acknowledged as if
// it were, and neither is one that could not be kept.
func (s *service) acceptPostLocked(ctx context.Context, pkt *mesh.Packet, c *meshcorehost.Client, m *member,
	plain []byte, text string, ts uint32, retry bool, corr correlation.ID,
) {
	if !retry {
		if len(text) > maxPostText {
			s.log.Warn("post refused: too long", zap.String("corr", corr.Short()), zap.Int("bytes", len(text)))
			s.refused++
			return
		}
		if err := s.storePostLocked(ctx, c.PubKey, text, corr); err != nil {
			s.log.Error("post refused: not persisted", zap.String("corr", corr.Short()), zap.Error(err))
			s.refused++
			return
		}
		m.lastKept = ts
	}
	ack, err := mesh.BuildCommandAck(plain, c.PubKey[:])
	if err != nil {
		return
	}
	priority, source := meshcorehost.RouteHome(ack, pkt, c.Out, mesh.TransportKey{})
	s.sendLocked(ack, "post-ack", uint8(priority), textAckDelay, corr)
	logging.Trace(s.log, "post acknowledged", zap.String("corr", corr.Short()),
		zap.String("route", source), zap.Bool("retry", retry))
}

// handleRequest serves an authenticated REQ: the keep-alive that
// moves a member's cursor and is answered with what is left to read,
// the status a companion's page shows, the access list an admin may
// ask for.
func (s *service) handleRequest(ctx context.Context, pkt *mesh.Packet, corr correlation.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, plain := s.table.OpenSession(s.id, pkt.Payload)
	if c == nil {
		return
	}
	ts, body, err := mesh.UnframeAdmin(plain)
	if err != nil || len(body) == 0 {
		return
	}
	// The reference's REQ guard is "<", where its TXT guard is "<=":
	// an equal keep-alive is served again.
	if ts < c.LastTimestamp {
		s.log.Debug("request replay refused", zap.String("corr", corr.Short()))
		return
	}
	now := time.Now()
	if err := s.table.Advance(c, ts, now); err != nil {
		s.log.Warn("the member store refused the replay guard — request not served",
			zap.String("corr", corr.Short()), zap.Error(err))
		return
	}
	c.Active = true
	m := s.member(c.PubKey)
	m.failures = 0
	if body[0] == mesh.ReqKeepAlive {
		// Direct only, the reference's rule: a flooded keep-alive is
		// not one, and the request handler answers nothing for it.
		if pkt.IsRouteDirect() {
			s.keepAliveLocked(c, m, plain, body, corr)
		}
		return
	}
	if c.Out == nil && !c.Asks.Allow(now) {
		s.log.Debug("request rate-limited — flood answers", zap.String("corr", corr.Short()))
		return
	}
	answer, answered := s.answerLocked(c, body)
	if !answered {
		return
	}
	s.replyLocked(ctx, pkt, meshcorehost.Answer{
		DestHash: c.PubKey[:mesh.PathHashSize], Secret: c.Secret, Tag: ts, Body: answer, Out: c.Out,
	}, "req-resp", corr)
}

// keepAliveLocked takes a member's cursor when it forces one, abandons
// the push in flight, and answers — direct only, the reference's rule
// — with the CRC both ends compute and how much is left to read.
func (s *service) keepAliveLocked(c *meshcorehost.Client, m *member, plain, body []byte, corr correlation.ID) {
	if since, err := mesh.ParseKeepAliveRequest(body); err == nil && since > 0 {
		m.syncSince, m.cursorDirty = since, true
	}
	m.pendingAck = 0
	if c.Out == nil {
		return // "RULE: only send keep_alive response DIRECT!"
	}
	crc, err := mesh.KeepAliveAckCRC(plain, c.PubKey[:])
	if err != nil {
		return
	}
	ack, err := mesh.BuildAck(mesh.FrameKeepAliveAck(crc, s.unsyncedLocked(c.PubKey, m)))
	if err != nil {
		return
	}
	meshcorehost.RouteDirect(ack, c.Out, mesh.TransportKey{})
	s.sendLocked(ack, "keepalive-ack", meshcorehost.PrioDirect, serverResponseDelay, corr)
}

// answerLocked composes the body of an authenticated answer: the
// status a companion's page shows, the access list an admin may ask
// for. answered is false for a question this room does not serve.
func (s *service) answerLocked(c *meshcorehost.Client, body []byte) ([]byte, bool) {
	switch body[0] {
	case mesh.ReqGetStatus:
		return s.statsLocked().AppendTo(nil), true
	case mesh.ReqGetAccessList:
		if !c.IsAdmin() || mesh.ParseAccessListRequest(body) != nil {
			return nil, false
		}
		return s.accessListLocked(), true
	default:
		return nil, false
	}
}

// accessListLocked is the reference's answer: the admins alone, by key.
func (s *service) accessListLocked() []byte {
	var entries []mesh.AccessEntry
	for _, e := range s.table.Entries() {
		if !e.Admin {
			continue
		}
		var row mesh.AccessEntry
		copy(row.PubKeyPrefix[:], e.PubKey[:])
		row.Permissions = e.Perms
		entries = append(entries, row)
	}
	return mesh.FrameAccessList(entries)
}

// handlePath learns the route a member taught, and takes the ACK it
// may carry. The reference sends no path back.
func (s *service) handlePath(pkt *mesh.Packet, corr correlation.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, plain := s.table.OpenSession(s.id, pkt.Payload)
	if c == nil {
		return
	}
	pr, err := mesh.DecodePathReturn(plain)
	if err != nil {
		return
	}
	now := time.Now()
	c.Out = &meshcorehost.OutPath{PathLen: pr.PathLen, Path: append([]byte(nil), pr.Path...), Learned: now}
	c.LastActive, c.Active = now, true
	if err := s.table.Save(c); err != nil {
		s.log.Warn("the taught route did not reach the store", zap.String("corr", corr.Short()), zap.Error(err))
	}
	s.log.Debug("a member taught us its route home", zap.String("corr", corr.Short()),
		zap.String("pubkey", hex.EncodeToString(c.PubKey[:6])), zap.Int("hops", int(pr.PathLen&63)))
	if pr.ExtraType == uint8(mesh.PayloadTypeAck) {
		if crc, err := mesh.ParseAck(pr.Extra); err == nil {
			s.ackReceivedLocked(crc, corr)
		}
	}
}

func (s *service) handleAck(payload []byte, corr correlation.ID) {
	crc, err := mesh.ParseAck(payload)
	if err != nil {
		return
	}
	s.ackReceived(crc, corr)
}

func (s *service) ackReceived(crc uint32, corr correlation.ID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ackReceivedLocked(crc, corr)
}

// ackReceivedLocked advances the cursor of the member whose push this
// acknowledges. An ACK matching no push is not ours, or too late.
func (s *service) ackReceivedLocked(crc uint32, corr correlation.ID) {
	for key, m := range s.members {
		if m.pendingAck == 0 || m.pendingAck != crc {
			continue
		}
		m.pendingAck, m.failures = 0, 0
		m.syncSince, m.cursorDirty = m.pushAt, true
		logging.Trace(s.log, "push acknowledged", zap.String("corr", corr.Short()),
			zap.String("pubkey", hex.EncodeToString(key[:6])), zap.Uint32("since", m.syncSince))
		return
	}
}

// replyLocked composes an answer the reference's way and queues it
// after the server's fixed pause.
func (s *service) replyLocked(_ context.Context, inbound *mesh.Packet, a meshcorehost.Answer,
	kind string, corr correlation.ID,
) {
	pkt, priority, source, err := meshcorehost.ComposeReply(inbound, a, s.id.PubKey[:mesh.PathHashSize])
	if err != nil {
		s.log.Warn("reply too large to compose", zap.String("corr", corr.Short()), zap.String("kind", kind), zap.Error(err))
		return
	}
	logging.Trace(s.log, "reply route selected", zap.String("corr", corr.Short()),
		zap.String("kind", kind), zap.String("route_source", source))
	s.sendLocked(pkt, kind, uint8(priority), serverResponseDelay, corr)
}

// sendLocked hands one composed packet to the pipeline. A dry gate
// counts it and sends nothing; the pipeline's queue refuses a full
// backlog, counted.
func (s *service) sendLocked(pkt *mesh.Packet, kind string, priority uint8, delay time.Duration,
	corr correlation.ID,
) {
	s.composed++
	if s.gate() == config.TXDry {
		logging.Trace(s.log, "emission composed, gate is dry", zap.String("kind", kind), zap.String("corr", corr.Short()))
		return
	}
	raw, err := pkt.MarshalBinary()
	if err != nil {
		s.log.Warn("emission not marshalled", zap.String("kind", kind), zap.Error(err))
		return
	}
	now := time.Now()
	item := origin.Emission{
		Frame: raw, Route: routeOf(pkt), Correlation: corr, Kind: kind, Priority: priority,
		NotBefore: now.Add(delay), Expires: now.Add(answerLife),
	}
	if !s.pipeline.Queue.Offer(item) {
		s.pipeline.Drop(item, "queue-full")
		s.dropped++
	}
}

var errNoStore = errors.New("no store behind this room")
