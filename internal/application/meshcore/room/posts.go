package room

// What the room remembers and how it hands it out: the ring of posts,
// its durable copy, the members' cursors, and the push clock that walks
// members round-robin and offers each the oldest post it has not read.

import (
	"bytes"
	"context"
	"encoding/hex"
	rand "math/rand/v2"
	"sort"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/confdb"
	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/correlation"
	"meshrunner.dev/lotor/internal/logging"
	"meshrunner.dev/lotor/internal/meshcorehost"

	mesh "meshrunner.dev/pkg/meshcore"
)

// The reference's push clock, by name: how long a post settles before
// it is offered, the pace between pushes, the walk between idle
// members, how long a push may wait for its ACK by route, how many
// unanswered pushes stall a member, and how lazily cursors reach disk.
const (
	postSyncDelay    = 6 * time.Second
	syncPushInterval = 1200 * time.Millisecond
	syncIdleInterval = syncPushInterval / 8
	pushAckFlood     = 12 * time.Second
	pushAckBase      = 4 * time.Second
	pushAckPerHop    = 2 * time.Second
	maxPushFailures  = 3
	cursorFlushDelay = 5 * time.Second
	// storeWait bounds one write. It is taken under the service mutex —
	// see the package doc — so a disk that has stopped answering costs
	// the room this long, once per write, and then a refused ACK the
	// client retries: not a frozen RF loop for as long as the disk sulks.
	storeWait = 3 * time.Second
)

// post is one thing said in the room: when, by whom, what.
type post struct {
	at     uint32
	author [mesh.PubKeySize]byte
	text   string
	corr   correlation.ID
}

// uniqueNowLocked is the reference's getCurrentTimeUnique: the room's
// clock, strictly increasing within a run, because two posts stamped
// alike would be one post to a client's cursor.
func (s *service) uniqueNowLocked() uint32 { return s.clock.Now(time.Now()) }

// storePostLocked keeps one post: on disk first when history persists —
// a post acknowledged is a post kept — then in the ring, the oldest
// giving way past the configured depth.
func (s *service) storePostLocked(ctx context.Context, author [mesh.PubKeySize]byte, text string,
	corr correlation.ID,
) error {
	p := post{at: s.uniqueNowLocked(), author: author, text: text, corr: corr}
	if s.p.PersistHistory {
		if s.store == nil {
			return errNoStore
		}
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), storeWait)
		defer cancel()
		if _, err := s.store.SaveRoomPost(ctx, s.name, confdb.RoomPost{
			At: p.at, Author: p.author, Text: p.text, Correlation: corr.String(),
		}, s.p.History); err != nil {
			return err
		}
	}
	s.posts = append(s.posts, p)
	if len(s.posts) > s.p.History {
		s.posts = s.posts[len(s.posts)-s.p.History:]
	}
	s.posted++
	s.nextPush = time.Now().Add(pushNotifyDelay)
	s.log.Info("post stored", zap.String("corr", corr.Short()),
		zap.String("author", hex.EncodeToString(author[:6])), zap.Uint32("at", p.at), zap.Int("bytes", len(text)))
	return nil
}

// loadHistory restores what the store holds: the posts, newest ring
// deep, and every member's cursor. A room that does not persist its
// history reads none back — the RAM ring the reference runs, and the
// cursors reseed themselves from what each member offers at login.
func (s *service) loadHistory(ctx context.Context) error {
	if s.store == nil || !s.p.PersistHistory {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, storeWait)
	defer cancel()
	posts, err := s.store.LoadRoomPosts(ctx, s.name)
	if err != nil {
		return err
	}
	cursors, err := s.store.LoadRoomCursors(ctx, s.name)
	if err != nil {
		return err
	}
	if len(posts) > s.p.History {
		// A memory shortened since these were kept: the surplus leaves
		// the store now rather than waiting for the next post to make
		// room, so the store and the ring agree from the first minute.
		posts = posts[len(posts)-s.p.History:]
		if err := s.store.PruneRoomPosts(ctx, s.name, s.p.History); err != nil {
			s.log.Warn("the store kept more history than configured", zap.Error(err))
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.posts = s.posts[:0]
	for _, p := range posts {
		// A post keeps the correlation it was received under, so the
		// journal follows it from reception to every push across a
		// restart; a row without one is a row from before.
		corr, err := correlation.Parse(p.Correlation)
		if err != nil {
			s.log.Warn("a stored post carries an unreadable correlation", zap.Error(err))
		}
		s.posts = append(s.posts, post{at: p.At, author: p.Author, text: p.Text, corr: corr})
		s.clock.Observe(p.At)
	}
	for _, c := range cursors {
		// A cursor is a member's: a row whose key a durable table no
		// longer holds — its member evicted while the room was down — is
		// not resurrected as a phantom the push clock would serve. A
		// memory-only table knows nobody yet, and keeps every cursor
		// for the members who will log back in.
		if s.table.Durable() && s.table.Get(c.PubKey[:]) == nil {
			continue
		}
		s.member(c.PubKey).syncSince = c.SyncSince
	}
	return nil
}

// flushCursors writes the cursors that moved since the last flush —
// the reference's lazy five seconds, so an ACK never costs an fsync.
// Nothing is written when history does not persist: the flag is the
// operator's word that this room touches the disk for nothing.
func (s *service) flushCursors(ctx context.Context) {
	if s.store == nil || !s.p.PersistHistory {
		return
	}
	s.mu.Lock()
	dirty := make([]confdb.RoomCursor, 0)
	for key, m := range s.members {
		if m.cursorDirty {
			dirty = append(dirty, confdb.RoomCursor{PubKey: key, SyncSince: m.syncSince})
			m.cursorDirty = false
		}
	}
	s.mu.Unlock()
	if len(dirty) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, storeWait)
	defer cancel()
	// One transaction for the flush: one fsync for the batch, and a
	// refusal re-dirties every cursor it carried — none of them landed.
	if err := s.store.SaveRoomCursors(ctx, s.name, dirty); err != nil {
		s.log.Warn("the members' cursors did not reach the store", zap.Error(err), zap.Int("cursors", len(dirty)))
		s.mu.Lock()
		for _, c := range dirty {
			if m := s.members[c.PubKey]; m != nil && m.syncSince == c.SyncSince {
				m.cursorDirty = true
			}
		}
		s.mu.Unlock()
	}
}

// unsyncedLocked counts what a member has not read — the reference's
// getUnsyncedCount, which ignores the settling delay.
func (s *service) unsyncedLocked(key [mesh.PubKeySize]byte, m *member) uint8 {
	n := 0
	for _, p := range s.posts {
		if p.at > m.syncSince && p.author != key {
			n++
		}
	}
	return uint8(min(n, 255))
}

// runPush is the reference's loop: sweep the pushes that timed out,
// offer one member one post, pace by whether anything went out. The
// cursor flush rides the same clock. Nothing expires: a room's normal
// member is a reader who says nothing for hours and expects its
// pushes, so, as in the reference, a member stays until the table
// makes room for a newer one.
func (s *service) runPush(ctx context.Context) {
	flush := time.NewTicker(s.delays.flush)
	defer flush.Stop()
	for {
		s.mu.Lock()
		wait := time.Until(s.nextPush)
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			s.flushCursors(context.WithoutCancel(ctx))
			return
		case <-flush.C:
			s.flushCursors(ctx)
		case <-time.After(max(wait, 0)):
			s.pushDue(time.Now())
		}
	}
}

// pushDue is one turn of the clock at now: timed-out pushes counted,
// one member served, the next turn scheduled. A turn before its time
// does nothing and moves nothing — the reference re-tests its schedule
// on every pass, and a login or a post that pushed the schedule out
// while the clock was already armed must be honoured, not overwritten.
func (s *service) pushDue(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if now.Before(s.nextPush) {
		return
	}
	for _, m := range s.members {
		if m.pendingAck != 0 && now.After(m.ackDeadline) {
			m.failures++
			m.pendingAck = 0
		}
	}
	keys := s.memberKeysLocked()
	pushed := false
	if len(keys) > 0 {
		s.nextClient %= len(keys)
		key := keys[s.nextClient]
		s.nextClient = (s.nextClient + 1) % len(keys)
		pushed = s.pushToLocked(now, key)
	}
	switch {
	case pushed:
		s.nextPush = now.Add(syncPushInterval)
	case len(keys) == 0:
		// Nobody to serve: the reference's loop spins anyway, but a
		// daemon need not wake eight times a second for an empty room.
		s.nextPush = now.Add(syncPushInterval)
	default:
		s.nextPush = now.Add(syncIdleInterval)
	}
}

// memberKeysLocked walks the table in a stable order, so round-robin
// means the same thing from one turn to the next.
func (s *service) memberKeysLocked() [][mesh.PubKeySize]byte {
	keys := make([][mesh.PubKeySize]byte, 0, len(s.table.By))
	for k := range s.table.By {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })
	return keys
}

// pushToLocked offers one member the oldest post it has not read: a
// signed-plain text carrying the author's prefix, sealed to the member,
// down its taught route or flooded, with the ACK it must return
// remembered against a deadline the route decides.
func (s *service) pushToLocked(now time.Time, key [mesh.PubKeySize]byte) bool {
	c := s.table.Live(key)
	if c == nil || !c.Active {
		return false
	}
	m := s.member(key)
	if m.pendingAck != 0 || m.failures >= maxPushFailures {
		return false
	}
	var chosen *post
	for i := range s.posts {
		p := &s.posts[i]
		if p.at <= m.syncSince || p.author == key || now.Before(time.Unix(int64(p.at), 0).Add(postSyncDelay)) {
			continue
		}
		chosen = p
		break
	}
	if chosen == nil {
		return false
	}
	plain, err := mesh.BuildSignedTextPlaintext(time.Unix(int64(chosen.at), 0), chosen.author[:mesh.SignedPrefixSize],
		chosen.text, uint8(rand.IntN(4))) //nolint:gosec // the reference's random attempt bits, not security
	if err != nil {
		return false
	}
	pkt, err := mesh.BuildDatagram(mesh.PayloadTypeTxtMsg, key[:mesh.PathHashSize],
		s.id.PubKey[:mesh.PathHashSize], c.Secret, plain)
	if err != nil {
		s.log.Warn("push not composed", zap.Error(err))
		return false
	}
	var priority int
	if c.Out != nil {
		priority = meshcorehost.RouteDirect(pkt, c.Out, mesh.TransportKey{})
		m.ackDeadline = now.Add(pushAckBase + pushAckPerHop*time.Duration(mesh.PathHops(c.Out.PathLen)+1))
	} else {
		// No route taught yet: the reference floods the push under
		// the room's default scope at its own hash width.
		priority = meshcorehost.RouteFloodFresh(pkt, s.p.pathHashWidth(), s.p.scope())
		m.ackDeadline = now.Add(pushAckFlood)
	}
	// A dry gate composes and counts and sends nothing — so it must
	// expect nothing back: an ACK armed for a frame that never left
	// would count three failures and stall every member for good.
	if s.gate() != config.TXDry {
		m.pendingAck = mesh.AckCRC(plain, key[:])
		m.pushAt = chosen.at
	}
	s.pushes++
	corr := correlation.New()
	logging.Trace(s.log, "post pushed", zap.String("corr", corr.Short()),
		zap.String("caused_by", chosen.corr.Short()),
		zap.String("pubkey", hex.EncodeToString(key[:6])), zap.Uint32("at", chosen.at))
	s.sendLocked(pkt, "post-push", uint8(priority), 0, corr)
	return true
}

// statsLocked is the reference's ServerStats about this room.
func (s *service) statsLocked() mesh.RoomStats {
	// The reference's err_events counts what its firmware could not
	// do with a frame; here that is a reception it could not decode
	// and an emission it gave up on. No battery is sensed for an
	// application in this cut, so BattMilliVolts stays zero.
	stats := mesh.RoomStats{
		TxQueueLen:    uint16(min(s.pipeline.Queue.Len(), 1<<16-1)),
		LastRSSI:      int16(s.lastRSSI),
		LastSNR:       s.lastSNR,
		RecvFlood:     uint32(min(s.recvFlood, 1<<32-1)),
		RecvDirect:    uint32(min(s.recvDirect, 1<<32-1)),
		FloodDups:     uint16(min(s.floodDups, 1<<16-1)),
		DirectDups:    uint16(min(s.directDups, 1<<16-1)),
		ErrEvents:     uint16(min(s.corrupt+s.dropped, 1<<16-1)),
		PacketsRecv:   uint32(min(s.heard, 1<<32-1)),
		PacketsSent:   uint32(min(s.sent, 1<<32-1)),
		SentFlood:     uint32(min(s.sentFlood, 1<<32-1)),
		SentDirect:    uint32(min(s.sentDirect, 1<<32-1)),
		TxAirtimeSecs: uint32(min(s.txAir/time.Second, 1<<32-1)),
		UptimeSecs:    uint32(time.Since(s.started) / time.Second),
		Posted:        uint16(min(s.posted, 1<<16-1)),
		PostPushes:    uint16(min(s.pushes, 1<<16-1)),
	}
	if s.rfDevice != nil {
		if nf, ok := s.rfDevice.NoiseFloor(); ok {
			stats.NoiseFloor = int16(nf.DBm)
		}
	}
	return stats
}
