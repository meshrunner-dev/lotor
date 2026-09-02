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
	storeWait        = 10 * time.Second
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
func (s *service) uniqueNowLocked() uint32 {
	now := uint32(time.Now().Unix())
	if now <= s.lastUnique {
		now = s.lastUnique + 1
	}
	s.lastUnique = now
	return now
}

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
// deep, and every member's cursor.
func (s *service) loadHistory(ctx context.Context) error {
	if s.store == nil {
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
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(posts) > s.p.History {
		posts = posts[len(posts)-s.p.History:]
	}
	s.posts = s.posts[:0]
	for _, p := range posts {
		s.posts = append(s.posts, post{at: p.At, author: p.Author, text: p.Text})
		s.lastUnique = max(s.lastUnique, p.At)
	}
	for _, c := range cursors {
		s.member(c.PubKey).syncSince = c.SyncSince
	}
	return nil
}

// flushCursors writes the cursors that moved since the last flush —
// the reference's lazy five seconds, so an ACK never costs an fsync.
func (s *service) flushCursors(ctx context.Context) {
	if s.store == nil {
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
	for _, c := range dirty {
		if err := s.store.SaveRoomCursor(ctx, s.name, c); err != nil {
			s.log.Warn("a member's cursor did not reach the store", zap.Error(err))
			s.mu.Lock()
			if m := s.members[c.PubKey]; m != nil && m.syncSince == c.SyncSince {
				m.cursorDirty = true
			}
			s.mu.Unlock()
		}
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
// idle table's expiry and the cursor flush ride the same clock.
func (s *service) runPush(ctx context.Context) {
	flush := time.NewTicker(cursorFlushDelay)
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
			s.mu.Lock()
			s.table.Expire(time.Now(), meshcorehost.SessionIdle)
			s.mu.Unlock()
		case <-time.After(max(wait, 0)):
			s.pushDue(time.Now())
		}
	}
}

// pushDue is one turn of the clock at now: timed-out pushes counted,
// one member served, the next turn scheduled.
func (s *service) pushDue(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
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
	if pushed {
		s.nextPush = now.Add(syncPushInterval)
	} else {
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
	plain, err := mesh.BuildSignedTextPlaintext(time.Unix(int64(chosen.at), 0), chosen.author[:4],
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
		m.ackDeadline = now.Add(pushAckBase + pushAckPerHop*time.Duration(int(c.Out.PathLen&63)+1))
	} else {
		pkt.Header = mesh.MakeHeader(mesh.RouteFlood, mesh.PayloadTypeTxtMsg, mesh.PayloadVer1)
		pkt.SetPathHashSizeAndCount(mesh.PathHashSize, 0)
		priority = meshcorehost.PrioFloodReply
		m.ackDeadline = now.Add(pushAckFlood)
	}
	m.pendingAck = mesh.AckCRC(plain, key[:])
	m.pushAt = chosen.at
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
	stats := mesh.RoomStats{
		PacketsRecv: uint32(min(s.heard, 1<<32-1)),
		PacketsSent: uint32(min(s.sent, 1<<32-1)),
		UptimeSecs:  uint32(time.Since(s.started) / time.Second),
		Posted:      uint16(min(s.posted, 1<<16-1)),
		PostPushes:  uint16(min(s.pushes, 1<<16-1)),
	}
	if s.rfDevice != nil {
		if nf, ok := s.rfDevice.NoiseFloor(); ok {
			stats.NoiseFloor = int16(nf.DBm)
		}
	}
	return stats
}
