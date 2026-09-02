package confdb

// A room server's memory: what was said in it, and how far each member
// has read. Data tables beside the revision trail, not in it — posts
// and cursors are not configuration mutations — the one stated
// exception the design records. Both are keyed by the application's
// name, so removing the application removes what it remembered.

import (
	"context"
	"fmt"
	"time"

	"meshrunner.dev/pkg/meshcore"
)

// RoomPost is one post as the store keeps it: the room's own clock
// when it was stored — the timestamp members synchronise on — its
// author's full key, the text, and the correlation of the frame that
// carried it, so the journal can follow a post from reception to
// every push.
type RoomPost struct {
	Seq         int64
	At          uint32
	Author      [meshcore.PubKeySize]byte
	Text        string
	Correlation string
}

// RoomCursor is how far one member has read: the room-clock timestamp
// of the newest post it acknowledged.
type RoomCursor struct {
	PubKey    [meshcore.PubKeySize]byte
	SyncSince uint32
}

// LoadRoomPosts reads a room's history, oldest first.
func (s *Store) LoadRoomPosts(ctx context.Context, app string) ([]RoomPost, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT seq, at, author, text, corr FROM room_posts WHERE app = ? ORDER BY seq", app)
	if err != nil {
		return nil, fmt.Errorf("load room %q posts: %w", app, err)
	}
	defer func() { _ = rows.Close() }()
	var out []RoomPost
	for rows.Next() {
		var p RoomPost
		var at int64
		var author []byte
		if err := rows.Scan(&p.Seq, &at, &author, &p.Text, &p.Correlation); err != nil {
			return nil, err
		}
		p.At = uint32(at)
		copy(p.Author[:], author)
		out = append(out, p)
	}
	return out, rows.Err()
}

// SaveRoomPost appends one post and forgets the oldest beyond keep —
// the ring's semantics, on disk. It returns the sequence the post
// took. A keep of zero keeps everything.
func (s *Store) SaveRoomPost(ctx context.Context, app string, p RoomPost, keep int) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	res, err := tx.ExecContext(ctx,
		`INSERT INTO room_posts(app, seq, at, author, text, corr)
		 VALUES(?, (SELECT COALESCE(MAX(seq), 0) + 1 FROM room_posts WHERE app = ?), ?, ?, ?, ?)`,
		app, app, int64(p.At), p.Author[:], p.Text, p.Correlation)
	if err != nil {
		return 0, fmt.Errorf("save room %q post: %w", app, err)
	}
	var seq int64
	if err := tx.QueryRowContext(ctx,
		"SELECT MAX(seq) FROM room_posts WHERE app = ?", app).Scan(&seq); err != nil {
		return 0, err
	}
	_ = res
	if keep > 0 {
		if _, err := tx.ExecContext(ctx,
			"DELETE FROM room_posts WHERE app = ? AND seq <= ?", app, seq-int64(keep)); err != nil {
			return 0, fmt.Errorf("prune room %q posts: %w", app, err)
		}
	}
	return seq, tx.Commit()
}

// ClearRoomHistory forgets everything a room said.
func (s *Store) ClearRoomHistory(ctx context.Context, app string) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM room_posts WHERE app = ?", app)
	return err
}

// LoadRoomCursors reads every member's cursor.
func (s *Store) LoadRoomCursors(ctx context.Context, app string) ([]RoomCursor, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT pubkey, sync_since FROM room_cursors WHERE app = ?", app)
	if err != nil {
		return nil, fmt.Errorf("load room %q cursors: %w", app, err)
	}
	defer func() { _ = rows.Close() }()
	var out []RoomCursor
	for rows.Next() {
		var c RoomCursor
		var pub []byte
		var since int64
		if err := rows.Scan(&pub, &since); err != nil {
			return nil, err
		}
		copy(c.PubKey[:], pub)
		c.SyncSince = uint32(since)
		out = append(out, c)
	}
	return out, rows.Err()
}

// SaveRoomCursor records how far one member has read.
func (s *Store) SaveRoomCursor(ctx context.Context, app string, c RoomCursor) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO room_cursors(app, pubkey, sync_since, updated) VALUES(?, ?, ?, ?)
		 ON CONFLICT(app, pubkey) DO UPDATE SET sync_since = excluded.sync_since, updated = excluded.updated`,
		app, c.PubKey[:], int64(c.SyncSince), time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("save room %q cursor: %w", app, err)
	}
	return nil
}

// ForgetRoomCursor drops one member's cursor — what a revocation does.
func (s *Store) ForgetRoomCursor(ctx context.Context, app string, pubKey []byte) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM room_cursors WHERE app = ? AND pubkey = ?", app, pubKey)
	return err
}

// ApplicationOwner is the acl table's owner key for an application:
// the instance-name grammar forbids the colon, so it can never collide
// with a relay's bare name.
func ApplicationOwner(app string) string { return KindApplication + ":" + app }
