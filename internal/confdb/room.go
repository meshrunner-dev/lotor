package confdb

// A room server's memory: what was said in it, which client requests
// were accepted, and how far each member has read. Data tables beside
// the revision trail, not in it — posts, receipts and cursors are not
// configuration mutations — the one stated exception the design
// records. All are keyed by the application's
// name, so removing the application removes what it remembered.

import (
	"context"
	"fmt"
	"time"
)

// roomKeySize is the width of a member's key as the room tables keep
// it — an Ed25519 public key. The store does not speak MeshCore to
// know how wide a column is; the room package, which does, converts
// from the library's own constant, and the two are the same 32.
const roomKeySize = 32

// RoomPost is one post as the store keeps it: the room's own clock
// when it was stored — the timestamp members synchronise on — its
// author's full key, the text, and the correlation of the frame that
// carried it, so the journal can follow a post from reception to
// every push.
type RoomPost struct {
	Seq         int64
	At          uint32
	Author      [roomKeySize]byte
	Text        string
	Correlation string
	// ClientTimestamp is input to SaveRoomPost: the client's timestamp
	// of the accepted post. Nonzero records a receipt in the same
	// transaction, separately from the history ring; LoadRoomReceipts
	// reads it back. Zero leaves receipts untouched, for posts without
	// a client request and callers predating receipt tracking.
	ClientTimestamp uint32
}

// RoomReceipt records the newest post accepted from one author. Its
// timestamp is the client's, distinct from the room clock in RoomPost.At,
// and it survives history pruning so a retry cannot append the post again.
type RoomReceipt struct {
	Author          [roomKeySize]byte
	ClientTimestamp uint32
}

// RoomCursor is how far one member has read: the room-clock timestamp
// of the newest post it acknowledged.
type RoomCursor struct {
	PubKey    [roomKeySize]byte
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
// took. A keep of zero keeps everything. The acceptance receipt lands
// in the same transaction: a failed post must never earn a receipt,
// and a kept post must not lose its receipt to an interrupted write.
func (s *Store) SaveRoomPost(ctx context.Context, app string, p RoomPost, keep int) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO room_posts(app, seq, at, author, text, corr)
		 VALUES(?, (SELECT COALESCE(MAX(seq), 0) + 1 FROM room_posts WHERE app = ?), ?, ?, ?, ?)`,
		app, app, int64(p.At), p.Author[:], p.Text, p.Correlation); err != nil {
		return 0, fmt.Errorf("save room %q post: %w", app, err)
	}
	// The sequence the row took: LastInsertId would be the rowid.
	var seq int64
	if err := tx.QueryRowContext(ctx,
		"SELECT MAX(seq) FROM room_posts WHERE app = ?", app).Scan(&seq); err != nil {
		return 0, err
	}
	if p.ClientTimestamp != 0 {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO room_receipts(app, author, client_timestamp) VALUES(?, ?, ?)
			 ON CONFLICT(app, author) DO UPDATE SET client_timestamp = excluded.client_timestamp`,
			app, p.Author[:], int64(p.ClientTimestamp)); err != nil {
			return 0, fmt.Errorf("save room %q post receipt: %w", app, err)
		}
	}
	if keep > 0 {
		if _, err := tx.ExecContext(ctx,
			"DELETE FROM room_posts WHERE app = ? AND seq <= ?", app, seq-int64(keep)); err != nil {
			return 0, fmt.Errorf("prune room %q posts: %w", app, err)
		}
	}
	return seq, tx.Commit()
}

// LoadRoomReceipts reads the acceptance timestamps the room needs to
// distinguish a retry from a post it never kept, even after a restart.
func (s *Store) LoadRoomReceipts(ctx context.Context, app string) ([]RoomReceipt, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT author, client_timestamp FROM room_receipts WHERE app = ?", app)
	if err != nil {
		return nil, fmt.Errorf("load room %q post receipts: %w", app, err)
	}
	defer func() { _ = rows.Close() }()
	var out []RoomReceipt
	for rows.Next() {
		var r RoomReceipt
		var author []byte
		var timestamp int64
		if err := rows.Scan(&author, &timestamp); err != nil {
			return nil, err
		}
		copy(r.Author[:], author)
		r.ClientTimestamp = uint32(timestamp)
		out = append(out, r)
	}
	return out, rows.Err()
}

// ForgetRoomReceipt drops an evicted author's acceptance timestamp:
// the former member no longer has a session whose retry it can prove.
func (s *Store) ForgetRoomReceipt(ctx context.Context, app string, author [roomKeySize]byte) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM room_receipts WHERE app = ? AND author = ?", app, author[:])
	return err
}

// PruneRoomPosts forgets every post beyond the newest keep — what a
// room whose history was shortened asks on its way up, so the surplus
// does not wait in the store for the next post to make room.
func (s *Store) PruneRoomPosts(ctx context.Context, app string, keep int) error {
	if keep <= 0 {
		return nil
	}
	_, err := s.db.ExecContext(ctx,
		`DELETE FROM room_posts WHERE app = ?
		   AND seq <= (SELECT COALESCE(MAX(seq), 0) FROM room_posts WHERE app = ?) - ?`,
		app, app, int64(keep))
	if err != nil {
		return fmt.Errorf("prune room %q posts: %w", app, err)
	}
	return nil
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
	return s.SaveRoomCursors(ctx, app, []RoomCursor{c})
}

// SaveRoomCursors records a flush of cursors in one transaction — one
// fsync for the batch, where a write per cursor would cost one each on
// a store that commits synchronously.
func (s *Store) SaveRoomCursors(ctx context.Context, app string, cursors []RoomCursor) error {
	if len(cursors) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	updated := time.Now().UTC().Format(time.RFC3339Nano)
	for _, c := range cursors {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO room_cursors(app, pubkey, sync_since, updated) VALUES(?, ?, ?, ?)
			 ON CONFLICT(app, pubkey) DO UPDATE SET sync_since = excluded.sync_since, updated = excluded.updated`,
			app, c.PubKey[:], int64(c.SyncSince), updated); err != nil {
			return fmt.Errorf("save room %q cursor: %w", app, err)
		}
	}
	return tx.Commit()
}

// ForgetRoomCursor drops one member's cursor — what an eviction does,
// and what a revocation will do when the room grows one.
func (s *Store) ForgetRoomCursor(ctx context.Context, app string, pubKey [roomKeySize]byte) error {
	_, err := s.db.ExecContext(ctx, "DELETE FROM room_cursors WHERE app = ? AND pubkey = ?", app, pubKey[:])
	return err
}

// ApplicationOwner is the acl table's owner key for an application:
// the kind, a slash and the name. The slash is what keeps the key from
// ever spelling a relay — the instance-name grammar forbids it, so no
// relay can be called "application/lobby" — where a colon, the
// natural first choice, is a legal name character.
func ApplicationOwner(app string) string { return KindApplication + "/" + app }
