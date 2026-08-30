package confdb

// The durable access store: non-guest MeshCore roles kept across a
// restart. Guests are live sessions and never enter this table. The
// replay guard is the load-bearing reason an access entry carries its
// runtime state: a last_timestamp reset to zero would let a past admin
// command replay.

import (
	"context"
	"database/sql"
	"time"
)

// ACLRow is one durable authorisation, as the store keeps it. The
// shared secret is not here: it is recomputed from the node identity
// and the peer key, so a credential the mesh never needs to see is one
// the disk never holds.
type ACLRow struct {
	PubKey        []byte
	Perms         byte
	LastTimestamp uint32
	// HasOut tells a taught route from none, the zero-hop adjacent
	// route (empty path, still a route) kept distinct from flood.
	HasOut     bool
	OutPath    []byte
	OutPathLen uint8
	Learned    time.Time
	LastActive time.Time
	// Granted marks a permission set explicitly, which outlives idle.
	Granted bool
}

// LoadACL reads one relay's durable authorisations. The engine also
// uses this read to remove guest rows left by older Lotor versions.
func (s *Store) LoadACL(ctx context.Context, relay string) ([]ACLRow, error) {
	// Freshest first, and explicitly: which entries a restart keeps
	// when the store holds more than the table has places for is a
	// policy, not whatever order the rows happen to come back in.
	rows, err := s.db.QueryContext(ctx,
		`SELECT pubkey, perms, last_timestamp, out_path, out_path_len, learned, last_active, granted
		   FROM acl WHERE relay = ? ORDER BY last_active DESC, pubkey`, relay)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []ACLRow
	for rows.Next() {
		var r ACLRow
		var perms, lastTS int64
		var outPath []byte
		var outLen sql.NullInt64
		var learned, lastActive sql.NullString
		var granted int64
		if err := rows.Scan(&r.PubKey, &perms, &lastTS, &outPath, &outLen,
			&learned, &lastActive, &granted); err != nil {
			return nil, err
		}
		r.Granted = granted != 0
		r.Perms = byte(perms)
		r.LastTimestamp = uint32(lastTS)
		r.OutPath = outPath
		if outLen.Valid {
			r.HasOut = true
			r.OutPathLen = uint8(outLen.Int64)
		}
		if learned.Valid && learned.String != "" {
			r.Learned, _ = time.Parse(time.RFC3339Nano, learned.String)
		}
		if lastActive.Valid {
			r.LastActive, _ = time.Parse(time.RFC3339Nano, lastActive.String)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// SaveACL writes one durable access entry, replacing whatever it held:
// an admin login, a timestamp advance, and a route learned all pass
// here. The protocol adapter is responsible for excluding guests.
func (s *Store) SaveACL(ctx context.Context, relay string, r ACLRow) error {
	return saveACLTx(ctx, s.db, relay, r)
}

// execer is whichever of the connection and a transaction is writing.
type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// saveACLTx is the one place an access row is written.
func saveACLTx(ctx context.Context, db execer, relay string, r ACLRow) error {
	var learned any
	if !r.Learned.IsZero() {
		learned = r.Learned.UTC().Format(time.RFC3339Nano)
	}
	// out_path_len is the presence signal: non-NULL (0 included) is a
	// route, NULL is none. The path bytes may be empty for a zero-hop
	// adjacent client, which is still a route.
	var outLen any
	if r.HasOut {
		outLen = int64(r.OutPathLen)
	}
	granted := int64(0)
	if r.Granted {
		granted = 1
	}
	_, err := db.ExecContext(ctx,
		`INSERT INTO acl(relay, pubkey, perms, last_timestamp, out_path, out_path_len, learned, last_active, granted)
		   VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(relay, pubkey) DO UPDATE SET
		   perms = excluded.perms, last_timestamp = excluded.last_timestamp,
		   out_path = excluded.out_path, out_path_len = excluded.out_path_len,
		   learned = excluded.learned, last_active = excluded.last_active,
		   granted = excluded.granted`,
		relay, r.PubKey, int64(r.Perms), int64(r.LastTimestamp),
		r.OutPath, outLen, learned, r.LastActive.UTC().Format(time.RFC3339Nano), granted)
	return err
}

// ForgetACL revokes one durable access entry. It also removes legacy
// guest rows during startup cleanup.
func (s *Store) ForgetACL(ctx context.Context, relay string, pubKey []byte) error {
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM acl WHERE relay = ? AND pubkey = ?", relay, pubKey)
	return err
}
