package confdb

// The session store: who has logged in over the air, and what each of
// them may still do, kept across a restart so a companion — an admin
// above all — is not asked to log in again every time its relay
// bounces. The replay guard is the load-bearing reason it persists:
// a last_timestamp reset to zero would let every past command replay.

import (
	"context"
	"database/sql"
	"time"
)

// ACLRow is one persisted session, as the store keeps it. The shared
// secret is not here: it is recomputed from the node identity and the
// peer key, so a credential the mesh never needs to see is one the
// disk never holds.
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

// LoadACL reads one relay's sessions, freshest bound applied by the
// caller: the store keeps what it was told, and how stale is too
// stale is the engine's policy, not the disk's.
func (s *Store) LoadACL(ctx context.Context, relay string) ([]ACLRow, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT pubkey, perms, last_timestamp, out_path, out_path_len, learned, last_active, granted
		   FROM acl WHERE relay = ?`, relay)
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

// SaveACL writes one session, replacing whatever it held: a login, a
// timestamp advance, a route learned all pass here.
func (s *Store) SaveACL(ctx context.Context, relay string, r ACLRow) error {
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
	_, err := s.db.ExecContext(ctx,
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

// ForgetACL drops one session — an eviction, an idle retirement, or a
// revoke.
func (s *Store) ForgetACL(ctx context.Context, relay string, pubKey []byte) error {
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM acl WHERE relay = ? AND pubkey = ?", relay, pubKey)
	return err
}
