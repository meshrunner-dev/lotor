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
	OutPath       []byte // nil when the client taught no route
	OutPathLen    uint8
	Learned       time.Time // zero when no route
	LastActive    time.Time
}

// LoadACL reads one relay's sessions, freshest bound applied by the
// caller: the store keeps what it was told, and how stale is too
// stale is the engine's policy, not the disk's.
func (s *Store) LoadACL(ctx context.Context, relay string) ([]ACLRow, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT pubkey, perms, last_timestamp, out_path, out_path_len, learned, last_active
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
		if err := rows.Scan(&r.PubKey, &perms, &lastTS, &outPath, &outLen,
			&learned, &lastActive); err != nil {
			return nil, err
		}
		r.Perms = byte(perms)
		r.LastTimestamp = uint32(lastTS)
		r.OutPath = outPath
		if outLen.Valid {
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
	var outLen any
	if r.OutPath != nil {
		outLen = int64(r.OutPathLen)
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO acl(relay, pubkey, perms, last_timestamp, out_path, out_path_len, learned, last_active)
		   VALUES(?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(relay, pubkey) DO UPDATE SET
		   perms = excluded.perms, last_timestamp = excluded.last_timestamp,
		   out_path = excluded.out_path, out_path_len = excluded.out_path_len,
		   learned = excluded.learned, last_active = excluded.last_active`,
		relay, r.PubKey, int64(r.Perms), int64(r.LastTimestamp),
		r.OutPath, outLen, learned, r.LastActive.UTC().Format(time.RFC3339Nano))
	return err
}

// ForgetACL drops one session — an eviction, an idle retirement, or a
// revoke.
func (s *Store) ForgetACL(ctx context.Context, relay string, pubKey []byte) error {
	_, err := s.db.ExecContext(ctx,
		"DELETE FROM acl WHERE relay = ? AND pubkey = ?", relay, pubKey)
	return err
}
