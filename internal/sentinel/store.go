package sentinel

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	// Pure-Go SQLite: the daemon cross-compiles with CGO disabled.
	_ "modernc.org/sqlite"
)

// MemoryJournal is the journal path that keeps everything in RAM —
// the mode for hosts whose storage dislikes continuous writes.
const MemoryJournal = ":memory:"

const schema = `
CREATE TABLE IF NOT EXISTS frames (
	txn          TEXT PRIMARY KEY,
	relay        TEXT NOT NULL,
	at_ms        INTEGER NOT NULL,
	bytes        INTEGER NOT NULL,
	rssi_dbm     REAL NOT NULL,
	snr_db       REAL NOT NULL,
	airtime_ms   REAL NOT NULL,
	ptype        TEXT NOT NULL DEFAULT '',
	route        TEXT NOT NULL DEFAULT '',
	path_len     INTEGER NOT NULL DEFAULT 0,
	verdict      TEXT NOT NULL DEFAULT '',
	duplicate_of TEXT NOT NULL DEFAULT '',
	node         TEXT NOT NULL DEFAULT '',
	pubkey       TEXT NOT NULL DEFAULT '',
	detail       TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS frames_at ON frames(at_ms);
CREATE TABLE IF NOT EXISTS relay_states (
	at_ms INTEGER NOT NULL,
	relay TEXT NOT NULL,
	state TEXT NOT NULL,
	err   TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS relay_states_at ON relay_states(at_ms);
`

// Frame is one journalled reception, judgement included once it lands.
type Frame struct {
	Txn         string
	Relay       string
	At          time.Time
	Bytes       int
	RSSI        float64
	SNR         float64
	Airtime     time.Duration
	Type        string
	Route       string
	PathLen     int
	Verdict     string
	DuplicateOf string
	Node        string
	PubKey      string
	Detail      string
}

// store is the journal's SQLite backend. A single connection serialises
// writers; the mesh's frame rate is nowhere near a bottleneck.
type store struct {
	db *sql.DB
}

func openStore(ctx context.Context, path string) (*store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if path != MemoryJournal {
		// WAL batches fsyncs — kinder to flash, and crash-safe enough
		// for an observation archive.
		if _, err := db.ExecContext(ctx,
			"PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;"); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("journal pragmas: %w", err)
		}
	}
	if _, err := db.ExecContext(ctx, schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("journal schema: %w", err)
	}
	if err := migrate(ctx, db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("journal migration: %w", err)
	}
	return &store{db: db}, nil
}

// migrate brings a journal created by an earlier schema up to date.
// CREATE IF NOT EXISTS leaves an existing table alone, so columns
// added since are grafted here, idempotently.
func migrate(ctx context.Context, db *sql.DB) error {
	cols := map[string]bool{}
	rows, err := db.QueryContext(ctx, `PRAGMA table_info(frames)`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var cid, notnull, pk int
		var name, ctype string
		var dflt any
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return err
		}
		cols[name] = true
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, col := range []string{"node", "pubkey", "detail"} {
		if !cols[col] {
			if _, err := db.ExecContext(ctx,
				fmt.Sprintf(`ALTER TABLE frames ADD COLUMN %s TEXT NOT NULL DEFAULT ''`, col)); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *store) insertHeard(ctx context.Context, f Frame) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO frames (txn, relay, at_ms, bytes, rssi_dbm, snr_db, airtime_ms)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		f.Txn, f.Relay, f.At.UnixMilli(), f.Bytes, f.RSSI, f.SNR,
		float64(f.Airtime)/float64(time.Millisecond),
	)
	return err
}

func (s *store) applyJudgement(ctx context.Context, txn string, f Frame) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE frames SET ptype = ?, route = ?, path_len = ?, verdict = ?, duplicate_of = ?,
		        node = ?, pubkey = ?, detail = ?
		 WHERE txn = ?`,
		f.Type, f.Route, f.PathLen, f.Verdict, f.DuplicateOf,
		f.Node, f.PubKey, f.Detail, txn,
	)
	return err
}

func (s *store) insertRelayState(ctx context.Context, at time.Time, relay, state, errText string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO relay_states (at_ms, relay, state, err) VALUES (?, ?, ?, ?)`,
		at.UnixMilli(), relay, state, errText,
	)
	return err
}

// prune drops everything older than the cutoff.
func (s *store) prune(ctx context.Context, before time.Time) error {
	cutoff := before.UnixMilli()
	if _, err := s.db.ExecContext(ctx, `DELETE FROM frames WHERE at_ms < ?`, cutoff); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `DELETE FROM relay_states WHERE at_ms < ?`, cutoff)
	return err
}

// RecentFrames returns the newest frames, newest first. A txn prefix
// filters — the short displayed form of an id finds its full row.
func (s *store) RecentFrames(ctx context.Context, txnPrefix string, limit int) ([]Frame, error) {
	q := `SELECT txn, relay, at_ms, bytes, rssi_dbm, snr_db, airtime_ms,
	             ptype, route, path_len, verdict, duplicate_of, node, pubkey, detail
	      FROM frames`
	args := []any{}
	if txnPrefix != "" {
		q += ` WHERE txn LIKE ?`
		args = append(args, escapeLike(txnPrefix)+"%")
	}
	q += ` ORDER BY at_ms DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []Frame
	for rows.Next() {
		var f Frame
		var atMS int64
		var airtimeMS float64
		if err := rows.Scan(&f.Txn, &f.Relay, &atMS, &f.Bytes, &f.RSSI, &f.SNR,
			&airtimeMS, &f.Type, &f.Route, &f.PathLen, &f.Verdict, &f.DuplicateOf,
			&f.Node, &f.PubKey, &f.Detail); err != nil {
			return nil, err
		}
		f.At = time.UnixMilli(atMS)
		f.Airtime = time.Duration(airtimeMS * float64(time.Millisecond))
		out = append(out, f)
	}
	return out, rows.Err()
}

// Node is one entry of the directory the mesh writes about itself.
type Node struct {
	Name     string
	Type     string
	PubKey   string
	Heard    int
	LastAt   time.Time
	BestRSSI float64
}

// Nodes lists every advertising node ever journalled, most recently
// heard first. Name and type come from the freshest advert that
// carried them.
func (s *store) Nodes(ctx context.Context) ([]Node, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT f.pubkey,
		       (SELECT node   FROM frames n WHERE n.pubkey = f.pubkey AND n.node   != '' ORDER BY n.at_ms DESC LIMIT 1),
		       (SELECT detail FROM frames n WHERE n.pubkey = f.pubkey AND n.detail != '' ORDER BY n.at_ms DESC LIMIT 1),
		       COUNT(*), MAX(f.at_ms), MAX(f.rssi_dbm)
		FROM frames f WHERE f.pubkey != ''
		GROUP BY f.pubkey ORDER BY MAX(f.at_ms) DESC`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var out []Node
	for rows.Next() {
		var n Node
		var name, typ sql.NullString
		var lastMS int64
		if err := rows.Scan(&n.PubKey, &name, &typ, &n.Heard, &lastMS, &n.BestRSSI); err != nil {
			return nil, err
		}
		n.Name, n.Type = name.String, typ.String
		n.LastAt = time.UnixMilli(lastMS)
		out = append(out, n)
	}
	return out, rows.Err()
}

// Chain returns a transaction's frames and everything linked to it:
// the original it duplicates, and the duplicates that point at it.
func (s *store) Chain(ctx context.Context, txnPrefix string) ([]Frame, error) {
	own, err := s.RecentFrames(ctx, txnPrefix, 16)
	if err != nil || len(own) == 0 {
		return own, err
	}
	seen := map[string]bool{}
	var out []Frame
	add := func(frames []Frame) {
		for _, f := range frames {
			if !seen[f.Txn] {
				seen[f.Txn] = true
				out = append(out, f)
			}
		}
	}
	add(own)
	for _, f := range own {
		if f.DuplicateOf != "" {
			orig, err := s.RecentFrames(ctx, f.DuplicateOf, 4)
			if err != nil {
				return nil, err
			}
			add(orig)
		}
		short := f.Txn[:min(len(f.Txn), 12)]
		dups, err := s.duplicatesOf(ctx, short)
		if err != nil {
			return nil, err
		}
		add(dups)
	}
	return out, nil
}

func (s *store) duplicatesOf(ctx context.Context, short string) ([]Frame, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT txn FROM frames WHERE duplicate_of = ? ORDER BY at_ms`, short)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var txns []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		txns = append(txns, t)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	var out []Frame
	for _, t := range txns {
		f, err := s.RecentFrames(ctx, t, 1)
		if err != nil {
			return nil, err
		}
		out = append(out, f...)
	}
	return out, nil
}

// VerdictCounts sums a relay's judgements by verdict.
func (s *store) VerdictCounts(ctx context.Context, relay string) (map[string]int, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT verdict, COUNT(*) FROM frames WHERE relay = ? AND verdict != '' GROUP BY verdict`,
		relay)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := map[string]int{}
	for rows.Next() {
		var v string
		var n int
		if err := rows.Scan(&v, &n); err != nil {
			return nil, err
		}
		out[v] = n
	}
	return out, rows.Err()
}

// FrameCount is the journal's current size.
func (s *store) FrameCount(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM frames`).Scan(&n)
	return n, err
}

func (s *store) Close() error { return s.db.Close() }

func escapeLike(prefix string) string {
	r := strings.NewReplacer(`%`, `\%`, `_`, `\_`, `\`, `\\`)
	return r.Replace(prefix)
}
