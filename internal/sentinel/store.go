package sentinel

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
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
CREATE INDEX IF NOT EXISTS frames_pubkey ON frames(pubkey, at_ms);
CREATE INDEX IF NOT EXISTS frames_dup ON frames(duplicate_of);
CREATE TABLE IF NOT EXISTS relay_states (
	at_ms INTEGER NOT NULL,
	relay TEXT NOT NULL,
	state TEXT NOT NULL,
	err   TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS relay_states_at ON relay_states(at_ms);
CREATE TABLE IF NOT EXISTS noise (
	relay      TEXT PRIMARY KEY,
	count      INTEGER NOT NULL DEFAULT 0,
	last_at_ms INTEGER NOT NULL,
	last_err   TEXT NOT NULL DEFAULT ''
);
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
	db.SetMaxIdleConns(1)
	if path != MemoryJournal {
		// WAL batches fsyncs — kinder to flash, and crash-safe enough
		// for an observation archive.
		// auto_vacuum must precede table creation; on a journal that
		// predates it, prune keeps working and only page reuse differs.
		if _, err := db.ExecContext(ctx,
			"PRAGMA auto_vacuum=INCREMENTAL; PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;"); err != nil {
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
	// The upsert touches only the heard columns: a redelivered
	// FrameHeard must not blank a judgement already applied.
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO frames (txn, relay, at_ms, bytes, rssi_dbm, snr_db, airtime_ms)
		 VALUES (?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(txn) DO UPDATE SET
		   relay = excluded.relay, at_ms = excluded.at_ms, bytes = excluded.bytes,
		   rssi_dbm = excluded.rssi_dbm, snr_db = excluded.snr_db,
		   airtime_ms = excluded.airtime_ms`,
		f.Txn, f.Relay, f.At.UnixMilli(), f.Bytes, f.RSSI, f.SNR,
		float64(f.Airtime)/float64(time.Millisecond),
	)
	return err
}

// errJudgementOrphan reports a judgement whose heard row never made
// the journal (a bus drop between the two events); the row is created
// from the judgement so the frame is not lost twice.
var errJudgementOrphan = errors.New("judgement arrived for a frame the journal never heard")

func (s *store) applyJudgement(ctx context.Context, txn string, relay string, f Frame) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE frames SET ptype = ?, route = ?, path_len = ?, verdict = ?, duplicate_of = ?,
		        node = ?, pubkey = ?, detail = ?
		 WHERE txn = ?`,
		f.Type, f.Route, f.PathLen, f.Verdict, f.DuplicateOf,
		f.Node, f.PubKey, f.Detail, txn,
	)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err == nil && n == 0 {
		// The single writer makes this insert race-free; the heard
		// columns it cannot know are zeroed, honestly absent.
		if _, ierr := s.db.ExecContext(ctx,
			`INSERT INTO frames (txn, relay, at_ms, bytes, rssi_dbm, snr_db, airtime_ms,
			        ptype, route, path_len, verdict, duplicate_of, node, pubkey, detail)
			 VALUES (?, ?, ?, 0, 0, 0, 0, ?, ?, ?, ?, ?, ?, ?, ?)`,
			txn, relay, time.Now().UnixMilli(), f.Type, f.Route, f.PathLen,
			f.Verdict, f.DuplicateOf, f.Node, f.PubKey, f.Detail); ierr != nil {
			return ierr
		}
		return errJudgementOrphan
	}
	return nil
}

// recordCorrupt counts a corrupt reception. One aggregate row per
// relay: a noise storm publishes thousands of these, and the archive's
// job is to make the storm visible, not to spend the journal on it.
func (s *store) recordCorrupt(ctx context.Context, at time.Time, relay, errText string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO noise (relay, count, last_at_ms, last_err) VALUES (?, 1, ?, ?)
		 ON CONFLICT(relay) DO UPDATE SET
		   count = count + 1, last_at_ms = excluded.last_at_ms,
		   last_err = excluded.last_err`,
		relay, at.UnixMilli(), errText,
	)
	return err
}

// Noise is one relay's corrupt-reception tally.
type Noise struct {
	Relay   string
	Count   int
	LastAt  time.Time
	LastErr string
}

// Noise lists each relay's tally, most recently noisy first.
func (s *store) Noise(ctx context.Context) ([]Noise, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT relay, count, last_at_ms, last_err FROM noise ORDER BY last_at_ms DESC`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Noise
	for rows.Next() {
		var n Noise
		var lastMS int64
		if err := rows.Scan(&n.Relay, &n.Count, &lastMS, &n.LastErr); err != nil {
			return nil, err
		}
		n.LastAt = time.UnixMilli(lastMS)
		out = append(out, n)
	}
	return out, rows.Err()
}

func (s *store) insertRelayState(ctx context.Context, at time.Time, relay, state, errText string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO relay_states (at_ms, relay, state, err) VALUES (?, ?, ?, ?)`,
		at.UnixMilli(), relay, state, errText,
	)
	return err
}

// prune drops everything older than the cutoff and, when maxFrames is
// set, everything beyond the newest maxFrames rows — the journal is
// bounded in time always and in size when asked. Freed pages go back
// to the filesystem where auto_vacuum applies.
func (s *store) prune(ctx context.Context, before time.Time, maxFrames int) error {
	cutoff := before.UnixMilli()
	if _, err := s.db.ExecContext(ctx, `DELETE FROM frames WHERE at_ms < ?`, cutoff); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM relay_states WHERE at_ms < ?`, cutoff); err != nil {
		return err
	}
	if maxFrames > 0 {
		if _, err := s.db.ExecContext(ctx,
			`DELETE FROM frames WHERE txn IN (
			   SELECT txn FROM frames ORDER BY at_ms DESC LIMIT -1 OFFSET ?)`,
			maxFrames); err != nil {
			return err
		}
	}
	_, err := s.db.ExecContext(ctx, `PRAGMA incremental_vacuum;`)
	return err
}

// FrameQuery filters RecentFrames; zero values mean "any". The txn
// prefix — the short displayed form of an id — finds its full rows.
type FrameQuery struct {
	TxnPrefix string
	Relay     string
	Type      string
	Verdict   string
	Limit     int
}

// RecentFrames returns the newest matching frames, newest first.
// Filtering happens in SQL: a busy channel cannot starve a filtered
// view, and the txn prefix is an index range, not a LIKE.
func (s *store) RecentFrames(ctx context.Context, fq FrameQuery) ([]Frame, error) {
	q := `SELECT txn, relay, at_ms, bytes, rssi_dbm, snr_db, airtime_ms,
	             ptype, route, path_len, verdict, duplicate_of, node, pubkey, detail
	      FROM frames WHERE 1=1`
	args := []any{}
	if fq.TxnPrefix != "" {
		lo, hi := prefixRange(fq.TxnPrefix)
		q += ` AND txn >= ? AND txn < ?`
		args = append(args, lo, hi)
	}
	if fq.Relay != "" {
		q += ` AND relay = ?`
		args = append(args, fq.Relay)
	}
	if fq.Type != "" {
		q += ` AND ptype = ?`
		args = append(args, fq.Type)
	}
	if fq.Verdict != "" {
		q += ` AND verdict = ?`
		args = append(args, fq.Verdict)
	}
	q += ` ORDER BY at_ms DESC LIMIT ?`
	args = append(args, fq.Limit)

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
	// The directory is built from verified adverts only: name and type
	// come from the freshest ADVERT row, and rows of other types never
	// contribute a key. The pubkey index carries the subqueries.
	rows, err := s.db.QueryContext(ctx, `
		SELECT f.pubkey,
		       (SELECT node FROM frames n WHERE n.pubkey = f.pubkey
		          AND n.ptype = 'ADVERT' AND n.node != '' ORDER BY n.at_ms DESC LIMIT 1),
		       (SELECT detail FROM frames n WHERE n.pubkey = f.pubkey
		          AND n.ptype = 'ADVERT' AND n.detail != '' ORDER BY n.at_ms DESC LIMIT 1),
		       COUNT(*), MAX(f.at_ms), MAX(f.rssi_dbm)
		FROM frames f WHERE f.pubkey != '' AND f.ptype = 'ADVERT'
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

// Chain returns a transaction's frames and its whole duplicate
// family: the chain is resolved to its root first — the original
// every duplicate points at — then the root and all its duplicates
// are returned, siblings included, whichever member was asked about.
func (s *store) Chain(ctx context.Context, txnPrefix string) ([]Frame, error) {
	own, err := s.RecentFrames(ctx, FrameQuery{TxnPrefix: txnPrefix, Limit: 16})
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
	for _, f := range own {
		root := f
		if f.DuplicateOf != "" {
			orig, err := s.RecentFrames(ctx, FrameQuery{TxnPrefix: f.DuplicateOf, Limit: 1})
			if err != nil {
				return nil, err
			}
			if len(orig) == 1 {
				root = orig[0]
			}
		}
		add([]Frame{root})
		dups, err := s.duplicatesOf(ctx, root.Txn[:min(len(root.Txn), 12)])
		if err != nil {
			return nil, err
		}
		add(dups)
		add([]Frame{f})
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
		f, err := s.RecentFrames(ctx, FrameQuery{TxnPrefix: t, Limit: 1})
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

// prefixRange turns a string prefix into a half-open range: every
// string starting with the prefix sorts in [prefix, next), where next
// is the prefix with its last byte incremented. No wildcards exist,
// so nothing needs escaping.
func prefixRange(prefix string) (lo, hi string) {
	b := []byte(prefix)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xFF {
			b[i]++
			return prefix, string(b[:i+1])
		}
	}
	// All 0xFF bytes: everything from the prefix on matches.
	return prefix, "\xff\xff\xff\xff\xff"
}
