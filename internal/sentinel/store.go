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
	duplicate_of TEXT NOT NULL DEFAULT ''
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
}

// store is the journal's SQLite backend. A single connection serialises
// writers; the mesh's frame rate is nowhere near a bottleneck.
type store struct {
	db *sql.DB
}

func openStore(path string) (*store, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	if path != MemoryJournal {
		// WAL batches fsyncs — kinder to flash, and crash-safe enough
		// for an observation archive.
		if _, err := db.ExecContext(context.Background(),
			"PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;"); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("journal pragmas: %w", err)
		}
	}
	if _, err := db.ExecContext(context.Background(), schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("journal schema: %w", err)
	}
	return &store{db: db}, nil
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

func (s *store) applyJudgement(ctx context.Context, txn, ptype, route string,
	pathLen int, verdict, duplicateOf string,
) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE frames SET ptype = ?, route = ?, path_len = ?, verdict = ?, duplicate_of = ?
		 WHERE txn = ?`,
		ptype, route, pathLen, verdict, duplicateOf, txn,
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
	             ptype, route, path_len, verdict, duplicate_of
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
			&airtimeMS, &f.Type, &f.Route, &f.PathLen, &f.Verdict, &f.DuplicateOf); err != nil {
			return nil, err
		}
		f.At = time.UnixMilli(atMS)
		f.Airtime = time.Duration(airtimeMS * float64(time.Millisecond))
		out = append(out, f)
	}
	return out, rows.Err()
}

func (s *store) Close() error { return s.db.Close() }

func escapeLike(prefix string) string {
	r := strings.NewReplacer(`%`, `\%`, `_`, `\_`, `\`, `\\`)
	return r.Replace(prefix)
}
