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

// schemaTables and schemaIndexes are applied either side of the
// grafts: an index over a column migrate is about to add cannot be
// created before that column exists, and an older journal would
// fail its open rather than reach the migration that saves it.
const schemaTables = `
CREATE TABLE IF NOT EXISTS frames (
	txn          TEXT PRIMARY KEY,
	relay        TEXT NOT NULL,
	at_ms        INTEGER NOT NULL,
	bytes        INTEGER NOT NULL,
	rssi_dbm     REAL NOT NULL,
	snr_db       REAL NOT NULL,
	airtime_ms   REAL NOT NULL,
	signal_dbm   REAL NOT NULL DEFAULT 0,
	freq_err_hz  REAL NOT NULL DEFAULT 0,
	ptype        TEXT NOT NULL DEFAULT '',
	route        TEXT NOT NULL DEFAULT '',
	scope        TEXT NOT NULL DEFAULT '',
	path_len     INTEGER NOT NULL DEFAULT 0,
	verdict      TEXT NOT NULL DEFAULT '',
	duplicate_of TEXT NOT NULL DEFAULT '',
	node         TEXT NOT NULL DEFAULT '',
	pubkey       TEXT NOT NULL DEFAULT '',
	detail       TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS relay_states (
	at_ms INTEGER NOT NULL,
	relay TEXT NOT NULL,
	state TEXT NOT NULL,
	err   TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS noise (
	relay      TEXT PRIMARY KEY,
	count      INTEGER NOT NULL DEFAULT 0,
	last_at_ms INTEGER NOT NULL,
	last_err   TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS noise_floor (
	relay     TEXT PRIMARY KEY,
	at_ms     INTEGER NOT NULL,
	dbm       REAL NOT NULL,
	spread_db REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS tx (
	at_ms      INTEGER NOT NULL,
	relay      TEXT NOT NULL,
	txn        TEXT NOT NULL,
	kind       TEXT NOT NULL,
	airtime_ms REAL NOT NULL,
	power_dbm  INTEGER NOT NULL,
	shadow     INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS tx_drops (
	relay      TEXT NOT NULL,
	reason     TEXT NOT NULL,
	count      INTEGER NOT NULL DEFAULT 0,
	last_at_ms INTEGER NOT NULL,
	PRIMARY KEY (relay, reason)
);
CREATE TABLE IF NOT EXISTS metrics_raw (
	series TEXT NOT NULL,
	relay  TEXT NOT NULL,
	at_ms  INTEGER NOT NULL,
	value  REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS metrics_hourly (
	series TEXT NOT NULL,
	relay  TEXT NOT NULL,
	at_ms  INTEGER NOT NULL,
	min    REAL NOT NULL,
	avg    REAL NOT NULL,
	max    REAL NOT NULL,
	n      INTEGER NOT NULL,
	PRIMARY KEY (series, relay, at_ms)
);
CREATE TABLE IF NOT EXISTS metrics_daily (
	series TEXT NOT NULL,
	relay  TEXT NOT NULL,
	at_ms  INTEGER NOT NULL,
	min    REAL NOT NULL,
	avg    REAL NOT NULL,
	max    REAL NOT NULL,
	n      INTEGER NOT NULL,
	PRIMARY KEY (series, relay, at_ms)
);
`

const schemaIndexes = `CREATE INDEX IF NOT EXISTS frames_at ON frames(at_ms);
CREATE INDEX IF NOT EXISTS frames_pubkey ON frames(pubkey, at_ms);
CREATE INDEX IF NOT EXISTS frames_dup ON frames(duplicate_of);
-- The two columns a reader filters frames by. Both hold a handful of
-- distinct words over a journal that only grows, so the index is what
-- keeps "which words are there" a question worth asking at all.
CREATE INDEX IF NOT EXISTS frames_ptype ON frames(ptype);
CREATE INDEX IF NOT EXISTS frames_verdict ON frames(verdict);
CREATE INDEX IF NOT EXISTS relay_states_at ON relay_states(at_ms);
CREATE INDEX IF NOT EXISTS tx_txn ON tx(txn);
CREATE INDEX IF NOT EXISTS tx_at ON tx(at_ms);
CREATE INDEX IF NOT EXISTS metrics_raw_key ON metrics_raw(series, relay, at_ms);
`

// Frame is one journalled reception, judgement included once it lands.
type Frame struct {
	Txn         string
	Relay       string
	At          time.Time
	Bytes       int
	RSSI        float64
	SNR         float64
	SignalRSSI  float64
	FreqErrHz   float64
	Airtime     time.Duration
	Type        string
	Route       string
	Scope       string
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
	if _, err := db.ExecContext(ctx, schemaTables); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("journal schema: %w", err)
	}
	if err := migrate(ctx, db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("journal migration: %w", err)
	}
	if _, err := db.ExecContext(ctx, schemaIndexes); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("journal indexes: %w", err)
	}
	return &store{db: db}, nil
}

// graft is one column added to a table after it first shipped.
type graft struct{ column, ddl string }

// The column shapes the grafts use.
const (
	ddlText = "TEXT NOT NULL DEFAULT ''"
	ddlReal = "REAL NOT NULL DEFAULT 0"
	ddlInt  = "INTEGER NOT NULL DEFAULT 0"
)

// grafts lists every column added since a table first shipped. CREATE
// IF NOT EXISTS leaves an existing table alone, so a journal from an
// older build reaches the current schema through here — and every
// column added to a shipped table belongs on this list, or that
// journal fails its first insert.
var grafts = map[string][]graft{
	// Every frames column that carries a default is listed, not just
	// the ones added last: the list costs nothing on a current journal
	// and spares an older one a cryptic failure when an index reaches
	// for a column nobody grafted.
	"frames": {
		{"ptype", ddlText},
		{"route", ddlText},
		{"scope", ddlText},
		{"path_len", ddlInt},
		{"verdict", ddlText},
		{"duplicate_of", ddlText},
		{"node", ddlText},
		{"pubkey", ddlText},
		{"detail", ddlText},
		{"signal_dbm", ddlReal},
		{"freq_err_hz", ddlReal},
	},
	"noise_floor": {
		{"spread_db", ddlReal},
	},
}

// migrate brings a journal created by an earlier schema up to date,
// idempotently.
func migrate(ctx context.Context, db *sql.DB) error {
	for table, cols := range grafts {
		present, err := tableColumns(ctx, db, table)
		if err != nil {
			return err
		}
		for _, c := range cols {
			if present[c.column] {
				continue
			}
			if _, err := db.ExecContext(ctx,
				fmt.Sprintf(`ALTER TABLE %s ADD COLUMN %s %s`, table, c.column, c.ddl)); err != nil {
				return fmt.Errorf("graft %s.%s: %w", table, c.column, err)
			}
		}
	}
	return nil
}

// tableColumns reads a table's current column set.
func tableColumns(ctx context.Context, db *sql.DB, table string) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`PRAGMA table_info(%s)`, table))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	cols := map[string]bool{}
	for rows.Next() {
		var cid, notnull, pk int
		var name, ctype string
		var dflt any
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		cols[name] = true
	}
	return cols, rows.Err()
}

func (s *store) insertHeard(ctx context.Context, f Frame) error {
	// The upsert touches only the heard columns: a redelivered
	// FrameHeard must not blank a judgement already applied.
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO frames (txn, relay, at_ms, bytes, rssi_dbm, snr_db, airtime_ms, signal_dbm, freq_err_hz)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		 ON CONFLICT(txn) DO UPDATE SET
		   relay = excluded.relay, at_ms = excluded.at_ms, bytes = excluded.bytes,
		   rssi_dbm = excluded.rssi_dbm, snr_db = excluded.snr_db,
		   airtime_ms = excluded.airtime_ms, signal_dbm = excluded.signal_dbm,
		   freq_err_hz = excluded.freq_err_hz`,
		f.Txn, f.Relay, f.At.UnixMilli(), f.Bytes, f.RSSI, f.SNR,
		float64(f.Airtime)/float64(time.Millisecond), f.SignalRSSI, f.FreqErrHz,
	)
	return err
}

// errJudgementOrphan reports a judgement whose heard row never made
// the journal (a bus drop between the two events); the row is created
// from the judgement so the frame is not lost twice.
var errJudgementOrphan = errors.New("judgement arrived for a frame the journal never heard")

func (s *store) applyJudgement(ctx context.Context, txn string, relay string, f Frame) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE frames SET ptype = ?, route = ?, scope = ?, path_len = ?, verdict = ?, duplicate_of = ?,
		        node = ?, pubkey = ?, detail = ?
		 WHERE txn = ?`,
		f.Type, f.Route, f.Scope, f.PathLen, f.Verdict, f.DuplicateOf,
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
			        ptype, route, scope, path_len, verdict, duplicate_of, node, pubkey, detail)
			 VALUES (?, ?, ?, 0, 0, 0, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			txn, relay, time.Now().UnixMilli(), f.Type, f.Route, f.Scope, f.PathLen,
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

// upsertNoiseFloor keeps each relay's latest measured floor — the last
// value only, by design: the measurement is continuous, the archive's
// job here is just to remember where the floor stood.
func (s *store) upsertNoiseFloor(ctx context.Context, at time.Time, relay string, dbm, spreadDB float64) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO noise_floor (relay, at_ms, dbm, spread_db) VALUES (?, ?, ?, ?)
		 ON CONFLICT(relay) DO UPDATE SET
		   at_ms = excluded.at_ms, dbm = excluded.dbm, spread_db = excluded.spread_db`,
		relay, at.UnixMilli(), dbm, spreadDB,
	)
	return err
}

// The metrics tiers, RRD-style but in SQL: raw points age into hourly
// consolidation, hourly into daily, each tier bounded in time. The
// tables are generic — series names the measurement — because the
// noise floor will not be the last archived series.
const (
	metricRawKeep   = 24 * time.Hour
	metricDailyKeep = 2 * 365 * 24 * time.Hour

	hourMS = int64(time.Hour / time.Millisecond)
	dayMS  = 24 * hourMS
)

// insertMetric records one raw point of a series.
func (s *store) insertMetric(ctx context.Context, series, relay string, at time.Time, value float64) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO metrics_raw (series, relay, at_ms, value) VALUES (?, ?, ?, ?)`,
		series, relay, at.UnixMilli(), value)
	return err
}

// The two consolidation steps, one query each. Everything strictly
// older than ?1 (bucket-aligned) aggregates per ?2-wide bucket and
// merges into the destination — merged on conflict, so a bucket
// rolled in two passes still sums correctly.
const (
	rollupRawSQL = `
	INSERT INTO metrics_hourly (series, relay, at_ms, min, avg, max, n)
	SELECT series, relay, (at_ms/?2)*?2, MIN(value), AVG(value), MAX(value), COUNT(*)
	FROM metrics_raw WHERE at_ms < ?1 GROUP BY series, relay, (at_ms/?2)*?2
	ON CONFLICT(series, relay, at_ms) DO UPDATE SET
	  avg = (metrics_hourly.avg*metrics_hourly.n + excluded.avg*excluded.n)
	        / (metrics_hourly.n + excluded.n),
	  min = MIN(metrics_hourly.min, excluded.min),
	  max = MAX(metrics_hourly.max, excluded.max),
	  n   = metrics_hourly.n + excluded.n`

	rollupHourlySQL = `
	INSERT INTO metrics_daily (series, relay, at_ms, min, avg, max, n)
	SELECT series, relay, (at_ms/?2)*?2, MIN(min), SUM(avg*n)/SUM(n), MAX(max), SUM(n)
	FROM metrics_hourly WHERE at_ms < ?1 GROUP BY series, relay, (at_ms/?2)*?2
	ON CONFLICT(series, relay, at_ms) DO UPDATE SET
	  avg = (metrics_daily.avg*metrics_daily.n + excluded.avg*excluded.n)
	        / (metrics_daily.n + excluded.n),
	  min = MIN(metrics_daily.min, excluded.min),
	  max = MAX(metrics_daily.max, excluded.max),
	  n   = metrics_daily.n + excluded.n`
)

// rollupMetrics ages the tiers: raw older than a day consolidates into
// hourly buckets, hourly older than the journal's retention into daily
// ones, and daily rows fall off after two years.
func (s *store) rollupMetrics(ctx context.Context, now time.Time, retention time.Duration) error {
	// One transaction per pass: a consolidation folds rows into a
	// bucket and then deletes them, so a crash between the two would
	// double that bucket's count for good — the tiers are supposed to
	// be a lossless retelling, not an approximation that drifts.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	rawCut := (now.Add(-metricRawKeep).UnixMilli() / hourMS) * hourMS
	if _, err := tx.ExecContext(ctx, rollupRawSQL, rawCut, hourMS); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM metrics_raw WHERE at_ms < ?`, rawCut); err != nil {
		return err
	}
	hourlyCut := (now.Add(-retention).UnixMilli() / dayMS) * dayMS
	if _, err := tx.ExecContext(ctx, rollupHourlySQL, hourlyCut, dayMS); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM metrics_hourly WHERE at_ms < ?`, hourlyCut); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx,
		`DELETE FROM metrics_daily WHERE at_ms < ?`, now.Add(-metricDailyKeep).UnixMilli()); err != nil {
		return err
	}
	return tx.Commit()
}

// MetricBucket is one consolidated span of a series' history.
type MetricBucket struct {
	At  time.Time
	Min float64
	Avg float64
	Max float64
	N   int
}

// MetricHistory returns a series' buckets since the given instant,
// oldest first, across every tier: daily and hourly rows as stored,
// raw points consolidated per hour on the fly — one uniform shape.
func (s *store) MetricHistory(ctx context.Context, series, relay string, since time.Time) ([]MetricBucket, error) {
	q := `SELECT at_ms, min, avg, max, n FROM metrics_daily
	        WHERE series = ?1 AND relay = ?2 AND at_ms >= ?3
	      UNION ALL
	      SELECT at_ms, min, avg, max, n FROM metrics_hourly
	        WHERE series = ?1 AND relay = ?2 AND at_ms >= ?3
	      UNION ALL
	      SELECT (at_ms/?4)*?4, MIN(value), AVG(value), MAX(value), COUNT(*)
	        FROM metrics_raw
	        WHERE series = ?1 AND relay = ?2 AND at_ms >= ?3
	        GROUP BY (at_ms/?4)*?4
	      ORDER BY at_ms`
	rows, err := s.db.QueryContext(ctx, q, series, relay, since.UnixMilli(), hourMS)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []MetricBucket
	for rows.Next() {
		var b MetricBucket
		var atMS int64
		if err := rows.Scan(&atMS, &b.Min, &b.Avg, &b.Max, &b.N); err != nil {
			return nil, err
		}
		b.At = time.UnixMilli(atMS)
		out = append(out, b)
	}
	return out, rows.Err()
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

// prune drops everything older than the retention and, when maxFrames
// is set, everything beyond the newest maxFrames rows — the journal is
// bounded in time always and in size when asked. The metrics tiers age
// in the same pass. Freed pages go back to the filesystem where
// auto_vacuum applies.
func (s *store) prune(ctx context.Context, now time.Time, retention time.Duration, maxFrames int) error {
	cutoff := now.Add(-retention).UnixMilli()
	if _, err := s.db.ExecContext(ctx, `DELETE FROM frames WHERE at_ms < ?`, cutoff); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM relay_states WHERE at_ms < ?`, cutoff); err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `DELETE FROM tx WHERE at_ms < ?`, cutoff); err != nil {
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
	if err := s.rollupMetrics(ctx, now, retention); err != nil {
		return err
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
	q := `SELECT txn, relay, at_ms, bytes, rssi_dbm, snr_db, airtime_ms, signal_dbm, freq_err_hz,
	             ptype, route, scope, path_len, verdict, duplicate_of, node, pubkey, detail
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
			&airtimeMS, &f.SignalRSSI, &f.FreqErrHz,
			&f.Type, &f.Route, &f.Scope, &f.PathLen, &f.Verdict, &f.DuplicateOf,
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
	// HasRSSI is false when every row for this node came from a
	// judgement whose reception was lost: there is no measurement.
	HasRSSI bool
	// DriftHz averages the carrier offset of the node's frames whose
	// reception measured one: its crystal's health, in hertz.
	DriftHz float64
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
		       COUNT(*), MAX(f.at_ms),
		       MAX(CASE WHEN f.rssi_dbm != 0 THEN f.rssi_dbm END),
	       AVG(CASE WHEN f.freq_err_hz != 0 THEN f.freq_err_hz END)
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
		var best, drift sql.NullFloat64
		var lastMS int64
		if err := rows.Scan(&n.PubKey, &name, &typ, &n.Heard, &lastMS, &best, &drift); err != nil {
			return nil, err
		}
		n.Name, n.Type = name.String, typ.String
		// A row recovered from a judgement alone carries no reception
		// quality; 0 dBm would read as a hundred decibels too good.
		n.BestRSSI, n.HasRSSI = best.Float64, best.Valid
		n.DriftHz = drift.Float64
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

// FrameVocabulary is what the journal actually holds in the two
// columns a reader may filter on. A filter for a word nothing was
// ever recorded with matches nothing, so this is the only list worth
// offering to someone choosing one.
func (s *store) FrameVocabulary(ctx context.Context) (types, verdicts []string, err error) {
	if types, err = s.distinct(ctx,
		`SELECT DISTINCT ptype FROM frames WHERE ptype != '' ORDER BY 1`); err != nil {
		return nil, nil, err
	}
	if verdicts, err = s.distinct(ctx,
		`SELECT DISTINCT verdict FROM frames WHERE verdict != '' ORDER BY 1`); err != nil {
		return nil, nil, err
	}
	return types, verdicts, nil
}

// distinct reads one column of values.
func (s *store) distinct(ctx context.Context, query string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var v string
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// Relays lists every relay name the journal holds records for, from
// any table that carries one — configuration is not consulted, so a
// removed relay's archive stays addressable.
func (s *store) Relays(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT relay FROM frames
		UNION SELECT relay FROM relay_states
		UNION SELECT relay FROM noise
		UNION SELECT relay FROM noise_floor
		UNION SELECT relay FROM metrics_raw
		UNION SELECT relay FROM metrics_hourly
		UNION SELECT relay FROM metrics_daily
		UNION SELECT relay FROM tx
		UNION SELECT relay FROM tx_drops
		ORDER BY relay`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var r string
		if err := rows.Scan(&r); err != nil {
			return nil, err
		}
		out = append(out, r)
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

// Sent is one journalled emission — the duty ledger's row.
type Sent struct {
	At       time.Time
	Relay    string
	Kind     string
	Airtime  time.Duration
	PowerDBm int8
	Shadow   bool
}

// insertSent journals one emission, shadow or real.
func (s *store) insertSent(ctx context.Context, at time.Time, relay, txn, kind string,
	airtime time.Duration, powerDBm int8, shadow bool,
) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO tx (at_ms, relay, txn, kind, airtime_ms, power_dbm, shadow)
		 VALUES (?, ?, ?, ?, ?, ?, ?)`,
		at.UnixMilli(), relay, txn, kind,
		float64(airtime)/float64(time.Millisecond), powerDBm, shadow)
	return err
}

// SentFor lists the emissions of one transaction, oldest first. The
// argument is a prefix — the short form an operator reads in a log
// line addresses its rows, exactly as it does for receptions, and a
// full id is simply a prefix of itself.
func (s *store) SentFor(ctx context.Context, txn string) ([]Sent, error) {
	lo, hi := prefixRange(txn)
	rows, err := s.db.QueryContext(ctx,
		`SELECT at_ms, relay, kind, airtime_ms, power_dbm, shadow
		 FROM tx WHERE txn >= ? AND txn < ? ORDER BY at_ms`, lo, hi)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Sent
	for rows.Next() {
		var t Sent
		var atMS int64
		var airMS float64
		if err := rows.Scan(&atMS, &t.Relay, &t.Kind, &airMS, &t.PowerDBm, &t.Shadow); err != nil {
			return nil, err
		}
		t.At = time.UnixMilli(atMS)
		t.Airtime = time.Duration(airMS * float64(time.Millisecond))
		out = append(out, t)
	}
	return out, rows.Err()
}

// TxSince lists emissions since an instant, oldest first — what the
// airtime window needs to resume where a restart left it.
func (s *store) TxSince(ctx context.Context, since time.Time) ([]Sent, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT at_ms, relay, kind, airtime_ms, power_dbm, shadow
		 FROM tx WHERE at_ms >= ? ORDER BY at_ms`, since.UnixMilli())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []Sent
	for rows.Next() {
		var t Sent
		var atMS int64
		var airMS float64
		if err := rows.Scan(&atMS, &t.Relay, &t.Kind, &airMS, &t.PowerDBm, &t.Shadow); err != nil {
			return nil, err
		}
		t.At = time.UnixMilli(atMS)
		t.Airtime = time.Duration(airMS * float64(time.Millisecond))
		out = append(out, t)
	}
	return out, rows.Err()
}

// recordTxDrop tallies one refused emission by reason — bounded rows,
// like the corrupt tally.
func (s *store) recordTxDrop(ctx context.Context, at time.Time, relay, reason string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO tx_drops (relay, reason, count, last_at_ms) VALUES (?, ?, 1, ?)
		 ON CONFLICT(relay, reason) DO UPDATE SET
		   count = count + 1, last_at_ms = excluded.last_at_ms`,
		relay, reason, at.UnixMilli())
	return err
}

// TxDrop is one relay's refusal tally for one reason.
type TxDrop struct {
	Relay  string
	Reason string
	Count  int
	LastAt time.Time
}

// TxDrops lists the refusal tallies, most recent first.
func (s *store) TxDrops(ctx context.Context) ([]TxDrop, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT relay, reason, count, last_at_ms FROM tx_drops ORDER BY last_at_ms DESC`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []TxDrop
	for rows.Next() {
		var d TxDrop
		var lastMS int64
		if err := rows.Scan(&d.Relay, &d.Reason, &d.Count, &lastMS); err != nil {
			return nil, err
		}
		d.LastAt = time.UnixMilli(lastMS)
		out = append(out, d)
	}
	return out, rows.Err()
}
