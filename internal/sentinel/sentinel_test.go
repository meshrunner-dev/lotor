package sentinel

import (
	"context"
	"database/sql"
	"net"
	"os"
	"testing"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/txn"
)

func testSentinel(t *testing.T) *Sentinel {
	t.Helper()
	s, err := Open(context.Background(), MemoryJournal, time.Hour, 0, 0, bus.New(), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.store.Close() })
	return s
}

func TestHeardThenJudgedBecomesOneRow(t *testing.T) {
	// The judged event carries the reception whole — one event, one
	// row, atomically: a bus drop can no longer strand a row without
	// its verdict or a verdict without its reception. FrameHeard is
	// the live feed and is not journalled at all.
	s := testSentinel(t)
	id := txn.New()
	at := time.Now()

	s.Process(context.Background(), bus.FrameHeard{Relay: "meshcore-868", Txn: id, At: at})
	s.Process(context.Background(), bus.FrameJudged{
		Relay: "meshcore-868", Txn: id, At: at,
		Bytes: 132, RSSI: -69, SNR: 8.5, SignalRSSI: -74, FreqErrHz: 112,
		Airtime: 1295 * time.Millisecond,
		Verdict: "would-relay-flood", Type: "ADVERT", Route: "FLOOD", PathLen: 6,
		Node: "Wanadoo", PubKey: "de1234567890", Detail: "repeater",
	})

	frames, err := s.RecentFrames(context.Background(), FrameQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 {
		t.Fatalf("%d rows, want 1", len(frames))
	}
	f := frames[0]
	if f.Txn != id.String() || f.Verdict != "would-relay-flood" ||
		f.Type != "ADVERT" || f.Route != "FLOOD" || f.PathLen != 6 ||
		f.Bytes != 132 || f.RSSI != -69 || f.SignalRSSI != -74 || f.FreqErrHz != 112 ||
		f.Node != "Wanadoo" || f.PubKey != "de1234567890" || f.Detail != "repeater" {
		t.Errorf("row = %+v", f)
	}
}

func TestShortPrefixFindsItsTransaction(t *testing.T) {
	s := testSentinel(t)
	id := txn.New()
	s.Process(context.Background(), bus.FrameJudged{Relay: "r", Txn: id, At: time.Now()})
	s.Process(context.Background(), bus.FrameJudged{Relay: "r", Txn: txn.New(), At: time.Now()})

	frames, err := s.RecentFrames(context.Background(), FrameQuery{TxnPrefix: id.Short(), Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 || frames[0].Txn != id.String() {
		t.Fatalf("prefix search = %+v", frames)
	}
}

func TestRetentionPrunes(t *testing.T) {
	s := testSentinel(t)
	old := txn.New()
	fresh := txn.New()
	s.Process(context.Background(), bus.FrameJudged{Relay: "r", Txn: old, At: time.Now().Add(-2 * time.Hour)})
	s.Process(context.Background(), bus.FrameJudged{Relay: "r", Txn: fresh, At: time.Now()})

	if err := s.store.prune(context.Background(), time.Now(), time.Hour, 0, 0); err != nil {
		t.Fatal(err)
	}
	frames, err := s.RecentFrames(context.Background(), FrameQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 || frames[0].Txn != fresh.String() {
		t.Fatalf("after prune = %+v", frames)
	}
}

func TestRelayStatesJournalled(t *testing.T) {
	s := testSentinel(t)
	// The event's own clock dates the row: the drain consumes long
	// after the transition, and the archive must not show drain time.
	at := time.Now().Add(-42 * time.Second)
	s.Process(context.Background(), bus.RelayState{
		Relay: "meshcore-868", At: at, State: "error", Err: "radio gone"})

	var n int
	var atMS int64
	if err := s.store.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*), MAX(at_ms) FROM relay_states WHERE relay = 'meshcore-868' AND state = 'error'`,
	).Scan(&n, &atMS); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("relay_states rows = %d", n)
	}
	if atMS != at.UnixMilli() {
		t.Errorf("row dated %d, want the producer's %d", atMS, at.UnixMilli())
	}
}

func TestStateHistoryInterleavesRelaysAndObservers(t *testing.T) {
	s := testSentinel(t)
	ctx := context.Background()
	at := time.Now()
	s.Process(ctx, bus.RelayState{
		Relay: "meshcore-868", At: at, State: "error", Err: "radio gone",
	})
	s.Process(ctx, bus.ObserverState{
		Observer: "community", At: at.Add(time.Second), State: "lost", Cause: "EOF",
	})

	states, err := s.StateHistory(ctx, time.Time{}, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(states) != 2 {
		t.Fatalf("states = %+v", states)
	}
	if got := states[0]; got.Kind != "observer" || got.Name != "community" ||
		got.State != "lost" || got.Cause != "EOF" || got.At.UnixMilli() != at.Add(time.Second).UnixMilli() {
		t.Errorf("newest state = %+v", got)
	}
	if got := states[1]; got.Kind != "relay" || got.Name != "meshcore-868" ||
		got.State != "error" || got.Cause != "radio gone" {
		t.Errorf("oldest state = %+v", got)
	}
	states, err = s.StateHistory(ctx, at.Add(500*time.Millisecond), 10)
	if err != nil || len(states) != 1 || states[0].Kind != "observer" {
		t.Errorf("windowed states = %+v, %v", states, err)
	}
}

func TestNoiseFloorKeepsOnlyTheLastMeasure(t *testing.T) {
	s := testSentinel(t)
	first := time.Now().Add(-time.Minute)
	last := time.Now()
	s.Process(context.Background(), bus.NoiseFloor{
		Relay: "meshcore-868", At: first, DBm: -104, SpreadDB: 2})
	s.Process(context.Background(), bus.NoiseFloor{
		Relay: "meshcore-868", At: last, DBm: -101, SpreadDB: 7.5})

	var n int
	var atMS int64
	var dbm, spread float64
	if err := s.store.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*), MAX(at_ms), MAX(dbm), MAX(spread_db) FROM noise_floor WHERE relay = 'meshcore-868'`,
	).Scan(&n, &atMS, &dbm, &spread); err != nil {
		t.Fatal(err)
	}
	if n != 1 || dbm != -101 || spread != 7.5 || atMS != last.UnixMilli() {
		t.Errorf("noise_floor: rows=%d dbm=%v spread=%v at=%d — want one row with the last measure",
			n, dbm, spread, atMS)
	}

	var spreadPoints int
	if err := s.store.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM metrics_raw WHERE series = 'noise_spread'`,
	).Scan(&spreadPoints); err != nil {
		t.Fatal(err)
	}
	if spreadPoints != 2 {
		t.Errorf("noise_spread raw points = %d, want 2", spreadPoints)
	}
}

func TestNoiseFloorEventRollsBackAsAWhole(t *testing.T) {
	s := testSentinel(t)
	ctx := context.Background()
	// Fail the third projection of the event. Without one transaction,
	// the latest row and the floor series survived while spread did not.
	if _, err := s.store.db.ExecContext(ctx, `
		CREATE TRIGGER fail_noise_spread BEFORE INSERT ON metrics_raw
		WHEN NEW.series = 'noise_spread'
		BEGIN SELECT RAISE(FAIL, 'forced noise spread failure'); END;`); err != nil {
		t.Fatal(err)
	}
	s.Process(ctx, bus.NoiseFloor{
		Relay: "meshcore-868", At: time.Now(), DBm: -101, SpreadDB: 7.5,
	})

	for table, query := range map[string]string{
		"latest":  `SELECT COUNT(*) FROM noise_floor`,
		"metrics": `SELECT COUNT(*) FROM metrics_raw`,
	} {
		var n int
		if err := s.store.db.QueryRowContext(ctx, query).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n != 0 {
			t.Errorf("%s kept %d rows from a failed event", table, n)
		}
	}
}

func TestMetricsRollUpThroughTheTiers(t *testing.T) {
	s := testSentinel(t)
	ctx := context.Background()
	now := time.Now()

	// Two points in one hour two days ago (rolls to hourly, then that
	// hour is older than the test retention so it rolls again to
	// daily), plus one fresh point that must stay raw.
	old := now.Add(-48 * time.Hour).Truncate(time.Hour)
	for _, p := range []struct {
		at  time.Time
		dbm float64
	}{{old, -100}, {old.Add(time.Minute), -90}, {now, -82}} {
		s.Process(ctx, bus.NoiseFloor{Relay: "meshcore-868", At: p.at, DBm: p.dbm})
	}
	// Retention of 24h: the 48h-old hourly bucket ages into daily.
	if err := s.store.prune(ctx, now, 24*time.Hour, 0, 0); err != nil {
		t.Fatal(err)
	}

	// Each NoiseFloor event lands two raw points: its floor and its
	// spread — both series age through the tiers.
	var rawN, hourlyN, dailyN int
	for table, dst := range map[string]*int{
		"metrics_raw": &rawN, "metrics_hourly": &hourlyN, "metrics_daily": &dailyN,
	} {
		if err := s.store.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM `+table).Scan(dst); err != nil {
			t.Fatal(err)
		}
	}
	if rawN != 2 || hourlyN != 0 || dailyN != 2 {
		t.Fatalf("tiers raw=%d hourly=%d daily=%d, want 2/0/2", rawN, hourlyN, dailyN)
	}

	buckets, err := s.NoiseHistory(ctx, "meshcore-868", now.Add(-72*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(buckets) != 2 {
		t.Fatalf("history buckets = %+v, want 2", buckets)
	}
	day := buckets[0]
	if day.Min != -100 || day.Max != -90 || day.Avg != -95 || day.N != 2 {
		t.Errorf("daily bucket = %+v, want min -100 avg -95 max -90 n 2", day)
	}
	if buckets[1].Avg != -82 || buckets[1].N != 1 {
		t.Errorf("raw bucket = %+v, want avg -82 n 1", buckets[1])
	}
}

func TestCorruptReceptionsAreTallied(t *testing.T) {
	s := testSentinel(t)
	at := time.Now()
	for range 3 {
		s.Process(context.Background(), bus.FrameCorrupt{
			Relay: "meshcore-868", At: at, Err: "crc mismatch"})
	}

	noise, err := s.Noise(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(noise) != 1 {
		t.Fatalf("noise rows = %d, want 1", len(noise))
	}
	nz := noise[0]
	if nz.Relay != "meshcore-868" || nz.Count != 3 || nz.LastErr != "crc mismatch" {
		t.Errorf("noise = %+v", nz)
	}
	if nz.LastAt.UnixMilli() != at.UnixMilli() {
		t.Errorf("noise dated %v, want %v", nz.LastAt, at)
	}
}

func TestOrphanJudgementIsRecovered(t *testing.T) {
	s := testSentinel(t)
	id := txn.New()
	// The heard event was dropped by the bus; only the judgement lands.
	s.Process(context.Background(), bus.FrameJudged{
		Relay: "meshcore-868", Txn: id,
		Verdict: "would-relay-flood", Type: "ADVERT", Route: "FLOOD",
	})

	frames, err := s.RecentFrames(context.Background(), FrameQuery{Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 || frames[0].Verdict != "would-relay-flood" ||
		frames[0].Relay != "meshcore-868" {
		t.Fatalf("orphan not recovered: %+v", frames)
	}
}

func TestRedeliveredHeardPreservesJudgement(t *testing.T) {
	s := testSentinel(t)
	id := txn.New()
	heard := bus.FrameHeard{Relay: "r", Txn: id, At: time.Now(), Bytes: 10}
	s.Process(context.Background(), heard)
	s.Process(context.Background(), bus.FrameJudged{
		Relay: "r", Txn: id, Verdict: "would-relay-flood", Type: "GRP_TXT", Route: "FLOOD",
	})
	s.Process(context.Background(), heard) // redelivery must not blank the verdict

	frames, err := s.RecentFrames(context.Background(), FrameQuery{Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 || frames[0].Verdict != "would-relay-flood" {
		t.Fatalf("judgement blanked: %+v", frames)
	}
}

func TestChainFindsSiblingsFromADuplicate(t *testing.T) {
	s := testSentinel(t)
	root, dupA, dupB := txn.New(), txn.New(), txn.New()
	for _, ev := range []bus.Event{
		bus.FrameHeard{Relay: "r", Txn: root, At: time.Now()},
		bus.FrameJudged{Relay: "r", Txn: root, Verdict: "would-relay-flood"},
		bus.FrameHeard{Relay: "r", Txn: dupA, At: time.Now()},
		bus.FrameJudged{Relay: "r", Txn: dupA, Verdict: "duplicate", DuplicateOf: root.Short()},
		bus.FrameHeard{Relay: "r", Txn: dupB, At: time.Now()},
		bus.FrameJudged{Relay: "r", Txn: dupB, Verdict: "duplicate", DuplicateOf: root.Short()},
	} {
		s.Process(context.Background(), ev)
	}

	// Asking about one duplicate must surface the root AND the sibling.
	chain, err := s.Chain(context.Background(), dupA.Short())
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, f := range chain {
		got[f.Txn] = true
	}
	for _, want := range []txn.ID{root, dupA, dupB} {
		if !got[want.String()] {
			t.Errorf("chain misses %s (have %d members)", want.Short(), len(chain))
		}
	}
}

func TestNodesDirectoryIsAdvertOnly(t *testing.T) {
	s := testSentinel(t)
	adv, ctl := txn.New(), txn.New()
	for _, ev := range []bus.Event{
		bus.FrameHeard{Relay: "r", Txn: adv, At: time.Now()},
		bus.FrameJudged{Relay: "r", Txn: adv, Verdict: "would-relay-flood",
			Type: "ADVERT", Node: "Wanadoo", PubKey: "de247e12757f", Detail: "repeater"},
		bus.FrameHeard{Relay: "r", Txn: ctl, At: time.Now()},
		// A hostile or legacy row: a non-advert frame carrying a key.
		bus.FrameJudged{Relay: "r", Txn: ctl, Verdict: "heard-zero-hop",
			Type: "CONTROL", PubKey: "attacker00000", Detail: "discovery response"},
	} {
		s.Process(context.Background(), ev)
	}

	nodes, err := s.Nodes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 || nodes[0].PubKey != "de247e12757f" || nodes[0].Type != "repeater" {
		t.Fatalf("directory = %+v", nodes)
	}
}

func TestTxLedgerAndDrops(t *testing.T) {
	s := testSentinel(t)
	ctx := context.Background()
	id := txn.New()
	at := time.Now()
	s.Process(ctx, bus.FrameSent{
		Relay: "meshcore-868", Txn: id, At: at, Kind: "relay-flood",
		Airtime: 1200 * time.Millisecond, PowerDBm: -5, Shadow: true,
	})
	s.Process(ctx, bus.TxDropped{Relay: "meshcore-868", Txn: id, At: at, Reason: "lbt"})
	s.Process(ctx, bus.TxDropped{Relay: "meshcore-868", Txn: txn.New(), At: at, Reason: "lbt"})

	sent, err := s.SentFor(ctx, id.String())
	if err != nil {
		t.Fatal(err)
	}
	if len(sent) != 1 || !sent[0].Shadow || sent[0].Kind != "relay-flood" ||
		sent[0].PowerDBm != -5 || sent[0].Airtime != 1200*time.Millisecond {
		t.Fatalf("ledger = %+v", sent)
	}

	drops, err := s.TxDrops(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(drops) != 1 || drops[0].Reason != "lbt" || drops[0].Count != 2 {
		t.Fatalf("drops = %+v", drops)
	}

	var airtime float64
	if err := s.store.db.QueryRowContext(ctx,
		`SELECT value FROM metrics_raw WHERE series = 'tx_airtime'`).Scan(&airtime); err != nil {
		t.Fatal(err)
	}
	if airtime != 1.2 {
		t.Fatalf("tx_airtime point = %v, want 1.2 s", airtime)
	}
}

func TestSentEventAndAirtimeWindowCommitTogether(t *testing.T) {
	s := testSentinel(t)
	ctx := context.Background()
	at := time.Now()
	s.Process(ctx, bus.FrameSent{
		Relay: "r", Txn: txn.New(), At: at, Kind: "first", Airtime: time.Second,
	})
	if _, err := s.store.db.ExecContext(ctx, `
		CREATE TRIGGER fail_tx_airtime BEFORE INSERT ON metrics_raw
		WHEN NEW.series = 'tx_airtime'
		BEGIN SELECT RAISE(FAIL, 'forced airtime failure'); END;`); err != nil {
		t.Fatal(err)
	}
	s.Process(ctx, bus.FrameSent{
		Relay: "r", Txn: txn.New(), At: at.Add(time.Second), Kind: "failed", Airtime: 2 * time.Second,
	})
	if len(s.txWindows["r"]) != 1 {
		t.Fatalf("failed write advanced RAM window to %d emissions", len(s.txWindows["r"]))
	}
	if _, err := s.store.db.ExecContext(ctx, `DROP TRIGGER fail_tx_airtime`); err != nil {
		t.Fatal(err)
	}
	s.Process(ctx, bus.FrameSent{
		Relay: "r", Txn: txn.New(), At: at.Add(2 * time.Second), Kind: "third", Airtime: 4 * time.Second,
	})

	var txRows, metricRows int
	if err := s.store.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM tx`).Scan(&txRows); err != nil {
		t.Fatal(err)
	}
	if err := s.store.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM metrics_raw WHERE series = 'tx_airtime'`).Scan(&metricRows); err != nil {
		t.Fatal(err)
	}
	if txRows != 2 || metricRows != 2 {
		t.Fatalf("ledger/metrics rows = %d/%d, want 2/2", txRows, metricRows)
	}
	var lastWindow float64
	if err := s.store.db.QueryRowContext(ctx,
		`SELECT value FROM metrics_raw WHERE series = 'tx_airtime' ORDER BY at_ms DESC LIMIT 1`,
	).Scan(&lastWindow); err != nil {
		t.Fatal(err)
	}
	if lastWindow != 5 {
		t.Errorf("last window = %.1fs, want 5s: failed emission leaked into RAM", lastWindow)
	}
}

func TestOpenPrunesBeforeItSubscribes(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/journal.db"
	st, err := openStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.insertObserved(ctx, Frame{
		Txn: txn.New().String(), Relay: "r", At: time.Now().Add(-2 * time.Hour),
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	b := bus.New()
	s, err := Open(ctx, path, time.Hour, 0, 0, b, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = s.store.Close() }()
	if n, err := s.FrameCount(ctx); err != nil || n != 0 {
		t.Fatalf("frames immediately after Open = %d, %v; maintenance did not finish", n, err)
	}
	if dropped := s.sub.Dropped(); dropped != 0 {
		t.Errorf("subscription lost %d events before Run", dropped)
	}
}

func TestMigrateGraftsEveryColumnAddedSince(t *testing.T) {
	// A journal written by an older build carries the tables as they
	// were; opening it must reach today's schema, not fail the first
	// insert.
	path := t.TempDir() + "/old.db"
	old, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := old.ExecContext(context.Background(), `
		CREATE TABLE frames (
			txn TEXT PRIMARY KEY, relay TEXT NOT NULL, at_ms INTEGER NOT NULL,
			bytes INTEGER NOT NULL, rssi_dbm REAL NOT NULL, snr_db REAL NOT NULL,
			airtime_ms REAL NOT NULL);
		CREATE TABLE noise_floor (
			relay TEXT PRIMARY KEY, at_ms INTEGER NOT NULL, dbm REAL NOT NULL);`); err != nil {
		t.Fatal(err)
	}
	if err := old.Close(); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	st, err := openStore(ctx, path)
	if err != nil {
		t.Fatalf("opening an older journal: %v", err)
	}
	defer func() { _ = st.Close() }()

	for table, want := range map[string][]string{
		"frames":      {"node", "pubkey", "detail", "signal_dbm", "freq_err_hz"},
		"noise_floor": {"spread_db"},
	} {
		cols, err := tableColumns(ctx, st.db, table)
		if err != nil {
			t.Fatal(err)
		}
		for _, c := range want {
			if !cols[c] {
				t.Errorf("%s.%s was never grafted", table, c)
			}
		}
	}
	// And the grafted journal takes today's writes.
	s := &Sentinel{store: st}
	s.Process(ctx, bus.FrameJudged{
		Relay: "r", Txn: txn.New(), At: time.Now(), SignalRSSI: -70, FreqErrHz: 12})
	s.Process(ctx, bus.NoiseFloor{Relay: "r", At: time.Now(), DBm: -100, SpreadDB: 3})
	frames, err := st.RecentFrames(ctx, FrameQuery{Limit: 5})
	if err != nil || len(frames) != 1 || frames[0].SignalRSSI != -70 {
		t.Fatalf("frames = %+v, %v", frames, err)
	}
}

func TestDropsKeepTheirTransaction(t *testing.T) {
	// The chain's missing half: a refusal is findable by its txn, and
	// the by-reason tally moves in the same transaction.
	s := testSentinel(t)
	ctx := context.Background()
	id := txn.New()
	at := time.Now()
	s.Process(ctx, bus.TxDropped{Relay: "r", Txn: id, At: at, Reason: "duty", Kind: "relay-flood"})
	s.Process(ctx, bus.TxDropped{Relay: "r", Txn: txn.New(), At: at, Reason: "duty", Kind: "advert-flood"})

	events, err := s.DropsFor(ctx, id.Short())
	if err != nil || len(events) != 1 {
		t.Fatalf("DropsFor = %+v, %v", events, err)
	}
	e := events[0]
	if e.Txn != id.String() || e.Reason != "duty" || e.Kind != "relay-flood" ||
		e.At.UnixMilli() != at.UnixMilli() {
		t.Errorf("event = %+v", e)
	}
	drops, err := s.TxDrops(ctx)
	if err != nil || len(drops) != 1 || drops[0].Count != 2 {
		t.Errorf("aggregate = %+v, %v", drops, err)
	}
	// Retention prunes the events like every detailed row.
	if err := s.store.prune(ctx, at.Add(2*time.Hour), time.Hour, 0, 0); err != nil {
		t.Fatal(err)
	}
	if events, _ := s.DropsFor(ctx, id.Short()); len(events) != 0 {
		t.Error("the drop events escaped retention")
	}
}

func TestJournalHealthTellsTheStoryOnce(t *testing.T) {
	// A hundred failing writes are one WARN and a paced delta, not a
	// log storm feeding the disk saturation that caused them — and
	// the recovery is announced, so a rotated log cannot erase the
	// whole episode.
	s := testSentinel(t)
	ctx := context.Background()
	_ = s.store.Close() // every write now fails

	for range 100 {
		s.Process(ctx, bus.FrameJudged{Relay: "r", Txn: txn.New(), At: time.Now()})
	}
	h := s.Health()
	if h.Healthy || h.Failures != 100 || h.LastErr == "" || h.LastFailAt.IsZero() {
		t.Errorf("health = %+v", h)
	}
	// warnedAt was set by the first failure and the rest fold: the
	// throttle state proves at most 1 log in the burst window.
	if time.Since(s.warnedAt) > time.Second {
		t.Error("the throttle never armed")
	}
}

func TestMetricsRetentionBoundsTheDailyTier(t *testing.T) {
	s := testSentinel(t)
	ctx := context.Background()
	old := time.Now().Add(-72 * time.Hour)
	if err := s.store.insertMetric(ctx, "noise_floor", "r", old, -100); err != nil {
		t.Fatal(err)
	}
	// A short metrics retention sweeps what the fixed two-year keep
	// used to hold forever.
	if err := s.store.prune(ctx, time.Now(), time.Hour, 24*time.Hour, 0); err != nil {
		t.Fatal(err)
	}
	buckets, err := s.NoiseHistory(ctx, "r", time.Now().Add(-30*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(buckets) != 0 {
		t.Errorf("buckets = %+v — the metric outlived metrics_retention", buckets)
	}
}

func TestJournalIsBornPrivate(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/journal.db"
	st, err := openStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("journal mode = %o, want 600 — telemetry is not for every local account", info.Mode().Perm())
	}
}

func TestMetricsRetentionBoundsEveryTier(t *testing.T) {
	// The review's interaction: retention 30d and metrics_retention
	// 24h left a 48h-old point alive in the hourly tier — the knob
	// bounded only daily. Every tier answers to it now.
	s := testSentinel(t)
	ctx := context.Background()
	old := time.Now().Add(-48 * time.Hour)
	if err := s.store.insertMetric(ctx, "noise_floor", "r", old, -100); err != nil {
		t.Fatal(err)
	}
	if err := s.store.prune(ctx, time.Now(), 30*24*time.Hour, 24*time.Hour, 0); err != nil {
		t.Fatal(err)
	}
	buckets, err := s.NoiseHistory(ctx, "r", time.Now().Add(-30*24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if len(buckets) != 0 {
		t.Errorf("buckets = %+v — a tier outlived metrics_retention", buckets)
	}
}

func TestBusDropWarningsArePaced(t *testing.T) {
	// The second amplification source of OBS-004: under sustained
	// saturation every consume/publish cycle advanced the counter and
	// earned its own WARN. The first loss logs at once; the rest fold
	// until the window passes; the cumulative count stays in Health.
	s := testSentinel(t)
	sub := s.sub
	for range 300 {
		s.bus.Publish(bus.NoiseStarved{Relay: "r"})
	}
	drainOne := func() {
		select {
		case <-sub.C:
		default:
		}
	}
	warned := 0
	for range 100 {
		drainOne()
		s.bus.Publish(bus.NoiseStarved{Relay: "r"})
		s.bus.Publish(bus.NoiseStarved{Relay: "r"})
		before := s.reported
		s.reportDrops()
		if s.reported != before {
			warned++
		}
	}
	if warned > 1 {
		t.Errorf("%d warnings in one pacing window — the storm survived", warned)
	}
	if s.Health().BusDropped == 0 {
		t.Error("the cumulative count vanished with the pacing")
	}
}

func TestAnExposedJournalIsCorrectedOnOpen(t *testing.T) {
	// The installations already at 0644 are precisely the ones the
	// upgrade must protect.
	ctx := context.Background()
	path := t.TempDir() + "/journal.db"
	if err := os.WriteFile(path, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := openStore(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = st.Close() }()
	// The main file and whatever sidecars WAL made while open.
	for _, p := range []string{path, path + "-wal", path + "-shm"} {
		info, err := os.Stat(p)
		if err != nil {
			continue // a sidecar may not exist yet
		}
		if info.Mode().Perm()&0o077 != 0 {
			t.Errorf("%s mode = %o — still readable beyond the owner", p, info.Mode().Perm())
		}
	}
}

func TestBusLossRecoveryIsAnnouncedOnce(t *testing.T) {
	// The pacing folds the losses; the episode's END must still be
	// said, or silence after a WARN reads as "still losing". One
	// quiet pacing window closes the episode, exactly once.
	s := testSentinel(t)
	for range cap(s.sub.C) + 5 {
		s.bus.Publish(bus.NoiseStarved{Relay: "r"})
	}
	s.reportDrops()
	if !s.dropOpen {
		t.Fatal("losses happened but no episode opened")
	}
	// Still inside the window: quiet, but not yet over.
	s.reportDrops()
	if !s.dropOpen {
		t.Error("the episode closed before a full quiet window")
	}
	s.dropWarnAt = time.Now().Add(-healthLogEvery)
	s.reportDrops()
	if s.dropOpen {
		t.Error("a quiet window passed and the episode never closed")
	}
}

func TestTightenJournalRefusesWhatIsNotARegularFile(t *testing.T) {
	// Only a regular file may be protected: a symlink carries a chmod
	// to its target, and bending a mistaken directory or socket into
	// 0600 wrecks the misconfigured object before SQLite even gets to
	// refuse it. Every refusal must leave the object exactly as found.
	dir := t.TempDir()

	dpath := dir + "/dir.db"
	if err := os.Mkdir(dpath, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := tightenJournal(dpath); err == nil {
		t.Error("a directory was accepted as the journal")
	}
	if st, _ := os.Stat(dpath); st.Mode().Perm() != 0o755 {
		t.Errorf("the refused directory was modified to %o", st.Mode().Perm())
	}

	target := dir + "/target"
	if err := os.WriteFile(target, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	lpath := dir + "/link.db"
	if err := os.Symlink(target, lpath); err != nil {
		t.Fatal(err)
	}
	if err := tightenJournal(lpath); err == nil {
		t.Error("a symlink was accepted as the journal")
	}
	if st, _ := os.Stat(target); st.Mode().Perm() != 0o644 {
		t.Errorf("the chmod travelled through the symlink: %o", st.Mode().Perm())
	}

	spath := dir + "/sock.db"
	l, err := new(net.ListenConfig).Listen(context.Background(), "unix", spath)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = l.Close() }()
	if err := tightenJournal(spath); err == nil {
		t.Error("a socket was accepted as the journal")
	}

	// A healthy main file does not excuse its sidecars.
	mpath := dir + "/main.db"
	if err := os.WriteFile(mpath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, mpath+"-wal"); err != nil {
		t.Fatal(err)
	}
	if err := tightenJournal(mpath); err == nil {
		t.Error("a symlinked sidecar was accepted")
	}
	if st, _ := os.Stat(target); st.Mode().Perm() != 0o644 {
		t.Errorf("the sidecar chmod travelled through the symlink: %o", st.Mode().Perm())
	}
}
