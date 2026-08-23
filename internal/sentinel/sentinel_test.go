package sentinel

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/txn"
)

func testSentinel(t *testing.T) *Sentinel {
	t.Helper()
	s, err := Open(context.Background(), MemoryJournal, time.Hour, 0, bus.New(), zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.store.Close() })
	return s
}

func TestHeardThenJudgedBecomesOneRow(t *testing.T) {
	s := testSentinel(t)
	id := txn.New()
	at := time.Now()

	s.Process(context.Background(), bus.FrameHeard{
		Relay: "meshcore-868", Txn: id, At: at,
		Bytes: 132, RSSI: -69, SNR: 8.5, Airtime: 1295 * time.Millisecond,
	})
	s.Process(context.Background(), bus.FrameJudged{
		Relay: "meshcore-868", Txn: id,
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
		f.Bytes != 132 || f.RSSI != -69 ||
		f.Node != "Wanadoo" || f.PubKey != "de1234567890" || f.Detail != "repeater" {
		t.Errorf("row = %+v", f)
	}
}

func TestShortPrefixFindsItsTransaction(t *testing.T) {
	s := testSentinel(t)
	id := txn.New()
	s.Process(context.Background(), bus.FrameHeard{Relay: "r", Txn: id, At: time.Now()})
	s.Process(context.Background(), bus.FrameHeard{Relay: "r", Txn: txn.New(), At: time.Now()})

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
	s.Process(context.Background(), bus.FrameHeard{Relay: "r", Txn: old, At: time.Now().Add(-2 * time.Hour)})
	s.Process(context.Background(), bus.FrameHeard{Relay: "r", Txn: fresh, At: time.Now()})

	if err := s.store.prune(context.Background(), time.Now(), time.Hour, 0); err != nil {
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

func TestNoiseFloorKeepsOnlyTheLastMeasure(t *testing.T) {
	s := testSentinel(t)
	first := time.Now().Add(-time.Minute)
	last := time.Now()
	s.Process(context.Background(), bus.NoiseFloor{Relay: "meshcore-868", At: first, DBm: -104})
	s.Process(context.Background(), bus.NoiseFloor{Relay: "meshcore-868", At: last, DBm: -101})

	var n int
	var atMS int64
	var dbm float64
	if err := s.store.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*), MAX(at_ms), MAX(dbm) FROM noise_floor WHERE relay = 'meshcore-868'`,
	).Scan(&n, &atMS, &dbm); err != nil {
		t.Fatal(err)
	}
	if n != 1 || dbm != -101 || atMS != last.UnixMilli() {
		t.Errorf("noise_floor: rows=%d dbm=%v at=%d — want one row with the last measure", n, dbm, atMS)
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
	if err := s.store.prune(ctx, now, 24*time.Hour, 0); err != nil {
		t.Fatal(err)
	}

	var rawN, hourlyN, dailyN int
	for table, dst := range map[string]*int{
		"metrics_raw": &rawN, "metrics_hourly": &hourlyN, "metrics_daily": &dailyN,
	} {
		if err := s.store.db.QueryRowContext(ctx,
			`SELECT COUNT(*) FROM `+table).Scan(dst); err != nil {
			t.Fatal(err)
		}
	}
	if rawN != 1 || hourlyN != 0 || dailyN != 1 {
		t.Fatalf("tiers raw=%d hourly=%d daily=%d, want 1/0/1", rawN, hourlyN, dailyN)
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
