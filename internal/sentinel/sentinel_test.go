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
	s, err := Open(context.Background(), MemoryJournal, time.Hour, bus.New(), zap.NewNop())
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

	frames, err := s.RecentFrames(context.Background(), "", 10)
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

	frames, err := s.RecentFrames(context.Background(), id.Short(), 10)
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

	if err := s.store.prune(context.Background(), time.Now().Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	frames, err := s.RecentFrames(context.Background(), "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 1 || frames[0].Txn != fresh.String() {
		t.Fatalf("after prune = %+v", frames)
	}
}

func TestRelayStatesJournalled(t *testing.T) {
	s := testSentinel(t)
	s.Process(context.Background(), bus.RelayState{Relay: "meshcore-868", State: "error", Err: "radio gone"})

	var n int
	if err := s.store.db.QueryRowContext(context.Background(),
		`SELECT COUNT(*) FROM relay_states WHERE relay = 'meshcore-868' AND state = 'error'`,
	).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("relay_states rows = %d", n)
	}
}
