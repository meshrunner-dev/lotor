package sentinel

import (
	"testing"
	"time"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/correlation"
)

func TestDropReasonsKeepHistoricalJournalText(t *testing.T) {
	s := testSentinel(t)
	id := correlation.New()
	at := time.Now()
	for _, tc := range []struct {
		reason bus.DropReason
		label  string
	}{
		{bus.DropDuty, "duty"},
		{bus.DropLBT, "lbt"},
		{bus.DropSessionStore, "session-store"},
		{bus.DropComposeFailed, "compose-failed"},
		{bus.DropStationRestart, "station-restart"},
	} {
		t.Run(tc.label, func(t *testing.T) {
			for range 2 {
				s.Process(t.Context(), bus.TxDropped{
					SourceKind: bus.SourceApplication, Source: tc.label,
					Correlation: id, At: at, Reason: tc.reason, Kind: "answer",
				})
			}
		})
	}
	// A row written by an older or newer daemon may name a refusal this
	// binary never emits. Reading history must not parse that vocabulary
	// through the current process's closed enum.
	if err := s.store.recordTxDrop(t.Context(), at, "historical", id.String(), "unknown-legacy-reason", "answer"); err != nil {
		t.Fatal(err)
	}
	events, err := s.DropsFor(t.Context(), id.String())
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, event := range events {
		counts[event.Reason]++
	}
	for _, label := range []string{"duty", "lbt", "session-store", "compose-failed", "station-restart"} {
		if counts[label] != 2 {
			t.Errorf("journal reason %q has %d events, want 2", label, counts[label])
		}
	}
	if len(events) != 11 || counts["unknown-legacy-reason"] != 1 {
		t.Fatalf("journal lost textual history: %+v", counts)
	}
	aggregates, err := s.TxDrops(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(aggregates) != len(counts) {
		t.Fatalf("got %d aggregate reasons for %d distinct labels", len(aggregates), len(counts))
	}
	for _, aggregate := range aggregates {
		if aggregate.Count != counts[aggregate.Reason] {
			t.Errorf("aggregate differs from textual events: %+v", aggregate)
		}
	}
}
