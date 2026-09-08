package sentinel

import (
	"reflect"
	"testing"
	"time"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/correlation"
)

func TestTypedVerdictsKeepTextualJournalFiltersAndCounts(t *testing.T) {
	s := testSentinel(t)
	for _, verdict := range []bus.Verdict{bus.VerdictNone, bus.VerdictRelayFlood, bus.VerdictDuplicate} {
		s.Process(t.Context(), bus.FrameJudged{
			Relay: "r", Correlation: correlation.New(), At: time.Now(), Verdict: verdict,
		})
	}
	frames, err := s.RecentFrames(t.Context(), FrameQuery{Relay: "r", Verdict: "would-relay-flood", Limit: 10})
	if err != nil || len(frames) != 1 || frames[0].Verdict != "would-relay-flood" {
		t.Fatalf("the textual journal filter changed: %+v, %v", frames, err)
	}
	counts, err := s.VerdictCounts(t.Context(), "r")
	if err != nil || !reflect.DeepEqual(counts, map[string]int{"would-relay-flood": 1, "duplicate": 1}) {
		t.Fatalf("counts lost their labels or included the empty verdict: %v, %v", counts, err)
	}
	_, vocabulary, err := s.FrameVocabulary(t.Context())
	if err != nil || !reflect.DeepEqual(vocabulary, []string{"duplicate", "would-relay-flood"}) {
		t.Fatalf("journal vocabulary changed: %v, %v", vocabulary, err)
	}
}
