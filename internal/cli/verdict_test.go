package cli

import (
	"bytes"
	"strings"
	"testing"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/correlation"
)

func TestWatchKeepsTextualVerdictFiltersAndLabels(t *testing.T) {
	var out bytes.Buffer
	s := &session{out: &out}
	id := correlation.New()
	event := bus.FrameJudged{
		Correlation: id, Type: "ADVERT", Route: "FLOOD", PathLen: 2, Verdict: bus.VerdictRelayFlood,
	}
	if err := s.watchEvent(event, map[string]string{optVerdict: "would-relay-flood"}); err != nil {
		t.Fatal(err)
	}
	if got, want := out.String(), id.Short()+"  ADVERT FLOOD /2  would-relay-flood\r\n"; got != want {
		t.Fatalf("watch line = %q, want %q", got, want)
	}
	out.Reset()
	event.Verdict = bus.VerdictDuplicate
	if err := s.watchEvent(event, map[string]string{optVerdict: "would-relay-flood"}); err != nil {
		t.Fatal(err)
	}
	if out.Len() != 0 {
		t.Fatal("the textual verdict filter accepted a different judgement")
	}
	if err := s.watchEvent(event, map[string]string{optVerdict: "duplicate"}); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(out.String(), "  duplicate\r\n") {
		t.Fatalf("duplicate lost its public spelling: %q", out.String())
	}
}
