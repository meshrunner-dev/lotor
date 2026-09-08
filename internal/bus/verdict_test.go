package bus

import (
	"encoding/json"
	"testing"
)

func TestVerdictPreservesThePublishedVocabulary(t *testing.T) {
	want := map[Verdict]string{
		VerdictNone:           "",
		VerdictRelayFlood:     "would-relay-flood",
		VerdictRelayDirect:    "would-relay-direct",
		VerdictRelayTrace:     "would-relay-trace",
		VerdictDropFloodType:  "would-drop-flood-type",
		VerdictDropFloodShort: "would-drop-flood-truncated",
		VerdictDropBadAdvert:  "would-drop-invalid-advert",
		VerdictDropPathFull:   "would-drop-flood-path-full",
		VerdictDropFloodHops:  "would-drop-flood-hops",
		VerdictDropLoop:       "would-drop-flood-loop",
		VerdictDropScoped:     "would-drop-flood-scoped",
		VerdictSelfAdvert:     "self-advert",
		VerdictSameRadio:      "heard-on-our-own-radio",
		VerdictCommand:        "administration",
		VerdictZeroHop:        "heard-zero-hop",
		VerdictNotAddressed:   "direct-not-addressed",
		VerdictDiscover:       "discover-request",
		VerdictAnon:           "anon-request",
		VerdictDiscoverAnswer: "discover-answer",
		VerdictScopeAnswer:    "scopes-answer",
		VerdictRequest:        "authenticated-request",
		VerdictClientPath:     "client-route-home",
		VerdictTraceTransit:   "trace-transit",
		VerdictTraceNotUs:     "trace-not-addressed",
		VerdictTraceArrived:   "trace-arrived",
		VerdictBadVersion:     "unsupported-version",
		VerdictIgnored:        "ignored",
		VerdictMalformed:      "malformed",
		VerdictDuplicate:      "duplicate",
	}
	if len(want) != int(verdictCount) || VerdictNone != 0 {
		t.Fatal("the contract must cover every verdict, including the empty zero value")
	}
	for n := range 256 {
		verdict := Verdict(n)
		label, valid := want[verdict]
		if !valid {
			if verdict.String() != "unknown" {
				t.Errorf("invalid verdict %d has no safe display", n)
			}
			if _, err := verdict.MarshalText(); err == nil {
				t.Errorf("invalid verdict %d encoded as public text", n)
			}
			if _, err := json.Marshal(FrameJudged{Verdict: verdict}); err == nil { //nolint:musttag // preserve the event's existing default JSON field names
				t.Errorf("invalid verdict %d encoded as a JSON event", n)
			}
			continue
		}
		if got := verdict.String(); got != label {
			t.Errorf("verdict %d = %q, want %q", n, got, label)
		}
		text, err := verdict.MarshalText()
		if err != nil || string(text) != label {
			t.Errorf("verdict %d text = %q, %v", n, text, err)
		}
		var parsed Verdict
		if err := parsed.UnmarshalText([]byte(label)); err != nil || parsed != verdict {
			t.Errorf("verdict %q decoded as %d, %v", label, parsed, err)
		}
		encoded, err := json.Marshal(FrameJudged{Verdict: verdict}) //nolint:musttag // preserve the event's existing default JSON field names
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &fields); err != nil {
			t.Fatal(err)
		}
		var jsonLabel string
		if err := json.Unmarshal(fields["Verdict"], &jsonLabel); err != nil || jsonLabel != label {
			t.Errorf("event verdict changed JSON representation: %s, %v", fields["Verdict"], err)
		}
		var event FrameJudged
		if err := json.Unmarshal(encoded, &event); err != nil || event.Verdict != verdict { //nolint:musttag // decode the unchanged event JSON shape
			t.Errorf("verdict %q did not round-trip through its event: %v", label, err)
		}
	}
}

func TestVerdictRejectsUnknownTextWithoutChangingTheReceiver(t *testing.T) {
	for _, text := range []string{"unknown", "other", "Would-relay-flood", "would-relay-flood "} {
		verdict := VerdictDuplicate
		if err := verdict.UnmarshalText([]byte(text)); err == nil || verdict != VerdictDuplicate {
			t.Errorf("unknown text %q changed the verdict or was accepted: %v", text, err)
		}
	}
	for _, raw := range []string{`"unknown"`, `1`, `true`, `{}`, `[]`} {
		verdict := VerdictDuplicate
		if err := json.Unmarshal([]byte(raw), &verdict); err == nil || verdict != VerdictDuplicate {
			t.Errorf("non-verdict JSON %s changed the verdict or was accepted: %v", raw, err)
		}
	}
}
