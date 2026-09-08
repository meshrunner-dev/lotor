package bus

import (
	"encoding/json"
	"testing"
)

func TestDropReasonsKeepTheirTextAndJSONLabels(t *testing.T) {
	cases := []struct {
		reason DropReason
		label  string
	}{
		{DropNone, ""},
		{DropDry, "dry"},
		{DropRadioDown, "radio-down"},
		{DropDuty, "duty"},
		{DropDutyUnavailable, "duty-unavailable"},
		{DropExpired, "expired"},
		{DropCancelled, "cancelled"},
		{DropTXFailed, "tx-failed"},
		{DropLBTFailed, "lbt-failed"},
		{DropLBT, "lbt"},
		{DropQueueFull, "queue-full"},
		{DropMalformed, "malformed"},
		{DropDuplicate, "duplicate"},
		{DropRateLimited, "rate-limited"},
		{DropSessionStore, "session-store"},
		{DropSessionRestart, "session-restart"},
		{DropStationRestart, "station-restart"},
		{DropComposeFailed, "compose-failed"},
	}
	seenReasons, seenLabels := map[DropReason]bool{}, map[string]bool{}
	for _, tc := range cases {
		if seenReasons[tc.reason] || seenLabels[tc.label] {
			t.Fatalf("duplicate reason or label: %+v", tc)
		}
		seenReasons[tc.reason], seenLabels[tc.label] = true, true
		t.Run(tc.label, func(t *testing.T) {
			if tc.reason.String() != tc.label {
				t.Fatalf("String()=%q, want %q", tc.reason.String(), tc.label)
			}
			text, err := tc.reason.MarshalText()
			if err != nil || string(text) != tc.label {
				t.Fatalf("MarshalText()=%q, %v; want %q", text, err, tc.label)
			}
			var decoded DropReason
			if err := decoded.UnmarshalText([]byte(tc.label)); err != nil || decoded != tc.reason {
				t.Fatalf("UnmarshalText(%q)=%v, %v", tc.label, decoded, err)
			}
			data, err := json.Marshal(tc.reason)
			if err != nil || string(data) != `"`+tc.label+`"` {
				t.Fatalf("JSON=%s, %v; want quoted label %q", data, err, tc.label)
			}
			if err := json.Unmarshal(data, &decoded); err != nil || decoded != tc.reason {
				t.Fatalf("JSON round trip=%v, %v", decoded, err)
			}
		})
	}
	if len(seenReasons) != int(dropReasonCount) {
		t.Fatalf("label fixtures cover %d of %d reasons", len(seenReasons), dropReasonCount)
	}
}

func TestUnknownDropReasonsAreNotAcceptedAsValidText(t *testing.T) {
	for _, invalid := range []DropReason{dropReasonCount, 255} {
		if invalid.String() != "unknown" {
			t.Fatalf("invalid reason is not visible: %q", invalid.String())
		}
		if _, err := invalid.MarshalText(); err == nil {
			t.Fatal("invalid reason marshalled as text")
		}
		if _, err := json.Marshal(invalid); err == nil {
			t.Fatal("invalid reason marshalled as JSON")
		}
	}
	for _, text := range []string{"unknown", "future-refusal", "Duty", " duty", "duty ", "3"} {
		reason := DropLBT
		if err := reason.UnmarshalText([]byte(text)); err == nil || reason != DropLBT {
			t.Errorf("UnmarshalText(%q)=%v, %v; want refusal without mutation", text, reason, err)
		}
	}
	for _, data := range []string{`"unknown"`, `3`, `true`, `{}`, `[]`} {
		reason := DropDuty
		if err := json.Unmarshal([]byte(data), &reason); err == nil || reason != DropDuty {
			t.Errorf("Unmarshal(%s)=%v, %v; want refusal without mutation", data, reason, err)
		}
	}
}

func TestDroppedEventKeepsATextualJSONReason(t *testing.T) {
	//nolint:musttag // the existing bus event has no JSON tags; preserve its default field names
	data, err := json.Marshal(TxDropped{Reason: DropSessionStore, Kind: "answer"})
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		t.Fatal(err)
	}
	if string(fields["Reason"]) != `"session-store"` {
		t.Fatalf("event Reason=%s, want historical text", fields["Reason"])
	}
	var event TxDropped
	//nolint:musttag // exercise decoding the existing untagged event, not a surrogate DTO
	if err := json.Unmarshal(data, &event); err != nil || event.Reason != DropSessionStore || event.Kind != "answer" {
		t.Fatalf("event round trip=%+v, %v", event, err)
	}
}
