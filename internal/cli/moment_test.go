package cli

import (
	"strings"
	"testing"
	"time"
)

func TestAMomentIsWrittenTheWayTheViewsWriteOne(t *testing.T) {
	loc := time.FixedZone("test", 2*3600)
	now := time.Date(2026, 8, 28, 0, 30, 0, 0, loc)
	for _, c := range []struct {
		in   string
		want time.Time
	}{
		// A bare clock names its most recent occurrence: still today
		// when it has come round, yesterday when it has not.
		{"00:15", time.Date(2026, 8, 28, 0, 15, 0, 0, loc)},
		{"23:30", time.Date(2026, 8, 27, 23, 30, 0, 0, loc)},
		{"00:52:18", time.Date(2026, 8, 27, 0, 52, 18, 0, loc)},
		{"2026-08-27", time.Date(2026, 8, 27, 0, 0, 0, 0, loc)},
		{"2026-08-27 23:00", time.Date(2026, 8, 27, 23, 0, 0, 0, loc)},
		{"2026-08-27 23:00:05", time.Date(2026, 8, 27, 23, 0, 5, 0, loc)},
	} {
		got, err := parseMoment(c.in, now)
		if err != nil || !got.Equal(c.want) {
			t.Errorf("parseMoment(%q) = %v, %v — want %v", c.in, got, err, c.want)
		}
	}
	if _, err := parseMoment("midnightish", now); err == nil {
		t.Error("a non-moment parsed")
	}
}

func TestSelectorsRefuseSayingOneThingTwoWays(t *testing.T) {
	now := time.Now()
	for _, c := range []struct {
		opts map[string]string
		want string
	}{
		{map[string]string{optLast: "15m", optSince: "00:52"}, "say the window whole"},
		{map[string]string{optLast: "15m", optUntil: "00:52"}, "say the window whole"},
		{map[string]string{optSpan: "30s"}, "it needs around="},
		{map[string]string{optAround: "abcd", optSince: "00:52"}, "names its own window"},
		{map[string]string{optLast: "zz"}, "a count (50) or a span (15m)"},
		{map[string]string{optLast: "0"}, "one or more"},
		{map[string]string{optSince: "10:00", optUntil: "09:00"}, "inside out"},
		{map[string]string{optAround: "abcd", optSpan: "zz"}, "wants a duration"},
	} {
		_, err := parseFrameSelectors(c.opts, now)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("selectors(%v) err = %v, want %q", c.opts, err, c.want)
		}
	}
	// And the two faces of last= are read apart.
	if w, err := parseFrameSelectors(map[string]string{optLast: "50"}, now); err != nil || w.count != 50 || !w.since.IsZero() {
		t.Errorf("a count read wrong: %+v %v", w, err)
	}
	if w, err := parseFrameSelectors(map[string]string{optLast: "15m"}, now); err != nil || w.count != 0 ||
		!w.since.Equal(now.Add(-15*time.Minute)) {
		t.Errorf("a span read wrong: %+v %v", w, err)
	}
}

func TestTheWindowSelectsAndTheCapConfesses(t *testing.T) {
	deps := testDeps(t)
	orig, _ := seed(t, deps)
	// A span keeps what happened inside it and nothing older.
	if out := run(t, deps, "frames last=1h"); !strings.Contains(out, orig.Short()[:12]) {
		t.Errorf("the span missed a fresh frame:\n%s", out)
	}
	if out := run(t, deps, "frames until=2000-01-01"); !strings.Contains(out, "no frames match") {
		t.Errorf("a window before the journal matched it:\n%s", out)
	}
	// around= centres on the transaction it names.
	if out := run(t, deps, "frames around="+orig.Short()[:6]+" span=1h"); !strings.Contains(out, "duplicate") {
		t.Errorf("the window around a frame missed its duplicate:\n%s", out)
	}
	if out := run(t, deps, "frames around=ffffff"); !strings.Contains(out, "no transaction starts with") {
		t.Errorf("an unknown anchor was not refused:\n%s", out)
	}
	// A capped window owes its reader the total.
	if out := run(t, deps, "frames last=1 since=00:00"); !strings.Contains(out, "of 2 shown") {
		t.Errorf("the cap kept the total to itself:\n%s", out)
	}
}
