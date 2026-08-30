package cli

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// The window selectors: how frames picks a slice of the journal.
// Relative and absolute do not overlap — last reaches back from now,
// since and until name moments, around centres on one correlation —
// so each word keeps one meaning and the combinations that would
// blur them are refused.
const (
	optSince  = "since"
	optUntil  = "until"
	optAround = "around"
	optSpan   = "span"
)

// momentForms is every way a moment may be written, longest first so
// a value carrying seconds is not half-read by the minute form. The
// clock-only forms are the ones the views themselves print: what an
// operator reads in a column must be typeable in a filter.
var momentForms = []string{
	"2006-01-02 15:04:05",
	"2006-01-02 15:04",
	"2006-01-02",
	"15:04:05",
	"15:04",
}

// parseMoment reads a moment the way the views write one. A date-less
// clock names its most recent occurrence — today, or yesterday when
// the clock has not come round yet — because that is what someone
// retyping a time they just read means by it.
func parseMoment(text string, now time.Time) (time.Time, error) {
	for _, form := range momentForms {
		t, err := time.ParseInLocation(form, text, now.Location())
		if err != nil {
			continue
		}
		if strings.Contains(form, "2006") {
			return t, nil
		}
		at := time.Date(now.Year(), now.Month(), now.Day(),
			t.Hour(), t.Minute(), t.Second(), 0, now.Location())
		if at.After(now) {
			at = at.AddDate(0, 0, -1)
		}
		return at, nil
	}
	return time.Time{}, fmt.Errorf(
		`%q is not a moment — 00:52, 00:52:18, or "2006-01-02 15:04"`, text)
}

// frameSelectors is one frames line's answer to "which slice": the
// window edges, the count, and the correlation still to be resolved
// against the journal.
type frameSelectors struct {
	since, until time.Time
	count        int
	aroundPrefix string
	span         time.Duration
}

// windowed reports whether the line asked for a slice of time rather
// than merely a count — the case where the cap owes a confession.
func (w *frameSelectors) windowed() bool {
	return !w.since.IsZero() || !w.until.IsZero() || w.aroundPrefix != ""
}

// parseFrameSelectors reads the temporal words off a frames line and
// refuses the combinations that would say one thing two ways.
func parseFrameSelectors(opts map[string]string, now time.Time) (frameSelectors, error) {
	var w frameSelectors
	lastSpan, err := w.readLast(opts, now)
	if err != nil {
		return w, err
	}
	if err := w.readEdges(opts, now, lastSpan); err != nil {
		return w, err
	}
	return w, w.readAround(opts)
}

// readLast reads last= as either of its faces, and says which it was.
func (w *frameSelectors) readLast(opts map[string]string, now time.Time) (span bool, err error) {
	v, ok := opts[optLast]
	if !ok {
		return false, nil
	}
	if n, err := strconv.Atoi(v); err == nil {
		if n < 1 {
			return false, fmt.Errorf("%s= wants a count of one or more", optLast)
		}
		w.count = n
		return false, nil
	}
	if d, err := time.ParseDuration(v); err == nil && d > 0 {
		w.since = now.Add(-d)
		return true, nil
	}
	return false, fmt.Errorf("%s= wants a count (50) or a span (15m)", optLast)
}

// readEdges reads the absolute edges, refusing them beside a last=
// span: the span reaches back from now, and a moment beside it is a
// second way of saying where the window sits.
func (w *frameSelectors) readEdges(opts map[string]string, now time.Time, lastSpan bool) error {
	for _, sel := range []struct {
		name string
		into *time.Time
	}{{optSince, &w.since}, {optUntil, &w.until}} {
		v, ok := opts[sel.name]
		if !ok {
			continue
		}
		if lastSpan {
			return fmt.Errorf("a %s= span reaches back from now — with %s=, say the window whole",
				optLast, sel.name)
		}
		at, err := parseMoment(v, now)
		if err != nil {
			return err
		}
		*sel.into = at
	}
	if !w.since.IsZero() && !w.until.IsZero() && w.until.Before(w.since) {
		return fmt.Errorf("%s= is before %s= — the window is inside out", optUntil, optSince)
	}
	return nil
}

// readAround reads the anchored window, which names its own edges and
// so refuses every other selector.
func (w *frameSelectors) readAround(opts map[string]string) error {
	w.aroundPrefix = opts[optAround]
	if v, ok := opts[optSpan]; ok {
		if w.aroundPrefix == "" {
			return fmt.Errorf("%s= says how far around — it needs %s=", optSpan, optAround)
		}
		d, err := time.ParseDuration(v)
		if err != nil || d <= 0 {
			return fmt.Errorf("%s= wants a duration, like 30s", optSpan)
		}
		w.span = d
	}
	if w.aroundPrefix != "" && (!w.since.IsZero() || !w.until.IsZero()) {
		return fmt.Errorf("%s= names its own window — drop the other selectors", optAround)
	}
	return nil
}
