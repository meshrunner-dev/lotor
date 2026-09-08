package cli

import (
	"strings"
	"testing"
)

func TestLayoutWideGlyphWrapLeavesTheCorrectCursorCell(t *testing.T) {
	text := strings.Repeat("a", 79) + "🦝XY"
	l := layoutText(text, len(text)-1, 80)
	if len(l.rows) != 2 || l.rows[1] != "🦝XY" || l.cursor != (screenPosition{row: 1, col: 3}) {
		t.Fatalf("wide wrap = %+v", l)
	}
	if l.end != (screenPosition{row: 1, col: 4}) {
		t.Fatalf("end = %+v", l.end)
	}
}

func TestLayoutWrapBoundaryAndGraphemeCursor(t *testing.T) {
	for _, tc := range []struct {
		text   string
		cursor int
		want   screenPosition
	}{
		{"abcd", 4, screenPosition{col: 4}},
		{"abcde", 4, screenPosition{row: 1}},
		{"e\u0301x", len("e\u0301"), screenPosition{col: 1}},
		{"👩‍💻x", len("👩‍💻"), screenPosition{col: 2}},
	} {
		l := layoutText(tc.text, tc.cursor, 4)
		if l.cursor != tc.want {
			t.Errorf("layout(%q,%d) cursor = %+v, want %+v", tc.text, tc.cursor, l.cursor, tc.want)
		}
	}
}

func TestLayoutViewportFollowsBothEndsOfALongDraft(t *testing.T) {
	text := strings.Repeat("x", 526)
	atEnd := layoutText(text, len(text), 40)
	top, bottom := atEnd.view(0, 8)
	if top != 6 || bottom != 14 {
		t.Fatalf("end viewport = %d:%d", top, bottom)
	}
	atStart := layoutText(text, 0, 40)
	top, bottom = atStart.view(top, 8)
	if top != 0 || bottom != 8 || atStart.cursor != (screenPosition{}) {
		t.Fatalf("home viewport = %d:%d, cursor %+v", top, bottom, atStart.cursor)
	}
}

func TestLayoutClippedRowsRetainTheirColour(t *testing.T) {
	l := layoutText("\x1b[36mabcdef\x1b[0m", 5, 3)
	if len(l.rows) != 2 || l.rows[1] != "\x1b[36mdef\x1b[0m" {
		t.Fatalf("colours across wrap: %#v", l.rows)
	}
	if l.cursor != (screenPosition{row: 1, col: 2}) || plainBytes("\x1b[36m🦝> \x1b[m") != len("🦝> ") {
		t.Fatalf("painted cursor = %+v", l.cursor)
	}
}

func TestEditorKeepsTallDraftCursorInTheViewport(t *testing.T) {
	var out strings.Builder
	ed := newEditor(strings.NewReader(""), &out)
	ed.width, ed.height = 40, 8
	ed.set("set identity=" + strings.Repeat("0123456789abcdef", 32))
	if ed.viewRows != 8 || ed.screenRow != 7 || ed.viewTop == 0 {
		t.Fatalf("end viewport: row=%d, top=%d, rows=%d", ed.screenRow, ed.viewTop, ed.viewRows)
	}
	out.Reset()
	ed.control(0x01)
	if ed.cur != 0 || ed.screenRow != 0 || ed.viewTop != 0 || ed.viewRows != 8 {
		t.Fatalf("home viewport: cursor=%d, row=%d, top=%d, rows=%d", ed.cur, ed.screenRow, ed.viewTop, ed.viewRows)
	}
	if strings.Count(out.String(), "\r\n") != 7 || !strings.Contains(out.String(), "> set identity=") {
		t.Fatalf("home repaint = %q", out.String())
	}
	want := string(ed.buf)
	ed.resize(20, 4)
	if ed.width != 20 || ed.height != 4 || ed.viewRows != 4 || ed.screenRow != 0 || string(ed.buf) != want {
		t.Fatalf("resize lost draft or cursor: %+v", ed)
	}
}

func TestEditorWideWrapUsesLayoutForMidLineCursor(t *testing.T) {
	var out strings.Builder
	ed := newEditor(strings.NewReader(""), &out)
	ed.width, ed.height = 80, 24
	ed.set(strings.Repeat("a", 77) + "🦝XY")
	out.Reset()
	ed.arrow('D')
	if ed.screenRow != 1 || !strings.HasSuffix(out.String(), "\r\x1b[3C") {
		t.Fatalf("cursor after wide wrap = row %d, output %q", ed.screenRow, out.String())
	}
}

func TestEditorSuspendedResizeKeepsTypeaheadWithoutDrawing(t *testing.T) {
	var out strings.Builder
	ed := newEditor(strings.NewReader(""), &out)
	ed.suspended = true
	ed.resize(80, 24)
	if err := ed.insertRune('q'); err != nil {
		t.Fatal(err)
	}
	ed.render()
	ed.dropBelow()
	if out.Len() != 0 || string(ed.buf) != "q" || ed.width != 80 || ed.height != 24 {
		t.Fatalf("suspended editor = %q, output %q", string(ed.buf), out.String())
	}
}

func TestEditorResizeAcceptsPartiallyKnownDimensions(t *testing.T) {
	var out strings.Builder
	ed := newEditor(strings.NewReader(""), &out)
	ed.resize(80, 0)
	ed.resize(40, 0)
	if ed.width != 40 || ed.height != 0 {
		t.Fatalf("width-only resize = %dx%d", ed.width, ed.height)
	}
	ed.resize(0, 10)
	if ed.width != 40 || ed.height != 10 {
		t.Fatalf("height-only resize = %dx%d", ed.width, ed.height)
	}
	out.Reset()
	ed.resize(0, 0)
	if out.Len() != 0 || ed.width != 40 || ed.height != 10 {
		t.Fatalf("unknown dimensions changed layout: %dx%d, output %q", ed.width, ed.height, out.String())
	}
}
