//go:build !lean

package cli

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

const (
	keyUp    = "\x1b[A"
	keyDown  = "\x1b[B"
	keyLeft  = "\x1b[D"
	keyRight = "\x1b[C"
)

// edit runs the editor over scripted keystrokes and returns the lines
// it yielded.
func edit(t *testing.T, keys string) []string {
	t.Helper()
	ed := newEditor(strings.NewReader(keys), &bytes.Buffer{})
	var lines []string
	for {
		line, err := ed.readLine()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				t.Fatalf("editor error: %v", err)
			}
			return lines
		}
		lines = append(lines, line)
	}
}

func TestTypingAndEnter(t *testing.T) {
	got := edit(t, "status\r")
	if len(got) != 1 || got[0] != "status" {
		t.Fatalf("lines = %q", got)
	}
}

func TestHistoryRecallAndRerun(t *testing.T) {
	// Run two commands, then up-up recalls the older, enter reruns it.
	got := edit(t, "nodes\rstatus\r"+keyUp+keyUp+"\r")
	want := []string{"nodes", "status", "nodes"}
	if len(got) != 3 {
		t.Fatalf("lines = %q", got)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("line %d = %q, want %q", i, got[i], w)
		}
	}
}

func TestHistoryEditWithCursor(t *testing.T) {
	// Recall "frames", walk the cursor left over "s", insert nothing
	// there but append at the right edge after coming back.
	got := edit(t, "frames\r"+keyUp+keyLeft+keyLeft+"XY"+keyRight+keyRight+"Z\r")
	if len(got) != 2 || got[1] != "framXYesZ" {
		t.Fatalf("edited line = %q", got)
	}
}

func TestDownRestoresTheDraft(t *testing.T) {
	// Start typing, go up into history, come back down: the draft is
	// where it was left.
	got := edit(t, "nodes\r"+"stat"+keyUp+keyDown+"us\r")
	if len(got) != 2 || got[1] != "status" {
		t.Fatalf("draft = %q", got)
	}
}

func TestCtrlCAbandonsTheLine(t *testing.T) {
	// Ctrl+C hands the REPL an empty line — the draft dies, the REPL
	// keeps the prompt, and a watch would stop.
	got := edit(t, "garbage\x03status\r")
	if len(got) != 2 || got[0] != "" || got[1] != "status" {
		t.Fatalf("lines = %q", got)
	}
}

func TestLatin1NoiseCannotEatTheEnter(t *testing.T) {
	// 0xE9 promises two continuations; Enter is not one. The noise
	// decays to U+FFFD and the line still finishes on this Enter.
	got := edit(t, "ok\xe9\rquit\r")
	if len(got) != 2 || got[0] != "ok\uFFFD" || got[1] != "quit" {
		t.Fatalf("lines = %q", got)
	}
}

func TestLoneEscDoesNotEatTheNextKey(t *testing.T) {
	// An ESC with nothing buffered behind it is a keypress, not a
	// sequence: it must not block waiting for an intro byte, and the
	// line already typed still comes through.
	got := edit(t, "ok\x1b")
	if len(got) != 1 || got[0] != "ok" {
		t.Fatalf("lines = %q", got)
	}
}

func TestSS3ArrowsWalkHistory(t *testing.T) {
	// Application cursor mode sends ESC O A for up.
	got := edit(t, "nodes\r\x1bOA\r")
	if len(got) != 2 || got[1] != "nodes" {
		t.Fatalf("lines = %q", got)
	}
}

func TestMouseReportIsSwallowedWhole(t *testing.T) {
	// SGR mouse tracking left on by a crashed program sends
	// ESC [ < 0 ; 33 ; 21 M — none of it may leak into the line.
	got := edit(t, "\x1b[<0;33;21Mok\r")
	if len(got) != 1 || got[0] != "ok" {
		t.Fatalf("lines = %q", got)
	}
}

func TestFinalLineWithoutNewline(t *testing.T) {
	got := edit(t, "status")
	if len(got) != 1 || got[0] != "status" {
		t.Fatalf("lines = %q", got)
	}
}

func TestLineBoundIsInBytes(t *testing.T) {
	// 4-byte emoji: the byte bound trips near maxLineBytes/4 runes,
	// not at 4× that.
	flood := strings.Repeat("🦝", maxLineBytes/4+2) + "\r"
	ed := newEditor(strings.NewReader(flood), &bytes.Buffer{})
	_, err := ed.readLine()
	if !errors.Is(err, errLineTooLong) {
		t.Fatalf("emoji flood: %v", err)
	}
}

func TestBackspaceAndUTF8(t *testing.T) {
	// The raccoon is a 4-byte rune; backspace removes it whole.
	got := edit(t, "n🦝\x7fodes\r")
	if len(got) != 1 || got[0] != "nodes" {
		t.Fatalf("lines = %q", got)
	}
}

func TestHistoryDepthIsBounded(t *testing.T) {
	h := &history{}
	for i := range historyDepth + 10 {
		h.add(strings.Repeat("x", 1) + string(rune('a'+i%26)) + strings.Repeat("y", i%3))
	}
	if h.size() > historyDepth {
		t.Fatalf("history holds %d lines, cap is %d", h.size(), historyDepth)
	}
	if _, ok := h.at(historyDepth); ok {
		t.Error("history serves entries beyond its depth")
	}
}

func TestHistorySkipsRepeats(t *testing.T) {
	h := &history{}
	h.add("status")
	h.add("status")
	h.add("")
	if h.size() != 1 {
		t.Fatalf("history size = %d, want 1", h.size())
	}
}

func TestAppendingEchoesWithoutRepaint(t *testing.T) {
	var out bytes.Buffer
	ed := newEditor(strings.NewReader("ok\r"), &out)
	if _, err := ed.readLine(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "\x1b[K") {
		t.Errorf("plain typing repainted the line: %q", out.String())
	}
	if !strings.Contains(out.String(), "ok") {
		t.Errorf("echo incomplete: %q", out.String())
	}
	// The REPL owns the prompt; the editor must not race it.
	if strings.Contains(out.String(), "> ") {
		t.Errorf("editor printed a prompt of its own: %q", out.String())
	}
}

func TestEnterVariantsYieldOneLineEach(t *testing.T) {
	// A raw terminal sends \r alone; telnet clients send \r\n or
	// \r\x00 — one line per Enter in every dialect, and a lone \r
	// must not wait for a second keystroke.
	got := edit(t, "a\rb\r\nc\r\x00d\n")
	want := []string{"a", "b", "c", "d"}
	if len(got) != len(want) {
		t.Fatalf("lines = %q", got)
	}
	for i, w := range want {
		if got[i] != w {
			t.Errorf("line %d = %q, want %q", i, got[i], w)
		}
	}
}

func TestShellKillsAndMotions(t *testing.T) {
	// Ctrl+W eats the last word (and its separator), shell-style.
	got := edit(t, "frames last=5 json\x17\x1750\r")
	if len(got) != 1 || got[0] != "frames 50" {
		t.Fatalf("ctrl+w = %q", got)
	}
	// Ctrl+U wipes to the start; what follows is the whole line.
	got = edit(t, "garbage\x15status\r")
	if len(got) != 1 || got[0] != "status" {
		t.Fatalf("ctrl+u = %q", got)
	}
	// Ctrl+A jumps home (insert lands at the front), Ctrl+E back to
	// the end (append resumes there).
	got = edit(t, "odes\x01n\x05\r")
	if len(got) != 1 || got[0] != "nodes" {
		t.Fatalf("ctrl+a/e = %q", got)
	}
	// Ctrl+W mid-line only eats what precedes the cursor.
	got = edit(t, "aa bb\x1b[D\x1b[D\x17cc\r")
	if len(got) != 1 || got[0] != "ccbb" {
		t.Fatalf("ctrl+w mid-line = %q", got)
	}
	// Ctrl+K is Ctrl+U's mirror: it keeps what precedes the cursor.
	// Five lefts park it before " json"; the kill takes the rest.
	got = edit(t, "nodes json\x1b[D\x1b[D\x1b[D\x1b[D\x1b[D\x0b\r")
	if len(got) != 1 || got[0] != "nodes" {
		t.Fatalf("ctrl+k = %q", got)
	}
	// At the end of the line it has nothing to kill and must not eat
	// the line itself.
	got = edit(t, "nodes\x0b\r")
	if len(got) != 1 || got[0] != "nodes" {
		t.Fatalf("ctrl+k at end = %q", got)
	}
}

func TestCtrlLClearsTheScreenAndKeepsTheDraft(t *testing.T) {
	// The draft survives the clear — the screen is redrawn around it,
	// which is the whole point of clearing mid-command.
	got := edit(t, "stat\x0cus\r")
	if len(got) != 1 || got[0] != "status" {
		t.Fatalf("ctrl+l = %q", got)
	}
	// And it must actually clear: home, then erase the display.
	var out bytes.Buffer
	ed := newEditor(strings.NewReader("x\x0c\r"), &out)
	if _, err := ed.readLine(); err != nil {
		t.Fatalf("readLine: %v", err)
	}
	if !strings.Contains(out.String(), "\x1b[H\x1b[2J") {
		t.Errorf("no clear sequence in %q", out.String())
	}
}

func TestReverseSearchFindsAndRuns(t *testing.T) {
	// Two commands in history; Ctrl+R, a query, Enter runs the match.
	got := edit(t, "scopes\rstatus\r\x12sco\r")
	if len(got) != 3 || got[2] != "scopes" {
		t.Fatalf("lines = %q, want the search to yield scopes", got)
	}
}

func TestReverseSearchWalksOlder(t *testing.T) {
	// Two matches for "s": Ctrl+R twice reaches the older one.
	got := edit(t, "scopes\rstatus\r\x12s\x12\r")
	if len(got) != 3 || got[2] != "scopes" {
		t.Fatalf("lines = %q, want the second Ctrl+R to reach scopes", got)
	}
}

func TestReverseSearchArrowKeepsTheMatchForEditing(t *testing.T) {
	// An arrow leaves search mode with the match in the buffer —
	// cursor keys mean "I want to edit this one" — and typing then
	// appends to it.
	got := edit(t, "scopes\r\x12sco"+keyRight+"!\r")
	if len(got) != 2 || got[1] != "scopes!" {
		t.Fatalf("lines = %q, want scopes! after the arrow and an edit", got)
	}
}

func TestReverseSearchAbandons(t *testing.T) {
	// Ctrl+G abandons: the line comes back empty and Enter sends it.
	got := edit(t, "scopes\r\x12sco\x07quit\r")
	if len(got) != 2 || got[1] != "quit" {
		t.Fatalf("lines = %q, want the search abandoned", got)
	}
}

func TestReverseSearchBackspaceWidens(t *testing.T) {
	// A too-narrow query fails; backspace widens it back to a match.
	got := edit(t, "scopes\r\x12scoz\x7f\r")
	if len(got) != 2 || got[1] != "scopes" {
		t.Fatalf("lines = %q", got)
	}
}

func TestTheHelpKeyCyclesThroughItsLevels(t *testing.T) {
	// Pressed again it moves on rather than repeating itself, and any
	// other keystroke puts the cycle back to the start.
	var out bytes.Buffer
	ed := newEditor(strings.NewReader("\x1bOP\x1bOP\x1bOPx\x1bOP\r"), &out)
	var levels []int
	ed.helpFor = func(_ string, level int) string {
		levels = append(levels, level)
		return "L\r\n"
	}
	if _, err := ed.readLine(); err != nil {
		t.Fatal(err)
	}
	want := []int{0, 1, 2, 0}
	if len(levels) != len(want) {
		t.Fatalf("levels = %v", levels)
	}
	for i := range want {
		if levels[i] != want[i] {
			t.Fatalf("levels = %v, want %v", levels, want)
		}
	}
}

func TestF1AsksTheSameQuestionAsTheHelpKey(t *testing.T) {
	// Both keys reach the same answer: '?' is the one a hand finds,
	// F1 the one a console operator expects.
	for _, keys := range []string{"/relay\x1bOP\r", "/relay\x1b[11~\r", "/relay?\r"} {
		var out bytes.Buffer
		ed := newEditor(strings.NewReader(keys), &out)
		asked := ""
		ed.helpFor = func(line string, _ int) string { asked = line; return "HELP\r\n" }
		if _, err := ed.readLine(); err != nil {
			t.Fatalf("%q: %v", keys, err)
		}
		if asked != "/relay" {
			t.Errorf("%q asked for help on %q", keys, asked)
		}
		if !strings.Contains(out.String(), "HELP") {
			t.Errorf("%q printed no help", keys)
		}
	}
}

func TestSearchQueryRidesInThePrompt(t *testing.T) {
	var out bytes.Buffer
	ed := newEditor(strings.NewReader("scopes\r\x12sco\r"), &out)
	ed.prompt = func(search string) string {
		if search != "" {
			return "[me@host] [" + search + "]> "
		}
		return "[me@host] > "
	}
	for range 2 {
		if _, err := ed.readLine(); err != nil {
			t.Fatal(err)
		}
	}
	got := out.String()
	// The query is in the prompt, and the draft below is the command.
	if !strings.Contains(got, "[me@host] [sco]> scopes") {
		t.Errorf("the search did not ride in the prompt:\n%q", got)
	}
	if strings.Contains(got, "reverse-search") {
		t.Errorf("the search still writes a line of its own:\n%q", got)
	}
}

func TestAWrappedLineRepaintsFromItsFirstRow(t *testing.T) {
	// The paste that used to flood the screen: a line three rows deep,
	// then one more keystroke. The repaint must climb to the block's
	// first row and clear down — never repaint from the middle, which
	// scrolls the top rows away on every keystroke.
	var out strings.Builder
	ed := newEditor(strings.NewReader(""), &out)
	ed.width = 20
	ed.paint = func(s string) string { return s }
	ed.prompt = func(string) string { return "> " }
	ed.set(strings.Repeat("a", 40)) // 2 + 40 cells: three rows
	out.Reset()
	ed.set(strings.Repeat("a", 41))
	got := out.String()
	if !strings.HasPrefix(got, "\x1b[2A") {
		t.Errorf("the repaint did not climb to the first row: %q", got)
	}
	if !strings.Contains(got, "\x1b[J") {
		t.Errorf("the repaint did not clear the block: %q", got)
	}
	if strings.Contains(got, "\r\n") {
		t.Errorf("a repaint scrolled: %q", got)
	}
}

func TestEnterOnAWrappedLineDropsBelowTheBlock(t *testing.T) {
	// Enter with the cursor mid-block must first step under the whole
	// block, or the command's output prints into the wrapped tail.
	var out strings.Builder
	ed := newEditor(strings.NewReader(strings.Repeat("a", 40)+"\x01\r"), &out)
	ed.width = 20
	ed.paint = func(s string) string { return s }
	ed.prompt = func(string) string { return "> " }
	line, err := ed.readLine()
	if err != nil || line != strings.Repeat("a", 40) {
		t.Fatalf("readLine = %q, %v", line, err)
	}
	// Ctrl+A parked the cursor on row 0; the block ends on row 2.
	if !strings.Contains(out.String(), "\x1b[2B\r\n") {
		t.Errorf("Enter did not drop below the block:\n%q", out.String())
	}
}

func TestAnUnmeasuredTerminalKeepsTheOldRepaint(t *testing.T) {
	// Width zero is the wrap-blind editor as it always was: no climb,
	// no clear-down, the single-row erase.
	var out strings.Builder
	ed := newEditor(strings.NewReader(""), &out)
	ed.prompt = func(string) string { return "> " }
	ed.set("hello")
	if got := out.String(); !strings.Contains(got, "\r\x1b[J> hello") {
		t.Errorf("render = %q", got)
	}
}

func TestReportColumnsReadsTheAnswer(t *testing.T) {
	for _, c := range []struct {
		in   string
		want int
	}{
		{"\x1b[24;100R", 100},
		{"\x1b[1;9999R", 9999},
		{"\x1b[24R", 0},
		{"garbage", 0},
	} {
		if got := reportColumns([]byte(c.in)); got != c.want {
			t.Errorf("reportColumns(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

func TestVisCellsIgnoresTheEscapes(t *testing.T) {
	painted := "\x1b[36m/relay\x1b[m \x1b[35mprint\x1b[m"
	if got := visCells(painted); got != len("/relay print") {
		t.Errorf("visCells = %d, want %d", got, len("/relay print"))
	}
	if got := visCells("[admin@lab] > "); got != 14 {
		t.Errorf("plain prompt = %d", got)
	}
}
