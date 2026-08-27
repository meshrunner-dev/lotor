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
	got := edit(t, "frames --last 5\x17\x1750\r")
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
