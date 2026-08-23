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
	got := edit(t, "garbage\x03status\r")
	if len(got) != 1 || got[0] != "status" {
		t.Fatalf("lines = %q", got)
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
	if !strings.Contains(out.String(), "> ok") {
		t.Errorf("echo incomplete: %q", out.String())
	}
}
