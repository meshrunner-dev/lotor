//go:build !lean

package cli

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
)

func TestReverseSearchMalformedUTF8CannotConsumeCancellation(t *testing.T) {
	ed := newEditor(iotest.OneByteReader(strings.NewReader("\x12sta\xe9\x03\r\r")), io.Discard)
	ed.hist.add("status")
	got, err := ed.readLine()
	if err != nil || got != "" || ed.pending != nil {
		t.Fatalf("Ctrl+C after invalid UTF-8 submitted=%q error=%v pending=%v", got, err, ed.pending)
	}
	if ed.search != nil || ed.byteLen() != 0 {
		t.Fatal("cancellation retained a query or draft")
	}
}

func TestReverseSearchUTF8QueryAndLateReport(t *testing.T) {
	for _, keys := range []string{"\x12sta\x1b[30;1Rtus\r", "\x12é\x1b[30;1R🦝\r"} {
		ed := newEditor(iotest.OneByteReader(strings.NewReader(keys)), io.Discard)
		ed.hist.add("status é🦝")
		got, err := ed.readLine()
		if err != nil || got != "status é🦝" {
			t.Fatalf("query %q submitted=%q error=%v", keys, got, err)
		}
	}
}

func TestReverseSearchUsesTheCommandByteBudget(t *testing.T) {
	for _, text := range []string{strings.Repeat("a", maxLineBytes) + "b", strings.Repeat("🦝", maxLineBytes/4) + "a"} {
		ed := newEditor(strings.NewReader("\x12"+text), io.Discard)
		_, err := ed.readLine()
		if !errors.Is(err, errLineTooLong) || ed.search.query.byteLen() > maxLineBytes {
			t.Fatalf("query overflow: error=%v bytes=%d", err, ed.search.query.byteLen())
		}
	}
}

func TestEditorGraphemeBackspaceAndDeleteRepaint(t *testing.T) {
	for _, forward := range []bool{false, true} {
		var out bytes.Buffer
		ed := newEditor(strings.NewReader(""), &out)
		ed.width = 80
		ed.paint = func(s string) string { return s }
		ed.set("name=Cafe\u0301")
		out.Reset()
		if forward {
			ed.cur = ed.prevBoundary()
			_, _, _ = ed.applyInput(editorInput{kind: inputDelete})
		} else {
			ed.backspace()
		}
		if got := string(ed.buf); got != "name=Caf" || out.Len() == 0 || strings.Contains(out.String(), "\u0301") {
			t.Fatalf("grapheme deletion: line=%q output=%q", got, out.String())
		}
	}
}

func TestEditorCompletionAppliesByteSpanAndRetainsSuffix(t *testing.T) {
	ed := newEditor(strings.NewReader(""), io.Discard)
	ed.set("é=/ra suffix")
	ed.cur = 5
	ed.completeAt = func(line string, cursorByte int) completionEdit {
		if line != "é=/ra suffix" || cursorByte != 6 {
			t.Fatalf("completion context=%q cursor=%d", line, cursorByte)
		}
		return completionEdit{Start: 3, End: 6, Text: "/radio"}
	}
	ed.completeLine()
	checkEditBuffer(t, &ed.editBuffer, "é=/radio suffix", 8)
}

func TestEditorCompletionCannotExceedBudgetOrSplitUTF8(t *testing.T) {
	ed := newEditor(strings.NewReader(""), io.Discard)
	ed.set("é suffix")
	for _, edit := range []completionEdit{{Start: 1, End: 2, Text: "x"}, {Start: 0, End: 2, Text: strings.Repeat("x", maxLineBytes)}} {
		ed.completeAt = func(string, int) completionEdit { return edit }
		ed.completeLine()
		checkEditBuffer(t, &ed.editBuffer, "é suffix", 8)
	}
}
