package cli

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func checkEditBuffer(t *testing.T, b *editBuffer, want string, cursor int) {
	t.Helper()
	if got := string(b.buf); got != want || b.cur != cursor || b.byteLen() != len(want) {
		t.Fatalf("buffer=%q cursor=%d bytes=%d; want %q cursor=%d bytes=%d", got, b.cur, b.byteLen(), want, cursor, len(want))
	}
}

func TestEditBufferTracksBytesAcrossEdits(t *testing.T) {
	var b editBuffer
	if !b.setText("é🦝xy") {
		t.Fatal("initial text refused")
	}
	b.cur = 1
	if b.cursorByte() != 2 || !b.insert('\u4e2d') {
		t.Fatal("unicode insertion refused")
	}
	checkEditBuffer(t, &b, "é\u4e2d🦝xy", 2)
	b.remove(1, 3)
	checkEditBuffer(t, &b, "éxy", 1)
	if !b.replaceBytes(2, 3, "🦝") {
		t.Fatal("replacement refused")
	}
	checkEditBuffer(t, &b, "é🦝y", 2)
	b.reset()
	checkEditBuffer(t, &b, "", 0)
}

func TestEditBufferLimitRejectsAtomicallyAndDeletionReleasesBudget(t *testing.T) {
	var b editBuffer
	full := strings.Repeat("🦝", maxLineBytes/4)
	if !b.setText(full) || b.insert('x') || b.setText(full+"x") || b.replaceBytes(0, 0, "x") {
		t.Fatal("byte limit not respected")
	}
	checkEditBuffer(t, &b, full, utf8.RuneCountInString(full))
	b.remove(0, 1)
	if !b.insert('\u4e2d') || !b.insert('x') || b.insert('y') {
		t.Fatal("removed bytes were not made available exactly")
	}
	checkEditBuffer(t, &b, "\u4e2dx"+strings.TrimPrefix(full, "🦝"), 2)
}

func TestEditBufferRejectsInvalidCompletionSpans(t *testing.T) {
	var b editBuffer
	if !b.setText("é🦝x") {
		t.Fatal("initial text refused")
	}
	for _, span := range [][2]int{{-1, 0}, {2, 1}, {0, 8}, {1, 2}, {2, 3}, {3, 3}} {
		if b.replaceBytes(span[0], span[1], "a") {
			t.Fatalf("accepted invalid byte span %v", span)
		}
		checkEditBuffer(t, &b, "é🦝x", 3)
	}
	if b.replaceBytes(0, 2, "\xff") {
		t.Fatal("accepted invalid UTF-8 replacement")
	}
	checkEditBuffer(t, &b, "é🦝x", 3)
}

func TestEditBufferMotionsKeepGraphemesWhole(t *testing.T) {
	for _, cluster := range []string{"e\u0301", "❤️", "👨‍👩‍👦", "👍🏽", "🇫🇷"} {
		t.Run(cluster, func(t *testing.T) {
			var b editBuffer
			if !b.setText("a" + cluster + "z") {
				t.Fatal("initial text refused")
			}
			b.cur = 1
			b.cur = b.nextBoundary()
			if b.cur != 1+utf8.RuneCountInString(cluster) || b.prevBoundary() != 1 {
				t.Fatalf("motion splits %q: cursor=%d previous=%d", cluster, b.cur, b.prevBoundary())
			}
			b.remove(b.prevBoundary(), b.cur)
			checkEditBuffer(t, &b, "az", 1)
		})
	}
}

func TestEditBufferInsertionCannotStrandCursorInsideNewGrapheme(t *testing.T) {
	var b editBuffer
	if !b.setText("\u0301x") {
		t.Fatal("initial text refused")
	}
	b.cur = 0
	if !b.insert('e') {
		t.Fatal("insertion refused")
	}
	checkEditBuffer(t, &b, "e\u0301x", 2)
}
