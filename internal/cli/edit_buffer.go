package cli

import (
	"slices"
	"unicode/utf8"

	"github.com/clipperhouse/uax29/v2/graphemes"
)

// editBuffer owns both the command draft and the reverse-search query's
// text policy. Positions are runes while the limit and completion spans
// are UTF-8 bytes; mutations update both representations together.
type editBuffer struct {
	buf   []rune
	cur   int
	bytes int
}

func (b *editBuffer) reset() {
	b.buf, b.cur, b.bytes = b.buf[:0], 0, 0
}

func (b *editBuffer) byteLen() int { return b.bytes }

func (b *editBuffer) cursorByte() int { return runeBytes(b.buf[:b.cur]) }

func runeBytes(text []rune) int {
	n := 0
	for _, r := range text {
		n += utf8.RuneLen(r)
	}
	return n
}

// replace applies a rune span atomically: an oversized replacement leaves
// the original draft and cursor untouched. The cursor follows the inserted
// text, including when that text is empty (deletion).
func (b *editBuffer) replace(start, end int, text []rune) bool {
	size := b.bytes - runeBytes(b.buf[start:end]) + runeBytes(text)
	if size > maxLineBytes {
		return false
	}
	b.buf = slices.Replace(b.buf, start, end, text...)
	b.cur, b.bytes = start+len(text), size
	b.snapForward()
	return true
}

func (b *editBuffer) insert(r rune) bool {
	return b.replace(b.cur, b.cur, []rune{r})
}

func (b *editBuffer) remove(start, end int) {
	// Deletion only releases capacity from the byte budget.
	_ = b.replace(start, end, nil)
}

func (b *editBuffer) setText(text string) bool {
	if len(text) > maxLineBytes {
		return false
	}
	return b.replace(0, len(b.buf), []rune(text))
}

// Cursor motions and character deletions respect displayed graphemes:
// combining marks, emoji modifiers and joiners belong to their base.
func (b *editBuffer) prevBoundary() int {
	at := 0
	iter := graphemes.FromString(string(b.buf))
	for iter.Next() {
		end := at + utf8.RuneCountInString(iter.Value())
		if end >= b.cur {
			return at
		}
		at = end
	}
	return at
}

func (b *editBuffer) nextBoundary() int {
	at := 0
	iter := graphemes.FromString(string(b.buf))
	for iter.Next() {
		at += utf8.RuneCountInString(iter.Value())
		if at > b.cur {
			return at
		}
	}
	return at
}

// Insertion can join a following mark or regional indicator into a new
// grapheme. Keep the insertion cursor outside that resulting cluster.
func (b *editBuffer) snapForward() {
	if b.cur == 0 || b.cur == len(b.buf) {
		return
	}
	at := 0
	iter := graphemes.FromString(string(b.buf))
	for iter.Next() {
		at += utf8.RuneCountInString(iter.Value())
		if at >= b.cur {
			b.cur = at
			return
		}
	}
}

// replaceBytes is the completion boundary. Reject offsets inside a rune
// rather than splitting a character or silently applying a different span.
func (b *editBuffer) replaceBytes(start, end int, text string) bool {
	if start < 0 || end < start || end > b.bytes ||
		b.bytes-(end-start)+len(text) > maxLineBytes || !utf8.ValidString(text) {
		return false
	}
	left, right, offset := -1, -1, 0
	for i := 0; i <= len(b.buf); i++ {
		if offset == start {
			left = i
		}
		if offset == end {
			right = i
			break
		}
		if i < len(b.buf) {
			offset += utf8.RuneLen(b.buf[i])
		}
	}
	if left < 0 || right < 0 {
		return false
	}
	return b.replace(left, right, []rune(text))
}
