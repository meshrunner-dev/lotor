package cli

import (
	"strings"

	"github.com/clipperhouse/uax29/v2/graphemes"
	"github.com/mattn/go-runewidth"
)

// screenPosition is a terminal cell, not an index in the text. A column
// equal to the width denotes the terminal's deferred wrap at a row's end.
type screenPosition struct{ row, col int }

// editLayout keeps the logical rows independent of the viewport. Each row
// carries its own SGR state so clipping its predecessors cannot lose colour.
type editLayout struct {
	rows   []string
	cursor screenPosition
	end    screenPosition
}

// layoutText lays out painted text with a cursor counted in unpainted UTF-8
// bytes. Graphemes that do not fit move whole to the next row; adding widths
// and dividing would incorrectly count the unused cell before a wide glyph.
func layoutText(painted string, cursor, width int) editLayout {
	b := layoutBuilder{width: width, target: cursor}
	it := graphemes.FromString(painted)
	it.AnsiEscapeSequences = true
	for it.Next() {
		b.add(it.Value())
	}
	if !b.found {
		b.layout.cursor = b.position
	}
	b.layout.end = b.position
	b.flush()
	return b.layout
}

type layoutBuilder struct {
	layout   editLayout
	line     strings.Builder
	style    string
	position screenPosition
	width    int
	offset   int
	target   int
	found    bool
}

func (b *layoutBuilder) add(cluster string) {
	if strings.HasPrefix(cluster, "\x1b[") {
		b.line.WriteString(cluster)
		if cluster == "\x1b[m" || cluster == "\x1b[0m" {
			b.style = ""
		} else {
			b.style += cluster
		}
		return
	}
	if cluster == "\n" || cluster == "\r\n" {
		if !b.found && b.target < b.offset+len(cluster) {
			b.layout.cursor, b.found = b.position, true
		}
		b.offset += len(cluster)
		b.flush()
		b.position = screenPosition{row: b.position.row + 1}
		b.line.WriteString(b.style)
		return
	}
	cells := runewidth.StringWidth(cluster)
	// A one-column terminal cannot display a two-cell glyph. Keep the
	// original text in the buffer and show one replacement cell instead.
	shown := cluster
	if b.width > 0 && cells > b.width {
		shown, cells = "?", 1
	}
	if b.width > 0 && cells > 0 && b.position.col+cells > b.width {
		b.flush()
		b.position = screenPosition{row: b.position.row + 1}
		b.line.WriteString(b.style)
	}
	if !b.found && b.target < b.offset+len(cluster) {
		b.layout.cursor, b.found = b.position, true
	}
	b.line.WriteString(shown)
	b.position.col += cells
	b.offset += len(cluster)
}

func (b *layoutBuilder) flush() {
	b.layout.rows = append(b.layout.rows, b.line.String())
	b.line.Reset()
}

// view chooses the consecutive rows containing the cursor. Preserve the
// current window where possible; scrolling through a draft should not jump
// its first visible row with every key. Unknown height keeps all rows.
func (l editLayout) view(top, height int) (first, last int) {
	if height <= 0 || len(l.rows) <= height {
		return 0, len(l.rows)
	}
	top = max(0, min(top, len(l.rows)-height))
	if l.cursor.row < top {
		top = l.cursor.row
	} else if l.cursor.row >= top+height {
		top = l.cursor.row - height + 1
	}
	return top, min(len(l.rows), top+height)
}

// plainBytes is the width-independent cursor offset through a painted
// prompt. Both prompt metrics and line layout use the same ANSI iterator.
func plainBytes(painted string) int {
	n := 0
	it := graphemes.FromString(painted)
	it.AnsiEscapeSequences = true
	for it.Next() {
		if !strings.HasPrefix(it.Value(), "\x1b") {
			n += len(it.Value())
		}
	}
	return n
}

// visCells counts painted text with the same grapheme iterator as layout.
func visCells(painted string) int {
	l := layoutText(painted, plainBytes(painted), 0)
	return l.end.col
}
