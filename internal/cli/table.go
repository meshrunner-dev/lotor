package cli

import (
	"fmt"
	"io"
	"strings"
	"unicode"

	"github.com/mattn/go-runewidth"
)

// table aligns columns by display width: emoji and wide glyphs count
// for the terminal cells they occupy, which text/tabwriter's rune
// counting does not.
type table struct {
	rows [][]string
	// weight carries each row's mark and the cell it lands on, kept
	// apart from the cells themselves for the same reason the
	// header's is: escape bytes occupy no terminal cells, and the
	// width arithmetic must not count them.
	weight    []string
	weightCol []int
	// head is the column names, kept apart from the rows: the width
	// arithmetic runs on the plain text and the emphasis is added at
	// the last moment, because escape bytes count for cells they do
	// not occupy.
	head []string
	// bold says the transport renders emphasis; a pipe gets the same
	// names with nothing around them.
	bold bool
}

const columnGap = 2

func (t *table) row(cells ...string) {
	t.rowAs("", 0, cells...)
}

// rowAs adds a row where the cell at col carries a mark. The mark
// sits on the one cell it is about — the column that answers the
// question — rather than on the whole line, which would tint the
// values as well and say something about them that is not meant.
func (t *table) rowAs(mark string, col int, cells ...string) {
	t.rows = append(t.rows, cells)
	t.weight = append(t.weight, mark)
	t.weightCol = append(t.weightCol, col)
}

// header names the columns. They are shown as typed — upper case is
// the caller's choice — and are laid out with the rows so a name
// wider than its column still widens it.
func (t *table) header(cells ...string) {
	t.head = cells
}

func (t *table) flush(w io.Writer) error {
	rows := t.rows
	if t.head != nil {
		if len(rows) == 0 {
			// Column names over nothing describe an empty room rather
			// than showing one; the caller says what empty means.
			t.rows, t.head = nil, nil
			return nil
		}
		rows = append([][]string{t.head}, rows...)
	}
	widths := columnWidths(rows)
	for n, r := range rows {
		header := n == 0 && t.head != nil
		var b strings.Builder
		for i, c := range r {
			shown := c
			switch {
			case header && t.bold:
				// Cell by cell, so the gaps between columns stay bare.
				shown = emphasis + c + cReset
			case !header && t.bold:
				if mark := t.markFor(n, i); mark != "" {
					shown = mark + c + cReset
				}
			}
			b.WriteString(shown)
			if i < len(r)-1 {
				b.WriteString(strings.Repeat(" ", widths[i]-runewidth.StringWidth(c)+columnGap))
			}
		}
		line := strings.TrimRight(b.String(), " ")
		if _, err := fmt.Fprintf(w, "%s\r\n", line); err != nil {
			return err
		}
	}
	t.rows, t.head, t.weight, t.weightCol = nil, nil, nil, nil
	return nil
}

// columnWidths measures each column by the widest cell in it,
// counting terminal cells rather than runes.
func columnWidths(rows [][]string) []int {
	var widths []int
	for _, r := range rows {
		for i, c := range r {
			if i >= len(widths) {
				widths = append(widths, 0)
			}
			if cw := runewidth.StringWidth(c); cw > widths[i] {
				widths[i] = cw
			}
		}
	}
	return widths
}

// markFor returns the mark a cell carries, if it is the one the row's
// mark was meant for. n counts the rendered lines, the header
// included; the marks are indexed by row.
func (t *table) markFor(n, col int) string {
	if t.head != nil {
		n-- // the header is not one of the rows
	}
	if n < 0 || n >= len(t.weight) || t.weight[n] == "" || t.weightCol[n] != col {
		return ""
	}
	return t.weight[n]
}

// TerminalSafe neutralises what free text may try to type into the
// operator's terminal: control and formatting runes — escape
// sequences, bidi overrides — become '?', graphic runes pass. It is
// exported inside the daemon so replies assembled by the manager use
// the same policy as the CLI's own views.
func TerminalSafe(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsGraphic(r) {
			return r
		}
		return '?'
	}, s)
}

// printable is the local spelling used throughout the CLI views.
func printable(s string) string { return TerminalSafe(s) }

// quoted wraps a mesh-sourced string in double quotes so its start
// and end are unmistakable among keywords and column gaps — names
// carry spaces. Inner quotes are escaped: a name cannot close its own
// quotation. Neutralisation rides along.
func quoted(s string) string {
	return `"` + strings.ReplaceAll(printable(s), `"`, `\"`) + `"`
}

// unnamed is what a view shows for a node that has never said what it
// calls itself. It is not an empty pair of quotes: a name we do not
// have and a name that is empty should not read alike.
const unnamed = "—"

// meshName renders a name that came off the air. Every view that shows
// one goes through here — a name is attacker-chosen text arriving on a
// public band, and a view that formats it itself is a view that will
// eventually forget to neutralise it.
func meshName(s string) string {
	if s == "" {
		return unnamed
	}
	return quoted(s)
}
