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
}

const columnGap = 2

func (t *table) row(cells ...string) {
	t.rows = append(t.rows, cells)
}

func (t *table) flush(w io.Writer) error {
	var widths []int
	for _, r := range t.rows {
		for i, c := range r {
			if i >= len(widths) {
				widths = append(widths, 0)
			}
			if cw := runewidth.StringWidth(c); cw > widths[i] {
				widths[i] = cw
			}
		}
	}
	for _, r := range t.rows {
		var b strings.Builder
		for i, c := range r {
			b.WriteString(c)
			if i < len(r)-1 {
				b.WriteString(strings.Repeat(" ", widths[i]-runewidth.StringWidth(c)+columnGap))
			}
		}
		if _, err := fmt.Fprintf(w, "%s\r\n", strings.TrimRight(b.String(), " ")); err != nil {
			return err
		}
	}
	t.rows = nil
	return nil
}

// printable neutralises what the mesh may try to type into the
// operator's terminal: control and formatting runes — escape
// sequences, bidi overrides — become '?', graphic runes pass.
func printable(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsGraphic(r) {
			return r
		}
		return '?'
	}, s)
}
