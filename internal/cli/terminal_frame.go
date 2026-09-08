package cli

import (
	"fmt"
	"io"
	"strings"
)

type frameContent struct{ body, footer string }

// terminalFrame keeps the last complete view so a resize can lay it out
// again without running the command. Its cursor rests at column zero of
// the last physical row; all displayed rows fit in the known viewport.
type terminalFrame struct {
	content    frameContent
	rows       int
	footerRows int
}

func (f *terminalFrame) render(out io.Writer, width, height int, reanchor bool) error {
	rows := frameRows(f.content.body, width)
	footer := frameRows(f.content.footer, width)
	if height > 0 {
		footer = footer[:min(len(footer), height)]
		rows = rows[:min(len(rows), height-len(footer))]
	}
	rows = append(rows, footer...)
	if len(rows) == 0 {
		rows = []string{""}
	}
	var b strings.Builder
	if reanchor {
		// Terminal reflow invalidates the old origin. Start afresh using
		// the new dimensions rather than climbing the obsolete height.
		b.WriteString("\x1b[H\x1b[2J")
	} else if f.rows > 1 {
		fmt.Fprintf(&b, "\x1b[%dA", f.rows-1)
	}
	b.WriteString("\r\x1b[J")
	for i, row := range rows {
		if i > 0 {
			b.WriteString("\r\n")
		}
		b.WriteString(row)
		b.WriteString(cReset)
	}
	b.WriteByte('\r')
	_, err := io.WriteString(out, b.String())
	if err == nil {
		f.rows, f.footerRows = len(rows), len(footer)
	}
	return err
}

// end removes every physical footer row, retaining the last visible body.
// The next command or prompt starts where that footer began.
func (f *terminalFrame) end(out io.Writer) error {
	var b strings.Builder
	if f.footerRows > 1 {
		fmt.Fprintf(&b, "\x1b[%dA", f.footerRows-1)
	}
	if f.footerRows == 0 && f.rows > 0 {
		b.WriteString("\r\n")
	}
	b.WriteString("\r\x1b[J")
	_, err := io.WriteString(out, b.String())
	return err
}

func frameRows(text string, width int) []string {
	if text == "" {
		return nil
	}
	// A trailing line ending terminates the last row, rather than adding
	// a blank body row between the view and its separately drawn footer.
	text = strings.TrimSuffix(strings.TrimSuffix(text, "\n"), "\r")
	return layoutText(text, 0, width).rows
}
