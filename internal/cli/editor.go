package cli

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/mattn/go-runewidth"
)

// editor is a character-mode line editor: the transport delivers raw
// keystrokes, the daemon echoes and edits. Up/down walk the session's
// history, left/right move the cursor, Ctrl+C abandons the line,
// Ctrl+D on an empty line ends the session. Lines it yields feed the
// same channel the plain reader feeds — the REPL cannot tell them
// apart.
type editor struct {
	in   *bufio.Reader
	out  io.Writer
	hist history

	buf  []rune
	cur  int
	walk int    // history position; -1 = editing a fresh line
	kept string // the fresh line kept aside while walking history
}

func newEditor(r io.Reader, w io.Writer) *editor {
	return &editor{in: bufio.NewReader(r), out: w, walk: -1}
}

var errLineTooLong = errors.New("line too long")

// readLine edits one line to completion, prompt included.
func (e *editor) readLine() (string, error) {
	e.buf, e.cur, e.walk = e.buf[:0], 0, -1
	fmt.Fprint(e.out, "> ")
	for {
		c, err := e.in.ReadByte()
		if err != nil {
			return "", err
		}
		switch {
		case c == '\r' || c == '\n':
			return e.finishLine(c)
		case c == 0x03: // Ctrl+C: abandon the line
			fmt.Fprint(e.out, "^C\r\n> ")
			e.buf, e.cur, e.walk = e.buf[:0], 0, -1
		case c == 0x04: // Ctrl+D: end of session on an empty line
			if len(e.buf) == 0 {
				fmt.Fprint(e.out, "\r\n")
				return "", io.EOF
			}
		case c == 0x7f || c == 0x08: // backspace
			if e.cur > 0 {
				e.buf = append(e.buf[:e.cur-1], e.buf[e.cur:]...)
				e.cur--
				e.render()
			}
		case c == 0x1b: // escape sequence
			if err := e.escape(); err != nil {
				return "", err
			}
		case c >= 0x20: // printable byte; multi-byte runes assemble
			if err := e.insertByte(c); err != nil {
				return "", err
			}
		}
	}
}

// finishLine closes the edit: telnet's cr-lf may arrive as \r\n or
// \r\x00, so a trailing partner is swallowed without blocking on a
// lone \r.
func (e *editor) finishLine(c byte) (string, error) {
	if c == '\r' {
		if next, err := e.in.Peek(1); err == nil && (next[0] == '\n' || next[0] == 0) {
			_, _ = e.in.Discard(1)
		}
	}
	fmt.Fprint(e.out, "\r\n")
	line := strings.TrimSpace(string(e.buf))
	e.hist.add(line)
	return line, nil
}

// escape handles CSI arrows; anything else is swallowed unmoved.
func (e *editor) escape() error {
	c, err := e.in.ReadByte()
	if err != nil {
		return err
	}
	if c != '[' {
		return nil
	}
	c, err = e.in.ReadByte()
	if err != nil {
		return err
	}
	switch c {
	case 'A': // up: older
		if line, ok := e.hist.at(e.walk + 1); ok {
			if e.walk == -1 {
				e.kept = string(e.buf)
			}
			e.walk++
			e.set(line)
		}
	case 'B': // down: newer, then the kept fresh line
		switch {
		case e.walk > 0:
			e.walk--
			if line, ok := e.hist.at(e.walk); ok {
				e.set(line)
			}
		case e.walk == 0:
			e.walk = -1
			e.set(e.kept)
		}
	case 'C': // right
		if e.cur < len(e.buf) {
			e.cur++
			e.render()
		}
	case 'D': // left
		if e.cur > 0 {
			e.cur--
			e.render()
		}
	default:
		// Longer sequences (delete, home…) end with a letter or '~';
		// drain digits and separators so they neither edit nor leak.
		for c >= '0' && c <= ';' {
			if c, err = e.in.ReadByte(); err != nil {
				return err
			}
		}
	}
	return nil
}

// insertByte assembles UTF-8 input byte by byte at the cursor.
func (e *editor) insertByte(c byte) error {
	raw := []byte{c}
	// Continuation bytes of a multi-byte rune follow immediately.
	for n := runeLen(c) - 1; n > 0; n-- {
		b, err := e.in.ReadByte()
		if err != nil {
			return err
		}
		raw = append(raw, b)
	}
	if len(e.buf) >= maxLineBytes {
		return errLineTooLong
	}
	r := []rune(string(raw))
	e.buf = append(e.buf[:e.cur], append(r, e.buf[e.cur:]...)...)
	e.cur += len(r)
	e.render()
	return nil
}

func runeLen(c byte) int {
	switch {
	case c < 0x80:
		return 1
	case c&0xE0 == 0xC0:
		return 2
	case c&0xF0 == 0xE0:
		return 3
	case c&0xF8 == 0xF0:
		return 4
	default:
		return 1
	}
}

// set replaces the whole line, cursor at the end.
func (e *editor) set(line string) {
	e.buf = []rune(line)
	e.cur = len(e.buf)
	e.render()
}

// render repaints the line and parks the cursor, width-aware — the
// mesh's emoji count for the cells they occupy here too.
func (e *editor) render() {
	line := string(e.buf)
	fmt.Fprintf(e.out, "\r\x1b[K> %s", line)
	if back := runewidth.StringWidth(line) - runewidth.StringWidth(string(e.buf[:e.cur])); back > 0 {
		fmt.Fprintf(e.out, "\x1b[%dD", back)
	}
}
