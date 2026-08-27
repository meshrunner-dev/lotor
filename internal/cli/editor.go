package cli

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"

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
	// crSeen pairs cr-lf statefully: a \r ends the line at once (a
	// raw terminal sends nothing after it — waiting would cost the
	// operator a second Enter), and the \n or \x00 a telnet client
	// appends is swallowed when it arrives, even on the next line.
	crSeen bool
	// pending holds the read error once the stream ends, so a final
	// line without a newline is still delivered before it.
	pending error

	// search, when non-nil, is a reverse history search in progress:
	// keystrokes build the query instead of the line.
	search *searchState

	// The session's hooks, all optional. They run on the transport's
	// goroutine — the session guards its own state against the REPL's.
	prompt   func(search string) string                     // what to repaint before the line
	complete func(line string) (add string, hints []string) // TAB
	helpFor  func(line string) string                       // the '?' key
	paint    func(line string) string                       // colours, same width
}

func newEditor(r io.Reader, w io.Writer) *editor {
	return &editor{in: bufio.NewReader(r), out: w, walk: -1}
}

var errLineTooLong = errors.New("line too long")

// readLine edits one line to completion. The REPL owns the prompt —
// printing it here would race the previous command's output — so the
// editor stays silent until the first keystroke; its repaints redraw
// the prompt whenever the line itself needs redrawing.
func (e *editor) readLine() (string, error) {
	if e.pending != nil {
		return "", e.pending
	}
	e.buf, e.cur, e.walk = e.buf[:0], 0, -1
	for {
		c, err := e.in.ReadByte()
		if err != nil {
			// A final line without a newline still counts: deliver it
			// now, the error on the next call.
			if len(e.buf) > 0 {
				e.pending = err
				return e.finishLine()
			}
			return "", err
		}
		if done, line, err := e.key(c); done {
			return line, err
		}
	}
}

// key applies one keystroke; done reports that the edit is over —
// a finished line, a failure, or the end of the session.
func (e *editor) key(c byte) (done bool, line string, err error) {
	if e.crSeen {
		e.crSeen = false
		if c == '\n' || c == 0 {
			return false, "", nil // the \r's partner, already handled
		}
	}
	if e.search != nil {
		return e.searchKey(c)
	}
	switch {
	case c == '\r' || c == '\n':
		e.crSeen = c == '\r'
		line, err = e.finishLine()
		return true, line, err
	case c == 0x04: // Ctrl+D: end of session on an empty line
		if len(e.buf) == 0 {
			fmt.Fprint(e.out, "\r\n")
			return true, "", io.EOF
		}
	case c == 0x1b: // escape sequence
		if err := e.escape(); err != nil {
			return true, "", err
		}
	case c < 0x20 || c == 0x7f: // control keys
		if e.control(c) {
			line, err = e.finishLine()
			return true, line, err
		}
	case c == '?' && e.helpFor != nil && e.cur == len(e.buf) && !insideQuote(e.buf):
		// '?' asks the same question F1 does, and is the one a hand
		// finds without knowing the console. Inside quotes it is text —
		// a password may carry one.
		e.help()
	default: // printable byte; multi-byte runes assemble
		if err := e.insertByte(c); err != nil {
			return true, "", err
		}
	}
	return false, "", nil
}

// control handles the shell's editing keys. It reports whether the
// key finished the line — Ctrl+C abandons the draft and hands the
// REPL an empty line, so the prompt stays the REPL's to print (and an
// empty line is exactly what stops a watch).
func (e *editor) control(c byte) (finished bool) {
	switch c {
	case 0x03: // Ctrl+C: abandon the line
		fmt.Fprint(e.out, "^C")
		e.buf, e.cur, e.walk = e.buf[:0], 0, -1
		return true
	case 0x7f, 0x08: // backspace
		e.backspace()
	case 0x17: // Ctrl+W: kill the word before the cursor
		e.killWord()
	case 0x15: // Ctrl+U: kill to the start of the line
		if e.cur > 0 {
			e.buf = append(e.buf[:0], e.buf[e.cur:]...)
			e.cur = 0
			e.render()
		}
	case 0x01: // Ctrl+A: start of line
		e.cur = 0
		e.render()
	case 0x05: // Ctrl+E: end of line
		e.cur = len(e.buf)
		e.render()
	case 0x09: // TAB: completion, at the end of the line only
		e.completeLine()
	case 0x12: // Ctrl+R: reverse history search
		e.search = &searchState{}
		e.findBack(0)
		e.renderSearch()
	}
	return false
}

// searchState is one reverse search: the query so far and where in
// the history the current match sits.
type searchState struct {
	query []rune
	at    int  // history index of the match
	found bool // whether anything matches the query
}

// searchKey applies one keystroke to the search. Enter takes the
// match and finishes the line — the shell family's behaviour, where a
// found command runs. Escape keeps it for editing instead, Ctrl+C or
// Ctrl+G abandons, another Ctrl+R walks to an older match, and any
// other control key leaves the search and applies normally.
func (e *editor) searchKey(c byte) (done bool, line string, err error) {
	switch {
	case c == '\r' || c == '\n':
		e.crSeen = c == '\r'
		e.acceptSearch()
		line, err = e.finishLine()
		return true, line, err
	case c == 0x12: // older match
		if e.search.found {
			e.findBack(e.search.at + 1)
		}
	case c == 0x03 || c == 0x07: // Ctrl+C, Ctrl+G: abandon
		e.search = nil
		e.buf, e.cur = e.buf[:0], 0
		e.render()
		return false, "", nil
	case c == 0x7f || c == 0x08: // backspace: shrink the query
		if n := len(e.search.query); n > 0 {
			e.search.query = e.search.query[:n-1]
		}
		e.findBack(0)
	case c == 0x1b: // escape: keep the match, back to editing
		e.acceptSearch()
		if err := e.escape(); err != nil {
			return true, "", err
		}
		e.render()
		return false, "", nil
	case c < 0x20:
		// Any other control key ends the search and applies to the
		// accepted line — Ctrl+A lands at its start, and so on.
		e.acceptSearch()
		e.render()
		return e.key(c)
	default:
		raw := []byte{c}
		for n := runeLen(c) - 1; n > 0; n-- {
			b, err := e.in.ReadByte()
			if err != nil {
				break
			}
			raw = append(raw, b)
		}
		r, _ := utf8.DecodeRune(raw)
		e.search.query = append(e.search.query, r)
		e.findBack(0)
	}
	e.renderSearch()
	return false, "", nil
}

// findBack looks for the newest history line at or after index from
// that contains the query, and puts it in the buffer.
func (e *editor) findBack(from int) {
	q := string(e.search.query)
	for i := from; ; i++ {
		line, ok := e.hist.at(i)
		if !ok {
			e.search.found = false
			return
		}
		if strings.Contains(line, q) {
			e.search.at, e.search.found = i, true
			e.buf = []rune(line)
			e.cur = len(e.buf)
			return
		}
	}
}

// acceptSearch leaves search mode, keeping whatever the buffer holds.
func (e *editor) acceptSearch() { e.search = nil }

// renderSearch draws the search through the one painter: the prompt
// carries the query, the draft below stays a command line with its own
// colours. A query that matches nothing is marked in the prompt rather
// than by silently keeping the last find.
func (e *editor) renderSearch() {
	if e.search == nil {
		return
	}
	e.render()
}

// completeLine asks the session what the last word could become. One
// candidate finishes the word; several print below the line and the
// draft comes back with whatever prefix they share.
func (e *editor) completeLine() {
	if e.complete == nil || e.cur != len(e.buf) {
		return
	}
	add, hints := e.complete(string(e.buf))
	if add != "" && e.byteLen()+len(add) <= maxLineBytes {
		e.buf = append(e.buf, []rune(add)...)
		e.cur = len(e.buf)
	}
	if len(hints) > 0 {
		fmt.Fprint(e.out, "\r\n"+strings.Join(hints, "  ")+"\r\n")
	}
	e.render()
}

// insideQuote reports an unclosed double quote before the cursor.
func insideQuote(buf []rune) bool {
	open := false
	for _, r := range buf {
		if r == '"' {
			open = !open
		}
	}
	return open
}

// backspace removes the rune before the cursor; at the end of the
// line it erases in place, no repaint for the common case.
func (e *editor) backspace() {
	if e.cur == 0 {
		return
	}
	atEnd := e.cur == len(e.buf)
	erased := runewidth.RuneWidth(e.buf[e.cur-1])
	e.buf = append(e.buf[:e.cur-1], e.buf[e.cur:]...)
	e.cur--
	if atEnd {
		fmt.Fprint(e.out, strings.Repeat("\b \b", erased))
	} else {
		e.render()
	}
}

// killWord removes the word before the cursor, shell-style: trailing
// spaces first, then the word itself.
func (e *editor) killWord() {
	if e.cur == 0 {
		return
	}
	i := e.cur
	for i > 0 && e.buf[i-1] == ' ' {
		i--
	}
	for i > 0 && e.buf[i-1] != ' ' {
		i--
	}
	e.buf = append(e.buf[:i], e.buf[e.cur:]...)
	e.cur = i
	e.render()
}

// finishLine closes the edit.
func (e *editor) finishLine() (string, error) {
	fmt.Fprint(e.out, "\r\n")
	line := strings.TrimSpace(string(e.buf))
	e.hist.add(line)
	return line, nil
}

// escape handles the arrow keys — CSI (ESC [ A) and the application
// mode's SS3 (ESC O A) — and swallows every other sequence whole.
func (e *editor) escape() error {
	// A lone ESC keypress arrives alone; a terminal's sequence arrives
	// as one burst. Nothing buffered behind the ESC means there is no
	// sequence to read — consuming the NEXT keystroke would eat it.
	if e.in.Buffered() == 0 {
		return nil
	}
	intro, err := e.in.ReadByte()
	if err != nil {
		return err
	}
	if intro != '[' && intro != 'O' {
		return nil
	}
	c, err := e.in.ReadByte()
	if err != nil {
		return err
	}
	// CSI parameter (0x30-0x3F) and intermediate (0x20-0x2F) bytes run
	// until a final byte; drain them so mouse reports and private-mode
	// answers neither edit nor leak into the line — but keep the
	// parameters, since a function key is told apart by them.
	var params []byte
	if intro == '[' {
		for c >= 0x20 && c <= 0x3F {
			params = append(params, c)
			if c, err = e.in.ReadByte(); err != nil {
				return err
			}
		}
	}
	// F1 the way every console family sends it: SS3 P, and the CSI
	// form older terminals use.
	if (intro == 'O' && c == 'P') || (intro == '[' && c == '~' && string(params) == "11") {
		e.help()
		return nil
	}
	e.arrow(c)
	return nil
}

// help answers the help key, whichever one was pressed: describe
// where the line stands, then hand the draft back untouched.
func (e *editor) help() {
	if e.helpFor == nil {
		return
	}
	e.search = nil
	fmt.Fprint(e.out, "\r\n"+e.helpFor(string(e.buf)))
	e.render()
}

// arrow applies a sequence's final byte when it names an arrow.
func (e *editor) arrow(c byte) {
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
	}
}

// insertByte assembles UTF-8 input at the cursor, strictly: only
// genuine continuation bytes join a lead byte, anything else is
// pushed back for its own turn — line noise or a latin-1 terminal
// yields U+FFFD instead of swallowing the Enter behind it. The echo
// therefore carries only whole, valid runes.
func (e *editor) insertByte(c byte) error {
	raw := []byte{c}
	for n := runeLen(c) - 1; n > 0; n-- {
		b, err := e.in.ReadByte()
		if err != nil {
			break // the partial rune decays to U+FFFD below
		}
		if b&0xC0 != 0x80 { // not a continuation: it is its own input
			_ = e.in.UnreadByte()
			break
		}
		raw = append(raw, b)
	}
	r, size := utf8.DecodeRune(raw)
	if size != len(raw) {
		r = utf8.RuneError
	}
	if e.byteLen()+utf8.RuneLen(r) > maxLineBytes {
		return errLineTooLong
	}
	atEnd := e.cur == len(e.buf)
	e.buf = append(e.buf[:e.cur], append([]rune{r}, e.buf[e.cur:]...)...)
	e.cur++
	if atEnd && e.paint == nil {
		// Appending at the end just echoes the keystroke: terminals
		// stay smooth and piped transcripts stay readable. A painted
		// line repaints instead — the keystroke may have changed what
		// class every token belongs to.
		fmt.Fprint(e.out, string(r))
	} else {
		e.render()
	}
	return nil
}

// byteLen is the line's UTF-8 size — the bound is in bytes, the same
// contract the plain reader enforces.
func (e *editor) byteLen() int {
	n := 0
	for _, r := range e.buf {
		n += utf8.RuneLen(r)
	}
	return n
}

// runeLen reads a UTF-8 lead byte's promise; invalid leads (stray
// continuations, 0xF5+) stand alone and decay to U+FFFD.
func runeLen(c byte) int {
	switch {
	case c < 0x80:
		return 1
	case c&0xE0 == 0xC0:
		return 2
	case c&0xF0 == 0xE0:
		return 3
	case c&0xF8 == 0xF0 && c <= 0xF4:
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
// mesh's emoji count for the cells they occupy here too. The cursor
// arithmetic runs on the plain text: colours change bytes, never
// cells.
func (e *editor) render() {
	prompt := "> "
	if e.prompt != nil {
		query := ""
		if e.search != nil {
			query = string(e.search.query)
		}
		prompt = e.prompt(query)
	}
	line := string(e.buf)
	shown := line
	if e.paint != nil {
		shown = e.paint(line)
	}
	fmt.Fprintf(e.out, "\r\x1b[K%s%s", prompt, shown)
	if back := runewidth.StringWidth(line) - runewidth.StringWidth(string(e.buf[:e.cur])); back > 0 {
		fmt.Fprintf(e.out, "\x1b[%dD", back)
	}
}
