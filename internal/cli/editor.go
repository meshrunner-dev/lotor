package cli

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"strings"
)

// editor is a character-mode line editor: the transport delivers raw
// keystrokes, the daemon echoes and edits. Up/down walk the session's
// history, left/right move the cursor, Ctrl+C abandons the line,
// Ctrl+D on an empty line ends the session. Lines it yields feed the
// same channel the plain reader feeds — the REPL cannot tell them
// apart.
type editor struct {
	editBuffer
	inputDecoder

	in   *bufio.Reader
	out  io.Writer
	hist history

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

	// The display owns these dimensions and the visible edit block. A tall
	// draft scrolls within its viewport, keeping the logical cursor visible.
	width, height int
	screenRow     int
	viewTop       int
	viewRows      int
	suspended     bool // a command owns the screen; keep typeahead in the buffer

	// The session's hooks, all optional. They run on the transport's
	// goroutine — the session guards its own state against the REPL's.
	prompt     func(search string) string                       // what to repaint before the line
	complete   func(line string) (add string, hints []string)   // legacy end-of-line hook
	completeAt func(line string, cursorByte int) completionEdit // TAB at the cursor
	helpFor    func(line string, level int) string              // the help key
	paint      func(line string) string                         // colours, same width

	// helpLevel is where the help cycle stands. Any other keystroke
	// puts it back to the start, so the cycle only ever runs while
	// the operator is pressing the key.
	helpLevel int
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
	e.reset()
	e.walk = -1
	for {
		c, err := e.in.ReadByte()
		if err != nil {
			if done, line, keyErr := e.applyInput(e.finish()); done {
				e.pending = err
				return line, keyErr
			}
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

// key is the byte-stream adapter used by readLine. Decoding happens before
// editing modes, and never reads ahead: the same events can be delivered
// by a session's input pump while its display is owned elsewhere.
func (e *editor) key(c byte) (done bool, line string, err error) {
	first, next := e.decode(c)
	if done, line, err := e.applyInput(first); done {
		return done, line, err
	}
	return e.applyInput(next)
}

func (e *editor) applyInput(input editorInput) (done bool, line string, err error) {
	if !e.acceptInput(input) {
		return false, "", nil
	}
	if e.search != nil {
		return e.searchInput(input)
	}
	switch input.kind {
	case inputRune:
		return e.runeInput(input.key)
	case inputControl:
		return e.controlInput(input.key)
	case inputArrow:
		e.arrow(byte(input.key))
	case inputDelete:
		if e.cur < len(e.buf) {
			e.remove(e.cur, e.nextBoundary())
			e.render()
		}
	case inputHelp:
		e.help()
	case inputEscape, inputNone:
	}
	return false, "", nil
}

// acceptInput pairs CR-LF independently of read boundaries, and preserves
// consecutive help presses without letting ignored metadata affect them.
func (e *editor) acceptInput(input editorInput) bool {
	if input.kind == inputNone {
		return false
	}
	if e.crSeen {
		e.crSeen = false
		if input.kind == inputControl && (input.key == '\n' || input.key == 0) {
			return false
		}
	}
	if input.kind != inputHelp && input.kind != inputEscape &&
		(input.kind != inputRune || input.key != '?') {
		e.helpLevel = 0
	}
	return true
}

func (e *editor) runeInput(key rune) (done bool, line string, err error) {
	if key == '?' && e.helpFor != nil && e.cur == len(e.buf) &&
		!insideQuoteAt(string(e.buf), e.cursorByte()) {
		e.help()
	} else if err := e.insertRune(key); err != nil {
		return true, "", err
	}
	return false, "", nil
}

func (e *editor) controlInput(key rune) (done bool, line string, err error) {
	switch key {
	case '\r', '\n':
		e.crSeen = key == '\r'
		line, err = e.finishLine()
		return true, line, err
	case 0x04:
		if len(e.buf) == 0 {
			fmt.Fprint(e.out, "\r\n")
			return true, "", io.EOF
		}
	default:
		if e.control(byte(key)) {
			line, err = e.finishLine()
			return true, line, err
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
		e.dropBelow()
		fmt.Fprint(e.out, "^C")
		e.reset()
		e.walk = -1
		return true
	case 0x7f, 0x08: // backspace
		e.backspace()
	case 0x17: // Ctrl+W: kill the word before the cursor
		e.killWord()
	case 0x15: // Ctrl+U: kill to the start of the line
		if e.cur > 0 {
			e.remove(0, e.cur)
			e.render()
		}
	case 0x0b: // Ctrl+K: kill to the end of the line
		if e.cur < len(e.buf) {
			e.remove(e.cur, len(e.buf))
			e.render()
		}
	case 0x0c: // Ctrl+L: clear the screen, keeping the line being edited
		// Home before erase: the cursor is somewhere down the drawing,
		// and screenRow counts rows that are about to stop existing.
		fmt.Fprint(e.out, "\x1b[H\x1b[2J")
		e.screenRow = 0
		e.render()
	case 0x01: // Ctrl+A: start of line
		e.cur = 0
		e.render()
	case 0x05: // Ctrl+E: end of line
		e.cur = len(e.buf)
		e.render()
	case 0x09: // TAB: completion at the cursor
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
	query editBuffer
	at    int  // history index of the match
	found bool // whether anything matches the query
}

// searchInput receives the same decoded keys as ordinary editing. Terminal
// reports never reach it; an actual Escape or navigation key accepts the
// match before normal editing resumes.
func (e *editor) searchInput(input editorInput) (done bool, line string, err error) {
	if input.kind == inputRune {
		if !e.search.query.insert(input.key) {
			return true, "", errLineTooLong
		}
		e.findBack(0)
		e.renderSearch()
		return false, "", nil
	}
	if input.kind == inputControl {
		switch input.key {
		case 0x12: // Ctrl+R: older match
			if e.search.found {
				e.findBack(e.search.at + 1)
			}
			e.renderSearch()
			return false, "", nil
		case 0x07: // Ctrl+G: clear the search, keep editing
			e.search = nil
			e.reset()
			e.render()
			return false, "", nil
		case 0x7f, 0x08:
			query := &e.search.query
			if n := len(query.buf); n > 0 {
				query.remove(query.prevBoundary(), n)
			}
			e.findBack(0)
			e.renderSearch()
			return false, "", nil
		}
	}
	// Ctrl+C follows the ordinary cancellation path (including handing
	// an empty line to a running watch); Enter runs the accepted match.
	e.acceptSearch()
	if input.kind != inputControl || (input.key != '\r' && input.key != '\n' && input.key != 0x03) {
		e.render()
	}
	return e.applyInput(input)
}

func (e *editor) searchText() string {
	if e.search == nil {
		return ""
	}
	return string(e.search.query.buf)
}

// findBack looks for the newest history line at or after index from
// that contains the query, and puts it in the buffer.
func (e *editor) findBack(from int) {
	q := e.searchText()
	for i := from; ; i++ {
		line, ok := e.hist.at(i)
		if !ok {
			e.search.found = false
			return
		}
		if strings.Contains(line, q) {
			e.search.at, e.search.found = i, true
			_ = e.setText(line)
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

// completeLine applies the grammar's byte span without losing text after
// the cursor. Both insertion and replacement consume the same buffer budget
// as typing and search; rejected completions leave the draft untouched.
func (e *editor) completeLine() {
	var edit completionEdit
	switch {
	case e.completeAt != nil:
		edit = e.completeAt(string(e.buf), e.cursorByte())
	case e.complete != nil && e.cur == len(e.buf):
		edit.Start, edit.End = e.byteLen(), e.byteLen()
		edit.Text, edit.Hints = e.complete(string(e.buf))
	default:
		return
	}
	if edit.Text != "" || edit.Start != edit.End {
		_ = e.replaceBytes(edit.Start, edit.End, edit.Text)
	}
	if len(edit.Hints) > 0 {
		e.dropBelow()
		fmt.Fprint(e.out, "\r\n"+strings.Join(edit.Hints, "  ")+"\r\n")
	}
	e.render()
}

// backspace removes one displayed grapheme. Redraw it through the same
// layout as typing: zero-width marks cannot be erased rune by rune.
func (e *editor) backspace() {
	if e.cur == 0 {
		return
	}
	e.remove(e.prevBoundary(), e.cur)
	e.render()
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
	e.remove(i, e.cur)
	e.render()
}

// finishLine closes the edit.
func (e *editor) finishLine() (string, error) {
	e.dropBelow()
	fmt.Fprint(e.out, "\r\n")
	line := strings.TrimSpace(string(e.buf))
	e.hist.add(line)
	return line, nil
}

// help answers the help key, whichever one was pressed: describe
// where the line stands, then hand the draft back untouched.
func (e *editor) help() {
	if e.helpFor == nil {
		return
	}
	e.search = nil
	text := e.helpFor(string(e.buf), e.helpLevel)
	e.helpLevel++
	e.dropBelow()
	fmt.Fprint(e.out, "\r\n"+text)
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
			e.cur = e.nextBoundary()
			e.render()
		}
	case 'D': // left
		if e.cur > 0 {
			e.cur = e.prevBoundary()
			e.render()
		}
	}
}

// insertRune receives only complete decoded runes, in either input mode.
func (e *editor) insertRune(r rune) error {
	atEnd := e.cur == len(e.buf)
	if !e.insert(r) {
		return errLineTooLong
	}
	if e.suspended {
		return nil
	}
	if atEnd && e.paint == nil && e.width <= 0 {
		fmt.Fprint(e.out, string(r))
	} else {
		e.render()
	}
	return nil
}

// set replaces the whole line, cursor at the end.
func (e *editor) set(line string) {
	if e.setText(line) {
		e.render()
	}
}

// render paints only the viewport containing the logical cursor. Layout and
// cursor placement share grapheme boundaries, wide-glyph wrapping and colour
// state; they cannot disagree about unused cells at a terminal's right edge.
func (e *editor) render() {
	if e.suspended {
		return
	}
	prompt := "> "
	if e.prompt != nil {
		prompt = e.prompt(e.searchText())
	}
	shown := string(e.buf)
	if e.paint != nil {
		shown = e.paint(shown)
	}
	l := layoutText(prompt+shown, plainBytes(prompt)+e.cursorByte(), e.width)
	first, last := l.view(e.viewTop, e.height)
	e.clearBlock()
	fmt.Fprint(e.out, strings.Join(l.rows[first:last], "\r\n"))
	// Each row restores its inherited style; stop it at the edit boundary.
	if strings.Contains(shown+prompt, "\x1b[") {
		fmt.Fprint(e.out, cReset)
	}
	e.viewTop, e.viewRows = first, last-first
	e.screenRow = l.cursor.row - first
	if l.cursor != l.end || last != len(l.rows) {
		if up := last - 1 - l.cursor.row; up > 0 {
			fmt.Fprintf(e.out, "\x1b[%dA", up)
		}
		fmt.Fprint(e.out, "\r")
		if l.cursor.col > 0 {
			fmt.Fprintf(e.out, "\x1b[%dC", l.cursor.col)
		}
	}
}

// clearBlock returns to the visible edit block's origin. It never attempts
// to reach logical rows that have already scrolled out of the viewport.
func (e *editor) clearBlock() {
	if e.screenRow > 0 {
		fmt.Fprintf(e.out, "\x1b[%dA", e.screenRow)
	}
	fmt.Fprint(e.out, "\r\x1b[J")
}

// resize reanchors after terminal reflow, whose treatment of an existing
// transcript varies between terminal emulators. Clearing the visible screen
// preserves scrollback and the draft, and avoids guessing where its origin
// moved. The following repaint always fits the new screen dimensions.
func (e *editor) resize(width, height int) {
	if width <= 0 {
		width = e.width
	}
	if height <= 0 {
		height = e.height
	}
	if width == e.width && height == e.height {
		return
	}
	e.width, e.height = width, height
	e.screenRow, e.viewRows = 0, 0
	if !e.suspended {
		fmt.Fprint(e.out, "\x1b[H\x1b[2J")
		e.render()
	}
}

// dropBelow leaves the visible edit block before a submitted line, help or
// completion candidates. Hidden draft rows stay in the buffer, not on screen.
func (e *editor) dropBelow() {
	if e.suspended {
		return
	}
	if down := e.viewRows - 1 - e.screenRow; down > 0 {
		fmt.Fprintf(e.out, "\x1b[%dB", down)
	}
	e.screenRow, e.viewRows, e.viewTop = 0, 0, 0
}
