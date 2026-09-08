package cli

import "unicode/utf8"

type editorInputKind uint8

const (
	inputNone editorInputKind = iota
	inputRune
	inputControl
	inputArrow
	inputDelete
	inputHelp
	inputEscape
)

type editorInput struct {
	kind editorInputKind
	key  rune
}

// inputDecoder interprets the byte stream before any editing mode sees
// it. UTF-8, CSI and SS3 can span arbitrary reads. A terminal report is
// consumed here, so its Escape cannot accept a reverse-search result.
// The decoder retains at most one UTF-8 rune and a bounded CSI prefix.
type inputDecoder struct {
	raw     [utf8.UTFMax]byte
	rawLen  int
	rawWant int

	escapeIntro  byte
	escapeParams [cursorReportMax]byte
	escapeLen    int
	escapeLong   bool
}

// decode returns at most two events: an incomplete rune or a bare Escape
// can be followed by a byte that is itself a key. Neither may consume that
// following key (in particular Enter, Ctrl+C or Ctrl+D).
func (d *inputDecoder) decode(c byte) (first, next editorInput) {
	if d.rawLen > 0 {
		return d.continuation(c)
	}
	if d.escapeIntro != 0 {
		return d.escape(c)
	}
	return d.begin(c), next
}

func (d *inputDecoder) continuation(c byte) (first, next editorInput) {
	if c&0xc0 != 0x80 {
		d.rawLen = 0
		return editorInput{kind: inputRune, key: utf8.RuneError}, d.begin(c)
	}
	d.raw[d.rawLen] = c
	d.rawLen++
	if d.rawLen < d.rawWant {
		return first, next
	}
	r, size := utf8.DecodeRune(d.raw[:d.rawLen])
	if size != d.rawLen {
		r = utf8.RuneError
	}
	d.rawLen = 0
	return editorInput{kind: inputRune, key: r}, next
}

func (d *inputDecoder) begin(c byte) editorInput {
	switch {
	case c == 0x1b:
		d.escapeIntro, d.escapeLen, d.escapeLong = c, 0, false
	case c < 0x20 || c == 0x7f:
		return editorInput{kind: inputControl, key: rune(c)}
	case c < utf8.RuneSelf:
		return editorInput{kind: inputRune, key: rune(c)}
	default:
		d.rawWant = runeLen(c)
		if d.rawWant == 1 {
			return editorInput{kind: inputRune, key: utf8.RuneError}
		}
		d.raw[0], d.rawLen = c, 1
	}
	return editorInput{}
}

func (d *inputDecoder) escape(c byte) (first, next editorInput) {
	intro := d.escapeIntro
	if intro == 0x1b {
		if c == '[' || c == 'O' {
			d.escapeIntro = c
			return first, next
		}
		d.escapeIntro = 0
		return editorInput{kind: inputEscape}, d.begin(c)
	}
	if intro == '[' && c >= 0x20 && c <= 0x3f {
		if d.escapeLen < len(d.escapeParams) {
			d.escapeParams[d.escapeLen] = c
			d.escapeLen++
		} else {
			d.escapeLong = true
		}
		return first, next
	}
	d.escapeIntro = 0
	if c < 0x40 || c > 0x7e {
		return d.begin(c), next
	}
	if d.escapeLong {
		return first, next
	}
	return d.sequenceKey(intro, c), next
}

func (d *inputDecoder) sequenceKey(intro, c byte) editorInput {
	if (intro == 'O' && c == 'P') ||
		(intro == '[' && c == '~' && string(d.escapeParams[:d.escapeLen]) == "11") {
		return editorInput{kind: inputHelp}
	}
	if c >= 'A' && c <= 'D' {
		return editorInput{kind: inputArrow, key: rune(c)}
	}
	if intro == '[' && c == '~' && string(d.escapeParams[:d.escapeLen]) == "3" {
		return editorInput{kind: inputDelete}
	}
	// CPR, mouse reports, unsupported keys and bracketed-paste delimiters
	// have no editing meaning. Payload bytes retain their normal meaning.
	return editorInput{}
}

// finish makes a truncated UTF-8 rune visible once at EOF. An unfinished
// terminal sequence contains no text; a bare Escape still leaves search.
func (d *inputDecoder) finish() editorInput {
	if d.rawLen > 0 {
		d.rawLen = 0
		return editorInput{kind: inputRune, key: utf8.RuneError}
	}
	intro := d.escapeIntro
	d.escapeIntro = 0
	if intro == 0x1b {
		return editorInput{kind: inputEscape}
	}
	return editorInput{}
}

// runeLen reads a UTF-8 lead byte's promise. Invalid leads stand alone;
// malformed encodings become U+FFFD without swallowing following controls.
func runeLen(c byte) int {
	switch {
	case c >= 0xc2 && c <= 0xdf:
		return 2
	case c >= 0xe0 && c <= 0xef:
		return 3
	case c >= 0xf0 && c <= 0xf4:
		return 4
	default:
		return 1
	}
}
