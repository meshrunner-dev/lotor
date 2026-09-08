package cli

import (
	"strings"
	"unicode/utf8"
)

// shellWord keeps both faces of a word: the value the command reads and
// its original span, which the editor must preserve when it paints or
// completes it. Quotes have the historical meaning here: they hold spaces,
// and backslashes remain literal bytes.
type shellWord struct {
	start, end int
	column     int
	text       string
	openQuote  bool
}

func lexLine(line string) []shellWord {
	var words []shellWord
	var value strings.Builder
	start, column, startColumn := -1, 1, 1
	quoted := false
	flush := func(end int) {
		if start < 0 {
			return
		}
		words = append(words, shellWord{
			start: start, end: end, column: startColumn,
			text: value.String(), openQuote: quoted,
		})
		value.Reset()
		start = -1
	}
	for offset, r := range line {
		if !quoted && (r == ' ' || r == '\t') {
			flush(offset)
			column++
			continue
		}
		if start < 0 {
			start, startColumn = offset, column
		}
		if r == '"' {
			quoted = !quoted
		} else {
			value.WriteRune(r)
		}
		column++
	}
	flush(len(line))
	return words
}

// splitArgs and the error-column variant share the lexer with the
// interactive views; parsing a submitted line never gives quotes a
// different meaning from the draft the operator saw.
func splitArgs(line string) []string {
	args, _ := splitArgsAt(line)
	return args
}

func splitArgsAt(line string) (args []string, columns []int) {
	for _, word := range lexLine(line) {
		args = append(args, word.text)
		columns = append(columns, word.column)
	}
	return args, columns
}

func insideQuoteAt(line string, cursorByte int) bool {
	cursorByte = wordBoundary(line, cursorByte)
	words := lexLine(line[:cursorByte])
	return len(words) > 0 && words[len(words)-1].openQuote
}

// wordBoundary holds an editor-supplied byte offset to a rune boundary.
func wordBoundary(line string, offset int) int {
	offset = max(0, min(len(line), offset))
	for offset > 0 && offset < len(line) && !utf8.RuneStart(line[offset]) {
		offset--
	}
	return offset
}
