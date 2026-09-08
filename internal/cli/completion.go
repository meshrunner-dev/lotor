package cli

import (
	"encoding/base64"
	"strings"
)

// completionEdit replaces a span of the original draft. Byte offsets
// let the grammar preserve the untouched suffix without knowing how the
// editor stores runes or lays them out on screen.
type completionEdit struct {
	Start, End int
	Text       string
	Hints      []string
}

// complete preserves the append-only seam used by older callers. The
// interactive editor uses completeAt, which also works inside the line.
func (s *session) complete(line string) (add string, hints []string) {
	edit := s.completeAt(line, len(line))
	completed := line[:edit.Start] + edit.Text + line[edit.End:]
	if tail, appended := strings.CutPrefix(completed, line); appended {
		return tail, edit.Hints
	}
	return "", edit.Hints
}

func (s *session) completeAt(line string, cursorByte int) completionEdit {
	cursorByte = wordBoundary(line, cursorByte)
	word := completionWordAt(line, cursorByte)
	add, hints := s.completeWord(word.prior, word.last, word.after)
	if add == "" {
		return completionEdit{Start: cursorByte, End: cursorByte, Hints: hints}
	}
	return word.finish(line, add, hints)
}

// completionWord retains the original spelling around the cursor and
// the semantic words around it. Its replacement ends at a field or
// path separator when the remainder of that word must survive.
type completionWord struct {
	edit                  completionEdit
	prior, after          []string
	last, raw             string
	openQuote, pathSuffix bool
	endQuote, keySuffix   bool
}

func completionWordAt(line string, cursor int) completionWord {
	out := completionWord{edit: completionEdit{Start: cursor, End: cursor}}
	before := lexLine(line[:cursor])
	for _, word := range before {
		out.prior = append(out.prior, word.text)
	}
	if len(before) > 0 && before[len(before)-1].end == cursor {
		word := before[len(before)-1]
		out.last, out.raw, out.openQuote = word.text, line[word.start:cursor], word.openQuote
		out.endQuote = word.openQuote
		out.edit.Start = word.start
		out.prior = out.prior[:len(out.prior)-1]
	}
	for _, word := range lexLine(line) {
		if word.start <= out.edit.Start && word.end > cursor {
			out.edit.End, out.pathSuffix, out.keySuffix = completionWordEnd(line, word, cursor)
			out.endQuote = insideQuoteAt(line, out.edit.End)
		} else if word.start >= cursor && word.start != out.edit.Start {
			out.after = append(out.after, word.text)
		}
	}
	return out
}

func completionWordEnd(line string, word shellWord, cursor int) (end int, pathSuffix, keySuffix bool) {
	// Completing a key retains its existing value, with the cursor just
	// after the '=' supplied by the completion.
	if equal := strings.IndexByte(line[word.start:word.end], '='); equal >= 0 {
		if cursor <= word.start+equal {
			return word.start + equal + 1, false, true
		}
		return word.end, false, false // slashes inside a value are ordinary text
	}
	if slash := strings.IndexByte(line[cursor:word.end], '/'); slash >= 0 {
		return cursor + slash + 1, true, false
	}
	return word.end, false, false
}

func (word completionWord) finish(line, add string, hints []string) completionEdit {
	finished := strings.HasSuffix(add, " ")
	more := strings.TrimSuffix(add, " ")
	if word.pathSuffix && finished {
		more += "/"
		finished = false
	}
	if word.pathSuffix && !strings.HasSuffix(more, "/") {
		word.edit.End-- // an ambiguous path prefix has not supplied '/'
	}
	if word.keySuffix && !strings.HasSuffix(more, "=") {
		// A common key prefix has not supplied '=' yet. Keep the
		// existing separator and value outside the replacement.
		word.edit.End--
	}
	raw := word.spell(more, finished)
	if finished {
		raw += " "
		// Consume one existing separator so completion moves the cursor
		// past it without duplicating it or touching the next argument.
		if word.edit.End < len(line) && (line[word.edit.End] == ' ' || line[word.edit.End] == '\t') {
			word.edit.End++
		}
	}
	word.edit.Text, word.edit.Hints = raw, hints
	return word.edit
}

// spell preserves the quote state where the replaced suffix ends,
// including quotes that open or close inside a key. A retained value
// then stays in its original quote context. Only a finished value
// closes a quote the operator has not closed yet.
func (word completionWord) spell(more string, finished bool) string {
	equal := strings.HasSuffix(more, "=")
	key := word.keySuffix || (!strings.Contains(word.last, "=") && equal)
	if key && equal {
		more = strings.TrimSuffix(more, "=")
	}
	quoted := word.endQuote && (!finished || key)
	raw := word.raw
	if !word.openQuote && strings.HasSuffix(raw, "\"") {
		raw = strings.TrimSuffix(raw, "\"") + more + "\""
	} else {
		raw += more
	}
	if word.openQuote != quoted {
		raw += "\""
	}
	if key && equal {
		raw += "="
	}
	return raw
}

// completeWord resolves the words before the cursor, then offers the
// unfinished word's vocabulary. Later arguments participate in choice
// resolution and in the set of already supplied flags, but never move
// the context from which the current word is completed.
func (s *session) completeWord(prior []string, last string, after []string) (string, []string) {
	if strings.HasPrefix(last, "/") {
		prior = append(prior, "/")
	}
	path, rest := s.resolveTree(prior)
	if len(rest) > 0 {
		if isAddVerb(rest[0]) && len(rest) == 1 {
			return "", nil // the next word is the new object's own name
		}
		if len(rest) > 1 && (rest[0] == verbRemove || rest[0] == verbDisable || rest[0] == verbEnable) {
			return "", nil // a collection mutation takes only one name
		}
		return s.completeArgs(path, append(rest, after...), last)
	}
	prefix := strings.TrimPrefix(last, "/")
	if i := strings.LastIndex(prefix, "/"); i >= 0 {
		next, leftover, ok := s.walkStep(path, prefix[:i])
		if !ok || leftover != "" {
			return "", nil
		}
		path, prefix = next, prefix[i+1:]
	}
	cands := s.candidatesAt(path)
	matched, common := match(prefix, names(cands))
	switch len(matched) {
	case 0:
		return "", nil
	case 1:
		tail := " "
		for _, candidate := range cands {
			if candidate.name == matched[0] && candidate.container {
				tail = "/"
			}
		}
		return matched[0][len(prefix):] + tail, nil
	default:
		return common[len(prefix):], s.listing(matched, cands)
	}
}

func encodedVerb(verb string) bool { return verb == verbSet64 || verb == verbAdd64 }

// semanticArgs unwraps complete encoded pairs for schema resolution.
// An incomplete draft stays incomplete; the command's strict decoder
// remains responsible for reporting malformed values on submission.
func semanticArgs(rest []string) []string {
	if len(rest) == 0 || !encodedVerb(rest[0]) {
		return rest
	}
	out := append([]string(nil), rest...)
	out[0] = strings.TrimSuffix(out[0], "64")
	for i, arg := range out[1:] {
		key, encoded, pair := strings.Cut(arg, "=")
		if !pair {
			continue
		}
		if value, err := base64.StdEncoding.DecodeString(encoded); err == nil {
			out[i+1] = key + "=" + string(value)
		}
	}
	return out
}
