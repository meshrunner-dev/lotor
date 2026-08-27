package cli

// The context tree: the configuration navigated the way a network
// console does it. A path names a place — /relay, /relay meshcore-868
// — and a line either goes there (pure path) or does something there
// (path then verb, or just a verb from wherever the session stands).
// Help, completion and colours all derive from the schema and from
// the live instances, so a context can only describe what actually
// exists.
//
// The flat commands stay: at the root they work as they always did,
// and from inside a context a leading slash reaches them — "/status"
// anywhere is the status command. One grammar grows around the other
// rather than replacing it mid-stride.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/schema"
)

// treeLine reports whether this line belongs to the tree engine: an
// absolute path from anywhere, or anything at all once the session
// stands inside a context.
func (s *session) treeLine(line string) bool {
	return line == "?" || strings.HasPrefix(line, "/") || len(s.curPath()) > 0
}

// curPath returns the session's context, safe from any goroutine —
// the editor's completion and paint hooks read it from the transport
// side while commands run on the REPL's.
func (s *session) curPath() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.path...)
}

func (s *session) setPath(p []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.path = p
}

// prompt names where the session stands. The REPL prints it, and the
// editor repaints it, so both read the same function.
func (s *session) prompt() string {
	p := s.curPath()
	if len(p) == 0 {
		return "> "
	}
	label := "[/" + strings.Join(p, " ") + "]"
	if s.colors {
		label = cCyan + label + cReset
	}
	return label + " > "
}

// relays is the live view when the daemon serves one.
func (s *session) relays() []RelayInfo {
	if s.deps.LiveRelays != nil {
		return s.deps.LiveRelays()
	}
	return s.deps.Relays
}

// radios is the live view when the daemon serves one.
func (s *session) radios() []RadioInfo {
	if s.deps.LiveRadios != nil {
		return s.deps.LiveRadios()
	}
	return s.deps.Radios
}

// traces is the live view when the daemon serves one.
func (s *session) traces() map[string][]config.Trace {
	if s.deps.LiveTraces != nil {
		return s.deps.LiveTraces()
	}
	return s.deps.Traces
}

// kindByName resolves a kind the tree serves — singletons included:
// they are contexts with no instance step.
func (s *session) kindByName(name string) *schema.Kind {
	for i := range s.deps.Kinds {
		if s.deps.Kinds[i].Name == name {
			return &s.deps.Kinds[i]
		}
	}
	return nil
}

// isSingleton reports whether a context path stands on a singleton.
func (s *session) isSingleton(path []string) bool {
	if len(path) != 1 {
		return false
	}
	k := s.kindByName(path[0])
	return k != nil && k.Singleton
}

// instances lists a kind's live objects and the choice each one made.
func (s *session) instances(kind string) map[string]string {
	out := map[string]string{}
	switch kind {
	case scopeRelay:
		for _, r := range s.relays() {
			out[r.Name] = r.Protocol
		}
	case scopeRadio:
		for _, r := range s.radios() {
			out[r.Name] = r.Driver
		}
	}
	return out
}

// resolveTree walks a token list from its starting point — the root
// for an absolute line, the session's context otherwise — consuming
// what names a place and returning where it arrived plus what is left
// to run there.
func (s *session) resolveTree(tokens []string) (path, rest []string) {
	path = s.curPath()
	if len(tokens) > 0 && strings.HasPrefix(tokens[0], "/") {
		path = nil
		if t := strings.TrimPrefix(tokens[0], "/"); t == "" {
			tokens = tokens[1:]
		} else {
			tokens = append([]string{t}, tokens[1:]...)
		}
	}
	for len(tokens) > 0 {
		t := tokens[0]
		switch {
		case t == "..":
			if len(path) > 0 {
				path = path[:len(path)-1]
			}
		case len(path) == 0:
			if s.kindByName(t) == nil {
				return path, tokens
			}
			path = append(path, t)
		case len(path) == 1:
			if _, ok := s.instances(path[0])[t]; !ok {
				return path, tokens
			}
			path = append(path, t)
		default:
			return path, tokens
		}
		tokens = tokens[1:]
	}
	return path, nil
}

// tree runs one line of the context grammar.
func (s *session) tree(ctx context.Context, line string) {
	tokens := splitArgs(line)
	path, rest := s.resolveTree(tokens)
	if len(rest) == 0 {
		// A pure path is navigation.
		s.setPath(path)
		return
	}
	verb, args := rest[0], rest[1:]
	switch {
	case verb == "?" || verb == "help":
		fmt.Fprint(s.out, s.treeHelp(path))
	case verb == cmdQuit || verb == "exit":
		s.dispatch(ctx, rest)
	case len(path) == 0:
		// A verb at the root is a flat command — "/status" from a
		// context lands here with path emptied.
		s.dispatch(ctx, rest)
	case verb == verbPrint:
		s.treePrint(ctx, path)
	case verb == verbSet || verb == verbUnset:
		if err := s.treeSet(ctx, path, verb, args); err != nil {
			fmt.Fprintf(s.out, "error: %s\r\n", err)
		}
	case verb == verbAdd || verb == verbRemove:
		if err := s.treeCreateRemove(ctx, path, verb, args); err != nil {
			fmt.Fprintf(s.out, "error: %s\r\n", err)
		}
	case verb == verbExport:
		if err := s.treeExport(path); err != nil {
			fmt.Fprintf(s.out, "error: %s\r\n", err)
		}
	case len(path) == 2 && s.mountedVerb(path[0], verb):
		// The flat command runs as itself, told which instance asked.
		s.dispatch(ctx, append(append([]string{verb}, args...), "--"+scopeRelay, path[1]))
	default:
		fmt.Fprintf(s.out, "error: no %q here — \"?\" says what this context offers\r\n", verb)
	}
}

// The mutation verbs.
const (
	verbSet    = "set"
	verbUnset  = "unset"
	verbExport = "export"
	verbAdd    = "add"
	verbRemove = "remove"
)

// treeCreateRemove serves add and remove, from a kind's collection —
// and remove from a singleton, whose whole block is the instance.
func (s *session) treeCreateRemove(ctx context.Context, path []string, verb string, args []string) error {
	if s.deps.Create == nil || s.deps.Remove == nil {
		return errors.New("this daemon has no mutation channel")
	}
	if s.deps.Privilege != Admin {
		return fmt.Errorf("%s is an admin verb — use the local console socket", verb)
	}
	if len(path) != 1 {
		return fmt.Errorf("%s works from the collection — /relay %s <name> …", verb, verb)
	}
	kind := path[0]
	if s.isSingleton(path) {
		if verb == verbAdd {
			return fmt.Errorf("/%s is one block — set its attributes instead", kind)
		}
		msg, err := s.deps.Remove(ctx, kind, "", "console")
		if err != nil {
			return err
		}
		fmt.Fprintf(s.out, "%s\r\n", msg)
		return nil
	}
	if len(args) == 0 {
		return fmt.Errorf("usage: %s <name> [attr=value …]", verb)
	}
	name, rest := args[0], args[1:]
	if verb == verbRemove {
		if len(rest) > 0 {
			return errors.New("remove takes one name and nothing else")
		}
		msg, err := s.deps.Remove(ctx, kind, name, "console")
		if err != nil {
			return err
		}
		fmt.Fprintf(s.out, "%s\r\n", msg)
		return nil
	}
	attrs := map[string]string{}
	for _, a := range rest {
		k, v, ok := strings.Cut(a, "=")
		if !ok {
			return fmt.Errorf("%q — add wants attr=value after the name", a)
		}
		attrs[k] = v
	}
	msg, err := s.deps.Create(ctx, kind, name, attrs, "console")
	if err != nil {
		return err
	}
	fmt.Fprintf(s.out, "%s\r\n", msg)
	return nil
}

// treeSet applies one line of changes: every attr=value on it lands
// in one transaction, so a retune touching three attributes bounces
// the relay once, not three times.
func (s *session) treeSet(ctx context.Context, path []string, verb string, args []string) error {
	if s.deps.Mutate == nil {
		return errors.New("this daemon has no mutation channel")
	}
	if s.deps.Privilege != Admin {
		return fmt.Errorf("%s is an admin verb — use the local console socket", verb)
	}
	if len(path) != 2 && !s.isSingleton(path) {
		return fmt.Errorf("%s works on an instance — /relay <name> %s …", verb, verb)
	}
	if len(args) == 0 {
		return fmt.Errorf("usage: %s attr=value … | unset attr …", verbSet)
	}
	set := map[string]string{}
	var unset []string
	for _, a := range args {
		name, value, has := strings.Cut(a, "=")
		switch {
		case verb == verbUnset:
			if has {
				return fmt.Errorf("unset takes attribute names, not %q", a)
			}
			unset = append(unset, name)
		case !has:
			return fmt.Errorf("%q — set wants attr=value", a)
		default:
			set[name] = value
		}
	}
	name := ""
	if len(path) == 2 {
		name = path[1]
	}
	msg, err := s.deps.Mutate(ctx, path[0], name, set, unset, "console")
	if err != nil {
		return err
	}
	fmt.Fprintf(s.out, "%s\r\n", msg)
	return nil
}

// treeExport prints an instance's explicit configuration as the set
// lines that would recreate it — pasteable, which is not scripting.
// Secrets stay masked: an export is for reading and diffing, and a
// private key does not belong in either.
func (s *session) treeExport(path []string) error {
	if len(path) != 2 {
		return errors.New("export works on an instance")
	}
	traces, ok := s.traces()[path[0]+" "+path[1]]
	if !ok {
		return fmt.Errorf("no configuration recorded for %s %s", path[0], path[1])
	}
	secret := s.secretAttrs(path[0] + " " + path[1])
	fmt.Fprintf(s.out, "# /%s %s\r\n", path[0], path[1])
	for _, t := range traces {
		if !strings.HasPrefix(t.Source, "override:") {
			continue // the preset's own values need no restating
		}
		if secret[t.Key] {
			fmt.Fprintf(s.out, "# %s is secret — an export cannot carry it\r\n", t.Key)
			continue
		}
		fmt.Fprintf(s.out, "set %s=%s\r\n", t.Key, exportValue(t.Value))
	}
	return nil
}

// exportValue renders one value the way set accepts it back.
func exportValue(v any) string {
	switch vv := v.(type) {
	case []any:
		parts := make([]string, len(vv))
		for i, p := range vv {
			parts[i] = fmt.Sprintf("%v", p)
		}
		return quoteIfSpaced(strings.Join(parts, ","))
	case []string:
		return quoteIfSpaced(strings.Join(vv, ","))
	default:
		return quoteIfSpaced(fmt.Sprintf("%v", v))
	}
}

func quoteIfSpaced(s string) string {
	if strings.ContainsAny(s, " \t") {
		return `"` + s + `"`
	}
	return s
}

// verbPrint is the tree's one universal verb, the console family's
// word for "show me".
const verbPrint = "print"

// mountedVerb reports whether a flat command serves this kind's
// instances directly.
func (s *session) mountedVerb(kind, verb string) bool {
	if kind != scopeRelay {
		return false
	}
	switch verb {
	case cmdNeighbours, cmdScopes, cmdDiscover, cmdAdvert:
		return true
	}
	return false
}

// treePrint shows a context: a kind lists its instances, an instance
// shows its effective configuration with provenance.
func (s *session) treePrint(ctx context.Context, path []string) {
	if s.isSingleton(path) {
		if err := s.showTraces(path[0]); err != nil {
			fmt.Fprintf(s.out, "error: %s\r\n", err)
		}
		return
	}
	if len(path) == 1 {
		if path[0] == scopeRelay {
			s.dispatch(ctx, []string{scopeRelay, verbList})
			return
		}
		tb := &table{}
		for _, r := range s.radios() {
			tb.row(r.Name, r.Driver, "relay "+r.Relay)
		}
		if err := tb.flush(s.out); err == nil && len(s.radios()) == 0 {
			fmt.Fprint(s.out, "no radios\r\n")
		}
		return
	}
	if err := s.showTraces(path[0] + " " + path[1]); err != nil {
		fmt.Fprintf(s.out, "error: %s\r\n", err)
	}
}

// treeHelp describes one place: what it holds, what runs there, and —
// on an instance — every attribute its choice gives it, one doc line
// each.
func (s *session) treeHelp(path []string) string {
	var b strings.Builder
	switch len(path) {
	case 0:
		b.WriteString("contexts:\r\n")
		for i := range s.deps.Kinds {
			k := s.deps.Kinds[i]
			fmt.Fprintf(&b, "  %-10s %s\r\n", s.color(cCyan, "/"+k.Name), k.Doc)
		}
		b.WriteString("the flat commands work here too — \"help\" lists them\r\n")
	case 1:
		if s.isSingleton(path) {
			fmt.Fprintf(&b, "verbs: %s\r\n",
				s.color(cGreen, strings.Join([]string{verbPrint, verbSet, verbUnset, verbRemove}, " ")))
			b.WriteString("attributes:\r\n")
			for _, a := range s.attrsAt(path) {
				fmt.Fprintf(&b, "  %-24s %s\r\n", s.color(cYellow, a.Name), a.Doc)
			}
			break
		}
		fmt.Fprintf(&b, "verbs: %s\r\n",
			s.color(cGreen, strings.Join([]string{verbPrint, verbAdd, verbRemove}, " ")))
		names := make([]string, 0, 4)
		inst := s.instances(path[0])
		for name := range inst {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			fmt.Fprintf(&b, "  %-16s %s\r\n", s.color(cCyan, name), inst[name])
		}
	case 2: //nolint:mnd // the tree is two levels deep by design
		verbs := []string{verbPrint, verbSet, verbUnset, verbExport}
		for _, v := range []string{cmdNeighbours, cmdScopes, cmdDiscover, cmdAdvert} {
			if s.mountedVerb(path[0], v) {
				verbs = append(verbs, v)
			}
		}
		fmt.Fprintf(&b, "verbs: %s\r\n", s.color(cGreen, strings.Join(verbs, " ")))
		b.WriteString("attributes:\r\n")
		for _, a := range s.attrsAt(path) {
			doc := a.Doc
			if a.Secret {
				doc += " (secret: never echoed)"
			}
			fmt.Fprintf(&b, "  %-24s %s\r\n", s.color(cYellow, a.Name), doc)
		}
	}
	return b.String()
}

// attrsAt resolves the attribute set a context offers: an instance's
// choice decides the contributed ones, a singleton has only its own.
func (s *session) attrsAt(path []string) []schema.Attr {
	if s.isSingleton(path) {
		return s.kindByName(path[0]).AttrsFor("")
	}
	if len(path) != 2 {
		return nil
	}
	k := s.kindByName(path[0])
	if k == nil {
		return nil
	}
	return k.AttrsFor(s.instances(path[0])[path[1]])
}

// complete finishes the word under construction: context names,
// instance names, verbs. It returns what to append when one candidate
// (or a common prefix) is certain, and the candidates when several
// remain.
func (s *session) complete(line string) (add string, hints []string) {
	tokens := splitArgs(line)
	last := ""
	if line != "" && !strings.HasSuffix(line, " ") && len(tokens) > 0 {
		last = tokens[len(tokens)-1]
		tokens = tokens[:len(tokens)-1]
	}
	// The path so far decides what may come next.
	prior := append([]string{}, tokens...)
	if last != "" && strings.HasPrefix(last, "/") {
		// completing "/rel": resolve nothing, offer from the root
		prior = append(prior, "/")
	}
	path, rest := s.resolveTree(prior)
	if len(rest) > 0 {
		return s.completeArgs(path, rest, last)
	}
	return finish(strings.TrimPrefix(last, "/"),
		s.candidatesAt(path, strings.HasPrefix(line, "/")))
}

// candidatesAt lists what may legally come next at one place.
func (s *session) candidatesAt(path []string, absolute bool) []string {
	var cands []string
	switch len(path) {
	case 0:
		for i := range s.deps.Kinds {
			cands = append(cands, s.deps.Kinds[i].Name)
		}
		if !absolute {
			cands = append(cands, commandNames()...)
		}
	case 1:
		if s.isSingleton(path) {
			cands = append(cands, verbPrint, verbSet, verbUnset, verbRemove)
			break
		}
		for name := range s.instances(path[0]) {
			cands = append(cands, name)
		}
		cands = append(cands, verbPrint, verbAdd, verbRemove)
	case 2:
		cands = append(cands, verbPrint, verbSet, verbUnset, verbExport,
			cmdNeighbours, cmdScopes, cmdDiscover, cmdAdvert)
	}
	return cands
}

// finish picks what the prefix may become: the whole word when one
// candidate remains, the shared advance plus the list when several do.
func finish(prefix string, cands []string) (add string, hints []string) {
	var matched []string
	for _, c := range cands {
		if strings.HasPrefix(c, prefix) {
			matched = append(matched, c)
		}
	}
	sort.Strings(matched)
	switch len(matched) {
	case 0:
		return "", nil
	case 1:
		return matched[0][len(prefix):] + " ", nil
	}
	common := matched[0]
	for _, m := range matched[1:] {
		for !strings.HasPrefix(m, common) {
			common = common[:len(common)-1]
		}
	}
	return common[len(prefix):], matched
}

// completeArgs finishes what comes after a verb: attribute names for
// set, unset and add — and, past the '=', an enum's values.
func (s *session) completeArgs(path, rest []string, last string) (add string, hints []string) {
	verb := rest[0]
	if verb != verbSet && verb != verbUnset && verbAdd != verb {
		return "", nil
	}
	attrs := s.attrsAt(path)
	if verb == verbAdd && len(path) == 1 && !s.isSingleton(path) {
		// completing "add <name> attr=…": the choice is on the line.
		attrs = s.attrsForAddLine(path[0], rest)
	}
	if attrs == nil {
		return "", nil
	}
	if attr, val, has := strings.Cut(last, "="); has {
		a, ok := schema.Find(attrs, attr)
		if !ok || len(a.Enum) == 0 {
			return "", nil
		}
		return finish(val, a.Enum)
	}
	names := make([]string, 0, len(attrs))
	for _, a := range attrs {
		if verb == verbUnset {
			names = append(names, a.Name)
			continue
		}
		names = append(names, a.Name+"=")
	}
	add, hints = finish(last, names)
	if verb != verbUnset {
		// The '=' is the boundary: nothing to append after it yet.
		add = strings.TrimSuffix(add, " ")
	}
	return add, hints
}

// attrsForAddLine resolves what a creation line may still say, from
// the choice it already made.
func (s *session) attrsForAddLine(kind string, rest []string) []schema.Attr {
	k := s.kindByName(kind)
	if k == nil {
		return nil
	}
	choice := ""
	for _, t := range rest {
		if v, ok := strings.CutPrefix(t, k.ChoiceAttr+"="); ok {
			choice = v
		}
	}
	return k.AttrsFor(choice)
}

// helpForLine is the '?' key: describe where the typed line stands.
func (s *session) helpForLine(line string) string {
	path, _ := s.resolveTree(splitArgs(line))
	return s.treeHelp(path)
}

// The console's colours, one per class of symbol: contexts and
// instances, verbs, attributes. Applied only when the transport is a
// raw terminal — piped sessions read plain text.
const (
	cReset  = "\x1b[0m"
	cCyan   = "\x1b[36m"
	cGreen  = "\x1b[32m"
	cYellow = "\x1b[33m"
	cDim    = "\x1b[2m"
)

func (s *session) color(code, text string) string {
	if !s.colors {
		return text
	}
	return code + text + cReset
}

// paintLine colours the line under the operator's fingers, token
// class by token class, as the tree understands what was typed so
// far.
func (s *session) paintLine(line string) string {
	if !s.colors {
		return line
	}
	var b strings.Builder
	rest := line
	path := s.curPath()
	if strings.HasPrefix(rest, "/") {
		path = nil
	}
	walking := true
	for rest != "" {
		token, after, _ := strings.Cut(rest, " ")
		switch {
		case token == "":
			// a run of spaces
		case walking && s.tokenInPath(&path, token):
			b.WriteString(cCyan + token + cReset)
		case strings.Contains(token, "="):
			name, val, _ := strings.Cut(token, "=")
			walking = false
			b.WriteString(cYellow + name + cReset + cDim + "=" + cReset + val)
		case walking:
			walking = false
			b.WriteString(cGreen + token + cReset)
		default:
			b.WriteString(token)
		}
		if after != "" || strings.HasSuffix(rest, " ") {
			b.WriteString(" ")
		}
		rest = after
	}
	return b.String()
}

// tokenInPath advances the walk when the token names a place.
func (s *session) tokenInPath(path *[]string, token string) bool {
	t := strings.TrimPrefix(token, "/")
	if t == ".." {
		if len(*path) > 0 {
			*path = (*path)[:len(*path)-1]
		}
		return true
	}
	switch len(*path) {
	case 0:
		if s.kindByName(t) != nil {
			*path = append(*path, t)
			return true
		}
	case 1:
		if _, ok := s.instances((*path)[0])[t]; ok {
			*path = append(*path, t)
			return true
		}
	}
	return token == "/"
}
