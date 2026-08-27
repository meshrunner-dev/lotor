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
	"slices"
	"sort"
	"strconv"
	"strings"

	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/schema"
)

// treeLine reports whether this line belongs to the tree engine: an
// absolute path from anywhere, or anything at all once the session
// stands inside a context.
func (s *session) treeLine(line string) bool {
	if line == "?" || strings.HasPrefix(line, "/") || len(s.curPath()) > 0 {
		return true
	}
	// The tree's own verbs work from the root without a slash. Standing
	// at the root is standing somewhere, so "export" there means what
	// "/export" means, and an operator should not have to know which
	// grammar a word belongs to before typing it.
	first, _, _ := strings.Cut(line, " ")
	return isTreeVerb(first)
}

// isTreeVerb reports whether a word is one of the tree's own. None of
// them names a flat command, which is what lets the root serve both
// vocabularies without either shadowing the other.
func isTreeVerb(word string) bool {
	switch word {
	case verbPrint, verbSet, verbUnset, verbAdd, verbRemove, verbExport:
		return true
	}
	return false
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

// prompt names who is standing where, in the shape a network console
// uses: the privilege and the system inside the brackets, the context
// path outside them, the caret last. It says the same thing at the
// root as anywhere else — a prompt that changes shape with depth
// makes an operator read it twice.
func (s *session) prompt() string { return s.promptWith("") }

// promptWith is the prompt carrying a history search's query, which
// lives inside it rather than on a line of its own: the draft below
// stays a command line, and the prompt is what says the session is
// searching.
func (s *session) promptWith(query string) string {
	priv := s.deps.Privilege
	if priv == "" {
		priv = ReadOnly
	}
	head := "[" + s.color(cUser, string(priv)) + "@" +
		s.color(cSystem, s.systemName()) + "] "
	body := ""
	if p := s.curPath(); len(p) > 0 {
		body = s.color(cPath, "/"+strings.Join(p, "/"))
	}
	if query != "" {
		if body != "" {
			body += " "
		}
		body += s.color(cQuery, "[") + query + s.color(cQuery, "]")
	}
	// The space after the bracket stays at every depth, the root
	// included: a prompt that changes shape with depth is read twice.
	return head + body + "> "
}

// systemName is what this installation calls itself.
func (s *session) systemName() string {
	if s.deps.SystemName != nil {
		if name := s.deps.SystemName(); name != "" {
			return name
		}
	}
	return "lotor"
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
		// A step may be written slash-joined or spaced — /relay/x and
		// /relay x name the same place. Only tokens still being read as
		// path are split, so a value carrying slashes is never touched.
		next, ok := s.walkStep(path, tokens[0])
		if !ok {
			return path, tokens
		}
		path = next
		tokens = tokens[1:]
	}
	return path, nil
}

// walkStep consumes one token as path steps, all of them or none: a
// half-consumed token would leave the session somewhere nobody typed.
func (s *session) walkStep(path []string, token string) ([]string, bool) {
	next := append([]string(nil), path...)
	for piece := range strings.SplitSeq(token, "/") {
		switch {
		case piece == "":
			continue // a doubled or trailing slash says nothing
		case piece == "..":
			if len(next) > 0 {
				next = next[:len(next)-1]
			}
		case len(next) == 0 && s.kindByName(piece) != nil:
			next = append(next, piece)
		case len(next) == 1 && !s.isSingleton(next):
			if _, ok := s.instances(next[0])[piece]; !ok {
				return nil, false
			}
			next = append(next, piece)
		default:
			return nil, false
		}
	}
	return next, true
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
	case verb == verbExport:
		// Every depth answers export — the root's is the whole
		// configuration, the backup an operator can read.
		if err := s.treeExport(path); err != nil {
			fmt.Fprintf(s.out, "error: %s\r\n", err)
		}
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
	case len(path) == 0:
		// Anything else at the root is a flat command — "/status" from
		// a context lands here with the path emptied. The tree's own
		// verbs are answered above, so the root is a context like any
		// other rather than a place where they stop working.
		s.dispatch(ctx, rest)
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
	if len(path) == 0 {
		return fmt.Errorf("%s needs a collection — /relay %s <name> …, or \"?\" for the contexts",
			verb, verb)
	}
	if len(path) != 1 {
		return fmt.Errorf("%s works from the collection — /%s %s <name> …", verb, path[0], verb)
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
	if len(path) == 0 {
		return fmt.Errorf("%s needs somewhere to work — /relay <name> %s …, or \"?\" for the contexts",
			verb, verb)
	}
	if len(path) != 2 && !s.isSingleton(path) {
		return fmt.Errorf("%s works on an instance — /%s <name> %s …", verb, path[0], verb)
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

// treeExport prints configuration as the absolute lines that would
// recreate it — pasteable anywhere, which is not scripting. From the
// root it exports everything, from a collection its instances, from
// an instance itself. Secrets stay masked: an export is for reading
// and diffing, and a private key does not belong in either.
func (s *session) treeExport(path []string) error {
	switch len(path) {
	case 0:
		// Radios before the relays that claim them: the paste has to
		// work in order.
		for _, kind := range []string{scopeRadio, scopeRelay} {
			names := make([]string, 0, 2)
			for name := range s.instances(kind) {
				names = append(names, name)
			}
			sort.Strings(names)
			for _, name := range names {
				s.exportInstance(kind, name)
			}
		}
		for i := range s.deps.Kinds {
			if k := s.deps.Kinds[i]; k.Singleton {
				s.exportSingleton(k.Name)
			}
		}
		return nil
	case 1:
		if s.isSingleton(path) {
			s.exportSingleton(path[0])
			return nil
		}
		names := make([]string, 0, 2)
		for name := range s.instances(path[0]) {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			s.exportInstance(path[0], name)
		}
		return nil
	default:
		if _, ok := s.traces()[path[0]+" "+path[1]]; !ok {
			return fmt.Errorf("no configuration recorded for %s %s", path[0], path[1])
		}
		s.exportInstance(path[0], path[1])
		return nil
	}
}

// exportInstance prints one add line that recreates the object: the
// structural attributes and the explicit overrides, never the
// preset's own values.
func (s *session) exportInstance(kind, name string) {
	traces, ok := s.traces()[kind+" "+name]
	if !ok {
		return
	}
	secret := s.secretAttrs(kind + " " + name)
	var pairs [][2]string
	var masked []string
	for _, t := range traces {
		if strings.HasPrefix(t.Source, "profile:") {
			continue // the preset restates itself
		}
		if secret[t.Key] {
			masked = append(masked, t.Key)
			continue
		}
		pairs = append(pairs, [2]string{t.Key, exportValue(t.Value)})
	}
	s.exportLine(kind, verbAdd, name, pairs)
	for _, key := range masked {
		s.exportComment(fmt.Sprintf("%s %s: %s is secret — an export cannot carry it",
			kind, name, key))
	}
}

// exportSingleton prints one set line for a block that exists.
func (s *session) exportSingleton(kind string) {
	traces, ok := s.traces()[kind]
	if !ok {
		return
	}
	pairs := make([][2]string, 0, len(traces))
	for _, t := range traces {
		pairs = append(pairs, [2]string{t.Key, exportValue(t.Value)})
	}
	s.exportLine(kind, verbSet, "", pairs)
}

// exportLine writes one recreating line, coloured by symbol class the
// way the line under the operator's fingers is: the context and the
// instance name read as places, the verb as a verb, the attribute
// names as attributes. A terminal shows the structure at a glance; a
// pipe gets the same text with nothing in it, because an export is
// also something machines read.
func (s *session) exportLine(kind, verb, name string, pairs [][2]string) {
	var b strings.Builder
	b.WriteString(s.color(cPath, "/"+kind))
	b.WriteString(" " + s.color(cVerb, verb))
	if name != "" {
		b.WriteString(" " + s.color(cPath, name))
	}
	for _, p := range pairs {
		b.WriteString(" " + s.color(cAttr, p[0]) + s.color(cPunct, "=") + p[1])
	}
	fmt.Fprintf(s.out, "%s\r\n", b.String())
}

// exportComment writes a line the paste ignores, dimmed so it reads as
// an aside rather than as configuration.
func (s *session) exportComment(text string) {
	fmt.Fprintf(s.out, "# %s\r\n", text)
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
	if len(path) == 0 {
		// The root holds no values of its own: what it can show is
		// what stands below it.
		tb := s.table()
		tb.header("CONTEXT", "HOLDS", "WHAT IT IS")
		for i := range s.deps.Kinds {
			k := s.deps.Kinds[i]
			held := "one block"
			if !k.Singleton {
				held = strconv.Itoa(len(s.instances(k.Name)))
			}
			tb.row("/"+k.Name, held, k.Doc)
		}
		_ = tb.flush(s.out)
		return
	}
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
		tb := s.table()
		tb.header("NAME", "DRIVER", "OWNER")
		for _, r := range s.radios() {
			owner := "unclaimed"
			if r.Relay != "" {
				owner = r.Relay
			}
			tb.row(r.Name, r.Driver, owner)
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
		s.writeVerbs(&b, path)
		b.WriteString("contexts:\r\n")
		for i := range s.deps.Kinds {
			k := s.deps.Kinds[i]
			fmt.Fprintf(&b, "  %-10s%s %s\r\n", s.color(cPath, "/"+k.Name),
				s.color(cPunct, " --"), k.Doc)
		}
		b.WriteString("the flat commands work here too — \"help\" lists them\r\n")
	case 1:
		s.writeVerbs(&b, path)
		if s.isSingleton(path) {
			b.WriteString("attributes:\r\n")
			for _, a := range s.attrsAt(path) {
				fmt.Fprintf(&b, "  %-24s%s %s\r\n", s.color(cAttr, a.Name), s.color(cPunct, " --"), a.Doc)
			}
			break
		}
		names := make([]string, 0, 4)
		inst := s.instances(path[0])
		for name := range inst {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			fmt.Fprintf(&b, "  %-16s%s %s\r\n", s.color(cPath, name), s.color(cPunct, " --"), inst[name])
		}
	case 2: //nolint:mnd // the tree is two levels deep by design
		s.writeVerbs(&b, path)
		b.WriteString("attributes:\r\n")
		for _, a := range s.attrsAt(path) {
			doc := a.Doc
			if a.Secret {
				doc += " (secret: never echoed)"
			}
			fmt.Fprintf(&b, "  %-24s%s %s\r\n", s.color(cAttr, a.Name), s.color(cPunct, " --"), doc)
		}
	}
	return b.String()
}

// writeVerbs names what this place answers.
func (s *session) writeVerbs(b *strings.Builder, path []string) {
	fmt.Fprintf(b, "verbs: %s\r\n", s.color(cVerb, strings.Join(s.verbsAt(path), " ")))
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

// verbsAt is what a place answers — the one list, read by the help
// that describes it and by the completion that offers it. Keeping
// them in step by hand is how export came to work everywhere and be
// offered in only some places.
func (s *session) verbsAt(path []string) []string {
	switch {
	case len(path) == 0:
		// The root holds no object, so nothing that needs one: print
		// shows what stands below, export takes the whole tree.
		return []string{verbPrint, verbExport}
	case s.isSingleton(path):
		// One block, always there: set it, clear it, take it away.
		return []string{verbPrint, verbSet, verbUnset, verbExport, verbRemove}
	case len(path) == 1:
		return []string{verbPrint, verbAdd, verbRemove, verbExport}
	default:
		verbs := []string{verbPrint, verbSet, verbUnset, verbExport}
		for _, v := range []string{cmdNeighbours, cmdScopes, cmdDiscover, cmdAdvert} {
			if s.mountedVerb(path[0], v) {
				verbs = append(verbs, v)
			}
		}
		return verbs
	}
}

// candidatesAt lists what may legally come next at one place: the
// verbs, plus whatever names a place one step further in.
func (s *session) candidatesAt(path []string, absolute bool) []string {
	cands := s.verbsAt(path)
	switch {
	case len(path) == 0:
		for i := range s.deps.Kinds {
			cands = append(cands, s.deps.Kinds[i].Name)
		}
		if !absolute {
			// The flat commands answer at the root too.
			cands = append(cands, commandNames()...)
		}
	case len(path) == 1 && !s.isSingleton(path):
		for name := range s.instances(path[0]) {
			cands = append(cands, name)
		}
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

// helpForLine answers the help key. Pressed again it moves on rather
// than repeating itself: what a place offers, then how to work the
// console, then back — because the second question is the one an
// operator has once the first is answered.
func (s *session) helpForLine(line string, level int) string {
	if level%2 == 1 {
		return keyHelp
	}
	path, _ := s.resolveTree(splitArgs(line))
	return s.treeHelp(path) + "\r\npress it again for the keys\r\n"
}

// keyHelp is what the console itself answers to.
const keyHelp = "" +
	"F1 or ?         what this place offers; again for these keys\r\n" +
	"[Tab]           complete the word; again to list the candidates\r\n" +
	"Ctrl-R          search the session's history, newest first\r\n" +
	"Up / Down       walk the history\r\n" +
	"Left / Right    move the cursor\r\n" +
	"Ctrl-A / Ctrl-E start and end of the line\r\n" +
	"Ctrl-W          delete the word before the cursor\r\n" +
	"Ctrl-U          delete to the start of the line\r\n" +
	"Ctrl-C          abandon the line\r\n" +
	"Ctrl-D          leave, on an empty line\r\n" +
	"\r\n" +
	"/               a place, from the root\r\n" +
	"..              one place up\r\n" +
	"/command        run a flat command from anywhere\r\n"

// The console's colours, named by the class of symbol they carry
// rather than by their hue: what matters is that a place, an action,
// an attribute and a value never look alike, and that a word the
// console has not resolved says so before it is submitted. Applied
// only when the transport is a raw terminal — piped sessions read
// plain text.
const (
	// cReset is the short form: terminals treat an empty parameter as
	// zero, and the shorter sequence is three bytes lighter on every
	// repaint of every keystroke.
	cReset = "\x1b[m"

	cPath   = "\x1b[36m"   // a place: a context or an instance
	cVerb   = "\x1b[35m"   // an action
	cAttr   = "\x1b[32m"   // an attribute's name
	cPunct  = "\x1b[33m"   // what joins them: the '=' of a pair
	cUnres  = "\x1b[31m"   // a word that names nothing yet
	cQuery  = "\x1b[33;1m" // the history search's own brackets
	cSystem = "\x1b[32m"   // this installation's name
	cUser   = "\x1b[36m"   // who is holding the session

	// emphasis is weight rather than hue: a table's column names have
	// no class of their own — they name the classes below them — so
	// giving them a colour would claim something untrue.
	emphasis = "\x1b[1m"
)

// table starts one aligned table that knows whether this session can
// show emphasis.
func (s *session) table() *table { return &table{bold: s.colors} }

func (s *session) color(code, text string) string {
	if !s.colors {
		return text
	}
	return code + text + cReset
}

// paintLine colours the line under the operator's fingers by what the
// console has made of it: places, actions, attribute names and values
// never look alike, and a word that names nothing yet — or names
// several things at once — stays red until it resolves. That last
// part is the useful half: it answers "have you understood me?"
// before the line is ever submitted.
func (s *session) paintLine(line string) string {
	if !s.colors {
		return line
	}
	w := &lineWalk{s: s, path: s.curPath()}
	if strings.HasPrefix(line, "/") {
		w.path = nil
	}
	var b strings.Builder
	rest := line
	for rest != "" {
		token, after, _ := strings.Cut(rest, " ")
		if token != "" {
			b.WriteString(w.paint(token))
		}
		if after != "" || strings.HasSuffix(rest, " ") {
			b.WriteString(" ")
		}
		rest = after
	}
	return b.String()
}

// lineWalk follows a line the way the grammar does, so the painter and
// the parser agree about what each word is.
type lineWalk struct {
	s     *session
	path  []string
	phase int // 0 walking the path, 1 expecting the verb, 2 its arguments
	verb  string
	attrs []schema.Attr
}

func (w *lineWalk) paint(token string) string {
	// A leading slash is itself resolved — it names the root — so it
	// keeps its colour even when what follows names nothing yet.
	lead := ""
	if w.phase == 0 && strings.HasPrefix(token, "/") {
		lead, token = w.s.color(cPath, "/"), token[1:]
		if token == "" {
			w.path = nil
			return lead
		}
	}
	return lead + w.paintRest(token)
}

func (w *lineWalk) paintRest(token string) string {
	switch w.phase {
	case 0:
		if painted, ok := w.paintPath(token); ok {
			return painted
		}
		w.phase = 1
		fallthrough
	case 1:
		w.phase, w.verb = 2, token
		w.attrs = w.s.attrsForLine(w.path, token)
		cands := w.s.verbsAt(w.path)
		if len(w.path) == 0 {
			cands = append(cands, commandNames()...)
		}
		return w.s.mark(cVerb, cands, token)
	default:
		return w.paintArg(token)
	}
}

// paintPath consumes a token that names a place, slash-joined or not,
// and reports whether it was one.
func (w *lineWalk) paintPath(token string) (string, bool) {
	next := append([]string(nil), w.path...)
	for piece := range strings.SplitSeq(token, "/") {
		if piece == "" {
			continue
		}
		switch {
		case piece == "..":
			if len(next) > 0 {
				next = next[:len(next)-1]
			}
		case len(next) == 0 && w.s.kindByName(piece) != nil:
			next = append(next, piece)
		case len(next) == 1 && !w.s.isSingleton(next) && w.s.instances(next[0])[piece] != "":
			next = append(next, piece)
		default:
			return "", false
		}
	}
	w.path = next
	return w.s.color(cPath, token), true
}

// paintArg colours one argument: a pair reads as name, joiner, value;
// a bare word is a name the operator is choosing, so nothing is
// claimed about it.
func (w *lineWalk) paintArg(token string) string {
	name, value, ok := strings.Cut(token, "=")
	if !ok {
		return token
	}
	names := make([]string, 0, len(w.attrs))
	for _, a := range w.attrs {
		names = append(names, a.Name)
	}
	return w.s.mark(cAttr, names, name) + w.s.color(cPunct, "=") + value
}

// mark paints a word in its class when it names exactly one candidate,
// and in the unresolved colour while it still names none or several.
func (s *session) mark(class string, cands []string, word string) string {
	if resolves(cands, word) {
		return s.color(class, word)
	}
	return s.color(cUnres, word)
}

// resolves reports whether a word names something this console would
// actually accept. It asks exactly the question the parser asks — a
// whole name, not a prefix — because the colour is a promise about
// what will happen on Enter, and a promise the parser will not keep is
// worse than no colour at all. Completion is the other half: TAB is
// what turns a prefix into a name.
func resolves(cands []string, word string) bool {
	return slices.Contains(cands, word)
}

// attrsForLine resolves which attributes a verb may name at a place —
// a creation line takes them from the choice typed on it, everything
// else from the instance standing there.
func (s *session) attrsForLine(path []string, verb string) []schema.Attr {
	if verb == verbAdd && len(path) == 1 {
		return s.kindAttrs(path[0], "")
	}
	return s.attrsAt(path)
}

// kindAttrs is a kind's attributes for one choice, choice-less when
// the line has not made it yet.
func (s *session) kindAttrs(kind, choice string) []schema.Attr {
	k := s.kindByName(kind)
	if k == nil {
		return nil
	}
	return k.AttrsFor(choice)
}
