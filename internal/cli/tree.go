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
	// At the root, the tree's own words work without a slash: standing
	// there is standing somewhere, so "export" means what "/export"
	// means and "radio" is the context it names. An operator should
	// not have to know which grammar a word belongs to before typing
	// it — which only holds because no context shares a name with a
	// flat command.
	first, _, _ := strings.Cut(line, " ")
	return isTreeVerb(first) || s.kindByName(first) != nil
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
	tokens, columns := splitArgsAt(line)
	path, rest := s.resolveTree(tokens)
	// Where the leftovers begin, so a refusal can point at the word.
	at := 1
	if consumed := len(tokens) - len(rest); consumed < len(columns) {
		at = columns[consumed]
	}
	if len(rest) == 0 {
		// A pure path is navigation.
		s.setPath(path)
		return
	}
	verb, args := rest[0], rest[1:]
	err := s.treeVerb(ctx, path, verb, args, rest)
	if err == nil {
		return
	}
	// A word the place did not answer to is pointed at, not merely
	// described: the operator sees which one was meant.
	if _, ok := errors.AsType[*unknownVerbError](err); ok {
		fmt.Fprintf(s.out, "error: %s (column %d)\r\n", err, at)
		return
	}
	fmt.Fprintf(s.out, "error: %s\r\n", err)
}

// treeVerb runs one verb at one place, and reports what to say when
// the place does not answer to it.
func (s *session) treeVerb(ctx context.Context, path []string,
	verb string, args, rest []string,
) error {
	switch {
	case verb == helpWord || verb == "help":
		fmt.Fprint(s.out, s.treeHelp(path))
	case verb == cmdQuit || verb == "exit":
		s.dispatch(ctx, rest)
	case verb == verbExport:
		// Every depth answers export — the root's is the whole
		// configuration, the backup an operator can read.
		return s.treeExport(path)
	case verb == verbPrint:
		s.treePrint(ctx, path)
	case verb == verbStatus && len(path) == 2:
		return s.treeStatus(ctx, path)
	case verb == verbSet || verb == verbUnset:
		return s.treeSet(ctx, path, verb, args)
	case verb == verbAdd || verb == verbRemove:
		return s.treeCreateRemove(ctx, path, verb, args)
	case len(path) == 0:
		// Anything else at the root is a flat command — "/status" from
		// a context lands here with the path emptied. The tree's own
		// verbs are answered above, so the root is a context like any
		// other rather than a place where they stop working.
		s.dispatch(ctx, rest)
	case len(path) == 2 && s.mountedVerb(path[0], verb):
		// The flat command runs as itself, told which instance asked.
		s.dispatch(ctx, append(append([]string{verb}, args...), path[0]+"="+path[1]))
	default:
		return &unknownVerbError{verb: verb}
	}
	return nil
}

// unknownVerb carries the word a place did not answer to, so the
// caller can say where on the line it was.
type unknownVerbError struct{ verb string }

func (e *unknownVerbError) Error() string {
	return fmt.Sprintf("no %q here — \"?\" says what this context offers", e.verb)
}

// The mutation verbs.
const (
	verbSet    = "set"
	verbUnset  = "unset"
	verbExport = "export"
	verbAdd    = "add"
	verbRemove = "remove"
	// verbStatus is an instance as it is running, where print is the
	// instance as it was configured. It lives in the tree rather than
	// in the flat table because it only ever answers about the thing
	// the session is standing in.
	verbStatus = "status"
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

// treeStatus shows the instance the session stands in, as it runs.
func (s *session) treeStatus(ctx context.Context, path []string) error {
	in := input{opts: map[string]string{path[0]: path[1]}}
	if path[0] == scopeRelay {
		return s.relayStatus(ctx, in)
	}
	return s.radioStatus(ctx, in)
}

// treePrint shows a context: a kind lists its instances, an instance
// shows its effective configuration with provenance.
func (s *session) treePrint(_ context.Context, path []string) {
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
		var err error
		if path[0] == scopeRelay {
			err = s.relayList()
		} else {
			err = s.radioList()
		}
		if err != nil {
			fmt.Fprintf(s.out, "error: %s\r\n", err)
		}
		return
	}
	if err := s.showTraces(path[0] + " " + path[1]); err != nil {
		fmt.Fprintf(s.out, "error: %s\r\n", err)
	}
}

// treeHelp describes one place, one line per thing that can be typed
// there: the word in the colour of its class, then what it does. No
// column padding — the separator does the aligning work a reader
// actually needs, and a long name would otherwise push every
// description away from the names it belongs to.
func (s *session) treeHelp(path []string) string {
	var b strings.Builder
	// A blank line first: help interrupts a line of work, and the
	// answer reads better with air above it than pressed against the
	// prompt it came from.
	b.WriteString("\r\n")
	var terms []term
	for _, t := range s.termsAt(path) {
		if !t.content {
			terms = append(terms, t)
		}
	}
	// ".." leads, then everything else by name — the order a reader
	// scans in when they do not yet know what they are looking for.
	rest := terms
	if len(rest) > 0 && rest[0].name == ".." {
		s.writeTerm(&b, rest[0])
		rest = rest[1:]
	}
	sort.Slice(rest, func(i, j int) bool { return rest[i].name < rest[j].name })
	for _, t := range rest {
		s.writeTerm(&b, t)
	}
	// A collection holds things the grammar does not name; say how to
	// reach them rather than listing what print already lists.
	if len(path) == 1 && !s.isSingleton(path) {
		fmt.Fprintf(&b, "%s%s%s\r\n",
			s.color(cPath, "<name>"), s.color(cPunct, " -- "),
			"go into one; "+verbPrint+" lists them")
	}
	// An instance also answers about its attributes, which are what
	// its verbs act on.
	if attrs := s.attrsAt(path); len(attrs) > 0 {
		b.WriteString("\r\n")
		for _, a := range attrs {
			doc := a.Doc
			if a.Secret {
				doc += " (secret: never echoed)"
			}
			s.writeTerm(&b, term{name: a.Name, class: cAttr, doc: doc})
		}
	}
	return b.String()
}

// writeTerm renders one entry: the word in its class, the separator
// that joins it to its meaning, the meaning.
func (s *session) writeTerm(b *strings.Builder, t term) {
	fmt.Fprintf(b, "%s%s%s\r\n", s.color(t.class, t.name),
		s.color(cPunct, " -- "), t.doc)
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
	cands := s.candidatesAt(path)
	prefix := strings.TrimPrefix(last, "/")
	matched, common := match(prefix, names(cands))
	switch len(matched) {
	case 0:
		return "", nil
	case 1:
		// A container is completed up to its separator: the operator
		// is mid-path, not mid-word, and the next TAB carries on from
		// there.
		tail := " "
		for _, c := range cands {
			if c.name == matched[0] && c.container {
				tail = "/"
			}
		}
		return matched[0][len(prefix):] + tail, nil
	default:
		return common[len(prefix):], s.listing(matched, cands)
	}
}

// term is one word an operator may type at a place: what it is
// called, which class it belongs to, what it does, and whether typing
// it leaves them mid-path. One type, because help, completion and the
// painter are three views of the same question — and three separate
// lists were three chances to answer it differently.
type term struct {
	name  string
	class string
	doc   string
	// container says the term has things under it, so completing it
	// leaves the operator mid-path rather than mid-word.
	container bool
	// content says the term is a thing that exists rather than a
	// word the grammar defines. Completion offers both; help stays
	// with the grammar, because what exists is print's answer and
	// naming it twice makes one of the two the stale copy.
	content bool
}

// verbDoc says what each of the tree's own verbs does, in the one line
// the help gives it.
var verbDoc = map[string]string{
	verbPrint:  "show what is here",
	verbStatus: "show it as it is running",
	verbExport: "print the lines that would recreate this",
	verbSet:    "change an attribute",
	verbUnset:  "clear an attribute, back to what the preset says",
	verbAdd:    "bring a new one into existence",
	verbRemove: "take one out of existence",
}

// termsAt lists everything typeable at one place.
func (s *session) termsAt(path []string) []term {
	var out []term
	if len(path) > 0 {
		out = append(out, term{name: "..", class: cPath, doc: "go up to " + s.parentOf(path)})
	}
	for _, v := range s.verbNamesAt(path) {
		doc := verbDoc[v]
		if doc == "" {
			if c := lookup(v); c != nil && len(c.forms) > 0 {
				doc = c.forms[0].desc // a mounted command describes itself
			}
		}
		out = append(out, term{name: v, class: cVerb, doc: doc})
	}
	switch {
	case len(path) == 0:
		// A context is typeable at the root with or without its
		// slash, so it is offered either way — which only holds
		// because no context shares a name with a flat command.
		for i := range s.deps.Kinds {
			k := s.deps.Kinds[i]
			out = append(out, term{
				name: k.Name, class: cPath, doc: k.Doc, container: !k.Singleton,
			})
		}
		for _, c := range commands {
			doc := ""
			if len(c.forms) > 0 {
				doc = c.forms[0].desc
			}
			out = append(out, term{name: c.name, class: cVerb, doc: doc})
		}
	case len(path) == 1 && !s.isSingleton(path):
		inst := s.instances(path[0])
		for name := range inst {
			out = append(out, term{name: name, class: cPath, doc: inst[name], content: true})
		}
	}
	return dedupe(out)
}

// parentOf names where ".." leads, so the entry says where it goes
// rather than merely that it goes.
func (s *session) parentOf(path []string) string {
	if len(path) < 2 {
		return "the root"
	}
	return "/" + strings.Join(path[:len(path)-1], "/")
}

// verbNamesAt is what a place answers, without the descriptions — the
// painter asks only whether a word is one of them.
func (s *session) verbNamesAt(path []string) []string {
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
		verbs := []string{verbPrint, verbStatus, verbSet, verbUnset, verbExport}
		for _, v := range []string{
			cmdNeighbours, cmdScopes, cmdDiscover, cmdAdvert,
		} {
			if s.mountedVerb(path[0], v) {
				verbs = append(verbs, v)
			}
		}
		return verbs
	}
}

// verbsAt is kept for the places that only need the names.
func (s *session) verbsAt(path []string) []string { return s.verbNamesAt(path) }

// dedupe keeps the first meaning of each name: a word that is both a
// place and a command is the place it leads to, since that is what the
// parser makes of it.
func dedupe(terms []term) []term {
	seen := make(map[string]bool, len(terms))
	out := terms[:0]
	for _, t := range terms {
		if seen[t.name] {
			continue
		}
		seen[t.name] = true
		out = append(out, t)
	}
	return out
}

// names strips the descriptions off, for the matching that does not
// need them.
func names(terms []term) []string {
	out := make([]string, 0, len(terms))
	for _, t := range terms {
		out = append(out, t.name)
	}
	return out
}

// candidatesAt lists what may legally come next at one place.
func (s *session) candidatesAt(path []string) []term { return s.termsAt(path) }

// listing orders and colours the candidates for the screen: places
// before actions, alphabetical within each, so an operator sees at a
// glance what is somewhere to go and what is something to do.
func (s *session) listing(matched []string, terms []term) []string {
	class := map[string]string{}
	for _, t := range terms {
		class[t.name] = t.class
	}
	places, actions := []string{}, []string{}
	for _, m := range matched {
		if class[m] == cPath {
			places = append(places, m)
			continue
		}
		actions = append(actions, m)
	}
	sort.Strings(places)
	sort.Strings(actions)
	out := make([]string, 0, len(matched))
	for _, m := range append(places, actions...) {
		out = append(out, s.color(class[m], m))
	}
	return out
}

// finishPlain completes against one class of candidate.
func (s *session) finishPlain(prefix string, cands []string, class string) (string, []string) {
	matched, common := match(prefix, cands)
	switch len(matched) {
	case 0:
		return "", nil
	case 1:
		return matched[0][len(prefix):] + " ", nil
	default:
		out := make([]string, 0, len(matched))
		for _, m := range matched {
			out = append(out, s.color(class, m))
		}
		return common[len(prefix):], out
	}
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

// argTermsFor is what may still be written after a verb: the
// attributes a mutation acts on, or the arguments a command takes.
// Help and completion both read it, so an argument nobody can
// discover cannot exist.
func (s *session) argTermsFor(path, rest []string) []term {
	verb := rest[0]
	switch verb {
	case verbSet, verbUnset, verbAdd:
		attrs := s.attrsAt(path)
		if verb == verbAdd && len(path) == 1 && !s.isSingleton(path) {
			// The choice is on the line, not in the store yet.
			attrs = s.attrsForAddLine(path[0], rest)
		}
		out := make([]term, 0, len(attrs))
		for _, a := range attrs {
			doc := a.Doc
			if a.Secret {
				doc += " (secret: never echoed)"
			}
			// unset names an attribute; the others give it a value.
			out = append(out, term{
				name: a.Name, class: cAttr, doc: doc,
				container: verb != verbUnset,
			})
		}
		return out
	default:
		c := lookup(verb)
		if c == nil {
			return nil
		}
		out := make([]term, 0, len(c.flags))
		for _, f := range c.flags {
			// A relay is already chosen when standing inside one.
			if f.name == scopeRelay && len(path) == 2 {
				continue
			}
			out = append(out, term{
				name: f.name, class: cAttr, doc: f.doc, container: f.valued,
			})
		}
		return out
	}
}

// completeArgs finishes what comes after a verb, and — past the '=' —
// the values a closed set allows.
func (s *session) completeArgs(path, rest []string, last string) (add string, hints []string) {
	terms := s.argTermsFor(path, rest)
	if len(terms) == 0 {
		return "", nil
	}
	if attr, val, has := strings.Cut(last, "="); has {
		for _, a := range s.attrsAt(path) {
			if a.Name == attr && len(a.Enum) > 0 {
				return s.finishPlain(val, a.Enum, cAttr)
			}
		}
		return "", nil
	}
	// A term that takes a value completes up to its '=', the way a
	// context completes up to its slash: the operator is mid-argument,
	// not mid-word.
	words := make([]string, 0, len(terms))
	takesValue := map[string]bool{}
	for _, t := range terms {
		word := t.name
		if t.container {
			word += "="
		}
		takesValue[word] = t.container
		words = append(words, word)
	}
	add, hints = s.finishPlain(last, words, cAttr)
	if strings.HasSuffix(strings.TrimSuffix(add, " "), "=") {
		add = strings.TrimSuffix(add, " ")
	}
	return add, hints
}

// match reports which candidates a prefix could still become, and how
// far they agree — the point past which only the operator can choose.
func match(prefix string, cands []string) (matched []string, common string) {
	for _, c := range cands {
		if strings.HasPrefix(c, prefix) {
			matched = append(matched, c)
		}
	}
	sort.Strings(matched)
	if len(matched) == 0 {
		return nil, prefix
	}
	common = matched[0]
	for _, m := range matched[1:] {
		for !strings.HasPrefix(m, common) {
			common = common[:len(common)-1]
		}
	}
	return matched, common
}

// helpForLine answers the help key. Pressed again it moves on rather
// than repeating itself: what a place offers, then how to work the
// console, then back — because the second question is the one an
// operator has once the first is answered.
func (s *session) helpForLine(line string, level int) string {
	if level%2 == 1 {
		return "\r\n" + keyHelp + "\r\npress it again for what this place offers\r\n"
	}
	path, rest := s.resolveTree(splitArgs(line))
	// A verb already typed changes the question: not "what can I do
	// here" but "what does this take". The line asked it, so answer
	// that one.
	if len(rest) > 0 {
		if terms := s.argTermsFor(path, rest); len(terms) > 0 {
			return s.renderTerms(rest[0], terms) + "\r\npress it again for the keys\r\n"
		}
	}
	return s.treeHelp(path) + "\r\npress it again for the keys\r\n"
}

// renderTerms lists what a verb takes, one entry per line, headed by
// the verb so the answer says what it is about.
func (s *session) renderTerms(verb string, terms []term) string {
	var b strings.Builder
	b.WriteString("\r\n")
	sorted := append([]term(nil), terms...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].name < sorted[j].name })
	for _, t := range sorted {
		name := t.name
		if t.container {
			name += "=" // it wants a value, and says so where it is read
		}
		s.writeTerm(&b, term{name: name, class: t.class, doc: t.doc})
	}
	if len(sorted) == 0 {
		fmt.Fprintf(&b, "%s takes no arguments\r\n", s.color(cVerb, verb))
	}
	return b.String()
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
