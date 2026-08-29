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
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

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
	// The word may already carry its path: "radio/" and
	// "radio/slot1" name the same place the bare word does, and
	// completion produces exactly those shapes.
	head, _, _ := strings.Cut(first, "/")
	return isTreeVerb(first) || s.kindByName(head) != nil
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
	case scopeMQTT:
		for _, mq := range s.mqtts() {
			out[mq.Name] = mq.URL
		}
	}
	return out
}

// mqtts is the live view of the observer connections.
func (s *session) mqtts() []MQTTInfo {
	if s.deps.LiveMQTTs != nil {
		return s.deps.LiveMQTTs()
	}
	return nil
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
		// /relay x name the same place, and /relay/x/print asks
		// something of it without a space anywhere. Only tokens still
		// being read as path are split, so a value carrying slashes is
		// never touched.
		next, leftover, ok := s.walkStep(path, tokens[0])
		if !ok {
			return path, tokens
		}
		path = next
		if leftover != "" {
			return path, append([]string{leftover}, tokens[1:]...)
		}
		tokens = tokens[1:]
	}
	return path, nil
}

// walkPiece takes one piece of a slash-joined token as a step of the
// path. It is the only place that answers "is this word a place from
// here", so the parser and the painter cannot disagree about it.
func (s *session) walkPiece(path []string, piece string) ([]string, bool) {
	switch {
	case piece == "":
		return path, true // a doubled or trailing slash says nothing
	case piece == "..":
		if len(path) > 0 {
			return path[:len(path)-1], true
		}
		return path, true
	case len(path) == 0 && s.kindByName(piece) != nil:
		return append(path, piece), true
	case len(path) == 1 && !s.isSingleton(path):
		if _, ok := s.instances(path[0])[piece]; !ok {
			return nil, false
		}
		return append(path, piece), true
	case len(path) == 1 && s.isSingleton(path) && drawerOn(path[0], piece) != nil,
		len(path) == 2 && !s.isSingleton(path[:1]) && drawerOn(path[0], piece) != nil:
		return append(path, piece), true
	case s.placeAt(path) == atDrawer:
		if _, ok := s.drawerKeys(path)[piece]; !ok {
			return nil, false
		}
		return append(path, piece), true
	}
	return nil, false
}

// walkStep consumes the leading path steps of one token and hands back
// what is left of it, because a token may name a place and then ask
// something of it in the same breath. ok is false when the first piece
// is not a step at all, which leaves the token to be read as a word.
func (s *session) walkStep(path []string, token string) (next []string, leftover string, ok bool) {
	next = append([]string(nil), path...)
	pieces := strings.Split(token, "/")
	for i, piece := range pieces {
		step, took := s.walkPiece(next, piece)
		if !took {
			return next, strings.Join(pieces[i:], "/"), i > 0
		}
		next = step
	}
	return next, "", true
}

// tree runs one line of the context grammar.
func (s *session) tree(ctx context.Context, line string) {
	tokens, columns := splitArgsAt(line)
	path, rest := s.resolveTree(tokens)
	// Where the leftovers begin, so a refusal can point at the word.
	at := 1
	if consumed := len(tokens) - len(rest); consumed < len(columns) {
		// The leftover is a suffix of the token it came out of, so the
		// difference in length is how far into it the word sits: a
		// refusal of /relay/x/zz points at zz, not at the path.
		at = columns[consumed] + len(tokens[consumed]) - len(rest[0])
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
	// A place answers to the verbs it lists and no others, so what
	// help prints and what the dispatcher honours cannot drift apart.
	// The root is the exception it has always been: the flat commands
	// live there too, and they are not the tree's to list.
	if !s.answersHere(path, verb) {
		return &unknownVerbError{verb: verb}
	}
	switch {
	case verb == helpWord || verb == wordHelp:
		fmt.Fprint(s.out, s.treeHelp(path))
	case slices.Contains(args, helpWord):
		// A verb asked about answers what it takes, whether the
		// question arrives as the key or as a word on the line. The
		// tree's verbs owe that as much as the flat commands do.
		fmt.Fprint(s.out, s.renderTerms(verb, s.argTermsFor(path, rest)))
	case verb == cmdQuit || verb == wordExit:
		s.dispatch(ctx, rest)
	case verb == verbExport:
		// Every depth answers export — the root's is the whole
		// configuration, the backup an operator can read.
		return s.treeExport(path)
	case verb == verbPrint:
		return s.treePrint(ctx, path, args)
	case verb == verbStatus && len(path) == 2:
		return s.treeStatus(ctx, path)
	case verb == verbSet || verb == verbUnset:
		return s.treeSet(ctx, path, verb, args)
	case verb == verbDisable || verb == verbEnable:
		return s.treeToggle(ctx, path, verb, args)
	case verb == verbAdd || verb == verbRemove:
		return s.treeCreateRemove(ctx, path, verb, args)
	case len(path) == 0:
		// Anything else at the root is a flat command — "/status" from
		// a context lands here with the path emptied. The tree's own
		// verbs are answered above, so the root is a context like any
		// other rather than a place where they stop working.
		s.rootDispatch(ctx, rest)
	case s.mountsHere(path, verb):
		// The flat command runs as itself, told what the place stands
		// for: standing somewhere is saying which.
		s.dispatch(ctx, append(append([]string{verb}, args...), s.scopeFlags(path)...))
	default:
		return &unknownVerbError{verb: verb}
	}
	return nil
}

// answersHere reports whether a place answers to a verb. Every place
// answers the two ways to ask what is here and the two ways to leave;
// beyond those it answers to what it lists and nothing else. The root
// is the exception it has always been: the flat commands live there
// too, and they are not the tree's to list.
func (s *session) answersHere(path []string, verb string) bool {
	switch verb {
	case helpWord, wordHelp, cmdQuit, wordExit:
		return true
	}
	return s.placeAt(path) == atRoot || slices.Contains(s.verbNamesAt(path), verb)
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
	// disable parks an object without losing its configuration —
	// sugar over set disabled=, the flag print marks with an X.
	verbDisable = "disable"
	verbEnable  = "enable"
)

// disableable says which kinds answer to disable and enable — the
// ones whose objects may exist without running.
func disableable(kind string) bool { return kind == scopeMQTT }

// treeToggle serves disable and enable: from the collection with a
// name on the line, from the instance with nothing — both land as the
// one mutation set disabled= would be.
func (s *session) treeToggle(ctx context.Context, path []string, verb string, args []string) error {
	if s.deps.Mutate == nil {
		return errors.New("this daemon has no mutation channel")
	}
	if s.deps.Privilege != Admin {
		return fmt.Errorf("%s is an admin verb — use the local console socket", verb)
	}
	var name string
	switch {
	case len(path) == 2:
		name = path[1]
		if len(args) > 0 {
			return fmt.Errorf("%s here works on %s — nothing else on the line", verb, name)
		}
	case len(args) != 1:
		return fmt.Errorf("usage: %s <name>", verb)
	default:
		name = args[0]
	}
	value := "yes"
	if verb == verbEnable {
		value = "no"
	}
	msg, err := s.deps.Mutate(ctx, path[0], name, map[string]string{"disabled": value}, nil, "console")
	if err != nil {
		return err
	}
	fmt.Fprintf(s.out, "%s\r\n", msg)
	return nil
}

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
	if verb == verbAdd && strings.Contains(name, "=") {
		return fmt.Errorf("%q reads like an attribute — the name comes first: %s <name> [attr=value …]",
			name, verbAdd)
	}
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
	attrs, err := attrPairs(rest)
	if err != nil {
		return err
	}
	msg, err := s.deps.Create(ctx, kind, name, attrs, "console")
	if err != nil {
		return err
	}
	fmt.Fprintf(s.out, "%s\r\n", msg)
	return nil
}

// attrPairs reads the attr=value words of an add line.
func attrPairs(words []string) (map[string]string, error) {
	attrs := map[string]string{}
	for _, a := range words {
		k, v, ok := strings.Cut(a, "=")
		if !ok {
			return nil, fmt.Errorf("%q — add wants attr=value after the name", a)
		}
		attrs[k] = v
	}
	return attrs, nil
}

// treeSet applies one line of changes: every attr=value on it lands
// in one transaction, so a retune touching three attributes bounces
// the relay once, not three times.
func (s *session) treeSet(ctx context.Context, path []string, verb string, args []string) error {
	// Standing in a drawer, set is the item's own — the access
	// list's role — and never the config door: what a drawer holds
	// is the mesh's doing, not the file's.
	if site := s.drawerSiteAt(path); site != nil {
		return s.drawerSet(ctx, site, verb, args)
	}
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

// drawerSet is set standing in a drawer: refused everywhere except on
// an item whose drawer offers one, under the same admin gate as the
// drawer's other mutations.
func (s *session) drawerSet(ctx context.Context, site *drawerSite, verb string, args []string) error {
	if site.d.itemSet == nil || site.item == "" {
		return fmt.Errorf("nothing is settable in a %s", site.d.name)
	}
	if verb == verbUnset {
		return fmt.Errorf("nothing to unset here — %s removes the entry", cmdRevoke)
	}
	if s.deps.Privilege != Admin {
		return fmt.Errorf("%s is an admin verb — use the local console socket", verb)
	}
	set := map[string]string{}
	for _, a := range args {
		name, value, has := strings.Cut(a, "=")
		if !has {
			return fmt.Errorf("%q — set wants attr=value", a)
		}
		set[name] = value
	}
	if len(set) == 0 {
		return fmt.Errorf("usage: %s attr=value …", verbSet)
	}
	return site.d.itemSet(s, ctx, site, set)
}

// treeExport prints configuration as the absolute lines that would
// recreate it — pasteable anywhere, which is not scripting. From the
// root it exports everything, from a collection its instances, from
// an instance itself. It withholds nothing, secrets included: an
// export whose purpose is to recreate a relay cannot leave out the
// part that makes it that relay.
func (s *session) treeExport(path []string) error {
	switch len(path) {
	case 0:
		// Every collection, radios first: a relay's add names its
		// radio and an observer's names its relay, so the paste has to
		// work in that order. Deriving the rest from the declared
		// kinds keeps a new collection from being forgotten here.
		kinds := []string{scopeRadio}
		for i := range s.deps.Kinds {
			if k := s.deps.Kinds[i]; !k.Singleton && k.Name != scopeRadio {
				kinds = append(kinds, k.Name)
			}
		}
		for _, kind := range kinds {
			s.exportCollection(kind)
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
		s.exportCollection(path[0])
		return nil
	default:
		if _, ok := s.traces()[path[0]+" "+path[1]]; !ok {
			return fmt.Errorf("no configuration recorded for %s %s", path[0], path[1])
		}
		s.exportInstance(path[0], path[1])
		return nil
	}
}

// exportCollection prints a kind's instances, names sorted.
func (s *session) exportCollection(kind string) {
	names := make([]string, 0, 2)
	for name := range s.instances(kind) {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		s.exportInstance(kind, name)
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
	var pairs [][2]string
	for _, t := range traces {
		if strings.HasPrefix(t.Source, "profile:") {
			continue // the preset restates itself
		}
		pairs = append(pairs, [2]string{t.Key, exportValue(t.Value)})
	}
	s.exportLine(kind, verbAdd, name, pairs)
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

// exportValue renders one value the way set accepts it back. Every
// list joins on commas whatever its element type, because commas are
// the only form the parser reads: a list rendered any other way makes
// a line that cannot be pasted back, which is the one thing an export
// has to be.
func exportValue(v any) string {
	rv := reflect.ValueOf(v)
	if rv.IsValid() && rv.Kind() == reflect.Slice &&
		rv.Type().Elem().Kind() != reflect.Uint8 {
		parts := make([]string, rv.Len())
		for i := range parts {
			parts[i] = fmt.Sprintf("%v", rv.Index(i).Interface())
		}
		return quoteIfSpaced(strings.Join(parts, ","))
	}
	return quoteIfSpaced(fmt.Sprintf("%v", v))
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

// print's arguments. They are orthogonal on purpose: one is about the
// shape of the answer, the other about what the answer leaves out.
const (
	// argDetail unfolds a summary. Only a collection has one, so it
	// is the only place the word means anything.
	argDetail = "detail"
	// argSecrets lifts the mask. print holds it up by default so a
	// scrollback does not end up carrying a private key by accident;
	// asking for it by name is the deliberate act.
	argSecrets = "show-secrets"
	// argInterval draws the same view over and over in place. It says
	// nothing about what the view holds — only how often it is asked
	// again.
	argInterval = "interval"
)

// intervalStop is what the status line under a repainting frame says.
// A live view here ends on a line, not a keystroke: the editor owns
// the keys, and every other live view in this console ends the same
// way.
const intervalStop = "enter stops"

// detailWidth is the column a detail paragraph wraps at. The console
// never measures the terminal — its probe asks whether one is there,
// not how wide — so this is the width nearly every terminal has at
// least.
const detailWidth = 80

// mountsHere reports whether a flat command answers at this place: on
// an instance it acts on, in a drawer that claims it, or on one of the
// things that drawer holds.
func (s *session) mountsHere(path []string, verb string) bool {
	if site := s.drawerSiteAt(path); site != nil {
		if site.item == "" {
			return slices.Contains(site.d.verbs, verb)
		}
		return slices.Contains(site.d.itemVerbs, verb)
	}
	switch s.placeAt(path) {
	case atInstance:
		return s.mountedVerb(path[0], verb)
	case atSingleton:
		c := lookup(verb)
		return c != nil && c.onOne && c.on == path[0]
	default:
		return false
	}
}

// scopeFlags names what the place stands for, in the words the command
// declares — the instance always, and the one thing inside a drawer
// when the session is standing on it.
func (s *session) scopeFlags(path []string) []string {
	site := s.drawerSiteAt(path)
	var out []string
	if (site == nil || site.instance != "") && len(path) >= 2 {
		out = append(out, path[0]+"="+path[1])
	}
	if site != nil && site.item != "" {
		out = append(out, site.d.itemFlag+"="+site.item)
	}
	return out
}

// mountedVerb reports whether a flat command serves this kind's
// instances directly. A command that declares a flag named after the
// kind is a command that acts on one of them, so standing inside an
// instance already answers which — the same reading that lets relay=
// complete with the relays. Nothing has to be listed twice.
func (s *session) mountedVerb(kind, verb string) bool {
	c := lookup(verb)
	if c == nil {
		return false
	}
	spec := c.flag(kind)
	return spec != nil && spec.valued && !claimedByDrawer(kind, verb)
}

// treeStatus shows the instance the session stands in, as it runs.
func (s *session) treeStatus(ctx context.Context, path []string) error {
	in := input{opts: map[string]string{path[0]: path[1]}}
	switch path[0] {
	case scopeRelay:
		return s.relayStatus(ctx, in)
	case scopeMQTT:
		return s.mqttStatus(path[1])
	default:
		return s.radioStatus(ctx, in)
	}
}

// treePrint shows a context: a kind lists its instances, an instance
// shows its effective configuration with provenance.
// printArgs is what one print line asked for: how the answer should
// be shaped, what it may leave out, and how often to ask again.
type printArgs struct {
	detail, secrets bool
	every           time.Duration
	// sel is the temporal slice, honoured only where a drawer says it
	// is windowed.
	sel frameSelectors
}

// printArgsFrom reads print's arguments, and refuses a word by naming
// the ones that would have worked where it was written.
func (s *session) printArgsFrom(path, args []string) (printArgs, error) {
	var want printArgs
	windowed := map[string]string{}
	site := s.drawerSiteAt(path)
	takesWindow := site != nil && site.d.windowed && site.item == ""
	for _, a := range args {
		key, value, valued := strings.Cut(a, "=")
		switch {
		case key == argDetail && !valued:
			want.detail = true
		case key == argSecrets && !valued:
			want.secrets = true
		case key == argInterval && !valued:
			return want, fmt.Errorf("%s wants a value — %s=2s", argInterval, argInterval)
		case key == argInterval:
			d, err := time.ParseDuration(value)
			if err != nil || d < time.Second {
				return want, fmt.Errorf("%s wants a duration of a second or more, like 2s", argInterval)
			}
			want.every = d
		case takesWindow && valued && isWindowWord(key):
			windowed[key] = value
		default:
			return want, fmt.Errorf("%s takes %s, not %q",
				verbPrint, humanList(names(s.printTerms(path))), key)
		}
	}
	if len(windowed) > 0 {
		sel, err := parseFrameSelectors(windowed, time.Now())
		if err != nil {
			return want, err
		}
		want.sel = sel
	}
	return want, nil
}

// isWindowWord says whether a key is one of the temporal selectors —
// the vocabulary frames speaks, honoured by the windowed drawers.
func isWindowWord(key string) bool {
	switch key {
	case optLast, optSince, optUntil, optAround, optSpan:
		return true
	}
	return false
}

func (s *session) treePrint(ctx context.Context, path []string, args []string) error {
	want, err := s.printArgsFrom(path, args)
	if err != nil {
		return err
	}
	detail, secrets, every := want.detail, want.secrets, want.every
	at := s.placeAt(path)
	summarises := at == atCollection || at == atDrawer
	switch {
	case detail && !summarises:
		// Only a summary can be unfolded, and a listing is the one
		// view that summarises. Everywhere else print already shows
		// every attribute there is.
		return fmt.Errorf("nothing here to %s: %s already shows every attribute",
			argDetail, verbPrint)
	case secrets && (at == atDrawer || at == atDrawerItem):
		return fmt.Errorf("nothing in a %s is masked: it holds what the mesh is doing, "+
			"not what it was told to do", path[2])
	case secrets && at == atRoot:
		return errors.New("the root holds no values, secret or otherwise")
	case secrets && at == atCollection && !detail:
		return fmt.Errorf("this listing shows no attributes — add %s", argDetail)
	}
	draw := func() error { return s.printOnce(ctx, path, want) }
	if every > 0 {
		return s.repaint(ctx, every, draw)
	}
	return draw()
}

// printOnce draws the view one time. It is the whole of print when no
// interval was asked for, and one frame of it when there was.
func (s *session) printOnce(ctx context.Context, path []string, want printArgs) error {
	detail, secrets := want.detail, want.secrets
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
		return tb.flush(s.out)
	}
	if s.isSingleton(path) {
		return s.showTraces(path[0], secrets)
	}
	if len(path) == 1 {
		// A collection is the one place print summarises: it names
		// its instances and little else. detail is what unfolds them.
		if detail {
			return s.printDetail(path[0], secrets)
		}
		switch path[0] {
		case scopeRelay:
			return s.relayList()
		case scopeMQTT:
			return s.mqttList()
		default:
			return s.radioList()
		}
	}
	switch s.placeAt(path) {
	case atDrawer:
		return s.printDrawer(ctx, path, detail, want.sel)
	case atDrawerItem:
		return s.printDrawerItem(ctx, path)
	default:
		return s.showTraces(path[0]+" "+path[1], secrets)
	}
}

// repaint draws a view over and over in the same place: the cursor
// goes back up by the frame's own height, and every line is rewritten
// and erased to its end, so a value that shrank leaves nothing of the
// longer one behind. A status line sits under the frame, which is
// where the cursor waits between draws.
func (s *session) repaint(ctx context.Context, every time.Duration, draw func() error) error {
	if !s.colors {
		return fmt.Errorf("%s draws in place, which needs a terminal", argInterval)
	}
	tick := time.NewTicker(every)
	defer tick.Stop()
	height := 0
	frame := func() error {
		body, err := s.capture(draw)
		if err != nil {
			return err
		}
		if height > 0 {
			fmt.Fprintf(s.out, "\x1b[%dA", height)
		}
		lines := strings.Split(strings.TrimSuffix(body, "\r\n"), "\r\n")
		for _, line := range lines {
			fmt.Fprintf(s.out, "\r%s\x1b[K\r\n", line)
		}
		// A frame that shrank leaves rows of the taller one below it,
		// so they are blanked and stay counted: the next draw has to
		// climb over them to reach the top.
		for i := len(lines); i < height; i++ {
			fmt.Fprint(s.out, "\r\x1b[K\r\n")
		}
		height = max(len(lines), height)
		fmt.Fprintf(s.out, "-- [%s]\x1b[K\r", intervalStop)
		return nil
	}
	done := func() { fmt.Fprint(s.out, "\x1b[K\r\n") }
	if err := frame(); err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			done()
			return nil
		case line, ok := <-s.lines:
			// The line that stopped the view still runs, so a session
			// never loses the command that ended it.
			done()
			if ok && line != "" {
				s.command(ctx, line)
			}
			return nil
		case <-tick.C:
			if err := frame(); err != nil {
				done()
				return err
			}
		}
	}
}

// capture runs a draw against a buffer instead of the transport, so a
// frame can be measured before any of it is sent.
func (s *session) capture(draw func() error) (string, error) {
	var b strings.Builder
	was := s.out
	s.out = &b
	err := draw()
	s.out = was
	return b.String(), err
}

// printDetail unfolds a collection: one paragraph per instance, every
// attribute it holds. Provenance is print's own answer and stays in
// its table — a paragraph has no column to put it in, and weight
// alone must never be the only thing that says it.
func (s *session) printDetail(kind string, secrets bool) error {
	names := make([]string, 0, 4)
	for name := range s.instances(kind) {
		names = append(names, name)
	}
	sort.Strings(names)
	if len(names) == 0 {
		fmt.Fprintf(s.out, "no %s configured\r\n", kind)
		return nil
	}
	gutter := 0
	for _, name := range names {
		gutter = max(gutter, len(name))
	}
	// A space before the handle, two after the longest of them.
	gutter += 3
	for i, name := range names {
		if i > 0 {
			fmt.Fprint(s.out, "\r\n")
		}
		masked := map[string]bool(nil)
		if !secrets {
			masked = s.secretAttrs(kind + " " + name)
		}
		traces := s.traces()[kind+" "+name]
		pairs := make([][2]string, 0, len(traces))
		for _, t := range traces {
			value := exportValue(t.Value)
			if masked[t.Key] {
				value = maskedValue
			}
			pairs = append(pairs, [2]string{t.Key, value})
		}
		s.writeDetail(name, gutter, pairs)
	}
	return nil
}

// writeDetail renders one object as a paragraph: its handle in the
// gutter, then its attributes as pairs packed to the width, each
// continuation line starting under the gutter so the handle column
// stays clear. The values arrive rendered — whoever holds them knows
// what they are, and a second pass over a value that was already
// written for reading is a second pair of quotes around it.
func (s *session) writeDetail(handle string, gutter int, pairs [][2]string) {
	var b strings.Builder
	b.WriteString(" " + s.color(cPath, handle))
	b.WriteString(strings.Repeat(" ", gutter-1-len(handle)))
	col := gutter
	for _, p := range pairs {
		// Width arithmetic runs on the plain text: the escapes take up
		// no columns, and counting them would wrap early.
		if width := len(p[0]) + 1 + len(p[1]) + 1; col > gutter && col+width > detailWidth {
			b.WriteString("\r\n" + strings.Repeat(" ", gutter))
			col = gutter
		}
		b.WriteString(s.color(cAttr, p[0]) + s.color(cPunct, "=") + p[1] + " ")
		col += len(p[0]) + 1 + len(p[1]) + 1
	}
	fmt.Fprintf(s.out, "%s\r\n", b.String())
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
	if at := s.placeAt(path); at == atCollection || at == atDrawer {
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
	name := s.color(t.class, t.name)
	if t.placeholder {
		// Chevrons are punctuation, and they say the word is a slot
		// rather than something to type as it stands.
		name = s.color(cPunct, "<") + name + s.color(cPunct, ">")
	}
	fmt.Fprintf(b, "%s%s%s\r\n", name, s.color(cPunct, " -- "), t.doc)
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
	// The word under construction may carry path steps of its own:
	// "radio/" stands inside radio, and what follows it is a verb.
	// Only the word still being read as path is split, so a value
	// carrying slashes never is.
	prefix := strings.TrimPrefix(last, "/")
	if i := strings.LastIndex(prefix, "/"); i >= 0 {
		next, leftover, ok := s.walkStep(path, prefix[:i])
		if !ok || leftover != "" {
			return "", nil // the steps so far name nowhere
		}
		path, prefix = next, prefix[i+1:]
	}
	cands := s.candidatesAt(path)
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
	// placeholder says the term stands for a value rather than naming
	// itself: help describes it, in chevrons, but completion has
	// nothing to offer for a word the operator invents.
	placeholder bool
	// content says the term is a thing that exists rather than a
	// word the grammar defines. Completion offers both; help stays
	// with the grammar, because what exists is print's answer and
	// naming it twice makes one of the two the stale copy.
	content bool
}

// instanceNameTerms completes an instance name where a collection
// verb wants one; on an instance there is nothing left to say.
func (s *session) instanceNameTerms(path []string) []term {
	if len(path) != 1 || s.isSingleton(path) {
		return nil
	}
	instances := s.instances(path[0])
	out := make([]term, 0, len(instances))
	for name, choice := range instances {
		out = append(out, term{name: name, class: cAttr, doc: choice})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// verbDoc says what each of the tree's own verbs does, in the one line
// the help gives it.
var verbDoc = map[string]string{
	verbPrint:   "show what is here",
	verbStatus:  "show it as it is running",
	verbExport:  "print the lines that would recreate this",
	verbSet:     "change an attribute",
	verbUnset:   "clear an attribute, back to what the preset says",
	verbAdd:     "bring a new one into existence",
	verbRemove:  "take one out of existence",
	verbDisable: "park it: keep the configuration, stop running it",
	verbEnable:  "unpark it and run it again",
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
		out = append(out, s.rootTerms()...)
	case len(path) == 1 && !s.isSingleton(path):
		inst := s.instances(path[0])
		for name := range inst {
			out = append(out, term{name: name, class: cPath, doc: inst[name], content: true})
		}
	case s.placeAt(path) == atSingleton || s.placeAt(path) == atInstance:
		// A drawer is grammar, not content: every holder of the kind
		// has the same ones, and they are named rather than listed by
		// print the way an instance is.
		for _, d := range drawersOn(path[0]) {
			out = append(out, term{name: d.name, class: cPath, doc: d.doc, container: true})
		}
	case s.placeAt(path) == atDrawer:
		held := s.drawerKeys(path)
		for key := range held {
			out = append(out, term{name: key, class: cPath, doc: held[key], content: true})
		}
	}
	return dedupe(out)
}

// rootTerms is what the root itself offers: the contexts, and the
// commands that do not live somewhere more specific. A context is
// typeable with or without its slash, so it is offered either way —
// which only holds because no context shares a name with a flat
// command.
func (s *session) rootTerms() []term {
	var out []term
	for i := range s.deps.Kinds {
		k := s.deps.Kinds[i]
		// A singleton used to be a leaf; one that holds a drawer is a
		// container like any collection, and completing it must leave
		// the operator mid-path rather than at a dead-ended space.
		out = append(out, term{
			name: k.Name, class: cPath, doc: k.Doc,
			container: !k.Singleton || len(drawersOn(k.Name)) > 0,
		})
	}
	for _, c := range commands {
		if commandHome(c) != "" {
			continue // it lives in its context, and is offered there
		}
		doc := ""
		if len(c.forms) > 0 {
			doc = c.forms[0].desc
		}
		out = append(out, term{name: c.name, class: cVerb, doc: doc})
	}
	return out
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
		// One block, always there: set it, clear it, take it away —
		// and whatever commands name the block as their subject.
		verbs := []string{verbPrint, verbSet, verbUnset, verbExport, verbRemove}
		for _, v := range commandNames() {
			if c := lookup(v); c != nil && c.onOne && c.on == path[0] {
				verbs = append(verbs, v)
			}
		}
		return verbs
	case len(path) == 1:
		verbs := []string{verbPrint, verbAdd, verbRemove, verbExport}
		if disableable(path[0]) {
			verbs = append(verbs, verbDisable, verbEnable)
		}
		return verbs
	default:
		if site := s.drawerSiteAt(path); site != nil {
			// A drawer holds what the mesh is doing. There is nothing
			// to set and nothing to export, so reading is all it
			// answers to — that, and the commands whose subject is
			// what it holds, or the one thing being stood on.
			verbs := []string{verbPrint}
			if site.item == "" {
				return append(verbs, site.d.verbs...)
			}
			verbs = append(verbs, site.d.itemVerbs...)
			if site.d.itemSet != nil {
				verbs = append(verbs, verbSet)
			}
			return verbs
		}
		verbs := []string{verbPrint, verbStatus, verbSet, verbUnset, verbExport}
		if disableable(path[0]) {
			verbs = append(verbs, verbDisable, verbEnable)
		}
		for _, v := range commandNames() {
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
	case verbPrint:
		return s.printTerms(path)
	case verbRemove, verbDisable, verbEnable:
		return s.instanceNameTerms(path)
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
		out := make([]term, 0, len(c.flags)+1)
		if c.takes != nil {
			out = append(out, term{
				name: c.takes.name, class: cAttr, doc: c.takes.doc, placeholder: true,
			})
		}
		for _, f := range c.flags {
			// The place fills what it stands for: the instance once
			// inside one, and the item when standing on it.
			if f.name == scopeRelay && len(path) >= 2 {
				continue
			}
			if site := s.drawerSiteAt(path); site != nil && f.name == site.d.itemFlag {
				continue
			}
			out = append(out, term{
				name: f.name, class: cAttr, doc: f.doc, container: f.valued,
			})
		}
		return out
	}
}

// printTerms is what print accepts where it is asked. A word is only
// offered where it would work: unfolding needs a summary to unfold,
// and lifting the mask needs values to lift it from.
func (s *session) printTerms(path []string) []term {
	unfold := term{
		name: argDetail, class: cAttr,
		doc: "open each one out into its attributes",
	}
	unmask := term{
		name: argSecrets, class: cAttr,
		doc: "show what " + verbPrint + " masks",
	}
	again := term{
		name: argInterval, class: cAttr, container: true,
		doc: "draw it again this often, in place",
	}
	switch s.placeAt(path) {
	case atCollection:
		return []term{unfold, unmask, again}
	case atSingleton, atInstance:
		return []term{unmask, again}
	case atDrawer:
		// Nothing here was configured, so nothing here is masked.
		out := []term{unfold, again}
		if site := s.drawerSiteAt(path); site != nil && site.d.windowed {
			out = append(out,
				term{name: optLast, class: cAttr, container: true,
					doc: "the newest slice — a count (50) or a span (15m)"},
				term{name: optSince, class: cAttr, container: true,
					doc: "window start, as the views print moments"},
				term{name: optUntil, class: cAttr, container: true,
					doc: "window end"},
				term{name: optAround, class: cAttr, container: true,
					doc: "the window around one revision, by id"},
				term{name: optSpan, class: cAttr, container: true,
					doc: "how far around, each side (default 1m)"},
			)
		}
		return out
	default:
		return []term{again}
	}
}

// humanList joins words the way a refusal reads them out.
func humanList(words []string) string {
	quoted := make([]string, len(words))
	for i, w := range words {
		quoted[i] = strconv.Quote(w)
	}
	switch len(quoted) {
	case 0:
		return "no arguments"
	case 1:
		return quoted[0]
	default:
		return strings.Join(quoted[:len(quoted)-1], ", ") + " or " + quoted[len(quoted)-1]
	}
}

// completeValue finishes what comes after a '=': the closed set an
// attribute allows, the values a flag declares for itself, or the
// names of whatever a flag named after a kind takes.
func (s *session) completeValue(path, rest []string, attr, val string) (string, []string) {
	for _, a := range s.attrsAt(path) {
		if a.Name == attr && len(a.Enum) > 0 {
			return s.finishPlain(val, a.Enum, cAttr)
		}
	}
	// The profile attribute completes from its kind's preset catalog,
	// resolved against whatever choice the line or the instance has
	// already made — plus "custom", the empty base every catalog
	// implies.
	if attr == "profile" && len(path) >= 1 {
		if k := s.kindByName(path[0]); k != nil && k.Profiles != nil {
			words := k.Profiles(s.choiceOn(k, path, rest))
			if !slices.Contains(words, "custom") {
				words = append(words, "custom")
			}
			sort.Strings(words)
			return s.finishPlain(val, words, cAttr)
		}
	}
	// A flag that knows its own values completes from them, declared
	// beside the flag rather than in a second list.
	if c := lookup(rest[0]); c != nil {
		if f := c.flag(attr); f != nil && f.values != nil {
			return s.finishPlain(val, f.values(s), cAttr)
		}
	}
	// A flag named after a kind takes that kind's names — relay= wants
	// a relay — so the names are what completes it. The flag needs no
	// declaration for this: it is called relay because a relay is what
	// it takes.
	if k := s.kindByName(attr); k != nil && !k.Singleton {
		held := s.instances(attr)
		words := make([]string, 0, len(held))
		for name := range held {
			words = append(words, name)
		}
		sort.Strings(words)
		return s.finishPlain(val, words, cPath)
	}
	return "", nil
}

// choiceOn resolves which choice governs a line: the instance's own
// when standing on one, overridden by a choice attribute already
// written on the line — the add case, where the object is not in the
// store yet.
func (s *session) choiceOn(k *schema.Kind, path, rest []string) string {
	choice := ""
	if k.ChoiceAttr == "" {
		return choice
	}
	if len(path) == 2 {
		choice = s.instances(path[0])[path[1]]
	}
	for _, t := range rest[1:] {
		if v, ok := strings.CutPrefix(t, k.ChoiceAttr+"="); ok {
			choice = v
		}
	}
	return choice
}

// completeArgs finishes what comes after a verb.
func (s *session) completeArgs(path, rest []string, last string) (add string, hints []string) {
	terms := s.argTermsFor(path, rest)
	if len(terms) == 0 {
		return "", nil
	}
	if attr, val, has := strings.Cut(last, "="); has {
		return s.completeValue(path, rest, attr, val)
	}
	// What the line already says is not offered again: a switch
	// spoken twice means nothing more, and TAB after "advert flood"
	// must not stutter flood down the line.
	used := map[string]bool{}
	for _, a := range rest[1:] {
		name, _, _ := strings.Cut(a, "=")
		used[unquoted(name)] = true
	}
	// A term that takes a value completes up to its '=', the way a
	// context completes up to its slash: the operator is mid-argument,
	// not mid-word.
	words := make([]string, 0, len(terms))
	takesValue := map[string]bool{}
	for _, t := range terms {
		if t.placeholder || used[t.name] {
			continue
		}
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
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].placeholder != sorted[j].placeholder {
			return sorted[i].placeholder // the slot leads: it comes first on the line
		}
		return sorted[i].name < sorted[j].name
	})
	for _, t := range sorted {
		name := t.name
		if t.container {
			name += "=" // it wants a value, and says so where it is read
		}
		s.writeTerm(&b, term{name: name, class: t.class, doc: t.doc, placeholder: t.placeholder})
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

	// Weight is the second axis, and it answers a different question
	// from hue. Hue says what a word IS — a place, an action, an
	// attribute. Weight says how firmly it was chosen: a value the
	// operator set stands out, a value that merely came with the
	// preset recedes, and the column names take emphasis because they
	// have no class of their own to colour.
	emphasis = "\x1b[1m"
	// chosen marks a value the operator set. Weight again, not hue:
	// every hue this console has means a kind of word, and a
	// provenance is not one.
	chosen = emphasis
	// recede is a grey named from the 256-colour cube, and the cube is
	// the point. Faint is the least reliably implemented attribute
	// there is, and the sixteen base colours are a theme's to
	// redefine — Solarized turns bright black into a tone a shade from
	// its own background, so a row meant to step back stepped out of
	// sight. Nothing remaps the cube.
	recede = "\x1b[38;5;244m"
)

// weightOf reads a provenance and answers how firmly the value was
// chosen. The source column still says it in words, because a pipe
// gets no weight at all and must lose nothing.
func weightOf(source string) string {
	switch {
	case strings.HasPrefix(source, "override:"):
		return chosen // set here, on purpose
	case strings.HasPrefix(source, "profile:"):
		return recede // arrived with the preset
	default:
		return "" // the store's own word, neither louder nor softer
	}
}

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
	for _, seg := range paintSegments(line) {
		if seg.space {
			b.WriteString(seg.text)
			continue
		}
		b.WriteString(w.paint(seg.text))
	}
	return b.String()
}

// paintSegment is one stretch of the line: a token as typed, or the
// whitespace between tokens.
type paintSegment struct {
	text  string
	space bool
}

// paintSegments cuts the line the way splitArgs does — a quote holds
// its spaces, closed or not yet — while keeping every character as
// typed, quotes and runs of blanks included, because the painter
// redraws the exact line under the operator's fingers.
func paintSegments(line string) []paintSegment {
	var out []paintSegment
	start, inQuote, spacing := 0, false, true
	for i, r := range line {
		if r == '"' {
			inQuote = !inQuote
		}
		isSpace := !inQuote && (r == ' ' || r == '	')
		if isSpace != spacing {
			if i > start {
				out = append(out, paintSegment{line[start:i], spacing})
			}
			start, spacing = i, isSpace
		}
	}
	if len(line) > start {
		out = append(out, paintSegment{line[start:], spacing})
	}
	return out
}

// unquoted is the word as the parser will read it — the quotes gone,
// their spaces kept — for classifying a token whose painted face
// keeps them.
func unquoted(s string) string {
	return strings.ReplaceAll(s, "\"", "")
}

// lineWalk follows a line the way the grammar does, so the painter and
// the parser agree about what each word is.
type lineWalk struct {
	s     *session
	path  []string
	phase int // 0 walking the path, 1 expecting the verb, 2 its arguments
	verb  string
	args  []string // what the verb accepts, whether switch or pair
	// takesValue says the verb reads a word of the operator's own
	// choosing here — a new instance's name, a key prefix. Such a
	// word is not one this console names, so it claims nothing.
	takesValue bool
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
		painted, leftover := w.paintPath(token)
		if leftover == "" {
			return painted
		}
		w.phase = 1
		return painted + w.paintRest(leftover)
	case 1:
		w.phase, w.verb = 2, token
		w.args = names(w.s.argTermsFor(w.path, []string{token}))
		w.takesValue = takesValue(token)
		cands := w.s.verbsAt(w.path)
		if len(w.path) == 0 {
			cands = append(cands, rootCommandNames()...)
		}
		return w.s.mark(cVerb, cands, token)
	default:
		return w.paintArg(token)
	}
}

// paintPath consumes the leading pieces of a token that name a place
// and hands back what is left, so the two halves of /relay/x/print
// each read in their own class. The separator belongs to the path it
// closes.
func (w *lineWalk) paintPath(token string) (painted, leftover string) {
	next := append([]string(nil), w.path...)
	pieces := strings.Split(token, "/")
	took := 0
	for _, piece := range pieces {
		step, ok := w.s.walkPiece(next, piece)
		if !ok {
			break
		}
		next, took = step, took+1
	}
	if took == 0 {
		return "", token
	}
	w.path = next
	head := strings.Join(pieces[:took], "/")
	if took == len(pieces) {
		return w.s.color(cPath, head), ""
	}
	return w.s.color(cPath, head+"/"), strings.Join(pieces[took:], "/")
}

// paintArg colours one argument: a pair reads as name, joiner, value,
// and a bare word as the switch it is.
func (w *lineWalk) paintArg(token string) string {
	name, value, ok := strings.Cut(token, "=")
	if !ok {
		// A bare word is a switch, or the name a verb like unset
		// takes: both are words the verb knows, so both are marked
		// against the one list that says what it accepts. What is
		// left is a value, and marking a value unresolved would say
		// the console expected to recognise it.
		if !slices.Contains(w.args, unquoted(token)) && w.takesValue {
			return token
		}
		return w.s.mark(cAttr, w.args, unquoted(token))
	}
	return w.s.mark(cAttr, w.args, unquoted(name)) + w.s.color(cPunct, "=") + value
}

// takesValue reports whether a verb reads a word the operator chose
// rather than one this console names: the instance a creation or a
// removal is about, or a command's own positional.
func takesValue(verb string) bool {
	if verb == verbAdd || verb == verbRemove {
		return true
	}
	c := lookup(verb)
	return c != nil && c.takes != nil
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
