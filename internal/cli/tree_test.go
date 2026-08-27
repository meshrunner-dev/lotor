package cli

import (
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/schema"
)

func TestTreeNavigationChangesThePrompt(t *testing.T) {
	deps := testDeps(t)
	deps.SystemName = func() string { return "lab-pi" }
	out := run(t, deps, "/relay", "?", "..")
	// The network-console shape: privilege and system in the brackets,
	// the context path outside them.
	if !strings.Contains(out, "[read-only@lab-pi] /relay> ") {
		t.Errorf("the prompt never showed the context:\n%s", out)
	}
	// The root wears the same shape, only without a path.
	if !strings.Contains(out, "[read-only@lab-pi] > ") {
		t.Errorf("the root prompt lost its shape:\n%s", out)
	}
	// Context help describes the grammar — the verbs, and that a name
	// is the step inward. What exists is print's answer, not help's.
	if !strings.Contains(out, "print") || !strings.Contains(out, "go into one") {
		t.Errorf("context help lacks its grammar:\n%s", out)
	}
	if strings.Contains(out, "meshcore-868") {
		t.Errorf("help enumerated what print already lists:\n%s", out)
	}
	// After .., the prompt is the root's again.
	if !strings.HasSuffix(strings.TrimSpace(out), "bye.") {
		t.Errorf("the session did not end cleanly:\n%s", out)
	}
}

func TestTreeInstancePrintMasksSecrets(t *testing.T) {
	out := run(t, testDeps(t), "/relay meshcore-868 print")
	if strings.Contains(out, "b5445dd625d531fc") {
		t.Fatalf("the private key echoed on the console:\n%s", out)
	}
	if !strings.Contains(out, maskedValue) {
		t.Errorf("no mask stood in for the secret:\n%s", out)
	}
	if !strings.Contains(out, "test 🦝") || !strings.Contains(out, "override:eu-868-narrow") {
		t.Errorf("print lost the ordinary values or their provenance:\n%s", out)
	}
}

func TestTreeAbsoluteCommandFromInsideAContext(t *testing.T) {
	out := run(t, testDeps(t), "/relay meshcore-868", "/status", "?")
	if !strings.Contains(out, "daemon") {
		t.Errorf("/status did not run from inside the context:\n%s", out)
	}
	// And the session still stands where it stood: the '?' after it
	// describes the instance, attributes included.
	if !strings.Contains(out, "node_name") || !strings.Contains(out, "the name on the air") {
		t.Errorf("the absolute command moved the session:\n%s", out)
	}
	// Secrets say so in the help.
	if !strings.Contains(out, "never echoed") {
		t.Errorf("the secret attribute is not marked:\n%s", out)
	}
}

func TestTreeMountedVerbTellsTheRelay(t *testing.T) {
	deps := testDeps(t)
	deps.Relays[0].Neighbours = func() []Neighbour { return nil }
	out := run(t, deps, "/relay meshcore-868 scopes")
	if !strings.Contains(out, "carries no scopes") {
		t.Errorf("the mounted verb did not reach the relay:\n%s", out)
	}
}

func TestTreeRefusesWhatIsNotThere(t *testing.T) {
	out := run(t, testDeps(t), "/relay ghost", "/relay meshcore-868 dance")
	// An unknown instance is not silently a verb.
	if !strings.Contains(out, "error:") {
		t.Errorf("nothing was refused:\n%s", out)
	}
	if !strings.Contains(out, `no "dance" here`) {
		t.Errorf("an unknown verb was not named:\n%s", out)
	}
}

func TestTreeCompletion(t *testing.T) {
	s := &session{deps: testDeps(t)}
	for _, c := range []struct{ line, add string }{
		{"/re", "lay/"}, // a container completes to its separator
		{"/relay mesh", "core-868 "},
		{"/relay meshcore-868 pr", "int "},
		{"/relay meshcore-868/neighbours disc", "over "},
	} {
		add, hints := s.complete(c.line)
		if add != c.add || hints != nil {
			t.Errorf("complete(%q) = %q %v, want %q", c.line, add, hints, c.add)
		}
	}
	// Several candidates: the shared prefix advances, the rest shows.
	if _, hints := s.complete("/r"); len(hints) != 2 {
		t.Errorf("complete(/r) hints = %v, want radio and relay", hints)
	}
	// Inside a context, completion is relative to it.
	s.setPath([]string{"relay"})
	if add, _ := s.complete("mesh"); add != "core-868 " {
		t.Errorf("relative completion = %q", add)
	}
}

func TestPaintClassifiesTokens(t *testing.T) {
	s := &session{deps: testDeps(t), colors: true}
	painted := s.paintLine("/relay meshcore-868 set node_name=x")
	for _, want := range []string{
		cPath + "/" + cReset,            // the slash names the root
		cPath + "relay" + cReset,        // a place
		cPath + "meshcore-868" + cReset, // a place
		cVerb + "set" + cReset,          // an action
		cAttr + "node_name" + cReset,    // an attribute
		cPunct + "=" + cReset,           // what joins the pair
	} {
		if !strings.Contains(painted, want) {
			t.Errorf("missing %q in %q", want, painted)
		}
	}
	// An argument is marked against what its own verb takes, not
	// against whatever the place holds: print does not act on
	// attributes, and says so before Enter.
	if arg := s.paintLine("/relay meshcore-868 print show-secrets"); !strings.Contains(arg, cAttr+"show-secrets"+cReset) {
		t.Errorf("print's own argument is not marked: %q", arg)
	}
	if arg := s.paintLine("/relay meshcore-868 print node_name=x"); !strings.Contains(arg, cUnres+"node_name"+cReset) {
		t.Errorf("print was offered an attribute it does not take: %q", arg)
	}
	// The value carries no colour of its own.
	if strings.Contains(painted, cReset+"x"+cReset) {
		t.Errorf("the value was painted: %q", painted)
	}
	// Without colors the line passes through untouched.
	if plain := (&session{deps: s.deps}).paintLine("/relay print"); plain != "/relay print" {
		t.Errorf("plain session painted anyway: %q", plain)
	}
}

func TestPaintMarksWhatItHasNotResolved(t *testing.T) {
	// The useful half: a word stays in the unresolved colour while it
	// names nothing — or several things — and takes its class colour
	// the moment it names exactly one.
	s := &session{deps: testDeps(t), colors: true}
	for _, c := range []struct {
		line, word, colour, why string
	}{
		{"/relay", "relay", cPath, "names exactly one context"},
		{"/zz", "zz", cUnres, "names nothing"},
		{"/relay meshcore-868 print", "print", cVerb, "a whole verb"},
		{"/relay meshcore-868 pri", "pri", cUnres, "a prefix is not a name — TAB makes it one"},
		{"/relay meshcore-868 zz", "zz", cUnres, "no verb answers to it"},
		{"/relay meshcore-868 set node_name=x", "node_name", cAttr, "a real attribute"},
		{"/relay meshcore-868 set zz=x", "zz", cUnres, "no attribute answers to it"},
		{"/relay/meshcore-868/print", "relay/meshcore-868/", cPath, "the place, and the separator it closes"},
		{"/relay/meshcore-868/print", "print", cVerb, "a verb joined by slashes is still a verb"},
	} {
		painted := s.paintLine(c.line)
		if !strings.Contains(painted, c.colour+c.word+cReset) {
			t.Errorf("paint(%q): %q should read as %s — got %q",
				c.line, c.word, c.why, painted)
		}
	}
	// The promise has to match what Enter would do: this console's
	// parser takes whole names, so a prefix stays marked even when
	// only one candidate could complete it.
	partial := s.paintLine("/rel")
	if !strings.Contains(partial, cUnres+"rel"+cReset) {
		t.Errorf("a prefix was passed off as resolved: %q", partial)
	}
}

func TestSlashJoinedPathsReachTheSamePlace(t *testing.T) {
	s := &session{deps: testDeps(t)}
	for _, line := range []string{"/relay meshcore-868", "/relay/meshcore-868"} {
		path, rest := s.resolveTree(splitArgs(line))
		if len(path) != 2 || path[0] != "relay" || path[1] != "meshcore-868" || rest != nil {
			t.Errorf("%q resolved to %v (rest %v)", line, path, rest)
		}
	}
	// A value carrying slashes is never mistaken for a path.
	path, rest := s.resolveTree(splitArgs("/radio slot1 set spi=/dev/spidev0.0"))
	if len(path) != 2 || len(rest) != 2 || rest[1] != "spi=/dev/spidev0.0" {
		t.Errorf("path %v rest %v", path, rest)
	}
	// A token may name a place and then ask something of it: every
	// spelling of the same request reaches the same verb at the same
	// place, so all of the configuration is one line away.
	for _, line := range []string{
		"/relay meshcore-868 set node_name=x",
		"/relay/meshcore-868 set node_name=x",
		"/relay/meshcore-868/set node_name=x",
	} {
		path, rest := s.resolveTree(splitArgs(line))
		if len(path) != 2 || len(rest) != 2 || rest[0] != "set" || rest[1] != "node_name=x" {
			t.Errorf("%q resolved to %v (rest %v)", line, path, rest)
		}
	}
	// A step that names nowhere ends the path rather than swallowing
	// the rest of the token, so the refusal names the word that failed.
	if path, rest := s.resolveTree(splitArgs("/relay/zz/print")); len(path) != 1 || len(rest) != 1 || rest[0] != "zz/print" {
		t.Errorf("path %v rest %v", path, rest)
	}
	// And completion refuses to guess past it.
	if add, hints := s.complete("/relay/zz/pri"); add != "" || len(hints) > 0 {
		t.Errorf("completed past a step that names nowhere: %q %v", add, hints)
	}
}

func TestTreeSetGoesThroughTheOneDoor(t *testing.T) {
	deps := testDeps(t)
	deps.Privilege = Admin
	var got struct {
		kind, name string
		set        map[string]string
		unset      []string
	}
	deps.Mutate = func(_ context.Context, kind, name string, set map[string]string,
		unset []string, principal string,
	) (string, error) {
		got.kind, got.name, got.set, got.unset = kind, name, set, unset
		if principal != "console" {
			t.Errorf("principal = %q", principal)
		}
		return "applied — relay meshcore-868 restarting", nil
	}
	out := run(t, deps, `/relay meshcore-868 set node_name="new name" session_limit=12`)
	if got.kind != "relay" || got.name != "meshcore-868" {
		t.Fatalf("mutation aimed at %s %s", got.kind, got.name)
	}
	if got.set["node_name"] != "new name" || got.set["session_limit"] != "12" {
		t.Fatalf("set = %v", got.set)
	}
	if !strings.Contains(out, "restarting") {
		t.Errorf("the answer never reached the operator:\n%s", out)
	}

	out = run(t, deps, "/relay meshcore-868 unset node_name")
	if len(got.unset) != 1 || got.unset[0] != "node_name" {
		t.Fatalf("unset = %v", got.unset)
	}
	_ = out
}

func TestTreeSetIsAdminOnly(t *testing.T) {
	deps := testDeps(t)
	called := false
	deps.Mutate = func(_ context.Context, _, _ string, _ map[string]string,
		_ []string, _ string,
	) (string, error) {
		called = true
		return "", nil
	}
	out := run(t, deps, "/relay meshcore-868 set node_name=x")
	if called {
		t.Fatal("a read-only session mutated the configuration")
	}
	if !strings.Contains(out, "admin") {
		t.Errorf("the refusal does not say why:\n%s", out)
	}
}

func TestTreeExportIsPasteableAndWhole(t *testing.T) {
	out := run(t, testDeps(t), "/relay meshcore-868 export")
	// The line is absolute — pasteable from anywhere — and quoted
	// values keep their quotes.
	if !strings.Contains(out, "/relay add meshcore-868 ") ||
		!strings.Contains(out, `node_name="test 🦝"`) {
		t.Errorf("export is not a recreating line:\n%s", out)
	}
	// The identity is the part that makes this relay that relay: an
	// export without it recreates a different node.
	if !strings.Contains(out, "identity=b5445dd625d531fc") {
		t.Fatalf("export left out the secret it needs to recreate:\n%s", out)
	}
}

func TestRootExportCoversEverything(t *testing.T) {
	deps := testDeps(t)
	deps.Traces["sentinel"] = []config.Trace{
		{Key: "journal", Value: "/var/lib/lotor/journal.db", Source: "config"},
	}
	out := run(t, deps, "/export")
	// Radios come before the relays that claim them: the paste has to
	// work in order.
	radioAt := strings.Index(out, "/radio add slot1")
	relayAt := strings.Index(out, "/relay add meshcore-868")
	if radioAt == -1 || relayAt == -1 || radioAt > relayAt {
		t.Errorf("export order or coverage wrong (radio %d, relay %d):\n%s", radioAt, relayAt, out)
	}
	if !strings.Contains(out, "/sentinel set journal=/var/lib/lotor/journal.db") {
		t.Errorf("the singleton block is missing:\n%s", out)
	}
}

func TestTreeAddAndRemoveGoThroughTheirDoors(t *testing.T) {
	deps := testDeps(t)
	deps.Privilege = Admin
	var created, removed string
	deps.Create = func(_ context.Context, kind, name string, attrs map[string]string,
		_ string,
	) (string, error) {
		created = kind + "/" + name + "/" + attrs["driver"]
		return "added — radio " + name, nil
	}
	deps.Remove = func(_ context.Context, kind, name, _ string) (string, error) {
		removed = kind + "/" + name
		return "removed — radio " + name, nil
	}
	out := run(t, deps, "/radio add hat2 driver=sx126x-spi", "/radio remove hat2")
	if created != "radio/hat2/sx126x-spi" {
		t.Fatalf("create saw %q", created)
	}
	if removed != "radio/hat2" {
		t.Fatalf("remove saw %q", removed)
	}
	if !strings.Contains(out, "added — radio hat2") || !strings.Contains(out, "removed — radio hat2") {
		t.Errorf("the answers never reached the operator:\n%s", out)
	}
}

func TestSingletonContextSetsAndPrints(t *testing.T) {
	deps := testDeps(t)
	deps.Privilege = Admin
	deps.Traces["sentinel"] = []config.Trace{
		{Key: "journal", Value: "/var/lib/lotor/journal.db", Source: "config"},
		{Key: "retention", Value: "720h0m0s", Source: "config"},
	}
	var got map[string]string
	deps.Mutate = func(_ context.Context, kind, name string, set map[string]string,
		_ []string, _ string,
	) (string, error) {
		if kind != "sentinel" || name != "" {
			t.Errorf("mutation aimed at %s %q", kind, name)
		}
		got = set
		return "applied — takes effect when the daemon restarts", nil
	}
	out := run(t, deps, "/sentinel", "print", "set retention=800h", "?")
	if !strings.Contains(out, "] /sentinel> ") {
		t.Errorf("the singleton has no context:\n%s", out)
	}
	if !strings.Contains(out, "/var/lib/lotor/journal.db") {
		t.Errorf("print shows no values:\n%s", out)
	}
	if got["retention"] != "800h" {
		t.Fatalf("set saw %v", got)
	}
	if !strings.Contains(out, "takes effect when the daemon restarts") {
		t.Errorf("the apply semantics were not named:\n%s", out)
	}
	if !strings.Contains(out, "how far back the journal reaches") {
		t.Errorf("? lists no attributes:\n%s", out)
	}
}

func TestCompletionReachesAttributesAndEnums(t *testing.T) {
	s := &session{deps: testDeps(t)}
	// After set, attribute names complete with their '='.
	add, _ := s.complete("/relay meshcore-868 set node_")
	if add != "name=" {
		t.Fatalf("attr completion = %q", add)
	}
	// After '=', a closed set completes its values.
	s2 := &session{deps: testDeps(t)}
	s2.deps.Kinds[0].Contributed = func(string) []schema.Attr {
		return []schema.Attr{{Name: "guest_access", Type: schema.String,
			Enum: []string{"blocked", "password", "open"}, Doc: "d"}}
	}
	add, _ = s2.complete("/relay meshcore-868 set guest_access=blo")
	if add != "cked " {
		t.Fatalf("enum completion = %q", add)
	}
	// unset completes bare names.
	add, _ = s.complete("/relay meshcore-868 unset node_")
	if add != "name " {
		t.Fatalf("unset completion = %q", add)
	}
	// add completes against the choice already on the line.
	add, _ = s.complete("/relay add r2 protocol=meshcore node_")
	if add != "name=" {
		t.Fatalf("add completion = %q", add)
	}
}

func TestTheRootIsAContextLikeAnyOther(t *testing.T) {
	deps := testDeps(t)
	deps.Traces["sentinel"] = []config.Trace{
		{Key: "journal", Value: "/var/lib/lotor/journal.db", Source: "config"},
	}
	// The tree's own verbs answer at the root, with or without the
	// slash — an operator should not have to know which grammar a word
	// belongs to before typing it.
	for _, cmd := range []string{"export", "/export"} {
		out := run(t, deps, cmd)
		if !strings.Contains(out, "/radio add slot1") {
			t.Errorf("%q exported nothing:\n%s", cmd, out)
		}
	}
	for _, cmd := range []string{"print", "/print"} {
		out := run(t, deps, cmd)
		if !strings.Contains(out, "/relay") || !strings.Contains(out, "/sentinel") {
			t.Errorf("%q listed no contexts:\n%s", cmd, out)
		}
	}
	// A flat command at the root still is one.
	if out := run(t, deps, "status"); !strings.Contains(out, "daemon") {
		t.Errorf("the flat commands stopped working:\n%s", out)
	}
	// And a mutation verb with nowhere to work says so usefully.
	deps.Privilege = Admin
	deps.Mutate = func(context.Context, string, string, map[string]string, []string, string) (string, error) {
		t.Fatal("a rootless set reached the manager")
		return "", nil
	}
	if out := run(t, deps, "set node_name=x"); !strings.Contains(out, "needs somewhere to work") {
		t.Errorf("a rootless set said %q", strings.TrimSpace(out))
	}
}

func TestExportColoursItsSymbolClasses(t *testing.T) {
	deps := testDeps(t)
	s := &session{deps: deps, colors: true, out: &strings.Builder{}}
	var b strings.Builder
	s.out = &b
	s.exportInstance(scopeRelay, "meshcore-868")
	got := b.String()
	if !strings.Contains(got, cPath+"/relay"+cReset) ||
		!strings.Contains(got, cVerb+"add"+cReset) ||
		!strings.Contains(got, cPath+"meshcore-868"+cReset) {
		t.Errorf("path and verb are not coloured:\n%q", got)
	}
	if !strings.Contains(got, cAttr+"node_name"+cReset) {
		t.Errorf("attribute names are not coloured:\n%q", got)
	}
	// A pipe reads the same text with nothing in it: an export is also
	// something machines read.
	var plain strings.Builder
	p := &session{deps: deps, out: &plain}
	p.exportInstance(scopeRelay, "meshcore-868")
	if strings.Contains(plain.String(), "\x1b[") {
		t.Errorf("a plain session got escape codes:\n%q", plain.String())
	}
	if !strings.Contains(plain.String(), "/relay add meshcore-868 ") {
		t.Errorf("the plain line lost its shape:\n%q", plain.String())
	}
}

func TestPromptNamesWhoAndWhere(t *testing.T) {
	deps := testDeps(t)
	deps.SystemName = func() string { return "lab-pi" }
	s := &session{deps: deps}
	if got := s.prompt(); got != "[read-only@lab-pi] > " {
		t.Errorf("root prompt = %q", got)
	}
	s.setPath([]string{"relay", "meshcore-868"})
	if got := s.prompt(); got != "[read-only@lab-pi] /relay/meshcore-868> " {
		t.Errorf("context prompt = %q", got)
	}
	// The privilege is part of the answer: an operator reads what they
	// may do without trying it.
	s.deps.Privilege = Admin
	if got := s.prompt(); !strings.HasPrefix(got, "[admin@lab-pi]") {
		t.Errorf("admin prompt = %q", got)
	}
	// A daemon that names no system still prompts.
	bare := &session{deps: testDeps(t)}
	if got := bare.prompt(); got != "[read-only@lotor] > " {
		t.Errorf("nameless prompt = %q", got)
	}
	// Coloured sessions paint the two halves by their classes.
	s.colors = true
	// Who is cyan, the system green, the path cyan — the brackets and
	// the caret carry no colour of their own.
	got := s.prompt()
	if !strings.Contains(got, "["+cUser+"admin"+cReset+"@"+cSystem+"lab-pi"+cReset+"] ") {
		t.Errorf("coloured head = %q", got)
	}
	if !strings.Contains(got, cPath+"/relay/meshcore-868"+cReset+"> ") {
		t.Errorf("coloured path = %q", got)
	}
}

func TestSystemNameIsHotAndSaysWhereItCameFrom(t *testing.T) {
	deps := testDeps(t)
	deps.Privilege = Admin
	deps.SystemName = func() string { return "lab-pi" }
	deps.Traces["system"] = []config.Trace{
		{Key: "name", Value: "lab-pi", Source: "hostname"},
	}
	out := run(t, deps, "/system print", "/system ?")
	// print does not pass a fallback off as a choice.
	if !strings.Contains(out, "hostname") {
		t.Errorf("print hides where the name came from:\n%s", out)
	}
	if !strings.Contains(out, "the machine's hostname") {
		t.Errorf("? does not describe the attribute:\n%s", out)
	}
}

func TestEveryVerbAPlaceAnswersIsAlsoOffered(t *testing.T) {
	// The help, the completion and the dispatch have to agree about
	// what a place answers. Keeping three lists in step by hand is
	// how export came to work everywhere and be offered in only some
	// places, so this walks the whole tree and checks the two
	// projections against the one list.
	s := &session{deps: testDeps(t)}
	places := [][]string{
		nil,
		{"relay"}, {"radio"}, {"sentinel"}, {"system"},
		{"relay", "meshcore-868"},
	}
	for _, path := range places {
		where := "/" + strings.Join(path, " ")
		verbs := s.verbsAt(path)
		if len(verbs) == 0 {
			t.Errorf("%s answers nothing", where)
		}
		offered := names(s.candidatesAt(path))
		for _, v := range verbs {
			if !slices.Contains(offered, v) {
				t.Errorf("%s answers %q but never offers it", where, v)
			}
			// And the help names it, so "?" and TAB tell one story.
			s.setPath(path)
			if !strings.Contains(s.treeHelp(path), v) {
				t.Errorf("%s answers %q but its help omits it", where, v)
			}
		}
		// Export is the one verb every place answers.
		if !slices.Contains(verbs, verbExport) {
			t.Errorf("%s does not answer export", where)
		}
	}
}

func TestCompletionOffersExportEverywhere(t *testing.T) {
	s := &session{deps: testDeps(t)}
	for _, line := range []string{
		"expo", "/expo",
		"/relay expo", "/radio expo",
		"/sentinel expo", "/system expo",
		"/relay meshcore-868 expo",
	} {
		if add, _ := s.complete(line); add != "rt " {
			t.Errorf("complete(%q) = %q, want the rest of export", line, add)
		}
	}
	// And print, which the root answers too.
	if add, _ := s.complete("pri"); add != "nt " {
		t.Errorf("complete(pri) at the root = %q", add)
	}
}

func TestListingsNameTheirColumns(t *testing.T) {
	deps := testDeps(t)
	for _, c := range []struct{ cmd, want string }{
		{"/radio print", "NAME"},
		{"/relay print", "PROTOCOL"},
		{"/relay meshcore-868 print", "ATTRIBUTE"},
		{"print", "CONTEXT"},
	} {
		out := run(t, deps, c.cmd)
		if !strings.Contains(out, c.want) {
			t.Errorf("%q names no columns (%q missing):\n%s", c.cmd, c.want, out)
		}
	}
}

func TestColumnNamesAreWeightNotColour(t *testing.T) {
	// Column names have no class of their own — they name the classes
	// below them — so they take emphasis, never a hue. And the width
	// arithmetic must run on the plain text: escape bytes occupy no
	// cells.
	var b strings.Builder
	tb := (&session{colors: true}).table()
	tb.header("NAME", "DRIVER")
	tb.row("slot1", "sx126x-spi")
	tb.row("a-much-longer-name", "x")
	if err := tb.flush(&b); err != nil {
		t.Fatal(err)
	}
	got := b.String()
	if !strings.Contains(got, emphasis+"NAME"+cReset) {
		t.Errorf("the header is not emphasised:\n%q", got)
	}
	for _, hue := range []string{cPath, cVerb, cAttr} {
		if strings.Contains(got, hue) {
			t.Errorf("a column name was given a hue:\n%q", got)
		}
	}
	// The second column starts at the same cell on every line.
	lines := strings.Split(strings.TrimRight(got, "\r\n"), "\r\n")
	col := -1
	for _, l := range lines {
		plain := strings.ReplaceAll(strings.ReplaceAll(l, emphasis, ""), cReset, "")
		at := strings.Index(plain, strings.Fields(plain)[1])
		if col == -1 {
			col = at
		} else if at != col {
			t.Errorf("column drifted to %d (want %d): %q", at, col, plain)
		}
	}
	// A plain session gets the names with nothing around them.
	var p strings.Builder
	pt := (&session{}).table()
	pt.header("NAME")
	pt.row("slot1")
	_ = pt.flush(&p)
	if strings.Contains(p.String(), "\x1b") {
		t.Errorf("a plain session got escapes: %q", p.String())
	}
}

func TestEmptyListingSaysSoInsteadOfShowingColumns(t *testing.T) {
	deps := testDeps(t)
	deps.Relays = nil
	deps.LiveRelays = func() []RelayInfo { return nil }
	out := run(t, deps, "/relay print")
	if strings.Contains(out, "PROTOCOL") {
		t.Errorf("column names described an empty room:\n%s", out)
	}
	if !strings.Contains(out, "no relays configured") {
		t.Errorf("nothing said the room was empty:\n%s", out)
	}
}

func TestCompletionListingIsGroupedAndColoured(t *testing.T) {
	s := &session{deps: testDeps(t), colors: true}
	// After a slash a single letter reaches both places and actions;
	// the listing puts the places first and paints each by its class,
	// so what is somewhere to go reads apart from what is something
	// to do.
	_, hints := s.complete("/s")
	if len(hints) < 2 {
		t.Fatalf("hints = %v", hints)
	}
	places := 0
	for _, h := range hints {
		if strings.HasPrefix(h, cPath) {
			places++
			continue
		}
		break // once actions start, no place may follow
	}
	if places == 0 {
		t.Fatalf("no place led the listing: %q", hints)
	}
	for _, h := range hints[places:] {
		if strings.HasPrefix(h, cPath) {
			t.Errorf("a place came after the actions: %q", hints)
		}
		if !strings.HasPrefix(h, cVerb) {
			t.Errorf("an action carries no class: %q", h)
		}
	}
	// A plain session gets the same names with nothing around them.
	_, plain := (&session{deps: s.deps}).complete("/s")
	for _, h := range plain {
		if strings.Contains(h, "\x1b") {
			t.Errorf("a plain session got escapes: %q", h)
		}
	}
}

func TestContainersCompleteToTheirSeparator(t *testing.T) {
	s := &session{deps: testDeps(t)}
	// A context holds instances, so completing it leaves the operator
	// mid-path rather than mid-word.
	if add, _ := s.complete("/rel"); add != "ay/" {
		t.Errorf("a container completed with %q", add)
	}
	// An instance holds verbs, not names: the path ends there.
	if add, _ := s.complete("/relay mesh"); add != "core-868 " {
		t.Errorf("an instance completed with %q", add)
	}
	// A singleton has no instance step either.
	if add, _ := s.complete("/sent"); add != "inel " {
		t.Errorf("a singleton completed with %q", add)
	}
}

func TestRefusalsPointAtTheWord(t *testing.T) {
	out := run(t, testDeps(t), "/relay meshcore-868 dance")
	if !strings.Contains(out, `no "dance" here — "?" says what this context offers (column 21)`) {
		t.Errorf("the refusal does not point at the word:\n%s", out)
	}
}

func TestCompletionOffersOnlyWhatWouldRun(t *testing.T) {
	s := &session{deps: testDeps(t)}
	// A context reaches its place with or without the slash, so both
	// spellings complete — and both run.
	for _, line := range []string{"sys", "/sys"} {
		if add, _ := s.complete(line); add != "tem " {
			t.Errorf("complete(%q) = %q", line, add)
		}
	}
	// Every name is offered once, and as the one thing it means.
	seen := map[string]int{}
	for _, c := range s.candidatesAt(nil) {
		seen[c.name]++
		if c.name == scopeRelay && c.class != cPath {
			t.Errorf("relay was offered as %q, not as the place it names", c.class)
		}
	}
	if seen[scopeRelay] != 1 {
		t.Errorf("relay was offered %d times", seen[scopeRelay])
	}
}

func TestHelpIsOneEntryPerLine(t *testing.T) {
	s := &session{deps: testDeps(t)}
	s.setPath([]string{"relay", "meshcore-868"})
	got := s.treeHelp([]string{"relay", "meshcore-868"})

	// A blank line opens it, then one entry per line, each joining a
	// word to its meaning with the same separator.
	if !strings.HasPrefix(got, "\r\n") {
		t.Errorf("no blank line above the answer: %q", got[:20])
	}
	for _, want := range []string{
		".. -- go up to /relay\r\n",
		"print -- show what is here\r\n",
		"set -- change an attribute\r\n",
		"node_name -- the name on the air\r\n",
		"identity -- the private key (secret: never echoed)\r\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing entry %q in:\n%s", want, got)
		}
	}
	// Nothing is padded into columns: the separator does the aligning
	// a reader needs, and a long name must not push the descriptions
	// away from the short ones.
	for line := range strings.SplitSeq(got, "\r\n") {
		if strings.Contains(line, " --") && strings.Contains(line, "  --") {
			t.Errorf("an entry was padded: %q", line)
		}
	}
}

func TestHelpNamesTheGrammarNotTheContents(t *testing.T) {
	s := &session{deps: testDeps(t)}
	// A collection: the sub-places the grammar defines are named, the
	// instances that happen to exist are not — those are print's.
	got := s.treeHelp([]string{"relay"})
	if strings.Contains(got, "meshcore-868") {
		t.Errorf("help listed what print already lists:\n%s", got)
	}
	if !strings.Contains(got, "<name> -- go into one") {
		t.Errorf("help does not say how to reach them:\n%s", got)
	}
	// The root: every context is grammar, so every context is named.
	root := s.treeHelp(nil)
	for _, want := range []string{"relay --", "radio --", "system --"} {
		if !strings.Contains(root, want) {
			t.Errorf("the root omits %q:\n%s", want, root)
		}
	}
	// But completion still offers the instances, because typing one
	// is exactly what the grammar allows.
	if !slices.Contains(names(s.termsAt([]string{"relay"})), "meshcore-868") {
		t.Error("completion lost the instances")
	}
}

func TestHelpColoursEachEntryByItsClass(t *testing.T) {
	s := &session{deps: testDeps(t), colors: true}
	got := s.treeHelp([]string{"relay", "meshcore-868"})
	if !strings.Contains(got, cPath+".."+cReset+cPunct+" -- "+cReset) {
		t.Errorf("the way up is not a place: %q", got)
	}
	if !strings.Contains(got, cVerb+"print"+cReset+cPunct+" -- "+cReset) {
		t.Errorf("a verb is not an action: %q", got)
	}
	if !strings.Contains(got, cAttr+"node_name"+cReset+cPunct+" -- "+cReset) {
		t.Errorf("an attribute is not an attribute: %q", got)
	}
}

func TestAVerbSaysWhatItTakes(t *testing.T) {
	s := &session{deps: testDeps(t)}
	// The regression this closes: nothing named the argument that
	// makes discover wait, so nobody could find it.
	got := s.helpForLine("/relay meshcore-868 discover ", 0)
	if !strings.Contains(got, "watch -- stay and print each answer as it lands\r\n") {
		t.Errorf("discover does not name its argument:\n%s", got)
	}
	// Standing inside a relay, the relay argument is already answered.
	if strings.Contains(got, "relay=") {
		t.Errorf("help offered a relay to a session already in one:\n%s", got)
	}
	// A mutation verb answers about the attributes it acts on, and
	// says which of them want a value.
	set := s.helpForLine("/relay meshcore-868 set ", 0)
	if !strings.Contains(set, "node_name= -- the name on the air\r\n") {
		t.Errorf("set does not name its attributes:\n%s", set)
	}
	// unset names them without the '=': it takes a name, not a value.
	un := s.helpForLine("/relay meshcore-868 unset ", 0)
	if !strings.Contains(un, "node_name -- the name on the air\r\n") {
		t.Errorf("unset asked for a value:\n%s", un)
	}
}

func TestCompletionReachesAVerbsArguments(t *testing.T) {
	s := &session{deps: testDeps(t)}
	// TAB after a verb finishes its arguments, and a switch completes
	// with a space where a parameter completes to its '='.
	if add, _ := s.complete("/relay meshcore-868 discover wat"); add != "ch " {
		t.Errorf("a switch completed with %q", add)
	}
	if add, _ := s.complete("/relay meshcore-868 set node_"); add != "name=" {
		t.Errorf("a parameter completed with %q", add)
	}
	if add, _ := s.complete("/relay meshcore-868 unset node_"); add != "name " {
		t.Errorf("unset completed with %q", add)
	}
}

func TestOneNameMeansOneThing(t *testing.T) {
	s := &session{deps: testDeps(t)}
	// Every context is typeable bare at the root, so a context name
	// must not also be a flat command: the word would mean two things
	// and the console could only ever paint one of them.
	flat := map[string]bool{}
	for _, c := range commands {
		flat[c.name] = true
		for _, a := range c.aliases {
			flat[a] = true
		}
	}
	for i := range s.deps.Kinds {
		if name := s.deps.Kinds[i].Name; flat[name] {
			t.Errorf("%q is both a context and a flat command", name)
		}
	}
	// And so the listing tells them apart: a place is a place.
	byName := map[string]string{}
	for _, term := range s.termsAt(nil) {
		byName[term.name] = term.class
	}
	if byName[scopeRadio] != cPath {
		t.Errorf("radio is offered as %q, not as a place", byName[scopeRadio])
	}
	if byName[cmdUndo] != cVerb {
		t.Errorf("undo is offered as %q, not as an action", byName[cmdUndo])
	}
	if byName[scopeRadio] == byName[cmdUndo] {
		t.Error("a place and an action read the same at the root")
	}
}

func TestABareContextNavigatesFromTheRoot(t *testing.T) {
	deps := testDeps(t)
	// Typed without its slash, from the root, a context is still the
	// place it names — the operator should not have to know which
	// grammar a word belongs to before typing it.
	out := run(t, deps, "radio", "print")
	if !strings.Contains(out, "] /radio> ") {
		t.Errorf("a bare context did not navigate:\n%s", out)
	}
	if !strings.Contains(out, "DRIVER") {
		t.Errorf("print did not reach the collection:\n%s", out)
	}
}

func TestStatusIsTheRunningView(t *testing.T) {
	out := run(t, testDeps(t), "/relay meshcore-868 status")
	if !strings.Contains(out, "state") || !strings.Contains(out, "waveform") {
		t.Errorf("status does not show the relay as it runs:\n%s", out)
	}
	// print stays the configured view, with its provenance.
	cfg := run(t, testDeps(t), "/relay meshcore-868 print")
	if !strings.Contains(cfg, "SOURCE") {
		t.Errorf("print stopped showing provenance:\n%s", cfg)
	}
}

func TestCompletionFollowsSlashesInTheWordItself(t *testing.T) {
	s := &session{deps: testDeps(t)}
	// A trailing slash stands inside the place it names: what comes
	// next is a verb or a name there, not a candidate called "radio/".
	add, hints := s.complete("radio/")
	if add != "" || len(hints) == 0 {
		t.Fatalf("radio/ offered add=%q hints=%v", add, hints)
	}
	for _, want := range []string{verbPrint, verbAdd, "slot1"} {
		if !slices.Contains(hints, want) {
			t.Errorf("radio/ does not offer %q — got %v", want, hints)
		}
	}
	// And it completes through the slash, at every depth.
	for _, c := range []struct{ line, add string }{
		{"radio/sl", "ot1 "},
		{"/relay/mesh", "core-868 "},
		{"/relay/meshcore-868/pri", "nt "},
		{"relay/meshcore-868/set", " "},
	} {
		if add, _ := s.complete(c.line); add != c.add {
			t.Errorf("complete(%q) = %q, want %q", c.line, add, c.add)
		}
	}
	// Steps that name nowhere offer nothing rather than guessing.
	if add, hints := s.complete("nowhere/"); add != "" || hints != nil {
		t.Errorf("a path to nowhere offered %q %v", add, hints)
	}
	// A value carrying slashes is never mistaken for a path.
	s.setPath([]string{"radio", "slot1"})
	if add, _ := s.complete("set spi=/dev/sp"); add != "" {
		t.Errorf("a value was walked as a path: %q", add)
	}
}

func TestWhatCompletionProducesIsWhatEnterAccepts(t *testing.T) {
	deps := testDeps(t)
	// TAB finishes a context with its separator, so the line it hands
	// back must be a line the console runs. Anything else makes
	// completion a trap.
	for _, c := range []struct{ line, prompt string }{
		{"radio", "] /radio> "},
		{"radio/", "] /radio> "},
		{"radio/slot1", "] /radio/slot1> "},
		{"/radio/", "] /radio> "},
		{"/radio/slot1", "] /radio/slot1> "},
		{"radio/slot1/", "] /radio/slot1> "},
	} {
		out := run(t, deps, c.line)
		if strings.Contains(out, "error:") {
			t.Errorf("%q was refused:\n%s", c.line, out)
		}
		if !strings.Contains(out, c.prompt) {
			t.Errorf("%q did not land at %q:\n%s", c.line, c.prompt, out)
		}
	}
	// And the pairing holds end to end: whatever TAB appends, Enter
	// accepts.
	s := &session{deps: deps}
	add, _ := s.complete("rad")
	line := "rad" + add
	if strings.TrimSpace(line) != "radio/" {
		t.Fatalf("completion produced %q", line)
	}
	if !s.treeLine(strings.TrimSpace(line)) {
		t.Errorf("%q completed to something the tree refuses", line)
	}
}

func TestProvenanceMarksTheSourceCellAlone(t *testing.T) {
	deps := testDeps(t)
	deps.Traces["relay meshcore-868"] = []config.Trace{
		{Key: "frequency_hz", Value: 869618000, Source: "profile:eu-868-narrow"},
		{Key: "node_name", Value: "lab", Source: "override:eu-868-narrow"},
		{Key: "protocol", Value: "meshcore", Source: "config"},
	}
	var b strings.Builder
	s := &session{deps: deps, colors: true, out: &b}
	if err := s.showTraces("relay meshcore-868", false); err != nil {
		t.Fatal(err)
	}
	got := b.String()

	// The mark sits on the cell it is about. Wrapping the line would
	// tint the value too, saying something about it that is not meant.
	for _, c := range []struct{ mark, source string }{
		{chosen, "override:eu-868-narrow"},
		{recede, "profile:eu-868-narrow"},
	} {
		if !strings.Contains(got, c.mark+c.source+cReset) {
			t.Errorf("%q does not carry its mark on the source cell:\n%q", c.source, got)
		}
	}
	// The attribute names and the values stay bare.
	for _, bare := range []string{"node_name", "frequency_hz", "lab", "869618000"} {
		for _, mark := range []string{chosen, recede, emphasis} {
			if strings.Contains(got, mark+bare) {
				t.Errorf("%q was marked, and only the source should be:\n%q", bare, got)
			}
		}
	}
	// What the store simply holds wears nothing.
	for line := range strings.SplitSeq(got, "\r\n") {
		if strings.Contains(line, "protocol") && strings.Contains(line, "\x1b[3") {
			t.Errorf("the store's own word was marked: %q", line)
		}
	}
	// A pipe loses every mark, so the source column has to carry the
	// whole answer on its own — and does.
	var plain strings.Builder
	p := &session{deps: deps, out: &plain}
	if err := p.showTraces("relay meshcore-868", false); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(plain.String(), "\x1b") {
		t.Errorf("a pipe got escapes: %q", plain.String())
	}
	for _, want := range []string{"profile:eu-868-narrow", "override:eu-868-narrow", "config"} {
		if !strings.Contains(plain.String(), want) {
			t.Errorf("the plain table lost %q", want)
		}
	}
}

func TestOnlyAskingForSecretsByNameShowsThem(t *testing.T) {
	masked := run(t, testDeps(t), "/relay meshcore-868 print")
	if !strings.Contains(masked, maskedValue) || strings.Contains(masked, "b5445dd625d531fc") {
		t.Errorf("print did not mask the identity:\n%s", masked)
	}
	shown := run(t, testDeps(t), "/relay meshcore-868 print show-secrets")
	if !strings.Contains(shown, "b5445dd625d531fc") {
		t.Errorf("the mask did not lift when asked:\n%s", shown)
	}
	// The two words are about different things, and neither does the
	// other's job: unfolding a listing does not lift the mask.
	folded := run(t, testDeps(t), "/relay print detail")
	if !strings.Contains(folded, maskedValue) || strings.Contains(folded, "b5445dd625d531fc") {
		t.Errorf("detail lifted a mask it was not asked to:\n%s", folded)
	}
	both := run(t, testDeps(t), "/relay print detail show-secrets")
	if !strings.Contains(both, "b5445dd625d531fc") {
		t.Errorf("the two words do not compose:\n%s", both)
	}
}

func TestPrintDetailUnfoldsACollection(t *testing.T) {
	// A collection is the one place print summarises — it names its
	// instances and little else — so detail is what opens them out.
	out := run(t, testDeps(t), "/relay print detail")
	if !strings.Contains(out, "meshcore-868") || !strings.Contains(out, "node_name=") {
		t.Errorf("the collection was not unfolded:\n%s", out)
	}
}

func TestPrintRefusesAWordWhereItWouldMeanNothing(t *testing.T) {
	out := run(t, testDeps(t), "/relay meshcore-868 print zz")
	if !strings.Contains(out, "zz") || !strings.Contains(out, argSecrets) {
		t.Errorf("the refusal does not name what print takes here:\n%s", out)
	}
	// An instance has no summary to unfold, so the word does not
	// apply there — and saying so beats accepting it and doing
	// nothing.
	if inst := run(t, testDeps(t), "/relay meshcore-868 print detail"); !strings.Contains(inst, "nothing here") {
		t.Errorf("an instance accepted detail:\n%s", inst)
	}
	// And a listing shows no attributes for a mask to hide.
	if coll := run(t, testDeps(t), "/relay print show-secrets"); !strings.Contains(coll, argDetail) {
		t.Errorf("a listing accepted show-secrets without detail:\n%s", coll)
	}
	if root := run(t, testDeps(t), "/print detail"); !strings.Contains(root, "nothing here") {
		t.Errorf("the root accepted detail:\n%s", root)
	}
}

func TestPrintOffersOnlyWhatWorksWhereItStands(t *testing.T) {
	s := &session{deps: testDeps(t)}
	s.setPath([]string{"relay", "meshcore-868"})
	if add, _ := s.complete("print s"); add != "how-secrets " {
		t.Errorf("show-secrets does not complete: %q", add)
	}
	// An instance is not offered a word that would be refused there.
	if help := s.helpForLine("print ", 0); !strings.Contains(help, argSecrets) ||
		strings.Contains(help, argDetail) {
		t.Errorf("an instance was offered the wrong words:\n%s", help)
	}
	s.setPath([]string{"relay"})
	if help := s.helpForLine("print ", 0); !strings.Contains(help, argDetail) ||
		!strings.Contains(help, argSecrets) {
		t.Errorf("a collection was not offered both:\n%s", help)
	}
	s.setPath([]string{"relay", "meshcore-868"})
	// The same question put the other way reaches the same answer.
	if out := run(t, testDeps(t), "/relay meshcore-868 print ?"); !strings.Contains(out, argSecrets) {
		t.Errorf("\"print ?\" did not answer what print takes:\n%s", out)
	}
	if out := run(t, testDeps(t), "/relay meshcore-868 set ?"); !strings.Contains(out, "node_name") {
		t.Errorf("\"set ?\" did not answer what set takes:\n%s", out)
	}
}

func TestExportRendersEveryListTheWaySetReadsItBack(t *testing.T) {
	// A list of whole numbers is stored as one, and had been printed
	// in the shape the language prints slices in — which the parser
	// does not read. An export nobody can paste back is not one.
	for _, c := range []struct{ in any }{{[]int{12, 13}}, {[]any{12, 13}}, {[]string{"12", "13"}}} {
		if got := exportValue(c.in); got != "12,13" {
			t.Errorf("exportValue(%v) = %q", c.in, got)
		}
	}
	attr := schema.Attr{Name: "enable_pins", Type: schema.Ints}
	if _, err := schema.Parse(attr, exportValue([]int{12, 13})); err != nil {
		t.Errorf("what export wrote does not parse back: %s", err)
	}
}

func TestABareWordIsASwitchOrAValue(t *testing.T) {
	s := &session{deps: testDeps(t), colors: true}
	// Every command spells a bare argument the same way, so every one
	// of them paints it the same way: a switch the command declared.
	for _, line := range []string{"discover watch", "advert flood", "frames watch"} {
		word := line[strings.LastIndex(line, " ")+1:]
		if painted := s.paintLine(line); !strings.Contains(painted, cAttr+word+cReset) {
			t.Errorf("%q: the switch is not marked: %q", line, painted)
		}
	}
	// A word no command declares is still refused out loud.
	if painted := s.paintLine("advert zz"); !strings.Contains(painted, cUnres+"zz"+cReset) {
		t.Errorf("an undeclared word was not marked: %q", painted)
	}
	// And a word the operator chose is not one this console names, so
	// it claims nothing about it rather than calling it unresolved.
	for _, line := range []string{"/radio add slot1 driver=x", "/relay remove meshcore-868"} {
		word := "slot1"
		if strings.Contains(line, "remove") {
			word = "meshcore-868"
		}
		if painted := s.paintLine(line); strings.Contains(painted, cUnres+word) {
			t.Errorf("%q: a chosen name was marked unresolved: %q", line, painted)
		}
	}
}

func TestAFlagNamedAfterAKindCompletesItsNames(t *testing.T) {
	s := &session{deps: testDeps(t)}
	// relay= wants a relay, and the flag needs no declaration to say
	// so: it is called relay because a relay is what it takes.
	for _, line := range []string{"advert relay=", "frames relay=", "discover relay="} {
		if add, _ := s.complete(line); add != "meshcore-868 " {
			t.Errorf("%q completed to %q", line, add)
		}
	}
	if add, _ := s.complete("advert relay=mesh"); add != "core-868 " {
		t.Errorf("a partial name did not finish: %q", add)
	}
}

func TestACommandThatTakesARelayIsReachableFromInsideOne(t *testing.T) {
	s := &session{deps: testDeps(t)}
	s.setPath([]string{scopeRelay, "meshcore-868"})
	// Standing in a relay answers the question relay= asks, so every
	// command that asks it works here — not a hand-kept subset of
	// them, which is how frames and tx came to be missing.
	verbs := s.verbsAt(s.curPath())
	for _, want := range []string{cmdAdvert, cmdFrames, "tx", "noise"} {
		if !slices.Contains(verbs, want) {
			t.Errorf("%q is not reachable from inside a relay: %v", want, verbs)
		}
	}
	// A command whose subject is what a drawer holds belongs in the
	// drawer, and only there: the instance does not answer to it.
	if slices.Contains(verbs, cmdDiscover) {
		t.Errorf("a drawer's command stayed on the instance too: %v", verbs)
	}
	inDrawer := s.verbsAt([]string{scopeRelay, "meshcore-868", drawerNeighbours})
	if !slices.Contains(inDrawer, cmdDiscover) {
		t.Errorf("the drawer does not answer to what fills it: %v", inDrawer)
	}
	// A command that names no relay stays where it was.
	if slices.Contains(verbs, cmdUndo) {
		t.Errorf("a command with no relay to act on was mounted: %v", verbs)
	}
}

func TestIntervalRedrawsWhereItStood(t *testing.T) {
	lines := make(chan string, 1)
	var b strings.Builder
	s := &session{deps: testDeps(t), colors: true, out: &b, lines: lines}
	frames := 0
	err := s.repaint(t.Context(), time.Millisecond, func() error {
		frames++
		if frames == 2 {
			lines <- "" // an empty line stops it after the second draw
		}
		fmt.Fprint(s.out, "one\r\nlonger\r\n")
		return nil
	})
	if err != nil || frames != 2 {
		t.Fatalf("repaint: %v after %d frames", err, frames)
	}
	got := b.String()
	// The second frame climbs back over the first rather than
	// scrolling past it.
	if !strings.Contains(got, "\x1b[2A") {
		t.Errorf("the frame did not redraw in place: %q", got)
	}
	// Every line is erased to its end, so a value that shrank leaves
	// nothing of the longer one behind.
	if strings.Count(got, "\x1b[K\r\n") < 4 {
		t.Errorf("lines are not erased as they are rewritten: %q", got)
	}
	if !strings.Contains(got, "-- ["+intervalStop+"]") {
		t.Errorf("the frame does not say what stops it: %q", got)
	}
}

func TestIntervalNeedsATerminalAndARealDuration(t *testing.T) {
	// Drawing in place is cursor movement, and a pipe has no cursor.
	if out := run(t, testDeps(t), "/relay meshcore-868 print interval=2s"); !strings.Contains(out, "terminal") {
		t.Errorf("a pipe was given a repainting view:\n%s", out)
	}
	for _, line := range []string{
		"/relay meshcore-868 print interval",
		"/relay meshcore-868 print interval=zz",
		"/relay meshcore-868 print interval=10ms",
	} {
		if out := run(t, testDeps(t), line); !strings.Contains(out, "error") {
			t.Errorf("%q was accepted:\n%s", line, out)
		}
	}
	// And it is discoverable where print is.
	s := &session{deps: testDeps(t)}
	s.setPath([]string{"relay", "meshcore-868"})
	if add, _ := s.complete("print i"); add != "nterval=" {
		t.Errorf("interval does not complete to its value: %q", add)
	}
}

// withNeighbour gives the test relay one neighbour to hold.
func withNeighbour(t *testing.T) Deps {
	t.Helper()
	deps := testDeps(t)
	var key [32]byte
	copy(key[:], []byte{0x0d, 0x13, 0x9b, 0x64, 0x21, 0xd0})
	deps.Relays[0].Neighbours = func() []Neighbour {
		return []Neighbour{{PubKey: key, Name: "Radio-Club", SNR: 12.25, Heard: time.Now()}}
	}
	return deps
}

func TestADrawerIsSomewhereToStand(t *testing.T) {
	deps := withNeighbour(t)
	// The listing names each one and little else; detail opens them
	// out; and one of them is a place of its own.
	out := run(t, deps, "/relay/meshcore-868/neighbours/print")
	if !strings.Contains(out, "0d139b6421d0") || !strings.Contains(out, "Radio-Club") {
		t.Errorf("the drawer did not list its neighbour:\n%s", out)
	}
	if d := run(t, deps, "/relay/meshcore-868/neighbours/print detail"); !strings.Contains(d, "name=") {
		t.Errorf("detail did not unfold the drawer:\n%s", d)
	}
	one := run(t, deps, "/relay/meshcore-868/neighbours/0d139b6421d0/print")
	if !strings.Contains(one, "Radio-Club") || !strings.Contains(one, "snr") {
		t.Errorf("standing in one showed nothing of it:\n%s", one)
	}
}

func TestADrawerHoldsNothingThatWasConfigured(t *testing.T) {
	deps := withNeighbour(t)
	// Nothing here was set, so a drawer answers to none of the verbs
	// that read or write configuration — and it refuses them the way
	// it refuses any word it does not list.
	for _, c := range []struct{ line, want string }{
		{"/relay/meshcore-868/neighbours/export", `no "export" here`},
		{"/relay/meshcore-868/neighbours/set name=x", `no "set" here`},
		{"/relay/meshcore-868/neighbours/print show-secrets", "nothing in a neighbours is masked"},
	} {
		if out := run(t, deps, c.line); !strings.Contains(out, c.want) {
			t.Errorf("%q did not say %q:\n%s", c.line, c.want, out)
		}
	}
}

func TestADrawerIsOfferedAndItsKeysAre(t *testing.T) {
	s := &session{deps: withNeighbour(t)}
	s.setPath([]string{scopeRelay, "meshcore-868"})
	// The drawer is grammar — every relay has it — so it completes to
	// its separator, the way a context does.
	if add, _ := s.complete("neigh"); add != "bours/" {
		t.Errorf("the drawer does not complete as a place: %q", add)
	}
	s.setPath([]string{scopeRelay, "meshcore-868", drawerNeighbours})
	if add, _ := s.complete("0d13"); add != "9b6421d0 " {
		t.Errorf("a key inside the drawer does not complete: %q", add)
	}
	// A key is content: print lists them, so help does not name them
	// twice.
	if help := s.treeHelp(s.curPath()); strings.Contains(help, "0d139b") {
		t.Errorf("help listed what print already lists:\n%s", help)
	}
}

func TestANameOffTheAirIsQuotedOnce(t *testing.T) {
	deps := testDeps(t)
	var key [32]byte
	copy(key[:], []byte{0x88, 0x2f, 0x6c, 0xdf, 0x02, 0x2d})
	deps.Relays[0].Neighbours = func() []Neighbour {
		return []Neighbour{{PubKey: key, Name: "FR91 Radiocom", SNR: 14, Heard: time.Now()}}
	}
	// The one function allowed to render a name off the air quotes it,
	// so nothing downstream may quote it again.
	out := run(t, deps, "/relay/meshcore-868/neighbours/print detail")
	if !strings.Contains(out, `name="FR91 Radiocom"`) || strings.Contains(out, `""`) {
		t.Errorf("the name was rendered twice:\n%s", out)
	}
	// A value that carries a space still has to survive as one pair.
	if !strings.Contains(out, `snr="+14.00 dB"`) {
		t.Errorf("a spaced value was not held together:\n%s", out)
	}
	// And a table leaves the separating to its columns.
	if table := run(t, deps, "/relay/meshcore-868/neighbours/print"); strings.Contains(table, `"+14.00 dB"`) {
		t.Errorf("the table quoted what its columns already separate:\n%s", table)
	}
}

func TestAskingANeighbourIsAVerbWhereTheNeighbourIs(t *testing.T) {
	deps := withNeighbour(t)
	deps.Privilege = Admin
	var asked []byte
	deps.Relays[0].AskScopes = func(prefix []byte) ([]string, error) {
		asked = prefix
		return []string{"eu", "fr-idf"}, nil
	}
	// Standing on one of them says which, so the verb takes no target
	// of its own: the path is the target.
	out := run(t, deps, "/relay/meshcore-868/neighbours/0d139b6421d0/ask-scopes")
	if !strings.Contains(out, "eu, fr-idf") {
		t.Errorf("the answer never came back:\n%s", out)
	}
	if hex.EncodeToString(asked) != "0d139b6421d0" {
		t.Errorf("the wrong neighbour was asked: %x", asked)
	}
	// It belongs to the neighbour, not to the relay or the drawer.
	s := &session{deps: deps}
	if v := s.verbsAt([]string{scopeRelay, "meshcore-868"}); slices.Contains(v, cmdAskScopes) {
		t.Errorf("the relay answers to it: %v", v)
	}
	if v := s.verbsAt([]string{scopeRelay, "meshcore-868", drawerNeighbours}); slices.Contains(v, cmdAskScopes) {
		t.Errorf("the drawer answers to it: %v", v)
	}
	if v := s.verbsAt([]string{scopeRelay, "meshcore-868", drawerNeighbours, "0d139b6421d0"}); !slices.Contains(v, cmdAskScopes) {
		t.Errorf("the neighbour does not answer to it: %v", v)
	}
	// And scopes carries no sub-verb any more.
	if refused := run(t, deps, "/relay/meshcore-868/scopes ask 0d13"); !strings.Contains(refused, "error") {
		t.Errorf("the sub-verb still runs:\n%s", refused)
	}
}

func TestAnUnnamedArgumentIsDeclaredLikeEveryOther(t *testing.T) {
	s := &session{deps: testDeps(t), colors: true}
	// It leads the help, in chevrons, because it is what comes first
	// on the line — and a word nobody can discover does not exist.
	if help := s.helpForLine("txn ", 0); !strings.Contains(help,
		cPunct+"<"+cReset+cAttr+"prefix"+cReset+cPunct+">"+cReset) {
		t.Errorf("the slot is not written as one:\n%s", help)
	}
	// Completion offers nothing for a word the operator invents.
	if add, hints := s.complete("txn pre"); add != "" || len(hints) > 0 {
		t.Errorf("a slot was offered as a word to type: %q %v", add, hints)
	}
	// The value carries no colour: this console has no opinion on it.
	if painted := s.paintLine("txn abc123"); strings.Contains(painted, cUnres+"abc123") {
		t.Errorf("a chosen value was marked unresolved: %q", painted)
	}
	// A command that declares no slot takes no bare word at all.
	if out := run(t, testDeps(t), "nodes stray"); !strings.Contains(out, "stray") {
		t.Errorf("an undeclared positional was swallowed:\n%s", out)
	}
	// And one that does takes exactly one.
	if out := run(t, testDeps(t), "txn aa bb"); !strings.Contains(out, "one prefix") {
		t.Errorf("a second positional was accepted:\n%s", out)
	}
}

func TestAFilterCompletesFromWhatTheJournalHolds(t *testing.T) {
	deps := testDeps(t)
	seed(t, deps)
	s := &session{deps: deps}
	// The vocabulary is read out of the journal, not from a list kept
	// beside it: what was never recorded cannot be filtered for.
	if _, hints := s.complete("frames verdict="); !slices.Contains(hints, "would-relay-flood") {
		t.Errorf("verdict= did not offer what the journal holds: %v", hints)
	}
	// One candidate finishes rather than listing.
	if add, _ := s.complete("frames verdict=would"); add != "-relay-flood " {
		t.Errorf("a lone verdict did not finish: %q", add)
	}
	if add, _ := s.complete("frames type=ADV"); add != "ERT " {
		t.Errorf("type= did not complete from the journal: %q", add)
	}
	// And the json the frames views used to produce is gone.
	if out := run(t, deps, "frames json"); !strings.Contains(out, "json") {
		t.Errorf("frames still answers to json:\n%s", out)
	}
}

func TestACommandLivesInItsContextAndTheRootSaysWhere(t *testing.T) {
	deps := withNeighbour(t)
	deps.Privilege = Admin
	// The root used to run these by quietly picking the only relay,
	// which was multi-relay hidden rather than handled. Now it points
	// at the place instead of guessing the instance.
	for _, c := range []struct{ line, home string }{
		{"scopes", "/relay/<name>"},
		{"advert", "/relay/<name>"},
		{"discover", "/relay/<name>/neighbours"},
		{"ask-scopes", "/relay/<name>/neighbours/<neighbour>"},
	} {
		out := run(t, deps, c.line)
		if !strings.Contains(out, "lives in "+c.home) {
			t.Errorf("%q was not pointed home:\n%s", c.line, out)
		}
	}
	// A question is answerable anywhere: refusing "scopes ?" would
	// refuse a question, and its answer names the home too.
	if out := run(t, deps, "scopes ?"); !strings.Contains(out, "lives in /relay/<name>") {
		t.Errorf("the question was refused or unanswered:\n%s", out)
	}
	// The root neither lists nor offers what does not live there.
	if help := run(t, deps, "help"); strings.Contains(help, "advert") {
		t.Errorf("the root help lists a homed command:\n%s", help)
	}
	s := &session{deps: deps}
	if add, _ := s.complete("adver"); add != "" {
		t.Errorf("the root completed a homed command: %q", add)
	}
	// Inside its context it is a word like any other.
	s.setPath([]string{scopeRelay, "meshcore-868"})
	if add, _ := s.complete("adver"); add != "t " {
		t.Errorf("the instance does not complete its own verb: %q", add)
	}
}
