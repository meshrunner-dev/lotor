package cli

import (
	"context"
	"slices"
	"strings"
	"testing"

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
	// Context help names the instances and what each one speaks.
	if !strings.Contains(out, "meshcore-868") || !strings.Contains(out, "print") {
		t.Errorf("context help lacks its content:\n%s", out)
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
	out := run(t, deps, "/relay meshcore-868 neighbours")
	if !strings.Contains(out, "nobody heard directly yet") {
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
		{"/re", "lay "},
		{"/relay mesh", "core-868 "},
		{"/relay meshcore-868 pr", "int "},
		{"/relay meshcore-868 disc", "over "},
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
	painted := s.paintLine("/relay meshcore-868 print node_name=x")
	for _, want := range []string{
		cPath + "/" + cReset,            // the slash names the root
		cPath + "relay" + cReset,        // a place
		cPath + "meshcore-868" + cReset, // a place
		cVerb + "print" + cReset,        // an action
		cAttr + "node_name" + cReset,    // an attribute
		cPunct + "=" + cReset,           // what joins the pair
	} {
		if !strings.Contains(painted, want) {
			t.Errorf("missing %q in %q", want, painted)
		}
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

func TestTreeExportIsPasteableAndKeepsSecrets(t *testing.T) {
	out := run(t, testDeps(t), "/relay meshcore-868 export")
	// The line is absolute — pasteable from anywhere — and quoted
	// values keep their quotes.
	if !strings.Contains(out, "/relay add meshcore-868 ") ||
		!strings.Contains(out, `node_name="test 🦝"`) {
		t.Errorf("export is not a recreating line:\n%s", out)
	}
	if strings.Contains(out, "b5445dd625d531fc") {
		t.Fatalf("export carried the private key:\n%s", out)
	}
	if !strings.Contains(out, "identity is secret") {
		t.Errorf("export does not say what it cannot carry:\n%s", out)
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
	if !strings.Contains(got, "# ") {
		t.Errorf("the secret comment is not dimmed:\n%q", got)
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
		offered := s.candidatesAt(path, true)
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
