package cli

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/schema"
	"meshrunner.dev/lotor/internal/update"
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
	if !strings.Contains(out, "has no regions") {
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

	// A quoted value holds its spaces: the painter cuts the line the
	// way the parser does, so the value never bleeds into a second,
	// unresolvable word — even while the closing quote is not yet
	// typed. Stripped of its colours, the line reads back as typed.
	for _, line := range []string{
		`/relay meshcore-868 set node_name="new name" identity=x`,
		`/relay meshcore-868 set node_name="new na`,
		`/relay meshcore-868  set  node_name=x`,
	} {
		painted := s.paintLine(line)
		if strings.Contains(painted, cUnres) {
			t.Errorf("quoted value painted unresolved: %q", painted)
		}
		if stripped := stripSGR(painted); stripped != line {
			t.Errorf("painting altered the line: %q vs %q", stripped, line)
		}
	}
}

// stripSGR removes the colour codes, leaving the characters as typed.
func stripSGR(s string) string {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == 0x1b {
			for i < len(s) && s[i] != 'm' {
				i++
			}
			continue
		}
		b.WriteByte(s[i])
	}
	return b.String()
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

func TestProfileValuesComplete(t *testing.T) {
	s := &session{deps: testDeps(t)}
	// One candidate left: it finishes, custom included in the offer.
	for _, c := range []struct{ line, add string }{
		{"/mqtt/add eu profile=meshm", "apper "},
		{"/mqtt/add eu profile=cus", "tom "},
		{"/radio/add g3 profile=lyra-zerow-station-", "g3 "},
		{"/radio/add g3 driver=sx126x-spi profile=rak6421-13300x-", "slot1 "},
	} {
		add, hints := s.complete(c.line)
		if add != c.add || hints != nil {
			t.Errorf("complete(%q) = %q %v, want %q", c.line, add, hints, c.add)
		}
	}
	// Two analyzers: the shared prefix advances, both show as hints.
	if _, hints := s.complete("/mqtt/add eu profile=analyzer"); len(hints) != 2 {
		t.Errorf("analyzer hints = %v", hints)
	}
}

func TestChoiceValuesCompleteWhileAdding(t *testing.T) {
	s := &session{deps: testDeps(t)}
	for _, c := range []struct{ line, add string }{
		{"/radio/add g3 driver=sx126", "x-spi "},
		{"/relay/add mc protocol=mesh", "core "},
	} {
		add, hints := s.complete(c.line)
		if add != c.add || hints != nil {
			t.Errorf("complete(%q) = %q %v, want %q", c.line, add, hints, c.add)
		}
	}
	s.setPath([]string{"radio"})
	if add, hints := s.complete("add g3 driver=sx126"); add != "x-spi " || hints != nil {
		t.Errorf("radio-context driver completion = %q %v", add, hints)
	}
}

func TestCompletionDoesNotStutterASpokenWord(t *testing.T) {
	s := &session{deps: testDeps(t)}
	// One switch, already on the line: TAB must not offer it again —
	// a real transcript once read "advert" then flood six times over.
	if add, hints := s.complete("/relay meshcore-868 advert flood "); add != "" || hints != nil {
		t.Errorf("flood re-offered: %q %v", add, hints)
	}
	// An attribute valued on the line is spoken too.
	add, hints := s.complete("/relay meshcore-868 print interval=2s ")
	for _, h := range append(hints, add) {
		if strings.Contains(h, "interval") {
			t.Errorf("interval re-offered: %q %v", add, hints)
		}
	}
}

func TestAddRefusesAnAttributeAsTheName(t *testing.T) {
	deps := testDeps(t)
	deps.Privilege = Admin
	deps.Create = func(context.Context, string, string, map[string]string, string) (string, error) {
		t.Fatal("a bad line reached the manager")
		return "", nil
	}
	deps.Remove = func(context.Context, string, string, string) (string, error) { return "", nil }
	out := run(t, deps, "/mqtt/add profile=analyzer-eu")
	if !strings.Contains(out, "the name comes first") {
		t.Errorf("guard did not speak:\n%s", out)
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
	deps := testDeps(t)
	deps.Privilege = Admin
	out := run(t, deps, "/relay meshcore-868 export")
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
	deps.Privilege = Admin
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
	deps.Privilege = Admin
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
	// spellings complete — and both run. /system holds the history
	// drawer now, so it completes as the container it is.
	for _, line := range []string{"sys", "/sys"} {
		if add, _ := s.complete(line); add != "tem/" {
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
	adminDeps := testDeps(t)
	adminDeps.Privilege = Admin
	masked := run(t, adminDeps, "/relay meshcore-868 print")
	if !strings.Contains(masked, maskedValue) || strings.Contains(masked, "b5445dd625d531fc") {
		t.Errorf("print did not mask the identity:\n%s", masked)
	}
	shown := run(t, adminDeps, "/relay meshcore-868 print show-secrets")
	if !strings.Contains(shown, "b5445dd625d531fc") {
		t.Errorf("the mask did not lift when asked:\n%s", shown)
	}
	// The two words are about different things, and neither does the
	// other's job: unfolding a listing does not lift the mask.
	folded := run(t, adminDeps, "/relay print detail")
	if !strings.Contains(folded, maskedValue) || strings.Contains(folded, "b5445dd625d531fc") {
		t.Errorf("detail lifted a mask it was not asked to:\n%s", folded)
	}
	both := run(t, adminDeps, "/relay print detail show-secrets")
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
	deps := testDeps(t)
	deps.Privilege = Admin
	out := run(t, deps, "/relay meshcore-868 print zz")
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
	if coll := run(t, deps, "/relay print show-secrets"); !strings.Contains(coll, argDetail) {
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
		if got := rawValue(c.in); got != "12,13" {
			t.Errorf("rawValue(%v) = %q", c.in, got)
		}
	}
	attr := schema.Attr{Name: "enable_pins", Type: schema.Ints}
	if _, err := schema.Parse(attr, rawValue([]int{12, 13})); err != nil {
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
	deps.Privilege = Admin
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
	deps.Relays[0].AskRegions = func(prefix []byte) ([]string, error) {
		asked = prefix
		return []string{"eu", "fr-idf"}, nil
	}
	// Standing on one of them says which, so the verb takes no target
	// of its own: the path is the target.
	out := run(t, deps, "/relay/meshcore-868/neighbours/0d139b6421d0/ask-regions")
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

func TestSessionsAreADrawerOnTheConsoleItself(t *testing.T) {
	deps := testDeps(t)
	deps.Kinds = append(deps.Kinds, schema.Kind{
		Name: "cli", Doc: "the operator listener", Singleton: true,
		Attrs: []schema.Attr{{Name: "session_limit", Type: schema.Int, Doc: "how many at once"}},
	})
	// A second session, standing somewhere, so the first can see it.
	other := &session{deps: deps, out: &strings.Builder{}, remote: "192.0.2.7:40312", colors: true}
	other.setPath([]string{scopeRelay, "meshcore-868"})
	otherDone := other.register()
	defer otherDone()

	out := run(t, deps, "/cli/sessions/print")
	for _, want := range []string{"read-only", "192.0.2.7:40312", "/relay/meshcore-868", "this session"} {
		if !strings.Contains(out, want) {
			t.Errorf("the table lacks %q:\n%s", want, out)
		}
	}
	// One of them is somewhere to stand: its id resolves as a step,
	// and print there shows the one session whole.
	one := run(t, deps, "/cli/sessions/1/print")
	if !strings.Contains(one, "192.0.2.7:40312") || !strings.Contains(one, "term") {
		t.Errorf("standing on a session showed nothing of it:\n%s", one)
	}
	// A drawer answers to print and nothing else, on a singleton as on
	// an instance.
	if refused := run(t, deps, "/cli/sessions/export"); !strings.Contains(refused, `no "export" here`) {
		t.Errorf("the drawer answered to export:\n%s", refused)
	}
	// And it is discoverable: the singleton offers it as a place.
	s := &session{deps: deps}
	s.setPath([]string{"cli"})
	if add, _ := s.complete("sess"); add != "ions/" {
		t.Errorf("the drawer does not complete as a place: %q", add)
	}
	// The whole TAB chain from the root holds together: a singleton
	// holding a drawer is a container, so completing it leaves the
	// operator mid-path — never at a space nothing follows from.
	s.setPath(nil)
	if add, _ := s.complete("/cl"); add != "i/" {
		t.Errorf("/cl did not complete into the container: %q", add)
	}
	if add, _ := s.complete("/cli/sess"); add != "ions/" {
		t.Errorf("/cli/sess did not finish: %q", add)
	}
	// A singleton that grew a drawer completes into it, like /cli did.
	if add, _ := s.complete("/syst"); add != "em/" {
		t.Errorf("/syst is a container now: %q", add)
	}
	if add, _ := s.complete("/system/hist"); add != "ory/" {
		t.Errorf("/system/hist did not finish: %q", add)
	}
}

func TestASessionSeesItsOwnLifeCycle(t *testing.T) {
	deps := testDeps(t)
	// register/remove bracket the repl: after a session ends, its row
	// is gone, and ids are never reused within a daemon's life.
	a := &session{deps: deps, out: &strings.Builder{}}
	doneA := a.register()
	b := &session{deps: deps, out: &strings.Builder{}}
	doneB := b.register()
	if a.id == b.id {
		t.Fatalf("two sessions share id %q", a.id)
	}
	doneA()
	if got := b.sessionKeys(""); len(got) != 1 {
		t.Errorf("a closed session still listed: %v", got)
	}
	c := &session{deps: deps, out: &strings.Builder{}}
	defer c.register()()
	if c.id == a.id {
		t.Errorf("id %q was reused", a.id)
	}
	doneB()
}

func TestAirSessionsAreADrawerOnTheRelay(t *testing.T) {
	deps := testDeps(t)
	var adjacent, routed, lost [32]byte
	adjacent[0], routed[0], lost[0] = 0xAA, 0xBB, 0xCC
	deps.Relays[0].AirSessions = func() ([]AirSession, error) {
		return []AirSession{
			// A taught route, hop by hop, is the whole point of asking.
			{PubKey: routed, HasPath: true, Path: []byte{0x4f, 0xa2}, LastActive: time.Now()},
			// Zero hops is knowledge too: the client is adjacent.
			{PubKey: adjacent, HasPath: true, Path: []byte{}, LastActive: time.Now()},
			// And no route yet is what it costs.
			{PubKey: lost, LastActive: time.Now()},
		}, nil
	}
	out := run(t, deps, "/relay/meshcore-868/sessions/print")
	for _, want := range []string{"4f→a2 (2 hops)", "adjacent (0 hops)", "none yet — answers flood",
		"direct", "flood", "guest"} {
		if !strings.Contains(out, want) {
			t.Errorf("the table lacks %q:\n%s", want, out)
		}
	}
	// One of them is somewhere to stand.
	one := run(t, deps, "/relay/meshcore-868/sessions/bb0000000000/print")
	if !strings.Contains(one, "4f→a2") {
		t.Errorf("standing on a session showed nothing of it:\n%s", one)
	}
	// Runtime through and through: print answers, nothing else does.
	if refused := run(t, deps, "/relay/meshcore-868/sessions/export"); !strings.Contains(refused, `no "export" here`) {
		t.Errorf("the drawer answered to export:\n%s", refused)
	}
	// A protocol without sessions says so.
	deps.Relays[0].AirSessions = nil
	if none := run(t, deps, "/relay/meshcore-868/sessions/print"); !strings.Contains(none, "keeps no over-the-air sessions") {
		t.Errorf("the absence was not named:\n%s", none)
	}
}

func TestUpdateCheckIsAVerbOnItsBlock(t *testing.T) {
	sec, pub, err := update.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	m := update.Manifest{
		Product: "lotor", Channel: "release", Version: "9.9.9",
		Published: time.Now().UTC().Truncate(time.Second),
		Artifacts: map[string]update.Artifact{
			runtime.GOOS + "/" + runtime.GOARCH: {
				URL: "https://example.org/lotor", SHA256: strings.Repeat("0", 64), Size: 42},
		},
		Notes: "everything, полностью",
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	sig := update.Sign(raw, sec, "channel:release")
	mux := http.NewServeMux()
	mux.HandleFunc("/release/manifest.json", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(raw) })
	mux.HandleFunc("/release/manifest.json.minisig", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(sig) })
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	deps := testDeps(t)
	deps.Version = "0.1.0"
	deps.UpdateTrust = func() ([]update.PublicKey, error) { return []update.PublicKey{pub}, nil }
	deps.Kinds = append(deps.Kinds, schema.Kind{
		Name: "update", Doc: "where this relay looks for newer versions of itself",
		Singleton: true, Attrs: config.UpdateAttrs(),
	})
	deps.Traces["update"] = []config.Trace{
		{Key: "channel", Value: "release", Source: "config"},
		{Key: "url", Value: srv.URL, Source: "config"},
	}

	out := run(t, deps, "/update/check")
	for _, want := range []string{"9.9.9", "0.1.0", "signed by"} {
		if !strings.Contains(out, want) {
			t.Errorf("the check does not say %q:\n%s", want, out)
		}
	}
	// The verb lives on its block: offered there, refused at the root
	// with the pointer home.
	if help := run(t, deps, "/update ?"); !strings.Contains(help, cmdCheck) {
		t.Errorf("the block does not offer check:\n%s", help)
	}
	if refused := run(t, deps, "check"); !strings.Contains(refused, "lives in /update") {
		t.Errorf("the root did not point home:\n%s", refused)
	}
	// A channel already running is said plainly.
	deps.Version = "9.9.9"
	if same := run(t, deps, "/update/check"); !strings.Contains(same, "nothing newer") {
		t.Errorf("parity was not named:\n%s", same)
	}
	// No trusted keys is the honest state before a key is minted.
	deps.UpdateTrust = func() ([]update.PublicKey, error) { return nil, nil }
	if none := run(t, deps, "/update/check"); !strings.Contains(none, "no trusted keys") {
		t.Errorf("keylessness was not named:\n%s", none)
	}
}

func TestUpdateInstallStagesAVerifiedUpdate(t *testing.T) {
	sec, pub, err := update.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	binary := []byte("#!/bin/sh\nexit 0\n") // a stand-in the stage can hash
	sum := sha256.Sum256(binary)
	mux := http.NewServeMux()
	var m update.Manifest
	var raw []byte
	mux.HandleFunc("/dev/manifest.json", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(raw) })
	mux.HandleFunc("/dev/manifest.json.minisig", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(update.Sign(raw, sec, "channel:dev"))
	})
	mux.HandleFunc("/artifact", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(binary) })
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	m = update.Manifest{
		Product: "lotor", Channel: "dev", Version: "0.2.0-dev.zz",
		Published: time.Now().UTC().Truncate(time.Second),
		Artifacts: map[string]update.Artifact{
			update.Platform(): {URL: srv.URL + "/artifact",
				SHA256: hex.EncodeToString(sum[:]), Size: int64(len(binary))},
		},
	}
	if raw, err = json.Marshal(m); err != nil {
		t.Fatal(err)
	}

	deps := testDeps(t)
	deps.Privilege = Admin
	deps.Version = "0.1.0"
	deps.StateDir = t.TempDir()
	deps.UpdateTrust = func() ([]update.PublicKey, error) { return []update.PublicKey{pub}, nil }
	deps.Kinds = append(deps.Kinds, schema.Kind{
		Name: "update", Doc: "updates", Singleton: true, Attrs: config.UpdateAttrs(),
	})
	deps.Traces["update"] = []config.Trace{
		{Key: "channel", Value: "dev", Source: "config"},
		{Key: "url", Value: srv.URL, Source: "config"},
	}

	out := run(t, deps, "/update/install")
	if !strings.Contains(out, "staged — the installer takes it from here") {
		t.Fatalf("the install did not stage:\n%s", out)
	}
	// The stage verifies whole against the same trust the installer
	// will hold it to.
	ready, err := update.VerifyStaged(update.StageDir(deps.StateDir), []update.PublicKey{pub})
	if err != nil || ready.Version != "0.2.0-dev.zz" {
		t.Fatalf("VerifyStaged = %+v, %v", ready, err)
	}
	// Nothing newer refuses unless forced.
	deps.Version = "0.2.0-dev.zz"
	deps.Traces["update"][0].Value = "dev"
	if refused := run(t, deps, "/update/install"); !strings.Contains(refused, "nothing newer") {
		t.Errorf("parity was staged anyway:\n%s", refused)
	}
	// Read-only sessions may not stage a binary.
	deps.Privilege = ReadOnly
	if denied := run(t, deps, "/update/install force"); !strings.Contains(denied, "admin") {
		t.Errorf("a read-only session staged an update:\n%s", denied)
	}
}

func TestSystemHistoryDrawer(t *testing.T) {
	deps := testDeps(t)
	var asked HistoryQuery
	deps.History = func(_ context.Context, q HistoryQuery) ([]HistoryEntry, int, error) {
		asked = q
		return []HistoryEntry{
			{ID: 52, At: time.Now().Add(-2 * time.Minute), Principal: "console",
				Kind: "mqtt", Name: "lab", Op: "set",
				Changes: []AttrDelta{{Attr: "iata", Old: "par", New: "PAR"}}},
			{ID: 51, At: time.Now().Add(-time.Hour), Principal: "console",
				Kind: "update", Op: "set",
				Changes: []AttrDelta{{Attr: "token", New: "<secret>"}}},
		}, 2, nil
	}
	out := run(t, deps, "/system/history/print")
	// Newest first, deltas spelled, a singleton named by its kind
	// alone, and the mask shown exactly as the store keeps it.
	at52, at51 := strings.Index(out, "52"), strings.Index(out, "51")
	if at52 == -1 || at51 == -1 || at52 > at51 {
		t.Errorf("order or coverage wrong:\n%s", out)
	}
	for _, want := range []string{"iata: par → PAR", "mqtt lab", "token: → <secret>", "update"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q:\n%s", want, out)
		}
	}
	// One revision, stood upon.
	item := run(t, deps, "/system/history/52/print")
	for _, want := range []string{"ATTRIBUTE", "console", "mqtt lab"} {
		if !strings.Contains(item, want) {
			t.Errorf("item view missing %q:\n%s", want, item)
		}
	}
	// The temporal selectors travel: frames' vocabulary, a revision
	// id for around=.
	run(t, deps, "/system/history/print last=2")
	if asked.Count != 2 {
		t.Errorf("last=2 asked %+v", asked)
	}
	run(t, deps, "/system/history/print last=2h")
	if asked.Since.IsZero() || time.Until(asked.Since) > -90*time.Minute {
		t.Errorf("last=2h asked %+v", asked)
	}
	run(t, deps, "/system/history/print around=45 span=30m")
	if asked.AroundID != 45 || asked.Span != 30*time.Minute {
		t.Errorf("around asked %+v", asked)
	}
	if out := run(t, deps, "/system/history/print around=nope"); !strings.Contains(out, "revision id") {
		t.Errorf("bad around accepted:\n%s", out)
	}
	// The selectors belong to windowed drawers alone.
	if out := run(t, deps, "/relay meshcore-868 neighbours print last=5"); !strings.Contains(out, "takes") {
		t.Errorf("neighbours accepted a window:\n%s", out)
	}

	// Without the dependency the drawer is honestly empty.
	deps.History = nil
	if out := run(t, deps, "/system/history/print"); !strings.Contains(out, "no changes recorded") {
		t.Errorf("empty line missing:\n%s", out)
	}
}

func TestHistoryConfessesItsCap(t *testing.T) {
	deps := testDeps(t)
	deps.History = func(_ context.Context, q HistoryQuery) ([]HistoryEntry, int, error) {
		return []HistoryEntry{{ID: 9, At: time.Now(), Principal: "console",
			Kind: "mqtt", Name: "lab", Op: "set"}}, 40, nil
	}
	out := run(t, deps, "/system/history/print since=00:00")
	if !strings.Contains(out, "newest 1 of 40 shown") {
		t.Errorf("no confession:\n%s", out)
	}
}

func TestACLDrawerGrantsAndRevokes(t *testing.T) {
	deps := testDeps(t)
	deps.Privilege = Admin
	var granted [][2]any // {pubkeyHex, verb}
	live := []Access{{Role: "admin", Granted: true, LastActive: time.Now()}}
	for i := range live[0].PubKey {
		live[0].PubKey[i] = byte(i)
	}
	for i := range deps.Relays {
		deps.Relays[i].Access = func() ([]Access, error) { return live, nil }
		deps.Relays[i].GrantRole = func(pub []byte, role string) error {
			granted = append(granted, [2]any{hex.EncodeToString(pub), "grant:" + role})
			return nil
		}
		deps.Relays[i].Revoke = func(pub []byte) error {
			granted = append(granted, [2]any{hex.EncodeToString(pub), "revoke"})
			return nil
		}
	}
	// The drawer lists the grant, its role and how it was earned.
	out := run(t, deps, "/relay meshcore-868 acl print")
	for _, want := range []string{"admin", "granted", "000102030405"} {
		if !strings.Contains(out, want) {
			t.Errorf("acl listing missing %q:\n%s", want, out)
		}
	}
	// grant needs a whole key; a short one is refused before the door.
	if out := run(t, deps, "/relay meshcore-868 acl grant key=abcd"); !strings.Contains(out, "64") {
		t.Errorf("short key not refused:\n%s", out)
	}
	if len(granted) != 0 {
		t.Fatalf("a bad grant reached the engine: %v", granted)
	}
	full := strings.Repeat("ab", 32)
	run(t, deps, "/relay meshcore-868 acl grant key="+full)
	if len(granted) != 1 || granted[0][0] != full || granted[0][1] != "grant:admin" {
		t.Fatalf("grant door saw %v", granted)
	}
	// A named lesser role travels by its word.
	run(t, deps, "/relay meshcore-868 acl grant key="+full+" role=read-only")
	if len(granted) != 2 || granted[1][1] != "grant:read-only" {
		t.Fatalf("role word lost: %v", granted)
	}
	// revoke names an entry by prefix; the engine gets the whole key.
	run(t, deps, "/relay/meshcore-868/acl/000102030405/revoke")
	if len(granted) != 3 || granted[2][1] != "revoke" ||
		granted[2][0] != "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f" {
		t.Fatalf("revoke door saw %v", granted)
	}
}

func TestACLEntryShowsItsKeyAndSetsItsRole(t *testing.T) {
	deps := testDeps(t)
	deps.Privilege = Admin
	var granted [][2]string // {pubkeyHex, role}
	live := []Access{{Role: "admin", Granted: true, LastActive: time.Now()}}
	for i := range live[0].PubKey {
		live[0].PubKey[i] = byte(i)
	}
	fullKey := hex.EncodeToString(live[0].PubKey[:])
	for i := range deps.Relays {
		deps.Relays[i].Access = func() ([]Access, error) { return live, nil }
		deps.Relays[i].GrantRole = func(pub []byte, role string) error {
			granted = append(granted, [2]string{hex.EncodeToString(pub), role})
			return nil
		}
	}
	// The whole key shows only where print stands on the entry: grant
	// wants it whole, and the listing's prefix is nowhere to copy it
	// from — while the listing itself stays a table, not a key dump.
	if out := run(t, deps, "/relay/meshcore-868/acl/000102030405/print"); !strings.Contains(out, fullKey) {
		t.Errorf("entry print hides the whole key:\n%s", out)
	}
	if out := run(t, deps, "/relay meshcore-868 acl print"); strings.Contains(out, fullKey) {
		t.Errorf("the listing spilled whole keys:\n%s", out)
	}
	// set role= from the entry: no key typed, the drawer names the
	// subject, the engine gets the whole key.
	run(t, deps, "/relay/meshcore-868/acl/000102030405/set role=read-write")
	if len(granted) != 1 || granted[0][0] != fullKey || granted[0][1] != "read-write" {
		t.Fatalf("role door saw %v", granted)
	}
	// Anything but the role is refused, and says what is settable.
	if out := run(t, deps, "/relay/meshcore-868/acl/000102030405/set name=x"); !strings.Contains(out, "role") {
		t.Errorf("wrong attribute not refused:\n%s", out)
	}
	// unset is not offered on an entry: removal is revoke's business,
	// and the place says what it answers to.
	if out := run(t, deps, "/relay/meshcore-868/acl/000102030405/unset role"); !strings.Contains(out, `no "unset" here`) {
		t.Errorf("unset not refused:\n%s", out)
	}
	// A drawer with nothing settable never offers the verb at all.
	if out := run(t, deps, "/relay/meshcore-868/neighbours/set x=y"); !strings.Contains(out, `no "set" here`) {
		t.Errorf("drawer-level set not refused:\n%s", out)
	}
	if len(granted) != 1 {
		t.Fatalf("a refused set reached the engine: %v", granted)
	}
	// The role door is the admin's, like the grant it goes through.
	deps.Privilege = ReadOnly
	if out := run(t, deps, "/relay/meshcore-868/acl/000102030405/set role=admin"); !strings.Contains(out, "admin verb") {
		t.Errorf("non-admin set not refused:\n%s", out)
	}
	if len(granted) != 1 {
		t.Fatalf("a non-admin set reached the engine: %v", granted)
	}
}

func TestACLEntrySetCompletesAndPaints(t *testing.T) {
	deps := testDeps(t)
	deps.Privilege = Admin
	live := []Access{{Role: "admin", Granted: true, LastActive: time.Now()}}
	for i := range live[0].PubKey {
		live[0].PubKey[i] = byte(i)
	}
	for i := range deps.Relays {
		deps.Relays[i].Access = func() ([]Access, error) { return live, nil }
	}
	s := &session{deps: deps, colors: true}
	s.setPath([]string{"relay", "meshcore-868", "acl", "000102030405"})

	// After set, the entry's one settable attribute completes…
	if add, _ := s.complete("set ro"); add != "le=" {
		t.Errorf("complete(set ro) = %q, want le=", add)
	}
	// …and its value completes from the role ladder.
	if _, hints := s.complete("set role="); len(hints) != 3 {
		t.Errorf("role values offered: %v, want the three roles", hints)
	}
	if add, _ := s.complete("set role=read-w"); !strings.HasPrefix(add, "rite") {
		t.Errorf("complete(set role=read-w) = %q", add)
	}
	// The painter resolves the words instead of leaving them red.
	painted := s.paintLine("set role=admin")
	if strings.Contains(painted, cUnres) {
		t.Errorf("something stayed unresolved: %q", painted)
	}
	if !strings.Contains(painted, cAttr+"role") {
		t.Errorf("role not painted as an attribute: %q", painted)
	}
}

func TestEnvelopeTextReadsThePresenceBit(t *testing.T) {
	// A ceiling of exactly 0 dBm is a declared board; "undeclared"
	// told the operator the opposite of what the envelope knows.
	zero := radio.Envelope{MaxTxPowerDBm: 0, MaxTxPowerSet: true}
	if got := envelopeText(zero); !strings.Contains(got, "max 0 dBm") {
		t.Errorf("declared zero ceiling shows %q", got)
	}
	if got := envelopeText(radio.Envelope{}); got != "envelope undeclared" {
		t.Errorf("an empty envelope shows %q", got)
	}
}

func TestSecretsAnswerToTheAdminAlone(t *testing.T) {
	// The non-disclosure matrix: the read-only listener carries no
	// authentication, so every mask-lifting surface — export at any
	// depth, show-secrets with and without detail — refuses below
	// Admin, and none of the refusals leak the value.
	seed := "abab1212abab1212abab1212abab1212abab1212abab1212abab1212abab1212"
	arm := func(p Privilege) Deps {
		deps := testDeps(t)
		deps.Privilege = p
		deps.Traces["relay meshcore-868"] = append(deps.Traces["relay meshcore-868"],
			config.Trace{Key: "identity", Value: seed, Source: "override:custom"})
		return deps
	}
	surfaces := []string{
		"/export",
		"/relay export",
		"/relay meshcore-868 export",
		"/relay meshcore-868 print show-secrets",
		"/relay print detail show-secrets",
	}
	for _, p := range []Privilege{ReadOnly, ""} {
		for _, line := range surfaces {
			out := run(t, arm(p), line)
			if strings.Contains(out, seed) {
				t.Errorf("privilege %q: %q disclosed the identity:\n%s", p, line, out)
			}
			if !strings.Contains(out, "admin") {
				t.Errorf("privilege %q: %q does not say who may:\n%s", p, line, out)
			}
		}
	}
	// The admin still gets the whole truth — an export that silently
	// masked would recreate a different node.
	for _, line := range surfaces {
		if out := run(t, arm(Admin), line); !strings.Contains(out, seed) {
			t.Errorf("admin: %q withheld the identity:\n%s", line, out)
		}
	}
	// And the plain read-only print still answers, masked.
	out := run(t, arm(ReadOnly), "/relay meshcore-868 print")
	if strings.Contains(out, seed) {
		t.Errorf("plain detail disclosed the identity:\n%s", out)
	}
	if !strings.Contains(out, "identity") {
		t.Errorf("plain detail lost the attribute row:\n%s", out)
	}
}

func TestExportCarriesInactiveScopes(t *testing.T) {
	// The design promise: switching profiles is non-destructive, so an
	// export that recreates the configuration must carry every scope —
	// the settings prepared for the other band survive the round trip,
	// and the last line restores the profile actually selected.
	deps := testDeps(t)
	deps.Privilege = Admin
	deps.Layers = func(kind, name string) (string, map[string]map[string]any, bool) {
		if kind != scopeRelay || name != "meshcore-868" {
			return "", nil, false
		}
		return "eu-868-narrow", map[string]map[string]any{
			"eu-868-narrow": {"node_name": "Raccoon City"},
			"eu-433":        {"node_name": "Winter Den", "tx_power_dbm": "3"},
		}, true
	}
	out := run(t, deps, "/relay meshcore-868 export")
	main := strings.Index(out, `node_name="Raccoon City"`)
	other := strings.Index(out, "profile=eu-433")
	kept := strings.Index(out, `node_name="Winter Den"`)
	restore := strings.LastIndex(out, "profile=eu-868-narrow")
	if main == -1 || other == -1 || kept == -1 || restore == -1 {
		t.Fatalf("export lost a scope:\n%s", out)
	}
	if main >= other || other > kept || kept >= restore {
		t.Errorf("export order broken (%d %d %d %d):\n%s", main, other, kept, restore, out)
	}
}

func TestExportedValuesSurviveTheirOwnGrammar(t *testing.T) {
	// Two grammars, deliberately: the historic quoted form reads
	// EXACTLY as it always did — an old export's backslashes are
	// bytes, never escapes — and the values that form cannot carry
	// (quotes, line breaks) travel under the set64 verb, a marker in
	// the command grammar no old export can have produced.

	// The legacy matrix from the review: replayed after the upgrade,
	// every backslash sequence stays the literal bytes it was.
	for _, c := range []struct{ line, want string }{
		{`set node_name="C:\new folder"`, `C:\new folder`},
		{`set node_name="tab\there"`, `tab\there`},
		{`set node_name="cr\rthere"`, `cr\rthere`},
		{`set node_name="double\\slash"`, `double\\slash`},
		{`set node_name="test raccoon"`, "test raccoon"},
	} {
		args := splitArgs(c.line)
		if len(args) != 2 {
			t.Fatalf("%q split into %q", c.line, args)
		}
		_, got, _ := strings.Cut(args[1], "=")
		if got != c.want {
			t.Errorf("legacy export changed: %q read as %q, want %q", c.line, got, c.want)
		}
	}

	// The current matrix: inline where the historic form is faithful,
	// set64 where it is not — and both round-trip byte-identical
	// through the REAL framing.
	for _, v := range []string{
		"plain",
		"two words",
		`back\slash`,
		"tab\there",
		`hunter "two"`,
		"line\nbreak",
		"line\rbreak",
		"crlf\r\nboth",
		`"`,
		"",
	} {
		var line string
		if inlineRenderable(v) {
			line = "set password=" + quoteIfSpaced(v)
		} else {
			line = "set64 password=" + base64.StdEncoding.EncodeToString([]byte(v))
		}
		framed, err := readBounded(bufio.NewReader(strings.NewReader(line + "\n")))
		if err != nil && !errors.Is(err, io.EOF) {
			t.Fatalf("%q framing: %v", v, err)
		}
		args := splitArgs(framed)
		if len(args) != 2 {
			t.Fatalf("%q rendered %q split into %q", v, line, args)
		}
		_, got, _ := strings.Cut(args[1], "=")
		if args[0] == "set64" {
			raw, err := base64.StdEncoding.DecodeString(got)
			if err != nil {
				t.Fatalf("%q decode: %v", v, err)
			}
			got = string(raw)
		}
		if got != v {
			t.Errorf("round trip lost bytes: %q → %q → %q", v, line, got)
		}
	}
}

func TestSet64IsTheOrdinaryMutation(t *testing.T) {
	// The verb decodes and then walks the same path set walks — the
	// same journal, the same gates — so a control-laden secret from an
	// export lands exactly as typed.
	deps := testDeps(t)
	deps.Privilege = Admin
	var got map[string]string
	deps.Mutate = func(ctx context.Context, kind, name string, set map[string]string, unset []string, principal string) (string, error) {
		got = set
		return "", nil
	}
	secret := "pa\nss\"word\""
	run(t, deps, "/relay meshcore-868 set64 guest_password="+
		base64.StdEncoding.EncodeToString([]byte(secret)))
	if got == nil || got["guest_password"] != secret {
		t.Errorf("set64 delivered %q, want %q", got["guest_password"], secret)
	}
	// A payload that is not base64 refuses by name.
	out := run(t, deps, "/relay meshcore-868 set64 guest_password=%%%")
	if !strings.Contains(out, "set64") {
		t.Errorf("bad payload refusal:\n%s", out)
	}
}

func TestEncodedExportStaysOneCommand(t *testing.T) {
	// One logical command, one line: as soon as any value needs the
	// encoded carrier, the WHOLE command travels under it — add64 for
	// a creation, set64 for a mutation — with the instance where the
	// grammar reads it: in the path for mutations, after the verb for
	// creations. Split in two, a creation whose halves need each
	// other (guest_access without its password) is refused with the
	// secret still in flight; and a singleton whose only value is
	// encoded must not leave an empty ordinary line behind.
	deps := testDeps(t)
	deps.Privilege = Admin
	deps.Traces["system"] = []config.Trace{
		{Key: "name", Source: "config", Value: "two\nlines"},
	}
	deps.Layers = func(kind, name string) (string, map[string]map[string]any, bool) {
		if kind != scopeRelay || name != "meshcore-868" {
			return "", nil, false
		}
		return "eu-868-narrow", map[string]map[string]any{
			"eu-868-narrow": {"guest_access": "password", "guest_password": `h"unter`},
			"eu-433":        {"node_name": "line\nbreak", "tx_power_dbm": "3"},
		}, true
	}
	out := run(t, deps, "export")

	decode := func(line string) map[string]string {
		t.Helper()
		got := map[string]string{}
		for _, w := range splitArgs(line) {
			k, v, ok := strings.Cut(w, "=")
			if !ok {
				continue
			}
			raw, err := base64.StdEncoding.DecodeString(v)
			if err != nil {
				t.Fatalf("%q: %s is not base64: %v", line, k, err)
			}
			got[k] = string(raw)
		}
		return got
	}
	var add64, scope64, restore, system string
	for l := range strings.SplitSeq(out, "\r\n") {
		switch {
		case strings.HasPrefix(l, "/relay add64 meshcore-868 "):
			add64 = l
		case strings.HasPrefix(l, "/relay meshcore-868 set64 "):
			scope64 = l
		case strings.HasPrefix(l, "/relay meshcore-868 set "):
			restore = l
		case strings.HasPrefix(l, "/system "):
			if system != "" {
				t.Errorf("the singleton left a second line: %q after %q", l, system)
			}
			system = l
		case strings.HasPrefix(l, "/relay add ") ||
			strings.HasPrefix(l, "/relay set") ||
			strings.HasPrefix(l, "/relay set64"):
			t.Errorf("a line the grammar refuses, or a split command: %q", l)
		}
	}
	if add64 == "" || scope64 == "" || restore == "" || system == "" {
		t.Fatalf("lines missing (add64=%q scope=%q restore=%q system=%q):\n%s",
			add64, scope64, restore, system, out)
	}
	pairs := decode(add64)
	if pairs["guest_access"] != "password" || pairs["guest_password"] != `h"unter` {
		t.Errorf("the creation split its halves: %v", pairs)
	}
	pairs = decode(scope64)
	if pairs["profile"] != "eu-433" || pairs["node_name"] != "line\nbreak" ||
		pairs["tx_power_dbm"] != "3" {
		t.Errorf("the scope switch travelled apart from its values: %v", pairs)
	}
	if !strings.HasPrefix(system, "/system set64 ") {
		t.Errorf("singleton line = %q, want one set64", system)
	}
	if got := decode(system)["name"]; got != "two\nlines" {
		t.Errorf("singleton value = %q", got)
	}
	if restore != "/relay meshcore-868 set profile=eu-868-narrow" {
		t.Errorf("restore line = %q", restore)
	}
}
