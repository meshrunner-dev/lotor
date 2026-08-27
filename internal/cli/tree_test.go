package cli

import (
	"strings"
	"testing"
)

func TestTreeNavigationChangesThePrompt(t *testing.T) {
	out := run(t, testDeps(t), "/relay", "?", "..")
	if !strings.Contains(out, "[/relay] > ") {
		t.Errorf("the prompt never showed the context:\n%s", out)
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
	if !strings.Contains(painted, cCyan+"/relay"+cReset) ||
		!strings.Contains(painted, cCyan+"meshcore-868"+cReset) {
		t.Errorf("path tokens not cyan: %q", painted)
	}
	if !strings.Contains(painted, cGreen+"print"+cReset) {
		t.Errorf("verb not green: %q", painted)
	}
	if !strings.Contains(painted, cYellow+"node_name"+cReset) {
		t.Errorf("attribute not yellow: %q", painted)
	}
	// Without colors the line passes through untouched.
	if plain := (&session{deps: s.deps}).paintLine("/relay print"); plain != "/relay print" {
		t.Errorf("plain session painted anyway: %q", plain)
	}
}
