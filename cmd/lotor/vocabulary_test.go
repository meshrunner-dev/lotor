package main

import (
	"context"
	"io"
	"slices"
	"strings"
	"testing"

	"meshrunner.dev/lotor/internal/application"
	"meshrunner.dev/lotor/internal/cli"
	"meshrunner.dev/lotor/internal/schema"
)

// editVocabulary drives the public editor and dispatch with the real
// daemon schemas. Only the mutation boundary is substituted, so these
// tests cannot conceal an omitted registry or a parser/type mismatch.
func editVocabulary(t *testing.T, kind schema.Kind, choice, line string) map[string]string {
	t.Helper()
	deps := cli.Deps{
		Kinds: buildKinds(), Privilege: cli.Admin,
		Relays:       []cli.RelayInfo{{Name: "existing", Protocol: choice}},
		Stations:     []cli.StationInfo{{Name: "existing", Protocol: choice}},
		Applications: []cli.ApplicationInfo{{Name: "existing", Type: choice}},
		Radios:       []cli.RadioInfo{{Name: "existing", Driver: choice}},
		LiveMQTTs:    func() []cli.MQTTInfo { return []cli.MQTTInfo{{Name: "existing"}} },
	}
	var edits []map[string]string
	deps.Mutate = func(_ context.Context, gotKind, _ string, set map[string]string, _ []string, _ string) (string, error) {
		if gotKind != kind.Name {
			t.Errorf("mutation kind = %q, want %q", gotKind, kind.Name)
		}
		edits = append(edits, set)
		return "updated", nil
	}
	deps.Create = func(_ context.Context, gotKind, _ string, attrs map[string]string, _ string) (string, error) {
		if gotKind != kind.Name {
			t.Errorf("creation kind = %q, want %q", gotKind, kind.Name)
		}
		edits = append(edits, attrs)
		return "created", nil
	}
	deps.Remove = func(context.Context, string, string, string) (string, error) { return "", nil }
	stream := struct {
		io.Reader
		io.Writer
	}{strings.NewReader(line + "\r"), io.Discard}
	cli.ServeEdited(context.Background(), stream, deps)
	if len(edits) != 1 {
		t.Fatalf("input %q reached the mutation boundary %d times", line, len(edits))
	}
	return edits[0]
}

func vocabularyKind(t *testing.T, name string) schema.Kind {
	t.Helper()
	for _, kind := range buildKinds() {
		if kind.Name == name {
			return kind
		}
	}
	t.Fatalf("no kind %q in the daemon", name)
	return schema.Kind{}
}

func TestBuildKindsBooleansCompleteAndStayTyped(t *testing.T) {
	checked := 0
	for _, kind := range buildKinds() {
		choices := []string{""}
		if choiceAttr, ok := schema.Find(kind.Attrs, kind.ChoiceAttr); ok {
			choices = choiceAttr.Enum
		}
		for _, choice := range choices {
			for _, attr := range kind.AttrsFor(choice) {
				if attr.Type != schema.Bool {
					continue
				}
				checked++
				t.Run(kind.Name+"/"+choice+"/"+attr.Name, func(t *testing.T) {
					checkBooleanEdits(t, kind, choice, attr)
				})
			}
		}
	}
	if checked < 19 {
		t.Fatalf("exercised only %d boolean attributes; the daemon exposes at least 19", checked)
	}
}

func checkBooleanEdits(t *testing.T, kind schema.Kind, choice string, attr schema.Attr) {
	t.Helper()
	if len(attr.Enum) != 0 || !slices.Equal(attr.Candidates(), []string{"true", "false"}) {
		t.Fatalf("boolean %s has a closed enum or lacks typed candidates: %+v", attr.Name, attr)
	}
	for _, word := range []string{"true", "false"} {
		for _, verb := range []string{"add", "set"} {
			line := "/" + kind.Name + " existing " + verb + " "
			if verb == "add" {
				line = "/" + kind.Name + " add created "
				if kind.ChoiceAttr != "" {
					line += kind.ChoiceAttr + "=" + choice + " "
				}
			}
			set := editVocabulary(t, kind, choice, line+attr.Name+"="+word[:1]+"\t")
			if set[attr.Name] != word {
				t.Fatalf("%s completed %s=%q, want %q", verb, attr.Name, set[attr.Name], word)
			}
			got, err := schema.Parse(attr, set[attr.Name])
			if err != nil || got != (word == "true") {
				t.Fatalf("%s parsed as %#v (%T), %v", attr.Name, got, got, err)
			}
		}
	}
}

func TestBuildKindsOpenSuggestionsCompleteWithoutClosingValues(t *testing.T) {
	for _, tc := range []struct {
		kind, choice, attr, prefix, word, free string
	}{
		{"relay", "meshcore", "identity", "ne", "new", strings.Repeat("ab", 32)},
		{"station", "meshcore", "identity", "ne", "new", strings.Repeat("cd", 32)},
		{"application", "meshcore-room", "identity", "ne", "new", strings.Repeat("ef", 32)},
		{"relay", "meshcore", "tx_power_dbm", "au", "auto", "14"},
		{"update", "", "channel", "de", "dev", "try-room-fix"},
	} {
		t.Run(tc.kind+"/"+tc.attr, func(t *testing.T) {
			kind := vocabularyKind(t, tc.kind)
			attr, ok := schema.Find(kind.AttrsFor(tc.choice), tc.attr)
			if !ok || len(attr.Enum) != 0 || !slices.Contains(attr.Candidates(), tc.word) {
				t.Fatalf("missing open vocabulary for %s: %+v", tc.attr, attr)
			}
			parsed, err := schema.Parse(attr, tc.free)
			if err != nil || parsed != tc.free {
				t.Fatalf("suggestions closed the field: Parse(%q) = %#v, %v", tc.free, parsed, err)
			}
			verbs := []string{"set", "add"}
			if kind.Singleton {
				verbs = []string{"set"}
			}
			for _, verb := range verbs {
				line := "/" + kind.Name + " existing set "
				if kind.Singleton {
					line = "/" + kind.Name + " set "
				} else if verb == "add" {
					line = "/" + kind.Name + " add created " + kind.ChoiceAttr + "=" + tc.choice + " "
				}
				set := editVocabulary(t, kind, tc.choice, line+tc.attr+"="+tc.prefix+"\t")
				if set[tc.attr] != tc.word {
					t.Fatalf("%s completed %q, want %q", verb, set[tc.attr], tc.word)
				}
			}
		})
	}
}

func TestBuildKindsApplicationProtocolFollowsItsBuilder(t *testing.T) {
	kind := vocabularyKind(t, "application")
	for _, name := range application.Registered() {
		builder, err := application.LookupType(name)
		if err != nil {
			t.Fatal(err)
		}
		got := kind.ValueSuggestions(name, "protocol")
		if !slices.Equal(got, []string{builder.Protocol}) {
			t.Errorf("%s protocol candidates = %v, want %q", name, got, builder.Protocol)
		}
	}
	if got := kind.ValueSuggestions("unknown-application", "protocol"); got == nil || len(got) != 0 {
		t.Fatalf("unknown application inherited protocol candidates: %v", got)
	}
	for _, line := range []string{
		"/application add created type=meshcore-room protocol=mes\t",
		"/application existing set protocol=mes\t",
	} {
		set := editVocabulary(t, kind, "meshcore-room", line)
		if set["protocol"] != "meshcore" {
			t.Fatalf("protocol completion = %q", set["protocol"])
		}
	}
}

func TestBuildKindsTCXOVocabularyReachesAddAndSet(t *testing.T) {
	kind := vocabularyKind(t, "radio")
	for _, line := range []string{
		"/radio add created driver=sx126x-spi tcxo=3\t",
		"/radio existing set tcxo=3\t",
	} {
		set := editVocabulary(t, kind, "sx126x-spi", line)
		if set["tcxo"] != "3.3" {
			t.Fatalf("TCXO completion = %q", set["tcxo"])
		}
	}
}
