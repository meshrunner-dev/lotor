package cli

import (
	"encoding/base64"
	"slices"
	"strings"
	"testing"
	"unicode/utf8"

	"meshrunner.dev/lotor/internal/schema"
)

func completionSession() *session {
	kinds := testKinds()
	kinds[0].Attrs = append(kinds[0].Attrs,
		schema.Attr{Name: "radio", Type: schema.String},
		schema.Attr{Name: "mode", Type: schema.String, Enum: []string{"dry", "on-air", "off"}},
	)
	return &session{deps: Deps{
		Kinds:  kinds,
		Relays: []RelayInfo{{Name: "r", Protocol: "meshcore"}},
		Radios: []RadioInfo{{Name: "slot1", Driver: "sx126x-spi"}},
	}}
}

func applyCompletion(line string, edit completionEdit) string {
	return line[:edit.Start] + edit.Text + line[edit.End:]
}

func TestCompletionEditsTheWordAtTheCursor(t *testing.T) {
	s := completionSession()
	for _, tc := range []struct {
		name, draft, want string
	}{
		{"path suffix", "/rel|ay/r/print", "/relay/r/print"},
		{"instance path suffix", "/relay/r|/print", "/relay/r/print"},
		{"attribute", "/relay r set mo| radio=slot1", "/relay r set mode= radio=slot1"},
		{"attribute before value", "/relay r set mo|de=off radio=slot1", "/relay r set mode=off radio=slot1"},
		{"value", "/relay r set mode=on-a| radio=slot1", "/relay r set mode=on-air radio=slot1"},
		{"value suffix", "/relay r set mode=on-a|ir radio=slot1", "/relay r set mode=on-air radio=slot1"},
		{"quoted value", "/relay r set mode=\"on-a|", "/relay r set mode=\"on-air\" "},
		{"closed quoted value", "/relay r set mode=\"on-a\"|", "/relay r set mode=\"on-air\" "},
		{"inside quoted value", "/relay r set mode=\"on-a|ir\" radio=slot1", "/relay r set mode=\"on-air\" radio=slot1"},
		{"later choice", "/relay add next node_na|me=hello protocol=meshcore", "/relay add next node_name=hello protocol=meshcore"},
		{"name matches attribute", "/relay add radio protocol=meshcore rad|", "/relay add radio protocol=meshcore radio="},
		{"new name is positional", "/relay add rad|", "/relay add rad"},
		{"only one removal", "/relay remove r |", "/relay remove r "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cursor := strings.IndexByte(tc.draft, '|')
			line := strings.Replace(tc.draft, "|", "", 1)
			edit := s.completeAt(line, cursor)
			if got := applyCompletion(line, edit); got != tc.want {
				t.Fatalf("completion = %q, want %q (edit %+v)", got, tc.want, edit)
			}
		})
	}
}

func TestQuotedCompletionSeparatesTheNextArgument(t *testing.T) {
	s := completionSession()
	line := `/relay r set mode="on-a`
	add, _ := s.complete(line)
	got := splitArgs(line + add + "radio=slot1")
	want := []string{"/relay", "r", "set", "mode=on-air", "radio=slot1"}
	if !slices.Equal(got, want) {
		t.Fatalf("completed command parsed as %q, want %q", got, want)
	}
}

func TestCommonCompletionStopsAtRuneBoundaries(t *testing.T) {
	s := completionSession()
	s.deps.Relays = []RelayInfo{{Name: "r-é1"}, {Name: "r-ê2"}}
	add, hints := s.complete("/relay r-")
	if add != "" || !utf8.ValidString(add) || !slices.Equal(hints, []string{"r-é1", "r-ê2"}) {
		t.Fatalf("common completion = %q, hints %q", add, hints)
	}
}

func TestEncodedCompletionProducesDecodableValues(t *testing.T) {
	s := completionSession()
	line := "/relay r set64 mode=ZH"
	add, _ := s.complete(line)
	args := splitArgs(line + add)
	decoded, err := decode64Pairs(verbSet64, args[3:])
	if err != nil || !slices.Equal(decoded, []string{"mode=dry"}) {
		t.Fatalf("encoded completion = %q; decoded %q, error %v", line+add, decoded, err)
	}
	choice := base64.StdEncoding.EncodeToString([]byte("meshcore"))
	for _, draft := range []string{
		"/relay add64 next protocol=" + choice + " node_na|",
		"/relay add64 next node_na| protocol=" + choice,
	} {
		cursor := strings.IndexByte(draft, '|')
		line = strings.Replace(draft, "|", "", 1)
		got := applyCompletion(line, s.completeAt(line, cursor))
		if !strings.Contains(got, "node_name=") {
			t.Fatalf("encoded choice did not resolve its contributed attributes: %q", got)
		}
	}
}
