package schema

import (
	"slices"
	"testing"
)

func TestBooleanCandidatesKeepBooleanParsing(t *testing.T) {
	a := Attr{Name: "enabled", Type: Bool}
	if got := a.Candidates(); !slices.Equal(got, []string{"true", "false"}) {
		t.Fatalf("boolean candidates = %v", got)
	}
	for text, want := range map[string]bool{"true": true, "yes": true, "false": false, "no": false} {
		got, err := Parse(a, text)
		if err != nil || got != want {
			t.Errorf("Parse(%q) = %#v (%T), %v; want boolean %v", text, got, got, err, want)
		}
	}
}

func TestSuggestionsNeverReplaceTypeValidation(t *testing.T) {
	for _, tc := range []struct {
		attr Attr
		text string
		want any
	}{
		{Attr{Name: "identity", Type: String, Suggestions: []string{"new"}}, "a-private-key", "a-private-key"},
		{Attr{Name: "channel", Type: String, Suggestions: []string{"dev"}}, "try-room-fix", "try-room-fix"},
		{Attr{Name: "count", Type: Int, Suggestions: []string{"10"}}, "12", 12},
	} {
		got, err := Parse(tc.attr, tc.text)
		if err != nil || got != tc.want {
			t.Errorf("%s: Parse(%q) = %#v, %v; want %#v", tc.attr.Name, tc.text, got, err, tc.want)
		}
	}
	if _, err := Parse(Attr{Name: "count", Type: Int, Suggestions: []string{"many"}}, "many"); err == nil {
		t.Fatal("a suggestion bypassed integer parsing")
	}
}

func TestClosedVocabularyAndSuggestionsRemainIndependent(t *testing.T) {
	a := Attr{Name: "mode", Type: String, Enum: []string{"off", "on"}, Suggestions: []string{"maybe"}}
	words := a.Candidates()
	if !slices.Equal(words, a.Enum) {
		t.Fatalf("closed candidates = %v", words)
	}
	words[0] = "mutated"
	if a.Enum[0] != "off" {
		t.Fatal("completion mutated the validation vocabulary")
	}
	if _, err := Parse(a, "maybe"); err == nil {
		t.Fatal("suggestion expanded a closed vocabulary")
	}
}

func TestEmptyValueRemainsValidWithoutHidingTheVocabulary(t *testing.T) {
	a := Attr{Name: "voltage", Type: String, Enum: []string{"", "1.8", "3.3"}}
	if got := a.Candidates(); !slices.Equal(got, []string{"1.8", "3.3"}) {
		t.Fatalf("candidates = %v; empty prefix must offer words", got)
	}
	if got, err := Parse(a, ""); err != nil || got != "" {
		t.Fatalf("explicit empty value = %#v, %v", got, err)
	}
}
