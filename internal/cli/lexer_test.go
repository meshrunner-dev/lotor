package cli

import (
	"slices"
	"strings"
	"testing"
)

func TestLexerKeepsSubmittedWordsAndOriginalSpans(t *testing.T) {
	line := "  /relay\tr set node_name=\"deux mots\" identity=\"\" owner=un\\deux"
	want := []string{"/relay", "r", "set", "node_name=deux mots", "identity=", "owner=un\\deux"}
	args, columns := splitArgsAt(line)
	if !slices.Equal(args, want) || !slices.Equal(columns, []int{3, 10, 12, 16, 38, 50}) {
		t.Fatalf("tokens = %q at %v", args, columns)
	}
	var painted strings.Builder
	for _, segment := range paintSegments(line) {
		painted.WriteString(segment.text)
	}
	if painted.String() != line {
		t.Fatalf("lexer did not preserve the painted draft: %q", painted.String())
	}
}

func TestLexerSharesQuoteStateAtTheCursor(t *testing.T) {
	line := `set node_name="où maintenant" identity=new`
	for _, tc := range []struct {
		prefix string
		open   bool
	}{
		{`set node_name=`, false},
		{`set node_name="où `, true},
		{`set node_name="où maintenant"`, false},
		{line, false},
	} {
		if got := insideQuoteAt(line, len(tc.prefix)); got != tc.open {
			t.Errorf("quote state after %q = %v, want %v", tc.prefix, got, tc.open)
		}
	}
}
