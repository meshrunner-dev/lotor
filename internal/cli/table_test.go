package cli

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/mattn/go-runewidth"
)

func TestColumnsAlignByDisplayWidth(t *testing.T) {
	tb := &table{}
	tb.row("FR94 OWL🗼HQ", "repeater")
	tb.row("Wanadoo", "repeater")
	var out bytes.Buffer
	if err := tb.flush(&out); err != nil {
		t.Fatal(err)
	}

	lines := strings.Split(strings.TrimRight(out.String(), "\r\n"), "\r\n")
	if len(lines) != 2 {
		t.Fatalf("%d lines", len(lines))
	}
	starts := make([]int, 0, len(lines))
	for _, line := range lines {
		before, _, found := strings.Cut(line, "repeater")
		if !found {
			t.Fatalf("no second column in %q", line)
		}
		starts = append(starts, runewidth.StringWidth(before))
	}
	if starts[0] != starts[1] {
		t.Errorf("second column starts at display cells %v — misaligned", starts)
	}
}

func TestPrintableNeutralisesControls(t *testing.T) {
	hostile := "evil\x1b[2Jname\u202eoops"
	got := printable(hostile)
	if strings.ContainsRune(got, '\x1b') || strings.ContainsRune(got, '\u202e') {
		t.Errorf("control runes survived: %q", got)
	}
	if !strings.Contains(got, "evil?") || !strings.Contains(got, "name?oops") {
		t.Errorf("unexpected neutralisation: %q", got)
	}
	if printable("FR94 OWL🗼HQ") != "FR94 OWL🗼HQ" {
		t.Error("graphic runes must pass untouched")
	}
}

func TestQuotedDelimitsAndEscapes(t *testing.T) {
	if got := quoted("RPT VHN HV4"); got != `"RPT VHN HV4"` {
		t.Errorf("quoted = %s", got)
	}
	// A name cannot close its own quotation.
	if got := quoted(`fake" end`); got != `"fake\" end"` {
		t.Errorf("escaped = %s", got)
	}
}

func TestMeshNameIsTheOneDoor(t *testing.T) {
	// A name we do not have and a name that is empty must not read
	// alike: one is a node that never said, the other a node that
	// said nothing.
	if got := meshName(""); got != unnamed {
		t.Errorf("no name rendered as %q", got)
	}
	if got := meshName("FR91 🦝 Radiocom"); got != `"FR91 🦝 Radiocom"` {
		t.Errorf("meshName = %s", got)
	}
	// Neutralisation and escaping ride along, because every view that
	// shows a name off the air comes through here.
	got := meshName("evil\x1b[2Jname\" end")
	if strings.ContainsRune(got, '\x1b') {
		t.Errorf("a control rune reached the terminal: %q", got)
	}
	if got != `"evil?[2Jname\" end"` {
		t.Errorf("meshName = %s", got)
	}
}

func TestWeightAvoidsTheAttributeNobodyImplements(t *testing.T) {
	// SGR 2 is the least reliably implemented attribute there is: a
	// terminal that will not dim reaches for a palette entry instead,
	// and plenty of themes colour that one blue — so the row meant to
	// step back arrives shouting, in a hue this console reserves for
	// something else. Grey needs no interpretation.
	if strings.Contains(recede, "[2m") {
		t.Errorf("recede uses faint: %q", recede)
	}
	// The sixteen base colours are a theme's to redefine; the cube is
	// not, which is the whole reason to name a grey from it.
	if !strings.HasPrefix(recede, "\x1b[38;5;") {
		t.Errorf("recede is not named from the 256-colour cube: %q", recede)
	}
	// And it borrows no class hue, since it says how firmly a value
	// was chosen rather than what kind of word it is.
	for _, hue := range []string{cPath, cVerb, cAttr, cPunct} {
		if recede == hue {
			t.Errorf("recede took the hue %q", hue)
		}
	}
}

func TestEveryTableKnowsWhetherItMayEmphasise(t *testing.T) {
	// A table built without its session silently loses emphasis the
	// day someone gives it a header — and nothing would fail. The
	// constructor is the one place that answer is decided.
	for _, file := range []string{"commands.go", "tree.go"} {
		b, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(b, []byte("&table{}")) {
			t.Errorf("%s builds a table outside s.table()", file)
		}
	}
}
