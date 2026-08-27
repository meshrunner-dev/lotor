package cli

import (
	"bytes"
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
