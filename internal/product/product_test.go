package product

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestIdentityInvariants(t *testing.T) {
	if Slug != strings.ToLower(Slug) || strings.ContainsAny(Slug, " /") {
		t.Errorf("slug %q is not a machine name", Slug)
	}
	if !strings.EqualFold(Name, Slug) {
		t.Errorf("name %q and slug %q name different things", Name, Slug)
	}
	for _, u := range []string{Homepage, UpdateBase} {
		if !strings.HasPrefix(u, "https://") || strings.HasSuffix(u, "/") {
			t.Errorf("url %q — https, no trailing slash", u)
		}
	}
	if !strings.HasSuffix(Homepage, "/"+Slug) || !strings.HasSuffix(UpdateBase, "/"+Slug) {
		t.Error("the public URLs do not end in the slug")
	}
	// The install ABI is spelled out, never derived — but today it
	// must actually match the shipped units and docs.
	if InstallBinary != "/usr/local/bin/"+Slug || InstallStateDir != "/var/lib/"+Slug ||
		InstallService != Slug {
		t.Error("the install ABI drifted from what the fleet runs")
	}
}

func TestMascotIsSoundAndCopied(t *testing.T) {
	lines := MascotLines()
	if len(lines) != 8 {
		t.Fatalf("%d mascot lines", len(lines))
	}
	width := utf8.RuneCountInString(lines[0])
	for i, l := range lines {
		if utf8.RuneCountInString(l) != width {
			t.Errorf("line %d is %d runes wide, want %d", i, utf8.RuneCountInString(l), width)
		}
	}
	// A caller defacing its copy must not deface anyone else's.
	lines[0] = "vandalised"
	if MascotLines()[0] != strings.Split(Mascot, "\n")[0] {
		t.Error("the mascot is shared mutable state")
	}
}
