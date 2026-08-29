package config

import "testing"

func TestInstanceNamesFollowOneGrammar(t *testing.T) {
	// A name is a handle: it walks the tree as a path step, stands
	// bare on a command line, and is written back by export to be
	// replayed. Everything the console spends on its own grammar is
	// therefore refused — and refused identically wherever a name
	// arrives, which is what makes an export restorable.
	for _, c := range []struct {
		name string
		ok   bool
		why  string
	}{
		{"meshcore-868", true, "the ordinary handle"},
		{"slot1", true, "digits ride along"},
		{"café", true, "graphic runes are names too"},
		{"obs.eu_1", true, "punctuation the grammar does not spend"},
		{"", false, "empty"},
		{"obs one", false, "space separates words"},
		{"obs\tone", false, "tab separates words"},
		{"eu/868", false, "slash walks the tree"},
		{`say"what`, false, "the quote delimits"},
		{"a=b", false, "equals opens a value"},
		{"bell\x07", false, "a control rune types into the terminal"},
		{"esc\x1b[31m", false, "an escape sequence paints it"},
		{"line\nbreak", false, "a line break ends the command"},
	} {
		err := ValidInstanceName(c.name)
		if (err == nil) != c.ok {
			t.Errorf("ValidInstanceName(%q) = %v, want ok=%v (%s)", c.name, err, c.ok, c.why)
		}
	}
}

func TestCanonicalNamesAreThemselvesUsable(t *testing.T) {
	// What the migration mints must pass the very door that refused
	// the original — otherwise a healed store still holds a name
	// nobody can address.
	for _, c := range []struct{ from, want string }{
		{"obs one", "obs-one"},
		{"eu/868", "eu-868"},
		{`say"what`, "say-what"},
		{"a=b", "a-b"},
		{"esc\x1b[31m", "esc-[31m"},
		{"already-fine", "already-fine"},
		{"", "unnamed"},
		// The substitution is mechanical: what a name is made of does
		// not change the rule, and every result is itself a handle.
		{" ", "-"},
		{"/", "-"},
	} {
		got := CanonicalInstanceName(c.from)
		if got != c.want {
			t.Errorf("CanonicalInstanceName(%q) = %q, want %q", c.from, got, c.want)
		}
		if err := ValidInstanceName(got); err != nil {
			t.Errorf("the canonical form of %q is itself refused: %s", c.from, err)
		}
	}
}

func TestValidateJudgesEveryCollectionsNames(t *testing.T) {
	// The map keys were never judged, so a YAML could name an
	// observer "obs one" — importable, unreachable, unrestorable.
	// Every collection answers to the rule now.
	for _, c := range []struct {
		what string
		f    *File
	}{
		{"relay", &File{
			Radios: map[string]Radio{"r": {Driver: "d"}},
			Relays: map[string]Relay{"mc one": {Protocol: "p", Radio: "r"}},
		}},
		{"radio", &File{Radios: map[string]Radio{"slot 1": {Driver: "d"}}}},
		{"sensor", &File{Sensors: map[string]Sensor{"bme/280": {Driver: "d"}}}},
		{"observer", &File{MQTT: map[string]MQTT{"obs one": {}}}},
	} {
		if err := c.f.Validate(false); err == nil {
			t.Errorf("%s: a name the console cannot spell passed Validate", c.what)
		}
	}
	// And a file whose handles are all spellable still passes.
	ok := &File{
		Radios:  map[string]Radio{"slot1": {Driver: "d"}},
		Relays:  map[string]Relay{"mc": {Protocol: "p", Radio: "slot1"}},
		Sensors: map[string]Sensor{"bme280": {Driver: "d"}},
		MQTT:    map[string]MQTT{"obs": {}},
	}
	if err := ok.Validate(false); err != nil {
		t.Errorf("a file of ordinary names was refused: %s", err)
	}
}
