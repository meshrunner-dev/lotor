// Package schema is the vocabulary of the configuration: every
// attribute an operator may set, declared once by whoever owns it —
// the protocol engine, the radio driver, the daemon — and consumed by
// every administration channel there is. The console derives its
// completion, its help and its colours from here; an API will derive
// its specification from the same place. That is the whole point: an
// attribute described once cannot be up to date in one channel and
// stale in another.
package schema

// Type says what shape a value has — for parsing, for completion,
// and one day for an API's specification.
type Type int

// The value shapes.
const (
	String Type = iota
	Int
	Float
	Bool
	Duration // Go syntax: "2h", "300ms"
	Words    // a list of names; also readable as one comma-joined line
	Ints     // a list of integers
)

// Apply says what it takes for a change to be felt.
type Apply int

const (
	// Restart bounces the owning relay: the running engine holds its
	// parameters by value, so the new ones need a new engine.
	Restart Apply = iota
	// Hot is felt without a bounce. Nothing earns this mark until the
	// engine actually reads the value live — the mark is a promise.
	Hot
)

// Attr describes one attribute.
type Attr struct {
	Name string
	Type Type
	// Enum, when set, is the closed list of valid values.
	Enum []string
	// Doc is one line, shown by the console's help and completion.
	Doc   string
	Apply Apply
	// Secret masks the value wherever it is displayed or exported —
	// a private key, a password.
	Secret bool
}

// Kind describes one class of configuration object.
type Kind struct {
	Name string
	Doc  string
	// Singleton kinds have no instances — the sentinel, the CLI.
	Singleton bool
	// Attrs are the structural attributes every instance has,
	// whatever its choice.
	Attrs []Attr
	// ChoiceAttr names the structural attribute whose value selects
	// the contributed attributes — "protocol" on a relay, "driver" on
	// a radio. Empty when the kind has no such choice.
	ChoiceAttr string
	// Contributed returns the attributes the named choice brings, nil
	// for a choice nobody registered.
	Contributed func(choice string) []Attr
	// Profiles lists the preset names the named choice offers.
	Profiles func(choice string) []string
}

// AttrsFor resolves a kind's full attribute set for one instance: the
// structural attributes plus whatever its choice contributes.
func (k Kind) AttrsFor(choice string) []Attr {
	out := append([]Attr(nil), k.Attrs...)
	if k.Contributed != nil && choice != "" {
		out = append(out, k.Contributed(choice)...)
	}
	return out
}

// Find returns an attribute by name, from a resolved set.
func Find(attrs []Attr, name string) (Attr, bool) {
	for _, a := range attrs {
		if a.Name == name {
			return a, true
		}
	}
	return Attr{}, false
}
