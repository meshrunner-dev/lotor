package application

import (
	"strings"
	"testing"
)

func TestTheRegistryHoldsATypeToItsProtocol(t *testing.T) {
	// A private registry for the test: the real one is populated by
	// the types' init functions and must not be disturbed.
	registryMu.Lock()
	saved := builders
	builders = map[string]Builder{}
	registryMu.Unlock()
	t.Cleanup(func() {
		registryMu.Lock()
		builders = saved
		registryMu.Unlock()
	})

	Register("echo", Builder{Protocol: "meshcore"})
	if _, err := Lookup("meshcore", "echo"); err != nil {
		t.Fatalf("the registered type was refused: %v", err)
	}
	// The same type under another mesh is a configuration error, named.
	if _, err := Lookup("lorawan", "echo"); err == nil || !strings.Contains(err.Error(), "speaks meshcore") {
		t.Errorf("a wrong protocol passed: %v", err)
	}
	if _, err := LookupType("nothing"); err == nil || !strings.Contains(err.Error(), "known: [echo]") {
		t.Errorf("an unknown type did not name the known ones: %v", err)
	}
	if got := Registered(); len(got) != 1 || got[0] != "echo" {
		t.Errorf("Registered = %v", got)
	}
	// A type must say what it speaks, and may be registered once.
	for name, builder := range map[string]Builder{"mute": {}, "echo": {Protocol: "meshcore"}} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("registering %q did not panic", name)
				}
			}()
			Register(name, builder)
		}()
	}
}
