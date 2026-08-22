package txn

import (
	"strings"
	"testing"
)

func TestShortIsPrefixOfFull(t *testing.T) {
	id := New()
	if !strings.HasPrefix(id.String(), id.Short()) {
		t.Fatalf("short %q is not a prefix of %q", id.Short(), id.String())
	}
	if len(id.Short()) != ShortLen {
		t.Fatalf("short length = %d", len(id.Short()))
	}
	if len(id.String()) != 32 {
		t.Fatalf("full length = %d", len(id.String()))
	}
}

func TestIDsAreDistinct(t *testing.T) {
	seen := make(map[ID]bool)
	for range 1000 {
		id := New()
		if seen[id] {
			t.Fatal("duplicate transaction id")
		}
		seen[id] = true
	}
}
