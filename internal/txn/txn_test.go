package txn

import (
	"context"
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

func TestTransactionCrossesAContextBoundary(t *testing.T) {
	id := New()
	ctx := WithContext(context.Background(), id)
	got, ok := FromContext(ctx)
	if !ok || got != id {
		t.Fatalf("context transaction = %s, %v; want %s, true", got, ok, id)
	}
	if _, ok := FromContext(context.Background()); ok {
		t.Fatal("an empty context invented a transaction")
	}
	if _, ok := FromContext(WithContext(context.Background(), ID{})); ok {
		t.Fatal("a zero transaction became valid correlation")
	}
}
