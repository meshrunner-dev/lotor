package update

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMalformedPendingDoesNotLookAbsent(t *testing.T) {
	state := t.TempDir()
	if err := os.MkdirAll(StageDir(state), 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(StageDir(state), pendingMarker)
	if err := os.WriteFile(path, []byte("interrupted JSON"), 0o644); err != nil {
		t.Fatal(err)
	}
	if p, err := ReadPending(state); p != nil || err == nil {
		t.Fatalf("malformed probation = %+v, %v; want a decode failure", p, err)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "interrupted JSON" {
		t.Fatalf("reading malformed probation changed its guard: %q, %v", got, err)
	}
}
