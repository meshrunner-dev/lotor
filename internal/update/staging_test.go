package update

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPreparationsAreExclusiveAndCannotOverwriteReady(t *testing.T) {
	state := t.TempDir()
	a, err := BeginStage(t.Context(), state)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if b, err := BeginStage(t.Context(), state); err == nil {
		b.Close()
		t.Fatal("two preparations acquired the same stage")
	}
	// An abandoned preparation is removed and a successor gets another path.
	old := a.Dir()
	a.Close()
	b, err := BeginStage(t.Context(), state)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if b.Dir() == old {
		t.Fatal("preparations reuse a mutable download path")
	}
	if _, err := os.Stat(old); !os.IsNotExist(err) {
		t.Fatal("abandoned preparation survived")
	}
	client, checked, pub := stagedChannel(t, []byte("published fixture"))
	art, _ := checked.Manifest.ArtifactFor(Platform())
	if _, err := client.Download(t.Context(), art, b.Dir()); err != nil {
		t.Fatal(err)
	}
	if err := b.Publish(checked, Platform()); err != nil {
		t.Fatal(err)
	}
	b.Close()
	if c, err := BeginStage(t.Context(), state); err == nil {
		c.Close()
		t.Fatal("a ready update was overwritten")
	}
	if _, err := VerifyStaged(StageDir(state), []PublicKey{pub}); err != nil {
		t.Fatalf("refused preparation damaged published stage: %v", err)
	}
}

func TestStageLeaseRecognizesDirectoryAliases(t *testing.T) {
	state, alias := t.TempDir(), t.TempDir()
	a, err := BeginStage(t.Context(), state)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err := os.Symlink(StageDir(state), StageDir(alias)); err != nil {
		t.Fatal(err)
	}
	if b, err := BeginStage(t.Context(), alias); err == nil {
		b.Close()
		t.Fatal("a directory alias bypassed the stage lease")
	}
}

func TestPendingPublicationReplacesALinkWithoutChangingItsTarget(t *testing.T) {
	state, other := t.TempDir(), filepath.Join(t.TempDir(), "unchanged")
	if err := os.MkdirAll(StageDir(state), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(other, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(other, filepath.Join(StageDir(state), pendingMarker)); err != nil {
		t.Fatal(err)
	}
	if err := WritePending(state, "1.2.3"); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(other); string(got) != "fixture" {
		t.Fatal("marker publication changed another file")
	}
	if p := ReadPending(state); p == nil || p.Version != "1.2.3" {
		t.Fatalf("pending = %+v", p)
	}
}
