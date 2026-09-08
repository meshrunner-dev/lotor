package update

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func signedStage(t *testing.T, dir string, binary []byte) []PublicKey {
	t.Helper()
	sec, pub := pair(t)
	sum := sha256.Sum256(binary)
	m := Manifest{Product: "lotor", Channel: "dev", Version: "0.2.0-dev.test",
		Published: time.Now().UTC(), Artifacts: map[string]Artifact{
			Platform(): {URL: "https://example.com/lotor", Size: int64(len(binary)), SHA256: hex.EncodeToString(sum[:])},
		}}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, stagedBinary), binary, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := WriteStage(dir, &Checked{Manifest: &m, Raw: raw, Sig: Sign(raw, sec, "channel:dev"), Key: pub}, Platform()); err != nil {
		t.Fatal(err)
	}
	return []PublicKey{pub}
}

func TestPreparedInstallKeepsItsOwnVerifiedCopy(t *testing.T) {
	dir, bindir := t.TempDir(), t.TempDir()
	binary := []byte("the approved fixture")
	keys := signedStage(t, dir, binary)
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = root.Close() }()
	target := filepath.Join(bindir, "lotor")
	p, err := prepareInstall(root, target, keys)
	if err != nil {
		t.Fatal(err)
	}
	defer p.close()
	info, err := os.Stat(p.dir)
	if err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("candidate directory is not private: %v, %v", info, err)
	}
	// Changing or removing the shared source after preparation cannot change
	// which already-verified fixture is installed into the temporary target.
	if err := os.WriteFile(filepath.Join(dir, stagedBinary), []byte("a later fixture"), 0o755); err != nil {
		t.Fatal(err)
	}
	if committed, err := p.replace(target); err != nil || !committed {
		t.Fatalf("replace = %t, %v", committed, err)
	}
	got, err := os.ReadFile(target)
	if err != nil || string(got) != string(binary) {
		t.Fatalf("private copy changed: %q, %v", got, err)
	}
}

func TestInstallRefusesChangedStageWithoutReplacingTheTarget(t *testing.T) {
	state, bindir := t.TempDir(), t.TempDir()
	dir := StageDir(state)
	keys := signedStage(t, dir, []byte("approved fixture"))
	target := filepath.Join(bindir, "lotor")
	if err := os.WriteFile(target, []byte("current fixture"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, stagedBinary), []byte("different fixture"), 0o755); err != nil {
		t.Fatal(err)
	}
	if ready, err := Install(t.Context(), state, target, keys); err == nil || ready != nil {
		t.Fatalf("changed candidate installed: %+v, %v", ready, err)
	}
	if got, _ := os.ReadFile(target); string(got) != "current fixture" {
		t.Fatal("refused installation changed the target")
	}
	if p, err := ReadPending(state); err != nil || p != nil {
		t.Fatalf("refused installation probation = %+v, %v", p, err)
	}
	entries, err := os.ReadDir(bindir)
	if err != nil || len(entries) != 1 {
		t.Fatalf("private candidate survived refusal: %v, %v", entries, err)
	}
}

func TestStageMarkerMustMatchTheSignedManifestAndHost(t *testing.T) {
	dir := t.TempDir()
	keys := signedStage(t, dir, []byte("approved fixture"))
	original, err := VerifyStaged(dir, keys)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"channel", "version", "hash", "platform"} {
		t.Run(field, func(t *testing.T) {
			marker := *original
			switch field {
			case "channel":
				marker.Channel = "another-channel"
			case "version":
				marker.Version = "another-version"
			case "hash":
				marker.SHA256 = "another-hash"
			case "platform":
				marker.Platform = "another/platform"
			}
			raw, err := json.Marshal(marker)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, readyMarker), raw, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := VerifyStaged(dir, keys); err == nil {
				t.Fatalf("unverified marker %s was accepted", field)
			}
		})
	}
}

func TestInstallerWaitCanBeCancelledWithoutConsumingTheStage(t *testing.T) {
	state := t.TempDir()
	stage, err := BeginStage(t.Context(), state)
	if err != nil {
		t.Fatal(err)
	}
	defer stage.Close()
	keys := signedStage(t, StageDir(state), []byte("fixture"))
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if ready, err := Install(ctx, state, filepath.Join(t.TempDir(), "lotor"), keys); ready != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("installer did not wait for publisher: %+v, %v", ready, err)
	}
	if _, err := VerifyStaged(StageDir(state), keys); err != nil {
		t.Fatalf("cancelled installer damaged stage: %v", err)
	}
}

func TestProbationPreservesThePreviousBinary(t *testing.T) {
	state, bindir := t.TempDir(), t.TempDir()
	keys := signedStage(t, StageDir(state), []byte("new fixture"))
	target := filepath.Join(bindir, "lotor")
	if err := os.WriteFile(target, []byte("old fixture"), 0o755); err != nil {
		t.Fatal(err)
	}
	if ready, err := Install(t.Context(), state, target, keys); err != nil || ready == nil {
		t.Fatalf("install = %+v, %v", ready, err)
	}
	if _, err := BeginStage(t.Context(), state); !errors.Is(err, ErrProbation) {
		t.Fatalf("preparation during probation = %v", err)
	}
	keys = signedStage(t, StageDir(state), []byte("next fixture"))
	if ready, err := Install(t.Context(), state, target, keys); !errors.Is(err, ErrProbation) || ready != nil {
		t.Fatalf("second install during probation = %+v, %v", ready, err)
	}
	if got, _ := os.ReadFile(target + ".prev"); string(got) != "old fixture" {
		t.Fatal("second install lost the rollback binary")
	}
}

func TestInstallReportsCommittedReplacementWhenCleanupFails(t *testing.T) {
	state, bindir := t.TempDir(), t.TempDir()
	dir := StageDir(state)
	keys := signedStage(t, dir, []byte("new fixture"))
	target := filepath.Join(bindir, "lotor")
	if err := os.WriteFile(target, []byte("old fixture"), 0o755); err != nil {
		t.Fatal(err)
	}
	// The unused fetch name cannot be removed as a file. Verification and
	// replacement can finish, but cleanup must report this obstruction.
	if err := os.MkdirAll(filepath.Join(dir, stagedFetch, "leftover"), 0o700); err != nil {
		t.Fatal(err)
	}
	ready, err := Install(t.Context(), state, target, keys)
	if ready == nil || err == nil {
		t.Fatalf("committed installation must retain its result beside cleanup error: %+v, %v", ready, err)
	}
	if got, _ := os.ReadFile(target); string(got) != "new fixture" {
		t.Fatal("committed result does not name the installed binary")
	}
	if p, err := ReadPending(state); err != nil || p == nil || p.Version != ready.Version {
		t.Fatalf("installed binary probation after cleanup failure = %+v, %v", p, err)
	}
	if got, _ := os.ReadFile(target + ".prev"); string(got) != "old fixture" {
		t.Fatal("cleanup failure lost the rollback binary")
	}
}
