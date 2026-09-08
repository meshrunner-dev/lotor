package update

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
)

func TestPendingPublicationIsReadableAcrossTheServiceBoundary(t *testing.T) {
	state := t.TempDir()
	if err := os.MkdirAll(StageDir(state), 0o700); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	// Changing the umask in a subprocess avoids changing the permissions
	// of unrelated concurrent tests. A restrictive installer umask must
	// not hide its marker from the daemon's different user and group.
	cmd := exec.CommandContext(t.Context(), executable, "-test.run=^TestPendingPublicationWithPrivateUmask$")
	cmd.Env = append(os.Environ(), "LOTOR_PENDING_TEST_STATE="+state)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("private-umask publisher: %s (%v)", out, err)
	}
	info, err := os.Stat(filepath.Join(StageDir(state), pendingMarker))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o644 {
		t.Fatalf("pending permissions = %04o; need read access for the daemon without granting other users write access", info.Mode().Perm())
	}
	if p, err := ReadPending(state); err != nil || p == nil || p.Version != "1.2.3" {
		t.Fatalf("published probation = %+v, %v", p, err)
	}
}

func TestPendingPublicationWithPrivateUmask(t *testing.T) {
	state := os.Getenv("LOTOR_PENDING_TEST_STATE")
	if state == "" {
		t.Skip("subprocess helper")
	}
	syscall.Umask(0o077)
	if err := WritePending(state, "1.2.3"); err != nil {
		t.Fatal(err)
	}
}

func TestUnreadablePendingDoesNotLookAbsent(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can read a mode-zero file")
	}
	state := t.TempDir()
	if err := WritePending(state, "1.2.3"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(StageDir(state), pendingMarker)
	if err := os.Chmod(path, 0); err != nil {
		t.Fatal(err)
	}
	p, err := ReadPending(state)
	if p != nil || !errors.Is(err, os.ErrPermission) {
		t.Fatalf("unreadable probation = %+v, %v; want permission failure", p, err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("reading unreadable probation removed its guard: %v", err)
	}
}
