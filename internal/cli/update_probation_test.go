package cli

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"meshrunner.dev/lotor/internal/config"
	"meshrunner.dev/lotor/internal/schema"
	"meshrunner.dev/lotor/internal/update"
)

func probationDeps(t *testing.T) Deps {
	t.Helper()
	deps := Deps{
		Privilege: Admin,
		StateDir:  t.TempDir(),
		Kinds: []schema.Kind{{
			Name: kindUpdate, Doc: "updates", Singleton: true, Attrs: config.UpdateAttrs(),
		}},
	}
	deps.UpdateTrust = func() ([]update.PublicKey, error) {
		t.Fatal("probation must be reported before consulting the update channel")
		return nil, nil
	}
	if err := update.WritePending(deps.StateDir, "1.2.3"); err != nil {
		t.Fatal(err)
	}
	return deps
}

func TestUpdateProbationCountdownUsesTheLiveDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		deps := probationDeps(t)
		// Installation age cannot shorten the current process's grace.
		time.Sleep(time.Hour)
		deadline := time.Now().Add(90 * time.Second)
		deps.UpdateProbation = func() update.ProbationStatus {
			return update.ProbationStatus{Deadline: deadline}
		}
		for _, command := range []string{"/update install", "/update install force"} {
			out := run(t, deps, command)
			for _, want := range []string{"90s remaining", "previous binary for rollback", "force does not bypass"} {
				if !strings.Contains(out, want) {
					t.Errorf("%s lacks %q:\n%s", command, want, out)
				}
			}
		}
		time.Sleep(89500 * time.Millisecond)
		if out := run(t, deps, "/update install force"); !strings.Contains(out, "1s remaining") {
			t.Fatalf("fractional second must round up:\n%s", out)
		}
		time.Sleep(500 * time.Millisecond)
		out := run(t, deps, "/update install force")
		if !strings.Contains(out, "awaiting validation") || strings.Contains(out, "remaining") {
			t.Fatalf("elapsed grace promised an installation before commit:\n%s", out)
		}
		// Only removal of the guard releases staging, even after the deadline.
		if err := update.ClearPending(deps.StateDir); err != nil {
			t.Fatal(err)
		}
		deps.UpdateTrust = func() ([]update.PublicKey, error) {
			return nil, errors.New("channel consulted after commit")
		}
		if out := run(t, deps, "/update install"); !strings.Contains(out, "channel consulted after commit") {
			t.Fatalf("committed probation still blocked installation:\n%s", out)
		}
	})
}

func TestUpdateProbationFailuresDoNotInventACountdown(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status update.ProbationStatus
		marker string
		want   string
	}{
		{name: "untracked", want: "validation is not running"},
		{name: "startup error", status: update.ProbationStatus{Err: os.ErrPermission}, want: "permission denied"},
		{name: "commit error", status: update.ProbationStatus{Deadline: time.Now().Add(-time.Second), Err: os.ErrPermission}, want: "validation failed"},
		{name: "malformed", marker: "invalid JSON", want: "cannot read probation marker"},
		{name: "unreadable", marker: "directory", want: "cannot read probation marker"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			deps := probationDeps(t)
			deps.UpdateProbation = func() update.ProbationStatus { return tc.status }
			marker := filepath.Join(update.StageDir(deps.StateDir), "pending")
			switch tc.marker {
			case "directory":
				if err := os.Remove(marker); err != nil {
					t.Fatal(err)
				}
				if err := os.Mkdir(marker, 0o700); err != nil {
					t.Fatal(err)
				}
			case "invalid JSON":
				if err := os.WriteFile(marker, []byte(tc.marker), 0o644); err != nil {
					t.Fatal(err)
				}
			}
			s := &session{deps: deps}
			err := s.updateInstall(t.Context(), input{opts: map[string]string{optForce: optOn}})
			if !errors.Is(err, update.ErrProbation) {
				t.Fatalf("lost typed guard: %v", err)
			}
			if !strings.Contains(err.Error(), tc.want) || strings.Contains(err.Error(), "remaining") {
				t.Fatalf("misleading probation: %v", err)
			}
			if tc.status.Err != nil && !errors.Is(err, tc.status.Err) {
				t.Fatalf("lost underlying error: %v", err)
			}
			if _, err := os.Lstat(marker); err != nil {
				t.Fatalf("failure removed the rollback guard: %v", err)
			}
		})
	}
}
