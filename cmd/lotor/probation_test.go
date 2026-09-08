package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"meshrunner.dev/lotor/internal/update"
)

func TestProbationCommitsOnlyAfterASurvivingProcess(t *testing.T) {
	for _, survives := range []bool{false, true} {
		name := "shutdown"
		if survives {
			name = "survives"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				state := t.TempDir()
				if err := update.WritePending(state, "1.2.3"); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				core, logs := observer.New(zap.DebugLevel)
				status := watchProbation(ctx, state, zap.New(core))
				if got := status(); got.Err != nil || time.Until(got.Deadline) != 90*time.Second {
					t.Fatalf("initial probation status = %+v", got)
				}
				synctest.Wait()
				time.Sleep(89 * time.Second)
				if got := status(); got.Err != nil || time.Until(got.Deadline) != time.Second {
					t.Fatalf("probation status before grace = %+v", got)
				}
				if p, err := update.ReadPending(state); err != nil || p == nil {
					t.Fatalf("probation ended before its grace: %+v, %v", p, err)
				}
				if !survives {
					cancel()
					synctest.Wait()
				}
				time.Sleep(time.Second)
				synctest.Wait()
				p, err := update.ReadPending(state)
				if err != nil || (p == nil) != survives {
					t.Fatalf("survives=%v: probation=%+v, %v", survives, p, err)
				}
				if (logs.FilterMessage("update committed").Len() == 1) != survives {
					t.Fatal("commit log did not match the completed grace")
				}
				if survives {
					if got := status(); got.Err != nil || !got.Deadline.IsZero() {
						t.Fatalf("committed probation still reports a wait: %+v", got)
					}
				}
			})
		})
	}
}

func TestProbationCountdownStartsWithEachProcess(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		state := t.TempDir()
		if err := update.WritePending(state, "1.2.3"); err != nil {
			t.Fatal(err)
		}
		// A machine can be off for a long time between installation and
		// startup. The install timestamp must never shorten its grace.
		time.Sleep(time.Hour)
		ctx, cancel := context.WithCancel(t.Context())
		status := watchProbation(ctx, state, zap.NewNop())
		first := status().Deadline
		if time.Until(first) != 90*time.Second {
			t.Fatalf("old marker shortened probation: %s", time.Until(first))
		}
		time.Sleep(60 * time.Second)
		cancel()
		synctest.Wait()
		// A restart has not proved uninterrupted liveness. Its independent
		// deadline must match the newly armed timer, not the previous one.
		status = watchProbation(t.Context(), state, zap.NewNop())
		if got := status(); got.Err != nil || time.Until(got.Deadline) != 90*time.Second || !got.Deadline.After(first) {
			t.Fatalf("restarted probation status = %+v", got)
		}
		time.Sleep(89 * time.Second)
		synctest.Wait()
		if p, err := update.ReadPending(state); err != nil || p == nil {
			t.Fatalf("restart did not preserve the full grace: %+v, %v", p, err)
		}
		time.Sleep(time.Second)
		synctest.Wait()
		if p, err := update.ReadPending(state); err != nil || p != nil {
			t.Fatalf("restarted process did not commit after its grace: %+v, %v", p, err)
		}
		if got := status(); got.Err != nil || !got.Deadline.IsZero() {
			t.Fatalf("restarted process still reports probation after commit: %+v", got)
		}
	})
}

func TestAbsentProbationHasNoCountdown(t *testing.T) {
	if status := watchProbation(t.Context(), t.TempDir(), zap.NewNop()); status != nil {
		t.Fatalf("absent marker acquired probation status: %+v", status())
	}
}

func TestUnreadableProbationIsReportedAndKept(t *testing.T) {
	state := t.TempDir()
	// A directory cannot be decoded as a marker even when tests run as
	// root. Keep it in place instead of silently treating it as absent.
	marker := filepath.Join(update.StageDir(state), "pending")
	if err := os.MkdirAll(marker, 0o700); err != nil {
		t.Fatal(err)
	}
	core, logs := observer.New(zap.DebugLevel)
	status := watchProbation(t.Context(), state, zap.New(core))
	if got := status(); got.Err == nil || !got.Deadline.IsZero() {
		t.Fatalf("unreadable marker acquired a countdown: %+v", got)
	}
	if logs.FilterMessage("could not read update probation").Len() != 1 {
		t.Fatal("unreadable probation was silently ignored")
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("unreadable probation was cleared: %v", err)
	}
	if err := (&updateRollbackCmd{State: state, Target: filepath.Join(t.TempDir(), "lotor")}).Run(); err == nil {
		t.Fatal("rollback treated an unreadable marker as no probation")
	}
}

func TestProbationCommitFailureStopsTheCountdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		state := t.TempDir()
		if err := update.WritePending(state, "1.2.3"); err != nil {
			t.Fatal(err)
		}
		core, logs := observer.New(zap.DebugLevel)
		status := watchProbation(t.Context(), state, zap.New(core))
		deadline := status().Deadline
		// A nonempty directory fails removal even under root. Leave that
		// obstruction in place so the guard keeps protecting the backup.
		marker := filepath.Join(update.StageDir(state), "pending")
		if err := os.Remove(marker); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(marker, "keep"), 0o700); err != nil {
			t.Fatal(err)
		}
		time.Sleep(90 * time.Second)
		synctest.Wait()
		if got := status(); got.Err == nil || !got.Deadline.Equal(deadline) {
			t.Fatalf("failed commit appears complete or still counting: %+v", got)
		}
		if logs.FilterMessage("could not commit the update").Len() != 1 || logs.FilterMessage("update committed").Len() != 0 {
			t.Fatal("commit failure did not match the update log")
		}
		if _, err := os.Stat(marker); err != nil {
			t.Fatalf("failed commit lost its guard: %v", err)
		}
	})
}
