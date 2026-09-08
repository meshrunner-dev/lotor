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
				watchProbation(ctx, state, zap.New(core))
				synctest.Wait()
				time.Sleep(89 * time.Second)
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
			})
		})
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
	watchProbation(t.Context(), state, zap.New(core))
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
