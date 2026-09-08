package update

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"meshrunner.dev/lotor/internal/product"
	"meshrunner.dev/lotor/internal/single"
)

// Staging owns one preparation, from download through selfcheck to publication.
// Close discards only that preparation and releases its cross-process lease.
type Staging struct {
	dir     string
	work    string
	release func()
}

// BeginStage refuses concurrent preparations and a stage already awaiting the
// installer. Even force must not overwrite an update the installer may consume.
func BeginStage(ctx context.Context, stateDir string) (*Staging, error) {
	dir := StageDir(stateDir)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	dir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return nil, err
	}
	release, err := acquireUpdate(ctx, dir, false)
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(filepath.Join(dir, readyMarker)); !os.IsNotExist(err) {
		release()
		if err != nil {
			return nil, err
		}
		return nil, errors.New("an update is already staged — wait for the installer")
	}
	if _, err := os.Lstat(filepath.Join(dir, pendingMarker)); !os.IsNotExist(err) {
		release()
		if err != nil {
			return nil, err
		}
		return nil, errors.New("an installed update is still on probation")
	}
	work, err := os.MkdirTemp(dir, ".prepare-")
	if err != nil {
		release()
		return nil, err
	}
	return &Staging{dir: dir, work: work, release: release}, nil
}

// Dir is the private directory in which Download and selfcheck work.
func (s *Staging) Dir() string { return s.work }

// Close discards an unfinished preparation without touching a published stage.
func (s *Staging) Close() {
	if s.work != "" {
		_ = os.RemoveAll(s.work)
		s.work = ""
	}
	if s.release != nil {
		s.release()
		s.release = nil
	}
}

// Publish moves a complete preparation into the watcher's existing layout. The
// ready marker arrives last, after the data and their directory are synced.
// The lease remains held until Close, including after a publication error.
func (s *Staging) Publish(checked *Checked, platform string) error {
	if s.work == "" {
		return errors.New("update preparation is closed")
	}
	if err := WriteStage(s.work, checked, platform); err != nil {
		return err
	}
	for _, name := range []string{stagedBinary, stagedManifest, stagedSig} {
		if err := os.Rename(filepath.Join(s.work, name), filepath.Join(s.dir, name)); err != nil {
			return err
		}
	}
	if err := syncDir(s.dir); err != nil {
		return err
	}
	if err := os.Rename(filepath.Join(s.work, readyMarker), filepath.Join(s.dir, readyMarker)); err != nil {
		return err
	}
	return syncDir(s.dir)
}

// acquireUpdate shares the daemon's kernel-backed instance lease. The installer
// waits for the publisher to release it after writing ready; an interactive
// preparation reports contention immediately. The caller bounds any wait.
func acquireUpdate(ctx context.Context, path string, wait bool) (func(), error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	// Resolve the parent too: different symlink spellings must share a lease,
	// while the final component may be a target not installed yet.
	parent, err := filepath.EvalSymlinks(filepath.Dir(abs))
	if err != nil {
		return nil, err
	}
	scope := filepath.Join(parent, filepath.Base(abs))
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		release, err := single.Acquire(ctx, product.Slug+"-update", scope)
		if err == nil {
			return release, nil
		}
		if !wait {
			return nil, fmt.Errorf("another update is in progress: %w", err)
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
