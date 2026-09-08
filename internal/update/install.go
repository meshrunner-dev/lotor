package update

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Install consumes one published stage. Verification and replacement form one
// operation: the shared source is copied into a private directory beside the
// target, that copy is held to the signed size and hash, and only those bytes
// can replace the executable. Both stage and target are leased until probation
// is armed and the stage is cleared. A non-nil Ready means replacement has
// committed: even if final syncing or cleanup fails, the caller must restart.
func Install(ctx context.Context, stateDir, target string, trusted []PublicKey) (*Ready, error) {
	// A watcher can wake before Publish's caller releases its lease. Bound
	// that wait independently of the caller's process lifetime.
	lockCtx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	dir, err := filepath.EvalSymlinks(StageDir(stateDir))
	if err != nil {
		return nil, err
	}
	releaseStage, err := acquireUpdate(lockCtx, dir, true)
	if err != nil {
		return nil, err
	}
	defer releaseStage()
	releaseTarget, err := acquireUpdate(lockCtx, target, true)
	if err != nil {
		return nil, err
	}
	defer releaseTarget()
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	if _, err := root.Lstat(pendingMarker); !os.IsNotExist(err) {
		if err != nil {
			return nil, err
		}
		return nil, errors.New("an installed update is still on probation")
	}
	candidate, err := prepareInstall(root, target, trusted)
	if err != nil {
		return nil, fmt.Errorf("stage refused: %w", err)
	}
	defer candidate.close()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := candidate.backup(target); err != nil {
		return nil, err
	}
	if err := writePending(root, candidate.ready.Version); err != nil {
		return nil, err
	}
	committed, err := candidate.replace(target)
	if !committed {
		return nil, errors.Join(err, root.Remove(pendingMarker))
	}
	// A non-nil Ready means replacement committed, including when syncing or
	// cleanup then fails. The caller must still restart into that binary.
	return candidate.ready, errors.Join(err, clearStage(root))
}

// preparedInstall never refers back to the daemon's mutable binary path.
type preparedInstall struct {
	dir   string
	ready *Ready
}

func (p *preparedInstall) close() { _ = os.RemoveAll(p.dir) }

func prepareInstall(root *os.Root, target string, trusted []PublicKey) (*preparedInstall, error) {
	ready, art, err := readStage(root, trusted)
	if err != nil {
		return nil, err
	}
	private, err := os.MkdirTemp(filepath.Dir(target), ".lotor-install-")
	if err != nil {
		return nil, err
	}
	p := &preparedInstall{dir: private, ready: ready}
	if err := p.copyBinary(root, art); err != nil {
		p.close()
		return nil, err
	}
	return p, nil
}

func (p *preparedInstall) copyBinary(root *os.Root, art Artifact) error {
	src, err := root.Open(stagedBinary)
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()
	dst, err := os.OpenFile(filepath.Join(p.dir, stagedBinary), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	err = copyVerified(dst, src, art)
	if err == nil {
		err = dst.Chmod(0o755)
	}
	if err == nil {
		err = dst.Sync()
	}
	if cerr := dst.Close(); err == nil {
		err = cerr
	}
	return err
}

func (p *preparedInstall) backup(target string) error {
	prev := target + ".prev"
	if err := os.Remove(prev); err != nil && !os.IsNotExist(err) {
		return err
	}
	if err := os.Link(target, prev); err != nil && !os.IsNotExist(err) {
		return err
	}
	return syncDir(filepath.Dir(target))
}

func (p *preparedInstall) replace(target string) (bool, error) {
	if err := os.Rename(filepath.Join(p.dir, stagedBinary), target); err != nil {
		return false, err
	}
	return true, syncDir(filepath.Dir(target))
}
