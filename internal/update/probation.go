package update

import (
	"errors"
	"time"
)

// ErrProbation protects the previous binary until the installed update has
// survived its liveness grace. Force only bypasses version comparison.
var ErrProbation = errors.New("update blocked during probation to preserve the previous binary for rollback; " +
	"force does not bypass this protection")

// ProbationStatus is a snapshot of the daemon's liveness check. Deadline is
// armed anew on each process start, independently of the marker's installation
// timestamp. A zero deadline means no check is running; Err reports a check
// that could not start or commit. The on-disk marker remains the install guard.
type ProbationStatus struct {
	Deadline time.Time
	Err      error
}
