package update

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"strings"
	"time"
)

const selfcheckTimeout = 2 * time.Minute

// Selfcheck gives the candidate a bounded opportunity to prove it starts and
// understands the database. Caller cancellation still ends the probe early.
func Selfcheck(ctx context.Context, binary, db string) error {
	return runSelfcheck(ctx, binary, db, selfcheckTimeout)
}

func runSelfcheck(ctx context.Context, binary, db string, timeout time.Duration) error {
	probeCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	//nolint:gosec // executing the verified candidate is the unprivileged selfcheck
	probe := exec.CommandContext(probeCtx, binary, "update", "selfcheck", "--db", db)
	// An inherited pipe held by a descendant must not outlive the probe.
	probe.WaitDelay = time.Second
	var out probeOutput
	probe.Stdout, probe.Stderr = &out, &out
	if err := probe.Run(); err != nil {
		if cause := probeCtx.Err(); cause != nil {
			return fmt.Errorf("the new binary's selfcheck did not finish: %w", cause)
		}
		return fmt.Errorf("the new binary fails its selfcheck: %s (%w)",
			strings.TrimSpace(out.buf.String()), err)
	}
	return nil
}

// A broken candidate can be noisy as well as slow. Retain enough diagnostics
// to explain the failure while continuing to drain its output.
type probeOutput struct{ buf bytes.Buffer }

func (b *probeOutput) Write(p []byte) (int, error) {
	const limit = 64 * 1024
	n := len(p)
	_, _ = b.buf.Write(p[:min(n, max(0, limit-b.buf.Len()))])
	return n, nil
}
