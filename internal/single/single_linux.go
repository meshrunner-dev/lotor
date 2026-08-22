//go:build linux

// Package single guards against two daemons running the same
// configuration. The lock is a Linux abstract unix socket: bound in a
// kernel namespace, invisible on the filesystem, exclusive by bind
// semantics, and released the instant the process dies — crash
// included. Everything a PID file wishes it were.
package single

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
)

// Acquire takes the instance lock for (app, scope) — scope is
// typically the absolute configuration path. The returned release
// drops the lock early; process death drops it regardless.
func Acquire(ctx context.Context, app, scope string) (release func(), err error) {
	// The abstract namespace caps names at sockaddr size; a hash keys
	// arbitrary paths deterministically.
	sum := sha256.Sum256([]byte(scope))
	name := "@" + app + ":" + hex.EncodeToString(sum[:12])

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "unix", name)
	if err != nil {
		return nil, fmt.Errorf("another %s run already holds this configuration (%s)", app, scope)
	}
	return func() { _ = ln.Close() }, nil
}
