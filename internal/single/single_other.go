//go:build !linux

package single

import "context"

// Acquire is a no-op off Linux: the daemon's target platform carries
// the real lock, and development hosts lose nothing but the guard.
func Acquire(_ context.Context, _, _ string) (release func(), err error) {
	return func() {}, nil
}
