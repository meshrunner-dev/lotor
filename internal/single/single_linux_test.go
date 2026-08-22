//go:build linux

package single

import (
	"context"
	"testing"
)

func TestSecondAcquireIsRefusedUntilRelease(t *testing.T) {
	ctx := context.Background()
	release, err := Acquire(ctx, "single-test", "/etc/some/config.yaml")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := Acquire(ctx, "single-test", "/etc/some/config.yaml"); err == nil {
		t.Fatal("second acquire on the same scope succeeded")
	}

	// A different scope is a different lock.
	release2, err := Acquire(ctx, "single-test", "/etc/other/config.yaml")
	if err != nil {
		t.Fatalf("different scope refused: %v", err)
	}
	release2()

	release()
	release3, err := Acquire(ctx, "single-test", "/etc/some/config.yaml")
	if err != nil {
		t.Fatalf("re-acquire after release refused: %v", err)
	}
	release3()
}
