//go:build lean

package web

import (
	"context"
	"errors"
)

// ListenAndServe in a lean build refuses: light builds carry no web
// UI and embed no filesystem at all — the configuration may still
// hold a web block, and the refusal names why nothing answers on it.
func ListenAndServe(_ context.Context, _ string, _ Deps) error {
	return errors.New("this build carries no web UI")
}
