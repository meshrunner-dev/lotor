// Package web is the daemon's embedded web UI — the third caller of
// the seam the console already speaks through: the manager's live
// views for what runs, the bus for what happens. It mutates nothing;
// the transport is plain HTTP, so the surface is read-only the way
// the telnet listener is, and it binds loopback unless the operator
// says otherwise.
//
// The whole server is a build-time option: light builds compile the
// stub next door and embed no filesystem at all.
package web

import (
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/cli"
	"meshrunner.dev/lotor/internal/sentinel"
)

// Deps is everything the web UI may consult — the same live views the
// console reads, never the manager itself.
type Deps struct {
	// Log tells the server's life; nil is quiet, which tests want.
	Log *zap.Logger

	Version  string
	Revision string
	Started  time.Time

	// SystemName is what this installation calls itself; nil or empty
	// falls back to the product slug, like the console prompt.
	SystemName func() string

	// The live views, shared with the console: what runs, right now.
	LiveRelays func() []cli.RelayInfo
	LiveMQTTs  func() []cli.MQTTInfo

	// Bus feeds the live counters and wakes the event stream; the web
	// UI subscribes like every other citizen. Nil degrades to
	// tick-only refresh.
	Bus *bus.Bus

	// Health reports the observation journal's condition; nil when no
	// journal runs.
	Health func() sentinel.Health
}
