// Package version names this build, for the places that must agree on
// it: the console banner and the firmware version a companion reads
// over the air.
package version

// Version is the daemon's release string. The workflows stamp it at
// build time (-ldflags -X); the default names an untracked local
// build, and reads as older than anything a channel offers.
var Version = "0.1.0-dev"
