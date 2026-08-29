// Package version names one build, precisely: the functional release
// the linker stamps, and the provenance Go itself records — revision,
// source time, tree state, toolchain and target. Every surface that
// speaks a version receives the same Info; nothing re-derives it.
package version

import (
	"fmt"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

// release is the functional version, the ONE datum the official build
// stamps (-ldflags -X …/version.release=1.2.3). The default names an
// untracked local build and reads as older than anything a channel
// offers.
var release = "0.0.0-dev"

// Packager fallbacks, for builds from an archive without .git — the
// official CI never sets them: the native VCS stamping wins whenever
// it exists.
var (
	fallbackRevision   = ""
	fallbackSourceTime = ""
	fallbackTree       = ""
)

// TreeState says whether the sources were exactly a commit.
type TreeState string

const (
	// Clean — the build is exactly its revision.
	Clean TreeState = "clean"
	// Dirty — local modifications rode along; the revision alone does
	// not reproduce these bytes.
	Dirty TreeState = "dirty"
	// Unknown — no VCS data at all (an archive build, `go run`).
	Unknown TreeState = "unknown"
)

// Info is one build's identity, whole.
type Info struct {
	Version    string    `json:"version"`
	Revision   string    `json:"revision,omitempty"`
	SourceTime time.Time `json:"source_time,omitzero"`
	Tree       TreeState `json:"tree"`
	VCS        string    `json:"vcs,omitempty"`
	GoVersion  string    `json:"go_version"`
	GOOS       string    `json:"goos"`
	GOARCH     string    `json:"goarch"`
}

// Current reads this binary's identity: the stamped release plus what
// the toolchain recorded natively.
func Current() Info {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		bi = nil
	}
	return fromBuildInfo(bi)
}

// fromBuildInfo assembles the identity from one BuildInfo — split out
// so a test can hand it any provenance it likes.
func fromBuildInfo(bi *debug.BuildInfo) Info {
	info := Info{
		Version:   release,
		Tree:      Unknown,
		GoVersion: runtime.Version(),
		GOOS:      runtime.GOOS,
		GOARCH:    runtime.GOARCH,
	}
	if bi != nil {
		for _, s := range bi.Settings {
			applySetting(&info, s.Key, s.Value)
		}
	}
	// The archive-build fallbacks apply only where the native data is
	// absent: a packager's claim must not override what the toolchain
	// itself witnessed.
	if info.Revision == "" && fallbackRevision != "" {
		info.Revision = fallbackRevision
	}
	if info.SourceTime.IsZero() && fallbackSourceTime != "" {
		if t, err := time.Parse(time.RFC3339, fallbackSourceTime); err == nil {
			info.SourceTime = t
		}
	}
	if info.Tree == Unknown && fallbackTree != "" {
		if fallbackTree == string(Clean) || fallbackTree == string(Dirty) {
			info.Tree = TreeState(fallbackTree)
		}
	}
	return info
}

// applySetting folds one recorded build setting into the identity.
func applySetting(info *Info, key, value string) {
	switch key {
	case "vcs":
		info.VCS = value
	case "vcs.revision":
		info.Revision = value
	case "vcs.time":
		if t, err := time.Parse(time.RFC3339, value); err == nil {
			info.SourceTime = t
		}
	case "vcs.modified":
		if value == "true" {
			info.Tree = Dirty
		} else {
			info.Tree = Clean
		}
	case "GOOS":
		info.GOOS = value
	case "GOARCH":
		info.GOARCH = value
	}
}

// ShortRevision is the displayed form: enough hex to be unambiguous,
// short enough for a status line.
func (i Info) ShortRevision() string {
	if len(i.Revision) > 12 {
		return i.Revision[:12]
	}
	return i.Revision
}

// String is the diagnostic block `lotor version` prints — every field
// a support exchange needs, aligned for eyes.
func (i Info) String() string {
	var b strings.Builder
	rev := i.Revision
	if rev == "" {
		rev = "unknown"
	}
	fmt.Fprintf(&b, "revision:    %s\n", rev)
	if !i.SourceTime.IsZero() {
		fmt.Fprintf(&b, "source time: %s\n", i.SourceTime.UTC().Format(time.RFC3339))
	}
	fmt.Fprintf(&b, "tree:        %s\n", i.Tree)
	fmt.Fprintf(&b, "toolchain:   %s\n", i.GoVersion)
	fmt.Fprintf(&b, "target:      %s/%s", i.GOOS, i.GOARCH)
	return b.String()
}
