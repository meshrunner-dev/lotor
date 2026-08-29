package version

import (
	"encoding/json"
	"runtime/debug"
	"strings"
	"testing"
	"time"
)

func settings(kv ...string) *debug.BuildInfo {
	bi := &debug.BuildInfo{}
	for i := 0; i < len(kv); i += 2 {
		bi.Settings = append(bi.Settings, debug.BuildSetting{Key: kv[i], Value: kv[i+1]})
	}
	return bi
}

func TestEveryFieldIsExtracted(t *testing.T) {
	info := fromBuildInfo(settings(
		"vcs", "git",
		"vcs.revision", "68e0999f12ab34cd56ef78ab90cd12ef34ab56cd",
		"vcs.time", "2026-08-29T19:28:43Z",
		"vcs.modified", "false",
		"GOOS", "linux",
		"GOARCH", "arm64",
	))
	if info.VCS != "git" || info.Tree != Clean ||
		info.Revision != "68e0999f12ab34cd56ef78ab90cd12ef34ab56cd" ||
		!info.SourceTime.Equal(time.Date(2026, 8, 29, 19, 28, 43, 0, time.UTC)) ||
		info.GOOS != "linux" || info.GOARCH != "arm64" {
		t.Errorf("info = %+v", info)
	}
	if info.ShortRevision() != "68e0999f12ab" {
		t.Errorf("short = %q", info.ShortRevision())
	}
}

func TestTreeStates(t *testing.T) {
	if fromBuildInfo(settings("vcs.modified", "true")).Tree != Dirty {
		t.Error("a modified tree did not read dirty")
	}
	if fromBuildInfo(settings("vcs.modified", "false")).Tree != Clean {
		t.Error("an unmodified tree did not read clean")
	}
	if fromBuildInfo(nil).Tree != Unknown {
		t.Error("no build info did not read unknown")
	}
}

func TestPackagerFallbacksYieldToNativeData(t *testing.T) {
	fallbackRevision, fallbackSourceTime, fallbackTree = "cafe", "2020-01-01T00:00:00Z", "clean"
	defer func() { fallbackRevision, fallbackSourceTime, fallbackTree = "", "", "" }()

	// No VCS data: the packager's claims stand in.
	info := fromBuildInfo(nil)
	if info.Revision != "cafe" || info.Tree != Clean || info.SourceTime.IsZero() {
		t.Errorf("fallbacks unused: %+v", info)
	}
	// Native data present: the toolchain's own witness wins.
	info = fromBuildInfo(settings(
		"vcs.revision", "beef", "vcs.modified", "true", "vcs.time", "2026-01-01T00:00:00Z"))
	if info.Revision != "beef" || info.Tree != Dirty || info.SourceTime.Year() != 2026 {
		t.Errorf("a packager claim overrode the toolchain: %+v", info)
	}
}

func TestFormatsAreStable(t *testing.T) {
	info := fromBuildInfo(settings(
		"vcs.revision", "68e0999f12ab34cd", "vcs.time", "2026-08-29T19:28:43Z",
		"vcs.modified", "false", "GOOS", "linux", "GOARCH", "arm64"))
	human := info.String()
	for _, want := range []string{
		"revision:    68e0999f12ab34cd",
		"source time: 2026-08-29T19:28:43Z",
		"tree:        clean",
		"target:      linux/arm64",
	} {
		if !strings.Contains(human, want) {
			t.Errorf("human form lost %q:\n%s", want, human)
		}
	}
	raw, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{`"version"`, `"revision"`, `"source_time"`, `"tree"`, `"goos"`, `"goarch"`} {
		if !strings.Contains(string(raw), key) {
			t.Errorf("json lost %s: %s", key, raw)
		}
	}
}
