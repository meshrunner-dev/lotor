package update

// The manifest is a channel's whole statement: one version, its
// artifacts by platform, and when it was said. The signature rides
// beside it; the sha256 inside carries the trust down to the bytes.

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Manifest names what a channel currently offers.
type Manifest struct {
	Product   string              `json:"product"`
	Channel   string              `json:"channel"`
	Version   string              `json:"version"`
	Published time.Time           `json:"published"`
	Artifacts map[string]Artifact `json:"artifacts"`
	Notes     string              `json:"notes,omitempty"`
}

// Artifact is one platform's binary: where it is, what it hashes to,
// how big it is.
type Artifact struct {
	URL    string `json:"url"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
}

// ParseManifest reads and checks one. Every field is load-bearing —
// a manifest that omits one is refused, not repaired, because the
// bytes were signed and a repair would verify something nobody wrote.
func ParseManifest(raw []byte) (*Manifest, error) {
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	var m Manifest
	if err := dec.Decode(&m); err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	switch {
	case m.Product == "":
		return nil, errors.New("manifest names no product")
	case m.Channel == "":
		return nil, errors.New("manifest names no channel")
	case m.Version == "":
		return nil, errors.New("manifest names no version")
	case m.Published.IsZero():
		return nil, errors.New("manifest carries no publication time")
	case len(m.Artifacts) == 0:
		return nil, errors.New("manifest offers no artifacts")
	}
	for platform, a := range m.Artifacts {
		if err := a.check(); err != nil {
			return nil, fmt.Errorf("manifest artifact %s: %w", platform, err)
		}
	}
	return &m, nil
}

func (a Artifact) check() error {
	switch {
	case !strings.HasPrefix(a.URL, "https://") && !loopbackHTTP(a.URL):
		return fmt.Errorf("url %q is not https", a.URL)
	case a.Size <= 0:
		return fmt.Errorf("size %d", a.Size)
	}
	if raw, err := hex.DecodeString(a.SHA256); err != nil || len(raw) != 32 {
		return fmt.Errorf("sha256 %q is not 32 hex bytes", a.SHA256)
	}
	return nil
}

// loopbackHTTP admits plain http for the machine's own addresses:
// the sha256 carries the integrity whatever the transport, nothing
// leaves the host to be read, and a local mirror or a test should not
// need a certificate to serve bytes to itself.
func loopbackHTTP(url string) bool {
	rest, ok := strings.CutPrefix(url, "http://")
	if !ok {
		return false
	}
	for _, host := range []string{"127.0.0.1", "localhost", "[::1]"} {
		if rest == host || strings.HasPrefix(rest, host+":") || strings.HasPrefix(rest, host+"/") {
			return true
		}
	}
	return false
}

// ArtifactFor picks the running platform's binary, named "linux/arm64"
// style, and says plainly when the channel does not build for it.
func (m *Manifest) ArtifactFor(platform string) (Artifact, error) {
	a, ok := m.Artifacts[platform]
	if !ok {
		built := make([]string, 0, len(m.Artifacts))
		for p := range m.Artifacts {
			built = append(built, p)
		}
		return Artifact{}, fmt.Errorf("channel %s builds %s, not %s",
			m.Channel, strings.Join(built, ", "), platform)
	}
	return a, nil
}
