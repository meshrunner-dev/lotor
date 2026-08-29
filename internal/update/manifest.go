package update

// The manifest is a channel's whole statement: one version, its
// artifacts by platform, and when it was said. The signature rides
// beside it; the sha256 inside carries the trust down to the bytes —
// which is why the artifact transport owes nothing, not even TLS: the
// hash decides, whatever pipe the bytes rode. A compressed artifact
// carries two hashes because there are two boundaries: sha256 proves
// the fetched bytes before anything parses them, binary_sha256 proves
// what unpacking produced and is what the installer re-checks.

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"meshrunner.dev/lotor/internal/product"
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
// how big it is. URL, SHA256 and Size always describe the same bytes
// — the ones the fetch brings back. When those bytes travel packed,
// Compression names how and the Binary pair describes what unpacking
// must produce; when Compression is empty the fetched bytes are the
// binary and the Binary fields are absent.
type Artifact struct {
	URL    string `json:"url"`
	SHA256 string `json:"sha256"`
	Size   int64  `json:"size"`
	// Compression is empty or "gzip". Anything else is a manifest this
	// binary cannot act on, refused at parse so the refusal names the
	// real problem instead of surfacing as a failed unpack.
	Compression  string `json:"compression,omitempty"`
	BinarySHA256 string `json:"binary_sha256,omitempty"`
	BinarySize   int64  `json:"binary_size,omitempty"`
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
	case m.Product != product.Slug:
		// A perfectly signed manifest for ANOTHER product must not
		// update this one: the signature proves who published it, not
		// that these bytes are ours to run. One shared signing key —
		// or one misrouted channel URL — must fail here, by name.
		return nil, fmt.Errorf("manifest is for product %q, this is %q", m.Product, product.Slug)
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
	// Plain http is admitted on purpose: the manifest arrived signed
	// over TLS and its sha256 pins the artifact's bytes, so the
	// artifact transport is just a pipe — a MITM there can produce a
	// failed hash and nothing else. The manifest host is where TLS
	// still matters, and that is the client's Base, not this URL.
	case !strings.HasPrefix(a.URL, "https://") && !strings.HasPrefix(a.URL, "http://"):
		return fmt.Errorf("url %q is not http(s)", a.URL)
	case a.Size <= 0:
		return fmt.Errorf("size %d", a.Size)
	}
	if err := checkHash("sha256", a.SHA256); err != nil {
		return err
	}
	switch a.Compression {
	case "":
		if a.BinarySHA256 != "" || a.BinarySize != 0 {
			return errors.New("binary fields without compression describe nothing")
		}
		return nil
	case "gzip":
		if a.BinarySize <= 0 {
			return fmt.Errorf("binary_size %d", a.BinarySize)
		}
		return checkHash("binary_sha256", a.BinarySHA256)
	}
	return fmt.Errorf("compression %q is not one this binary can undo", a.Compression)
}

func checkHash(name, hash string) error {
	if raw, err := hex.DecodeString(hash); err != nil || len(raw) != 32 {
		return fmt.Errorf("%s %q is not 32 hex bytes", name, hash)
	}
	return nil
}

// Binary names the installed binary's hash and size whatever the
// transport packing: the unpacked pair when the artifact travels
// compressed, the artifact's own otherwise. Everything downstream of
// the fetch — the stage, the installer's re-check — verifies against
// this, never against the transport form.
func (a Artifact) Binary() (sha256 string, size int64) {
	if a.Compression != "" {
		return a.BinarySHA256, a.BinarySize
	}
	return a.SHA256, a.Size
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
