package update

// The staging ground and the install itself, split across a privilege
// boundary on purpose. The unprivileged daemon downloads, verifies
// and stages under its own state directory; the privileged installer
// — a root oneshot systemd unit watching for the marker — re-verifies
// everything against its own trust store and only then touches the
// binary. A compromised daemon can therefore stage whatever it wants
// and gain nothing: the installer installs what carries a trusted
// signature, or nothing.

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

// Stage file names, under <state>/updates/.
const (
	stagedBinary = "lotor.next"
	// stagedFetch holds a compressed artifact between its download and
	// its unpacking — the window where the bytes are proved but not
	// yet a binary. It never outlives Download.
	stagedFetch    = "lotor.next.fetch"
	stagedManifest = "manifest.json"
	stagedSig      = "manifest.json.minisig"
	// readyMarker is what the installer's path unit watches for. It is
	// written last, after every byte beside it has been fsynced: its
	// existence is the statement that the stage is whole.
	readyMarker = "ready"
	// pendingMarker is the probation flag: written by the installer
	// before the restart, cleared by the daemon once the new binary
	// has run healthily for a while. Its survival past the grace says
	// the update went wrong.
	pendingMarker = "pending"
)

// StageDir is where a state directory keeps its staged update.
func StageDir(stateDir string) string { return filepath.Join(stateDir, "updates") }

// Ready is the marker's content: what was staged and why. SHA256 is
// the staged binary's — the unpacked bytes when the artifact travelled
// compressed — because the marker describes what sits on disk, not
// what rode the wire.
type Ready struct {
	Version  string    `json:"version"`
	Channel  string    `json:"channel"`
	Platform string    `json:"platform"`
	SHA256   string    `json:"sha256"`
	Staged   time.Time `json:"staged"`
}

// progressCounter reports the bytes going past it. It is a writer in
// the same fan-out as the file and the hash, so what it counts is
// exactly what was written and hashed — never what a reader merely
// offered.
type progressCounter struct {
	done, total int64
	report      func(done, total int64)
}

func (p *progressCounter) Write(b []byte) (int, error) {
	p.done += int64(len(b))
	p.report(p.done, p.total)
	return len(b), nil
}

// Download fetches one artifact into dir under the staged name,
// verifying size and sha256 as the bytes arrive, and unpacking the
// result when the artifact travels compressed. The order is the
// security property: nothing parses the fetched bytes until their
// hash has proved them against the signed manifest, so an attacker on
// the artifact transport — plain http by design — reaches a hash
// comparison and never the decompressor. Whatever fails is removed
// whole; a half-trusted binary is not a thing to leave lying about.
func (c *Client) Download(ctx context.Context, a Artifact, dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return "", err
	}
	dest := filepath.Join(dir, stagedBinary)
	if a.Compression == "" {
		if err := c.fetch(ctx, a, dest, 0o755); err != nil {
			_ = os.Remove(dest)
			return "", err
		}
		return dest, nil
	}
	fetched := filepath.Join(dir, stagedFetch)
	err := c.fetch(ctx, a, fetched, 0o600)
	if err == nil {
		// The fetched bytes are proved; only now may they be parsed.
		err = unpack(fetched, dest, a)
	}
	_ = os.Remove(fetched)
	if err != nil {
		_ = os.Remove(dest)
		return "", err
	}
	return dest, nil
}

// fetch brings one artifact's bytes to dest, holding them to the
// manifest's word — a.Size bytes hashing to a.SHA256 — and syncing
// before it returns. The caller owns the cleanup either way.
func (c *Client) fetch(ctx context.Context, a Artifact, dest string, mode os.FileMode) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.URL, nil)
	if err != nil {
		return err
	}
	// The one header GitHub's asset API needs to hand bytes instead
	// of JSON; every static host ignores it.
	req.Header.Set("Accept", "application/octet-stream")
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	resp, err := c.http().Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%s: %s", a.URL, resp.Status)
	}
	f, err := os.OpenFile(dest, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode) //nolint:gosec // the stage dir is ours
	if err != nil {
		return err
	}
	h := sha256.New()
	sink := io.MultiWriter(f, h)
	if c.Progress != nil {
		sink = io.MultiWriter(f, h, &progressCounter{total: a.Size, report: c.Progress})
	}
	n, err := io.Copy(sink, io.LimitReader(resp.Body, a.Size+1))
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil && n != a.Size {
		err = fmt.Errorf("artifact is %d bytes, the manifest says %d", n, a.Size)
	}
	if err == nil && hex.EncodeToString(h.Sum(nil)) != a.SHA256 {
		err = errors.New("artifact bytes do not hash to what the manifest promised")
	}
	return err
}

// unpack turns a proved fetch into the staged binary, held to the
// manifest's other promise: exactly binary_size bytes hashing to
// binary_sha256. The input was verified before this runs; the output
// bound stays anyway, because even an honestly signed manifest that
// mis-states its sizes must end in a clean error, never a filled
// disk. Streamed to the file, never held in RAM.
func unpack(from, to string, a Artifact) error {
	src, err := os.Open(from) //nolint:gosec // the stage dir is ours
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()
	gz, err := gzip.NewReader(src)
	if err != nil {
		return fmt.Errorf("proved artifact does not open as %s: %w", a.Compression, err)
	}
	f, err := os.OpenFile(to, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755) //nolint:gosec // a binary must be executable
	if err != nil {
		return err
	}
	binSum, binSize := a.Binary()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(f, h), io.LimitReader(gz, binSize+1))
	if err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if cerr := gz.Close(); err == nil {
		err = cerr
	}
	if err == nil && n != binSize {
		err = fmt.Errorf("artifact unpacks to %d bytes, the manifest says %d", n, binSize)
	}
	if err == nil && hex.EncodeToString(h.Sum(nil)) != binSum {
		err = errors.New("unpacked bytes do not hash to what the manifest promised")
	}
	return err
}

// WriteStage lays the verified statement beside the binary and drops
// the ready marker last, fsynced in order: the marker's existence is
// the statement that everything beside it is whole.
func WriteStage(dir string, checked *Checked, platform string) error {
	art, err := checked.Manifest.ArtifactFor(platform)
	if err != nil {
		return err
	}
	for _, part := range []struct {
		name string
		data []byte
	}{
		{stagedManifest, checked.Raw},
		{stagedSig, checked.Sig},
	} {
		if err := writeSynced(filepath.Join(dir, part.name), part.data, 0o644); err != nil {
			return err
		}
	}
	binSum, _ := art.Binary()
	ready, err := json.Marshal(Ready{
		Version:  checked.Manifest.Version,
		Channel:  checked.Manifest.Channel,
		Platform: platform,
		SHA256:   binSum,
		Staged:   time.Now().UTC(),
	})
	if err != nil {
		return err
	}
	return writeSynced(filepath.Join(dir, readyMarker), append(ready, '\n'), 0o644)
}

// VerifyStaged proves a stage from its own files alone, against the
// caller's trust store: signature over the manifest, manifest hash
// over the binary. The installer calls this as root with its own
// keys, which is what makes the boundary real; the daemon calls it
// too, to refuse a bad stage before bothering anyone.
func VerifyStaged(dir string, trusted []PublicKey) (*Ready, error) {
	rawReady, err := os.ReadFile(filepath.Join(dir, readyMarker)) //nolint:gosec // the stage dir is ours
	if err != nil {
		return nil, err
	}
	var ready Ready
	if err := json.Unmarshal(rawReady, &ready); err != nil {
		return nil, fmt.Errorf("ready marker: %w", err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, stagedManifest)) //nolint:gosec // the stage dir is ours
	if err != nil {
		return nil, err
	}
	sig, err := os.ReadFile(filepath.Join(dir, stagedSig)) //nolint:gosec // the stage dir is ours
	if err != nil {
		return nil, err
	}
	key, err := Verify(raw, sig, trusted)
	if err != nil {
		return nil, err
	}
	m, err := ParseManifest(raw)
	if err != nil {
		return nil, err
	}
	// The same pin the check enforced, held by the installer's own
	// hand: a stage signed by a key outside the channel's train is
	// refused however it got here.
	if !key.Vouches(m.Channel) {
		return nil, fmt.Errorf("key %s does not vouch for channel %s", key.Hex(), m.Channel)
	}
	art, err := m.ArtifactFor(ready.Platform)
	if err != nil {
		return nil, err
	}
	bin, err := os.ReadFile(filepath.Join(dir, stagedBinary)) //nolint:gosec // the stage dir is ours
	if err != nil {
		return nil, err
	}
	// The staged binary answers to the manifest's binary hash: for a
	// compressed artifact that is the unpacked pair, and the transport
	// form never reaches this boundary at all.
	binSum, _ := art.Binary()
	sum := sha256.Sum256(bin)
	if hex.EncodeToString(sum[:]) != binSum {
		return nil, errors.New("staged binary does not hash to what the signed manifest promises")
	}
	if m.Version != ready.Version {
		return nil, fmt.Errorf("ready marker says %s, the signed manifest %s", ready.Version, m.Version)
	}
	return &ready, nil
}

// ClearStage removes a stage, marker first: whatever interrupts the
// rest leaves no marker for the installer to act on.
func ClearStage(dir string) error {
	if err := os.Remove(filepath.Join(dir, readyMarker)); err != nil && !os.IsNotExist(err) {
		return err
	}
	for _, name := range []string{stagedBinary, stagedFetch, stagedManifest, stagedSig} {
		if err := os.Remove(filepath.Join(dir, name)); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

// Apply installs a verified stage over the target binary: the old one
// survives as .prev by hard link, the new arrives by rename on the
// same filesystem, and there is no instant without a binary at the
// path. The caller has verified the stage; this only moves bytes.
func Apply(dir, target string) error {
	staged := filepath.Join(dir, stagedBinary)
	next := filepath.Join(filepath.Dir(target), ".lotor.next")
	if err := copySynced(staged, next); err != nil {
		return err
	}
	prev := target + ".prev"
	if err := os.Remove(prev); err != nil && !os.IsNotExist(err) {
		_ = os.Remove(next)
		return err
	}
	if err := os.Link(target, prev); err != nil && !os.IsNotExist(err) {
		_ = os.Remove(next)
		return err
	}
	if err := os.Rename(next, target); err != nil {
		_ = os.Remove(next)
		return err
	}
	return syncDir(filepath.Dir(target))
}

// Rollback puts the previous binary back — the OnFailure unit's one
// move when a new version cannot hold the service up.
func Rollback(target string) error {
	prev := target + ".prev"
	if _, err := os.Stat(prev); err != nil {
		return fmt.Errorf("nothing to roll back to: %w", err)
	}
	if err := os.Rename(prev, target); err != nil {
		return err
	}
	return syncDir(filepath.Dir(target))
}

// Pending is the probation flag's content.
type Pending struct {
	Version string    `json:"version"`
	Since   time.Time `json:"since"`
}

// WritePending arms the probation before the restart.
func WritePending(stateDir, version string) error {
	raw, err := json.Marshal(Pending{Version: version, Since: time.Now().UTC()})
	if err != nil {
		return err
	}
	dir := StageDir(stateDir)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	return writeSynced(filepath.Join(dir, pendingMarker), append(raw, '\n'), 0o644)
}

// ReadPending reports the probation in force, or nil.
func ReadPending(stateDir string) *Pending {
	raw, err := os.ReadFile(filepath.Join(StageDir(stateDir), pendingMarker))
	if err != nil {
		return nil
	}
	var p Pending
	if json.Unmarshal(raw, &p) != nil {
		return nil
	}
	return &p
}

// ClearPending commits the update: the new binary has held.
func ClearPending(stateDir string) error {
	err := os.Remove(filepath.Join(StageDir(stateDir), pendingMarker))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func writeSynced(path string, data []byte, mode os.FileMode) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode) //nolint:gosec // stage paths are ours
	if err != nil {
		return err
	}
	if _, err = f.Write(data); err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}

func copySynced(from, to string) error {
	src, err := os.Open(from) //nolint:gosec // the verified stage
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()
	dst, err := os.OpenFile(to, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o755) //nolint:gosec // a binary
	if err != nil {
		return err
	}
	if _, err = io.Copy(dst, src); err == nil {
		err = dst.Sync()
	}
	if cerr := dst.Close(); err == nil {
		err = cerr
	}
	return err
}

func syncDir(dir string) error {
	d, err := os.Open(dir) //nolint:gosec // the target's own directory
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}

// Platform names the running system the way manifests key artifacts.
func Platform() string { return runtime.GOOS + "/" + runtime.GOARCH }
