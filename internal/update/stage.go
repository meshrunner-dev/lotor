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
	"crypto/rand"
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

// A progressing artifact may take minutes on a slow uplink. Bound
// only the initial response and each body read, not the whole transfer
// or local work such as writing progress to the console and syncing.
const artifactIdleTimeout = time.Minute

var errArtifactStalled = fmt.Errorf("artifact download made no progress for %s", artifactIdleTimeout)

type artifactReader struct {
	body  io.Reader
	timer *time.Timer
}

func (r *artifactReader) Read(p []byte) (int, error) {
	r.timer.Reset(artifactIdleTimeout)
	defer r.timer.Stop()
	return r.body.Read(p)
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
func (c *Client) fetch(ctx context.Context, a Artifact, dest string, mode os.FileMode) (fetchErr error) {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
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
	timer := time.AfterFunc(artifactIdleTimeout, func() { cancel(errArtifactStalled) })
	defer func() {
		timer.Stop()
		if errors.Is(context.Cause(ctx), errArtifactStalled) {
			fetchErr = errArtifactStalled
		}
	}()
	resp, err := c.http(0).Do(req)
	timer.Stop()
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
	body := &artifactReader{body: resp.Body, timer: timer}
	n, err := io.Copy(sink, io.LimitReader(body, a.Size+1))
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

// VerifyStaged checks a published stage without installing it. Install makes
// and verifies its own private copy; this read-only result is never permission
// to reopen the shared binary and install whatever it contains later.
func VerifyStaged(dir string, trusted []PublicKey) (*Ready, error) {
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = root.Close() }()
	ready, art, err := readStage(root, trusted)
	if err != nil {
		return nil, err
	}
	src, err := root.Open(stagedBinary)
	if err != nil {
		return nil, err
	}
	defer func() { _ = src.Close() }()
	if err := copyVerified(io.Discard, src, art); err != nil {
		return nil, err
	}
	return ready, nil
}

func readStage(root *os.Root, trusted []PublicKey) (*Ready, Artifact, error) {
	rawReady, err := root.ReadFile(readyMarker)
	if err != nil {
		return nil, Artifact{}, err
	}
	var ready Ready
	if err := json.Unmarshal(rawReady, &ready); err != nil {
		return nil, Artifact{}, fmt.Errorf("ready marker: %w", err)
	}
	raw, err := root.ReadFile(stagedManifest)
	if err != nil {
		return nil, Artifact{}, err
	}
	sig, err := root.ReadFile(stagedSig)
	if err != nil {
		return nil, Artifact{}, err
	}
	key, err := Verify(raw, sig, trusted)
	if err != nil {
		return nil, Artifact{}, err
	}
	m, err := ParseManifest(raw)
	if err != nil {
		return nil, Artifact{}, err
	}
	if !key.Vouches(m.Channel) {
		return nil, Artifact{}, fmt.Errorf("key %s does not vouch for channel %s", key.Hex(), m.Channel)
	}
	if ready.Platform != Platform() {
		return nil, Artifact{}, fmt.Errorf("stage is for %s, this host is %s", ready.Platform, Platform())
	}
	art, err := m.ArtifactFor(ready.Platform)
	if err != nil {
		return nil, Artifact{}, err
	}
	binSum, _ := art.Binary()
	if m.Version != ready.Version || m.Channel != ready.Channel || binSum != ready.SHA256 {
		return nil, Artifact{}, errors.New("ready marker does not match the signed manifest")
	}
	return &ready, art, nil
}

// copyVerified binds the bytes written to the signed binary size and hash.
// Install's destination is private to the installer throughout this operation.
func copyVerified(dst io.Writer, src io.Reader, art Artifact) error {
	sum, size := art.Binary()
	h := sha256.New()
	n, err := io.Copy(io.MultiWriter(dst, h), io.LimitReader(src, size))
	if err != nil {
		return err
	}
	var extra [1]byte
	more, err := io.ReadFull(src, extra[:])
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	if n != size || more != 0 {
		return fmt.Errorf("staged binary size does not match the signed size %d", size)
	}
	if hex.EncodeToString(h.Sum(nil)) != sum {
		return errors.New("staged binary does not hash to what the signed manifest promises")
	}
	return nil
}

// ClearStage removes a stage, marker first: whatever interrupts the
// rest leaves no marker for the installer to act on.
func ClearStage(dir string) error {
	root, err := os.OpenRoot(dir)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	return clearStage(root)
}

func clearStage(root *os.Root) error {
	for _, name := range []string{readyMarker, stagedBinary, stagedFetch, stagedManifest, stagedSig} {
		if err := root.Remove(name); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return syncRoot(root)
}

// Rollback puts the previous binary back — the OnFailure unit's one
// move when a new version cannot hold the service up.
func Rollback(target string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	release, err := acquireUpdate(ctx, target, true)
	if err != nil {
		return err
	}
	defer release()
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
	dir := StageDir(stateDir)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer func() { _ = root.Close() }()
	return writePending(root, version)
}

func writePending(root *os.Root, version string) error {
	raw, err := json.Marshal(Pending{Version: version, Since: time.Now().UTC()})
	if err != nil {
		return err
	}
	// Publish a new inode atomically; never truncate a daemon-controlled
	// destination or follow its final symlink while running as the installer.
	name := ".pending-" + rand.Text()
	f, err := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer func() { _ = root.Remove(name) }()
	if _, err = f.Write(append(raw, '\n')); err == nil {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	if err := root.Rename(name, pendingMarker); err != nil {
		return err
	}
	return syncRoot(root)
}

func syncRoot(root *os.Root) error {
	d, err := root.Open(".")
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
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
