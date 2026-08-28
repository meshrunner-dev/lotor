package update

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// stagedChannel serves a channel whose artifact is real bytes, and
// returns the checked statement a daemon would hold.
func stagedChannel(t *testing.T, binary []byte) (*Client, *Checked, PublicKey) {
	t.Helper()
	sec, pub := pair(t)
	sum := sha256.Sum256(binary)
	mux := http.NewServeMux()
	mux.HandleFunc("/artifact", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Accept") != "application/octet-stream" {
			t.Error("the asset request does not ask for bytes")
		}
		_, _ = w.Write(binary)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	m := Manifest{
		Product: "lotor", Channel: "dev", Version: "0.2.0-dev.abc",
		Published: time.Now().UTC().Truncate(time.Second),
		Artifacts: map[string]Artifact{
			Platform(): {URL: srv.URL + "/artifact", SHA256: hex.EncodeToString(sum[:]),
				Size: int64(len(binary))},
		},
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	checked := &Checked{Manifest: &m, Key: pub, Raw: raw, Sig: Sign(raw, sec, "channel:dev")}
	client := &Client{Base: srv.URL, Trusted: []PublicKey{pub}}
	return client, checked, pub
}

// packedChannel serves a channel whose artifact travels gzipped, and
// returns the checked statement a daemon would hold.
func packedChannel(t *testing.T, binary []byte) (*Client, *Checked) {
	t.Helper()
	sec, pub := pair(t)
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(binary); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	packed := buf.Bytes()
	packedSum, binSum := sha256.Sum256(packed), sha256.Sum256(binary)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(packed)
	}))
	t.Cleanup(srv.Close)
	m := Manifest{
		Product: "lotor", Channel: "dev", Version: "0.2.0-dev.abc",
		Published: time.Now().UTC().Truncate(time.Second),
		Artifacts: map[string]Artifact{
			Platform(): {URL: srv.URL + "/artifact.gz",
				SHA256: hex.EncodeToString(packedSum[:]), Size: int64(len(packed)),
				Compression:  "gzip",
				BinarySHA256: hex.EncodeToString(binSum[:]), BinarySize: int64(len(binary))},
		},
	}
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	checked := &Checked{Manifest: &m, Key: pub, Raw: raw, Sig: Sign(raw, sec, "channel:dev")}
	return &Client{Base: srv.URL, Trusted: []PublicKey{pub}}, checked
}

func TestDownloadVerifiesTheBytesItSaves(t *testing.T) {
	binary := []byte("pretend this is an ELF for the right architecture")
	client, checked, _ := stagedChannel(t, binary)
	dir := t.TempDir()
	art, _ := checked.Manifest.ArtifactFor(Platform())
	path, err := client.Download(context.Background(), art, dir)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(path)
	if string(got) != string(binary) {
		t.Fatal("the saved bytes are not the artifact")
	}
	// A hash the bytes do not match removes the file rather than
	// leaving a half-trusted binary lying about.
	art.SHA256 = strings.Repeat("f", 64)
	if _, err := client.Download(context.Background(), art, dir); err == nil {
		t.Fatal("a mismatched artifact was kept")
	}
	if _, err := os.Stat(filepath.Join(dir, stagedBinary)); !os.IsNotExist(err) {
		t.Error("the mismatched file was left behind")
	}
}

func TestACompressedArtifactUnpacksToProvedBytes(t *testing.T) {
	binary := []byte("pretend this is an ELF, once the gzip is undone")
	client, checked := packedChannel(t, binary)
	dir := t.TempDir()
	art, _ := checked.Manifest.ArtifactFor(Platform())
	path, err := client.Download(context.Background(), art, dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(path); string(got) != string(binary) {
		t.Fatal("the staged bytes are not the unpacked binary")
	}
	// The transport form does not outlive the download.
	if _, err := os.Stat(filepath.Join(dir, stagedFetch)); !os.IsNotExist(err) {
		t.Error("the fetched archive was left behind")
	}
	// The whole stage holds downstream: marker and installer re-check
	// speak the binary's hash, never the transport's.
	if err := WriteStage(dir, checked, Platform()); err != nil {
		t.Fatal(err)
	}
	binSum := sha256.Sum256(binary)
	ready, err := VerifyStaged(dir, []PublicKey{checked.Key})
	if err != nil || ready.SHA256 != hex.EncodeToString(binSum[:]) {
		t.Fatalf("VerifyStaged = %+v, %v", ready, err)
	}
}

func TestACompressedArtifactIsRefusedBeforeItIsParsed(t *testing.T) {
	binary := []byte("bytes nobody will ever unpack")
	client, checked := packedChannel(t, binary)
	dir := t.TempDir()
	art, _ := checked.Manifest.ArtifactFor(Platform())
	// Bytes the transport bent die on the fetch hash, upstream of the
	// decompressor; nothing survives on disk.
	bent := art
	bent.SHA256 = strings.Repeat("f", 64)
	if _, err := client.Download(context.Background(), bent, dir); err == nil {
		t.Fatal("bent transport bytes were accepted")
	}
	for _, name := range []string{stagedBinary, stagedFetch} {
		if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
			t.Errorf("%s was left behind", name)
		}
	}
	// A manifest whose binary promise does not match its own artifact
	// ends in a clean refusal: the wrong hash, and the wrong size —
	// the unpack bound — each on their own.
	wrongHash := art
	wrongHash.BinarySHA256 = strings.Repeat("f", 64)
	if _, err := client.Download(context.Background(), wrongHash, dir); err == nil {
		t.Error("a wrong binary hash was accepted")
	}
	short := art
	short.BinarySize = int64(len(binary) - 1)
	if _, err := client.Download(context.Background(), short, dir); err == nil ||
		!strings.Contains(err.Error(), "unpacks to") {
		t.Errorf("an understated binary_size was accepted: %v", err)
	}
}

func TestAStageVerifiesWholeOrNotAtAll(t *testing.T) {
	binary := []byte("bytes the manifest vouches for")
	client, checked, pub := stagedChannel(t, binary)
	dir := t.TempDir()
	art, _ := checked.Manifest.ArtifactFor(Platform())
	if _, err := client.Download(context.Background(), art, dir); err != nil {
		t.Fatal(err)
	}
	if err := WriteStage(dir, checked, Platform()); err != nil {
		t.Fatal(err)
	}
	ready, err := VerifyStaged(dir, []PublicKey{pub})
	if err != nil || ready.Version != "0.2.0-dev.abc" {
		t.Fatalf("VerifyStaged = %+v, %v", ready, err)
	}
	// The installer's own trust store is the boundary: a stage signed
	// by nobody it trusts is refused however honest the daemon was.
	_, stranger := pair(t)
	if _, err := VerifyStaged(dir, []PublicKey{stranger}); err == nil {
		t.Error("a stage verified under a stranger's store")
	}
	// And the channel pin holds at the installer's own hand: the same
	// key, trusted but pinned elsewhere, is refused for this train.
	pinned := pub
	pinned.Channels = []string{"release"}
	if _, err := VerifyStaged(dir, []PublicKey{pinned}); err == nil ||
		!strings.Contains(err.Error(), "does not vouch") {
		t.Errorf("the installer ignored the pin: %v", err)
	}
	// A bent binary is refused by the signed hash.
	if err := os.WriteFile(filepath.Join(dir, stagedBinary), []byte("swapped"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := VerifyStaged(dir, []PublicKey{pub}); err == nil {
		t.Error("a swapped binary verified")
	}
	// Clearing removes the marker first and everything after.
	if err := ClearStage(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, readyMarker)); !os.IsNotExist(err) {
		t.Error("the marker survived the clear")
	}
}

func TestApplyKeepsTheOldBinaryAndRollbackRestoresIt(t *testing.T) {
	dir, bindir := t.TempDir(), t.TempDir()
	target := filepath.Join(bindir, "lotor")
	if err := os.WriteFile(target, []byte("the old one"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, stagedBinary), []byte("the new one"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := Apply(dir, target); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(target); string(got) != "the new one" {
		t.Fatalf("target = %q", got)
	}
	if got, _ := os.ReadFile(target + ".prev"); string(got) != "the old one" {
		t.Fatalf("prev = %q", got)
	}
	if err := Rollback(target); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(target); string(got) != "the old one" {
		t.Fatalf("after rollback = %q", got)
	}
	// A second rollback has nothing to stand on and says so.
	if err := Rollback(target); err == nil {
		t.Error("a rollback without a prev succeeded")
	}
}

func TestProbationMarkersRoundTrip(t *testing.T) {
	state := t.TempDir()
	if p := ReadPending(state); p != nil {
		t.Fatal("a fresh state is on probation")
	}
	if err := WritePending(state, "1.2.3"); err != nil {
		t.Fatal(err)
	}
	p := ReadPending(state)
	if p == nil || p.Version != "1.2.3" {
		t.Fatalf("pending = %+v", p)
	}
	if err := ClearPending(state); err != nil || ReadPending(state) != nil {
		t.Fatal("the probation did not clear")
	}
	if err := ClearPending(state); err != nil {
		t.Fatal("clearing twice is not an error")
	}
}
