package update

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"
)

type downloadTransport func(*http.Request) (*http.Response, error)

func (f downloadTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// These tests run sequentially. Replacing the default transport keeps
// the real default Client timeout policy under test, with network waits
// driven by synctest rather than a minute of wall time or a real server.
func defaultDownloadTransport(t *testing.T, transport downloadTransport) {
	t.Helper()
	previous := http.DefaultTransport
	http.DefaultTransport = transport
	t.Cleanup(func() { http.DefaultTransport = previous })
}

type downloadBody struct {
	ctx    context.Context //nolint:containedctx // a response body follows its request; Read has no context argument
	data   []byte
	delay  time.Duration
	stall  bool
	reads  int
	closed bool
}

func (b *downloadBody) Read(p []byte) (int, error) {
	if len(b.data) == 0 {
		return 0, io.EOF
	}
	delay := b.delay
	if b.stall && b.reads > 0 {
		delay = 2 * time.Minute
	}
	select {
	case <-b.ctx.Done():
		return 0, b.ctx.Err()
	case <-time.After(delay):
	}
	p[0], b.data = b.data[0], b.data[1:]
	b.reads++
	return 1, nil
}

func (b *downloadBody) Close() error { b.closed = true; return nil }

func serveDownloadBody(t *testing.T, b *downloadBody) {
	t.Helper()
	defaultDownloadTransport(t, func(req *http.Request) (*http.Response, error) {
		b.ctx = req.Context()
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header),
			Body: b, ContentLength: int64(len(b.data))}, nil
	})
}

func timedArtifact(t *testing.T, compressed bool) (Artifact, []byte, []byte) {
	t.Helper()
	binary := []byte("ELF")
	wire := binary
	a := Artifact{URL: "https://download.test/lotor"}
	if compressed {
		var buf bytes.Buffer
		gz := gzip.NewWriter(&buf)
		if _, err := gz.Write(binary); err != nil {
			t.Fatal(err)
		}
		if err := gz.Close(); err != nil {
			t.Fatal(err)
		}
		wire = buf.Bytes()
		binarySum := sha256.Sum256(binary)
		a.Compression, a.BinarySize, a.BinarySHA256 = "gzip", int64(len(binary)), hex.EncodeToString(binarySum[:])
	}
	sum := sha256.Sum256(wire)
	a.Size, a.SHA256 = int64(len(wire)), hex.EncodeToString(sum[:])
	return a, wire, binary
}

func TestProgressingDownloadsCanOutliveTheMetadataTimeout(t *testing.T) {
	for _, tc := range []struct {
		name         string
		compressed   bool
		progressWait time.Duration
	}{
		{name: "binary"},
		{name: "gzip", compressed: true},
		{name: "slow-console", progressWait: 2 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				a, wire, binary := timedArtifact(t, tc.compressed)
				body := &downloadBody{data: wire, delay: 20 * time.Second}
				serveDownloadBody(t, body)
				var downloaded int64
				c := &Client{Progress: func(done, _ int64) {
					downloaded = done
					time.Sleep(tc.progressWait)
				}}
				start := time.Now()
				dir := t.TempDir()
				path, err := c.Download(t.Context(), a, dir)
				if err != nil {
					t.Fatalf("a progressing download failed after %s: %v", time.Since(start), err)
				}
				if time.Since(start) <= 30*time.Second || downloaded != a.Size || !body.closed {
					t.Fatalf("elapsed %s, progress %d, closed %t", time.Since(start), downloaded, body.closed)
				}
				got, err := os.ReadFile(path)
				if err != nil || !bytes.Equal(got, binary) {
					t.Fatalf("staged bytes = %q, %v", got, err)
				}
				if _, err := os.Stat(filepath.Join(dir, stagedFetch)); !os.IsNotExist(err) {
					t.Fatalf("temporary transport file survived: %v", err)
				}
			})
		})
	}
}

func TestStalledOrCancelledDownloadsRemovePartialFiles(t *testing.T) {
	for _, tc := range []struct {
		name       string
		compressed bool
		cancel     bool
	}{
		{name: "stalled-binary"},
		{name: "stalled-gzip", compressed: true},
		{name: "cancelled-binary", cancel: true},
		{name: "cancelled-gzip", compressed: true, cancel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				a, wire, _ := timedArtifact(t, tc.compressed)
				body := &downloadBody{data: wire, delay: time.Second, stall: true}
				serveDownloadBody(t, body)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				c := &Client{}
				wantErr, wantWait := errArtifactStalled, time.Minute+time.Second
				if tc.cancel {
					c.Progress = func(_, _ int64) { cancel() }
					wantErr, wantWait = context.Canceled, time.Second
				}
				dir, start := t.TempDir(), time.Now()
				if _, err := c.Download(ctx, a, dir); !errors.Is(err, wantErr) {
					t.Fatalf("Download = %v, want %v", err, wantErr)
				}
				if time.Since(start) != wantWait || !body.closed {
					t.Fatalf("waited %s, want %s; closed %t", time.Since(start), wantWait, body.closed)
				}
				for _, name := range []string{stagedBinary, stagedFetch, readyMarker} {
					if _, err := os.Stat(filepath.Join(dir, name)); !os.IsNotExist(err) {
						t.Errorf("failed download left %s: %v", name, err)
					}
				}
			})
		})
	}
}

func TestAnArtifactHostThatNeverAnswersIsBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		defaultDownloadTransport(t, func(req *http.Request) (*http.Response, error) {
			<-req.Context().Done()
			return nil, req.Context().Err()
		})
		a, _, _ := timedArtifact(t, false)
		start := time.Now()
		if _, err := (&Client{}).Download(t.Context(), a, t.TempDir()); !errors.Is(err, errArtifactStalled) {
			t.Fatalf("unresponsive host = %v", err)
		}
		if time.Since(start) != time.Minute {
			t.Fatalf("unresponsive host held the command for %s", time.Since(start))
		}
	})
}

func TestMetadataAndExplicitClientTimeoutsStillApply(t *testing.T) {
	for _, metadata := range []bool{false, true} {
		t.Run(map[bool]string{false: "explicit-client", true: "metadata"}[metadata], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				a, wire, _ := timedArtifact(t, false)
				serveDownloadBody(t, &downloadBody{data: wire, delay: 20 * time.Second})
				c := &Client{HTTP: &http.Client{Timeout: 10 * time.Second}}
				start, wantWait := time.Now(), 10*time.Second
				var err error
				if metadata {
					c.HTTP, wantWait = nil, 30*time.Second
					_, _, err = c.get(t.Context(), a.URL, "")
				} else {
					_, err = c.Download(t.Context(), a, t.TempDir())
					if c.HTTP.Timeout != wantWait {
						t.Fatal("Download changed the caller's HTTP timeout")
					}
				}
				if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != wantWait {
					t.Fatalf("timeout = %v after %s, want %s", err, time.Since(start), wantWait)
				}
			})
		})
	}
}
