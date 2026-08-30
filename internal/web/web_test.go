//go:build !lean

package web

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/cli"
	"meshrunner.dev/lotor/internal/product"
)

// get fetches one URL with the test's own deadline.
func get(t *testing.T, url string) *http.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// testDeps is a small live world: one relay, one observer, a bus.
func testDeps(b *bus.Bus) Deps {
	return Deps{
		Version:    "9.9.9-test",
		Revision:   "abcdef123456",
		Started:    time.Now().Add(-90 * time.Minute),
		SystemName: func() string { return "shack" },
		LiveRelays: func() []cli.RelayInfo {
			return []cli.RelayInfo{{
				Name: "mc", Protocol: "meshcore", Radio: "slot1",
				State: func() string { return "running" },
			}}
		},
		LiveMQTTs: func() []cli.MQTTInfo {
			return []cli.MQTTInfo{{Name: "obs", URL: "tcp://x:1", Disabled: true}}
		},
		Bus: b,
	}
}

func testServer(t *testing.T, deps Deps) *httptest.Server {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	srv := httptest.NewUnstartedServer(newMux(deps, newTally(ctx, deps.Bus)))
	srv.Config.BaseContext = func(net.Listener) context.Context { return ctx }
	srv.Start()
	t.Cleanup(srv.Close)
	return srv
}

func TestIndexAndAssetsComeFromTheBinary(t *testing.T) {
	srv := testServer(t, testDeps(nil))
	for path, contentType := range map[string]string{
		"/":                 "text/html",
		"/assets/app.js":    "text/javascript",
		"/assets/style.css": "text/css",
	} {
		resp := get(t, srv.URL+path)
		body := make([]byte, 64)
		n, _ := resp.Body.Read(body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s = %d", path, resp.StatusCode)
		}
		if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, contentType) {
			t.Errorf("%s content-type = %q, want %s", path, got, contentType)
		}
		if n == 0 {
			t.Errorf("%s served nothing", path)
		}
	}
	// The identity is NOT in the static bytes: it arrives through the
	// snapshot, so a rebrand never chases HTML.
	resp := get(t, srv.URL+"/")
	var page strings.Builder
	sc := bufio.NewScanner(resp.Body)
	for sc.Scan() {
		page.WriteString(sc.Text())
	}
	_ = resp.Body.Close()
	if strings.Contains(strings.ToLower(page.String()), product.Slug) {
		t.Errorf("the static page spells the product; the snapshot owns the identity")
	}
}

func TestStatusSpeaksTheWholeSnapshot(t *testing.T) {
	srv := testServer(t, testDeps(nil))
	resp := get(t, srv.URL+"/api/status")
	defer func() { _ = resp.Body.Close() }()
	var s status
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		t.Fatal(err)
	}
	if s.Product != product.Name || s.Version != "9.9.9-test" || s.System != "shack" {
		t.Errorf("identity = %q %q %q", s.Product, s.Version, s.System)
	}
	if s.UptimeSecs < 89*60 {
		t.Errorf("uptime = %ds", s.UptimeSecs)
	}
	if len(s.Relays) != 1 || s.Relays[0].State != "running" {
		t.Errorf("relays = %+v", s.Relays)
	}
	if len(s.Observers) != 1 || s.Observers[0].State != "disabled" {
		t.Errorf("observers = %+v", s.Observers)
	}
	if s.Journal != nil {
		t.Error("no journal runs, yet the snapshot invented one")
	}
}

// readEvent reads one SSE event's data line, or fails after the
// deadline the reader carries.
func readEvent(t *testing.T, sc *bufio.Scanner) status {
	t.Helper()
	for sc.Scan() {
		line := sc.Text()
		if data, ok := strings.CutPrefix(line, "data: "); ok {
			var s status
			if err := json.Unmarshal([]byte(data), &s); err != nil {
				t.Fatalf("event data %q: %v", data, err)
			}
			return s
		}
	}
	t.Fatal("the stream ended before an event arrived")
	return status{}
}

func TestEventStreamWakesOnStateChanges(t *testing.T) {
	b := bus.New()
	srv := testServer(t, testDeps(b))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	if got := resp.Header.Get("Content-Type"); got != "text/event-stream" {
		t.Fatalf("content-type = %q", got)
	}
	sc := bufio.NewScanner(resp.Body)

	// The first snapshot arrives on connection, unprompted.
	first := readEvent(t, sc)
	if first.System != "shack" {
		t.Errorf("first event system = %q", first.System)
	}
	// A state change wakes the stream without waiting for the tick.
	// The coalescing window may fold it into a short delay; what must
	// hold is that it arrives well before the 2s cadence.
	begun := time.Now()
	b.Publish(bus.RelayState{Relay: "mc", At: time.Now(), State: "error"})
	second := readEvent(t, sc)
	if since := time.Since(begun); since >= sseTick {
		t.Errorf("the state change waited for the tick: %s", since)
	}
	_ = second
}

func TestEventStreamEndsWithItsClient(t *testing.T) {
	// A client that leaves takes its handler and its subscription
	// with it: the next state change finds one fewer listener, not a
	// goroutine writing into a closed connection.
	b := bus.New()
	srv := testServer(t, testDeps(b))
	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, srv.URL+"/events", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	sc := bufio.NewScanner(resp.Body)
	readEvent(t, sc) // the stream is live
	cancel()
	_ = resp.Body.Close()
	// The handler's subscription closes with it; srv.Close (via
	// t.Cleanup) would hang on a handler that never returned.
}
