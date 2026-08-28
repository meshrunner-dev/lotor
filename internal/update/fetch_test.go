package update

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// host serves a signed channel the way the updates tree does.
func host(t *testing.T, sec SecretKey, m Manifest, sigNames ...string) *httptest.Server {
	t.Helper()
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	sig := Sign(raw, sec, "channel:"+m.Channel)
	if len(sigNames) == 0 {
		sigNames = []string{"manifest.json.minisig"}
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/"+m.Channel+"/manifest.json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"v1"`)
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = w.Write(raw)
	})
	for _, name := range sigNames {
		mux.HandleFunc("/"+m.Channel+"/"+name, func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write(sig)
		})
	}
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func channelManifest() Manifest {
	return Manifest{
		Product: "lotor", Channel: "release", Version: "1.4.2",
		Published: time.Now().UTC().Truncate(time.Second),
		Artifacts: map[string]Artifact{
			"linux/arm64": {URL: "https://example.org/lotor", SHA256: zeroes64, Size: 1},
		},
	}
}

func TestCheckVerifiesAndCheapChecksCostNothing(t *testing.T) {
	sec, pub := pair(t)
	srv := host(t, sec, channelManifest())
	c := &Client{Base: srv.URL, Trusted: []PublicKey{pub}}

	got, err := c.Check(context.Background(), "release", "")
	if err != nil || got.Manifest.Version != "1.4.2" || got.Key.ID != pub.ID {
		t.Fatalf("Check = %+v, %v", got, err)
	}
	// The next check rides the validator and costs a 304.
	if _, err := c.Check(context.Background(), "release", got.ETag); !errors.Is(err, ErrUnchanged) {
		t.Errorf("unchanged channel: %v", err)
	}
	// An empty trust set believes nothing, before any request.
	if _, err := (&Client{Base: srv.URL}).Check(context.Background(), "release", ""); err == nil {
		t.Error("a keyless client believed a channel")
	}
	// A signature under a stranger's key is refused by name.
	_, stranger := pair(t)
	sc := &Client{Base: srv.URL, Trusted: []PublicKey{stranger}}
	if _, err := sc.Check(context.Background(), "release", ""); err == nil ||
		!strings.Contains(err.Error(), "trusts") {
		t.Errorf("stranger's channel: %v", err)
	}
}

func TestRolloverOverlapServesPerKeySignatures(t *testing.T) {
	// During an overlap the publisher lays one signature per active
	// key. A relay that only trusts the outgoing key must still
	// verify: the primary file is the new key's, the per-key file is
	// the old one's.
	oldSec, oldPub := pair(t)
	newSec, _ := pair(t)
	m := channelManifest()
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/release/manifest.json", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(raw)
	})
	mux.HandleFunc("/release/manifest.json.minisig", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(Sign(raw, newSec, "era:2"))
	})
	mux.HandleFunc("/release/manifest.json."+oldPub.Hex()+".minisig",
		func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write(Sign(raw, oldSec, "era:1"))
		})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	c := &Client{Base: srv.URL, Trusted: []PublicKey{oldPub}}
	got, err := c.Check(context.Background(), "release", "")
	if err != nil || got.Key.ID != oldPub.ID {
		t.Fatalf("the overlap did not carry the old key's relay: %v", err)
	}
}

func TestCheckRefusesTheWrongChannelAndTheAbsent(t *testing.T) {
	sec, pub := pair(t)
	m := channelManifest() // says channel release
	srv := host(t, sec, m)
	c := &Client{Base: srv.URL, Trusted: []PublicKey{pub}}
	// A manifest that names another channel than the one asked for is
	// a swap, not a channel.
	mux := http.NewServeMux()
	raw, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	mux.HandleFunc("/dev/manifest.json", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(raw) })
	mux.HandleFunc("/dev/manifest.json.minisig", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(Sign(raw, sec, "x"))
	})
	swap := httptest.NewServer(mux)
	t.Cleanup(swap.Close)
	sc := &Client{Base: swap.URL, Trusted: []PublicKey{pub}}
	if _, err := sc.Check(context.Background(), "dev", ""); err == nil ||
		!strings.Contains(err.Error(), "the manifest says") {
		t.Errorf("a channel swap passed: %v", err)
	}
	if _, err := c.Check(context.Background(), "absent", ""); err == nil ||
		!strings.Contains(err.Error(), "not published") {
		t.Errorf("an absent channel: %v", err)
	}
}
