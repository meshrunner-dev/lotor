package mqtt

// The connection-era features: device tokens, the layered parameter
// set, and the neighbourhood snapshot. Golden where a wire contract
// is being promised, behavioural where the observer's loop is.

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/config"
)

func TestAuthTokenMatchesTheContract(t *testing.T) {
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = byte(i)
	}
	key := ed25519.NewKeyFromSeed(seed)
	pubHex := "2e880db04bf3b68828188746fff92f2d104d573f85c41973beaaafa445288c76"
	now := time.Unix(1756350000, 0)

	token, err := AuthToken(pubHex, "mqtt.example.net", "", 0, now,
		func(msg []byte) ([]byte, error) { return ed25519.Sign(key, msg), nil })
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("token has %d segments", len(parts))
	}
	head, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		t.Fatalf("header is not base64url: %v", err)
	}
	if string(head) != `{"alg":"Ed25519","typ":"JWT"}` {
		t.Errorf("header: %s", head)
	}
	body, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		t.Fatalf("payload is not base64url: %v", err)
	}
	var claims struct {
		PublicKey string `json:"publicKey"`
		Aud       string `json:"aud"`
		Iat       int64  `json:"iat"`
		Exp       int64  `json:"exp"`
	}
	if err := json.Unmarshal(body, &claims); err != nil {
		t.Fatal(err)
	}
	if claims.PublicKey != strings.ToUpper(pubHex) {
		t.Errorf("publicKey: %s", claims.PublicKey)
	}
	if claims.Aud != "mqtt.example.net" || claims.Iat != 1756350000 ||
		claims.Exp != 1756350000+86400 {
		t.Errorf("claims: %+v", claims)
	}
	// The third segment is the ecosystem's twist: uppercase hex, not
	// base64url, over the dotted first two segments.
	if parts[2] != strings.ToUpper(parts[2]) || len(parts[2]) != 128 {
		t.Errorf("signature segment: %q", parts[2])
	}
	sig, err := hex.DecodeString(strings.ToLower(parts[2]))
	if err != nil {
		t.Fatal(err)
	}
	if !ed25519.Verify(ed25519.PublicKey(key[ed25519.SeedSize:]),
		[]byte(parts[0]+"."+parts[1]), sig) {
		t.Error("signature does not verify over header.payload")
	}
	if got := JWTUsername(pubHex); got != "v1_"+strings.ToUpper(pubHex) {
		t.Errorf("username: %s", got)
	}

	// With an owner, the claim rides after the window, as written.
	owned, err := AuthToken(pubHex, "mqtt.example.net", strings.Repeat("ab", 32), 0, now,
		func(msg []byte) ([]byte, error) { return ed25519.Sign(key, msg), nil })
	if err != nil {
		t.Fatal(err)
	}
	mid, _ := base64.RawURLEncoding.DecodeString(strings.Split(owned, ".")[1])
	if !strings.Contains(string(mid), `"owner":"`+strings.Repeat("ab", 32)+`"`) {
		t.Errorf("owner claim missing: %s", mid)
	}
}

func TestOwnerFollowsTheEcosystemRule(t *testing.T) {
	if err := ValidOwner(""); err != nil {
		t.Error("empty owner refused")
	}
	if err := ValidOwner(strings.Repeat("Ab3", 21) + "c"); err != nil {
		t.Error("64 hex refused")
	}
	for _, bad := range []string{"abc", strings.Repeat("g", 64), strings.Repeat("a", 63)} {
		if err := ValidOwner(bad); err == nil {
			t.Errorf("ValidOwner(%q) accepted", bad)
		}
	}
}

func TestSchemaCoversParamsExactly(t *testing.T) {
	tags := map[string]bool{}
	pt := reflect.TypeFor[Params]()
	for field := range pt.Fields() {
		tag, _, _ := strings.Cut(field.Tag.Get("yaml"), ",")
		if tag == "" {
			t.Fatalf("field %s has no yaml tag", field.Name)
		}
		tags[tag] = true
	}
	for _, a := range Schema() {
		if !tags[a.Name] {
			t.Errorf("attribute %q names no Params field", a.Name)
		}
		delete(tags, a.Name)
	}
	for tag := range tags {
		t.Errorf("Params field %q has no attribute", tag)
	}
}

func TestDurationReadsBothSpellings(t *testing.T) {
	var d Duration
	if err := yaml.Unmarshal([]byte(`"55s"`), &d); err != nil || d.Std() != 55*time.Second {
		t.Errorf("text spelling: %v %v", d.Std(), err)
	}
	if err := yaml.Unmarshal([]byte(`60000000000`), &d); err != nil || d.Std() != time.Minute {
		t.Errorf("nanosecond spelling: %v %v", d.Std(), err)
	}
	if err := yaml.Unmarshal([]byte(`"snail"`), &d); err == nil {
		t.Error("nonsense accepted")
	}
}

func TestEveryPresetResolvesIntoParams(t *testing.T) {
	for name := range Presets() {
		l := config.Layered{Profile: name}
		effective, _, err := l.Resolve(Presets())
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		p, err := config.Decode[Params](effective)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if p.URL == "" {
			t.Errorf("%s: preset without a url", name)
		}
		if p.Audience != "" && !strings.HasPrefix(p.URL, "wss://") {
			t.Errorf("%s: device auth over %s", name, p.URL)
		}
	}
}

func TestNeighborsJSONPinsTheSchema(t *testing.T) {
	at := time.Date(2026, 8, 28, 3, 20, 14, 417494000, time.UTC)
	age := 42
	payload, err := NeighborsJSON(at, "Raccoon City", "2e88", "eu-868,eu-868-narrow", "eu-868",
		[]NeighborEntry{
			{PubKey: "aaaa", SNR: 7.5, HeardSecsAgo: age, Regions: "eu-868", Status: "responded"},
			{PubKey: "bbbb", SNR: -3.25, HeardUnknown: true, Status: "timeout"},
		}, 2)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"timestamp":"2026-08-28T03:20:14.417494+00:00","origin":"Raccoon City",` +
		`"origin_id":"2e88","total_neighbors":2,"queried_neighbors":2,"truncated":false,` +
		`"self":{"regions":"eu-868,eu-868-narrow","default_region":"eu-868","scopes":"eu-868,eu-868-narrow","default_scope":"eu-868"},` +
		`"neighbors":[{"pubkey":"aaaa","snr":7.5,"heard_secs_ago":42,"regions":"eu-868","scopes":"eu-868","status":"responded"},` +
		`{"pubkey":"bbbb","snr":-3.25,"heard_secs_ago":null,"regions":"","scopes":"","status":"timeout"}]}`
	if string(payload) != want {
		t.Errorf("neighbors message drifted:\n got %s\nwant %s", payload, want)
	}
}

// retainRecorder is the recorder with the retain flag kept.
type retainRecorder struct {
	mu   sync.Mutex
	msgs []struct {
		topic   string
		retain  bool
		payload []byte
	}
}

func (r *retainRecorder) Publish(topic string, _ byte, retain bool, payload []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, struct {
		topic   string
		retain  bool
		payload []byte
	}{topic, retain, append([]byte(nil), payload...)})
	return nil
}
func (r *retainRecorder) Connected() bool { return true }
func (r *retainRecorder) Close()          {}
func (r *retainRecorder) count() int      { r.mu.Lock(); defer r.mu.Unlock(); return len(r.msgs) }

func TestObserverSpeaksOnConnectAndWalksNeighbours(t *testing.T) {
	sink := &retainRecorder{}
	connects := make(chan struct{}, 1)
	cfg := Config{
		Instance: "t", Relay: "r", IATA: "PAR", OriginID: "feed", Origin: "n",
		Topic: DefaultTopic, Status: true, StatusInterval: time.Hour, Retain: true,
		Connects: connects, NeighborsInterval: time.Hour,
		Neighbors: func(context.Context) ([]NeighborEntry, int, bool) {
			return []NeighborEntry{{PubKey: "aaaa", SNR: 1, Status: "responded"}}, 1, true
		},
	}
	o := New(cfg, sink, zap.NewNop())
	b := bus.New()
	sub := b.Subscribe(4)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); o.Run(ctx, sub) }()

	// Nothing before the session lands: the failing blind first beat
	// is exactly what the connect signal replaced.
	time.Sleep(20 * time.Millisecond)
	if sink.count() != 0 {
		t.Fatalf("%d messages before any connection", sink.count())
	}
	connects <- struct{}{}
	deadline := time.After(5 * time.Second)
	for sink.count() < 2 {
		select {
		case <-deadline:
			t.Fatalf("connect earned %d messages, want status + neighbors", sink.count())
		case <-time.After(5 * time.Millisecond):
		}
	}
	cancel()
	sub.Close()
	<-done
	sink.mu.Lock()
	defer sink.mu.Unlock()
	classes := map[string]bool{}
	for _, m := range sink.msgs {
		classes[m.topic] = m.retain
	}
	for _, topic := range []string{"meshcore/PAR/feed/status", "meshcore/PAR/feed/neighbors"} {
		retain, seen := classes[topic]
		if !seen {
			t.Errorf("%s never published", topic)
		} else if !retain {
			t.Errorf("%s not retained despite retain=true", topic)
		}
	}
}

func TestIATAFollowsTheEcosystemRule(t *testing.T) {
	for in, want := range map[string]string{"": "", "par": "PAR", "DEN": "DEN", "9h1": "9H1"} {
		got, err := NormalizeIATA(in)
		if err != nil || got != want {
			t.Errorf("NormalizeIATA(%q) = %q %v, want %q", in, got, err, want)
		}
	}
	for _, bad := range []string{"pa", "pari", "p@r", "p r", "pa/", "célé"} {
		if _, err := NormalizeIATA(bad); err == nil {
			t.Errorf("NormalizeIATA(%q) accepted", bad)
		}
	}
}
