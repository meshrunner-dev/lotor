package mqtt

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/bus"
)

// advertFrame builds a real signed advert off the wire library, so
// the tests analyse the same bytes a radio would hand over.
func advertFrame(t *testing.T) ([]byte, *meshcore.LocalIdentity) {
	t.Helper()
	seed := make([]byte, meshcore.SeedSize)
	seed[0] = 7
	id, err := meshcore.LocalIdentityFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	pkt, err := meshcore.BuildAdvert(id, time.Unix(1756340000, 0), &meshcore.AdvertData{
		Type: meshcore.AdvTypeRepeater, Name: "obs-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := pkt.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return raw, id
}

func TestPacketJSONHonoursTheContract(t *testing.T) {
	raw, _ := advertFrame(t)
	pkt, err := meshcore.ParsePacket(raw)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 8, 28, 2, 32, 16, 123456000, time.UTC)
	payload, err := PacketJSON(raw, pkt, at, true, "Raccoon City", strings.Repeat("ab", 32), 12.25, -87.6)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(payload, &m); err != nil {
		t.Fatal(err)
	}
	// The quirks are the contract: numbers as strings, µs timestamps
	// with an explicit offset, dd/mm dates.
	for key, want := range map[string]string{
		"timestamp":   "2026-08-28T02:32:16.123456+00:00",
		"type":        "PACKET",
		"direction":   "rx",
		"time":        "02:32:16",
		"date":        "28/08/2026",
		"route":       "F",
		"packet_type": "4", // ADVERT rides as its wire value
		"SNR":         "12.2",
		"RSSI":        "-87",
		"origin":      "Raccoon City",
	} {
		if got, ok := m[key].(string); !ok || got != want {
			t.Errorf("%s = %v, want %q", key, m[key], want)
		}
	}
	if got, ok := m["len"].(string); !ok || got == "" || strings.HasPrefix(got, "0") {
		t.Errorf("len = %v", m["len"])
	}
	if got, ok := m["hash"].(string); !ok || len(got) != 16 {
		t.Errorf("hash = %v, want 8 bytes of hex", m["hash"])
	}
	if got, ok := m["raw"].(string); !ok || len(got) != 2*len(raw) {
		t.Errorf("raw = %v for %d bytes", m["raw"], len(raw))
	}
	if _, there := m["path"]; there {
		t.Error("a flood carried a path array")
	}
	// The tx direction says less, per the contract.
	payload, _ = PacketJSON(raw, pkt, at, false, "n", "id", 0, 0)
	var tx map[string]any
	_ = json.Unmarshal(payload, &tx)
	if tx["direction"] != "tx" {
		t.Errorf("direction = %v", tx["direction"])
	}
	for _, absent := range []string{"SNR", "RSSI", "score"} {
		if _, there := tx[absent]; there {
			t.Errorf("a tx frame carried %s", absent)
		}
	}
}

func TestDirectPathBecomesHopTokens(t *testing.T) {
	raw, _ := advertFrame(t)
	pkt, _ := meshcore.ParsePacket(raw)
	pkt.Header = meshcore.MakeHeader(meshcore.RouteDirect, meshcore.PayloadTypeAdvert, meshcore.PayloadVer1)
	pkt.Path, pkt.PathLen = []byte{0x4f, 0xa2}, 2
	reraw, err := pkt.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	pkt2, _ := meshcore.ParsePacket(reraw)
	payload, _ := PacketJSON(reraw, pkt2, time.Now(), true, "n", "id", 1, -80)
	var m struct {
		Route string   `json:"route"`
		Path  []string `json:"path"`
	}
	_ = json.Unmarshal(payload, &m)
	if m.Route != "D" || len(m.Path) != 2 || m.Path[0] != "4f" || m.Path[1] != "a2" {
		t.Errorf("route %q path %v", m.Route, m.Path)
	}
}

func TestStatusJSONOmitsWhatItDoesNotKnow(t *testing.T) {
	up, floor := 90, -104
	payload, err := StatusJSON(time.Unix(1756341136, 0), "n", "id",
		"sx126x-spi", "1.0", RadioString(869_618_000, 62_500, 8, 8), "lotor 1.0",
		Health{Repeat: "on", UptimeSecs: &up, NoiseFloor: &floor})
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(payload, &m)
	if m["status"] != "online" || m["repeat"] != "on" {
		t.Errorf("status/repeat = %v/%v", m["status"], m["repeat"])
	}
	if m["radio"] != "869.618000,62.5,8,8" {
		t.Errorf("radio = %v", m["radio"])
	}
	stats, ok := m["stats"].(map[string]any)
	if !ok {
		t.Fatalf("stats = %v", m["stats"])
	}
	if stats["uptime_secs"] != float64(90) || stats["noise_floor"] != float64(-104) {
		t.Errorf("stats = %v", stats)
	}
	for _, absent := range []string{"packets_sent", "errors", "battery_mv", "queue_len"} {
		if _, there := stats[absent]; there {
			t.Errorf("an unknown stat %s was invented", absent)
		}
	}
	// Nothing known at all: the stats object itself goes.
	payload, _ = StatusJSON(time.Now(), "n", "id", "m", "f", "r", "c", Health{})
	var bare map[string]any
	_ = json.Unmarshal(payload, &bare)
	if _, there := bare["stats"]; there {
		t.Error("an empty stats object was sent")
	}
}

func TestTopicsExpandAndRefuseHalfExpansion(t *testing.T) {
	got, err := BuildTopic(DefaultTopic, "PAR", "abcd", "", TopicPackets)
	if err != nil || got != "meshcore/PAR/abcd/packets" {
		t.Fatalf("BuildTopic = %q, %v", got, err)
	}
	if got, err := BuildTopic("meshrank/uplink/{token}/{device}/{type}", "", "abcd", "tok", TopicStatus); err != nil ||
		got != "meshrank/uplink/tok/abcd/status" {
		t.Fatalf("meshrank layout = %q, %v", got, err)
	}
	for _, c := range []struct{ template, iata string }{
		{DefaultTopic, ""},                         // empty level
		{"meshcore/{iata}/{devise}/{type}", "PAR"}, // typo'd placeholder survives → braces
		{"meshcore/+/{device}/{type}", "PAR"},      // wildcard
	} {
		if got, err := BuildTopic(c.template, c.iata, "abcd", "", TopicRaw); err == nil {
			t.Errorf("BuildTopic(%q, iata=%q) accepted %q", c.template, c.iata, got)
		}
	}
}

// recorder is the tests' Sink.
type recorder struct {
	mu   sync.Mutex
	msgs []struct {
		topic   string
		qos     byte
		payload []byte
	}
	fail bool
}

func (r *recorder) Publish(topic string, qos byte, _ bool, payload []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.fail {
		return errors.New("broker says no")
	}
	r.msgs = append(r.msgs, struct {
		topic   string
		qos     byte
		payload []byte
	}{topic, qos, append([]byte(nil), payload...)})
	return nil
}
func (r *recorder) Connected() bool { return true }
func (r *recorder) Close()          {}
func (r *recorder) count() int      { r.mu.Lock(); defer r.mu.Unlock(); return len(r.msgs) }

func observerRig(t *testing.T, cfg Config) (*recorder, *bus.Bus, func() Counters) {
	t.Helper()
	b := bus.New()
	sub := b.Subscribe(32)
	t.Cleanup(sub.Close)
	rec := &recorder{}
	if cfg.Topic == "" {
		cfg.Topic = DefaultTopic
	}
	if cfg.IATA == "" {
		cfg.IATA = "PAR"
	}
	o := New(cfg, rec, zap.NewNop())
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan struct{})
	go func() { o.Run(ctx, sub); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return rec, b, func() Counters { return o.Counters(sub) }
}

func settle(t *testing.T, rec *recorder, want int) {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for rec.count() < want {
		select {
		case <-deadline:
			t.Fatalf("published %d, want %d", rec.count(), want)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestObserverPublishesWhatItHears(t *testing.T) {
	raw, id := advertFrame(t)
	rec, b, counters := observerRig(t, Config{
		Instance: "t", Relay: "mc", RX: true, Packets: true,
		Origin: "Raccoon City", OriginID: "feed",
	})
	b.Publish(bus.FrameHeard{Relay: "mc", At: time.Now(), SNR: 9, RSSI: -80, Raw: raw})
	// Another relay's frame is not ours to describe.
	b.Publish(bus.FrameHeard{Relay: "other", At: time.Now(), Raw: raw})
	settle(t, rec, 1)
	if rec.msgs[0].topic != "meshcore/PAR/feed/packets" || rec.msgs[0].qos != 0 {
		t.Errorf("published to %q qos %d", rec.msgs[0].topic, rec.msgs[0].qos)
	}
	var m map[string]any
	_ = json.Unmarshal(rec.msgs[0].payload, &m)
	if m["direction"] != "rx" || m["origin"] != "Raccoon City" {
		t.Errorf("payload = %v", m)
	}
	_ = id
	if n := counters(); n.Published != 1 {
		t.Errorf("counters = %+v", n)
	}
}

func TestObserverTXModes(t *testing.T) {
	raw, id := advertFrame(t)
	// Default mode shares only this node's own adverts.
	rec, b, counters := observerRig(t, Config{
		Instance: "t", Relay: "mc", Packets: true, TX: TXSelfAdverts,
		OriginID: hex.EncodeToString(id.PubKey[:]),
	})
	b.Publish(bus.FrameSent{Relay: "mc", At: time.Now(), Raw: raw})
	settle(t, rec, 1)
	var m map[string]any
	_ = json.Unmarshal(rec.msgs[0].payload, &m)
	if m["direction"] != "tx" {
		t.Errorf("direction = %v", m["direction"])
	}
	// A relayed frame — somebody else's bytes — stays home in this
	// mode, counted as filtered.
	otherSeed := make([]byte, meshcore.SeedSize)
	otherSeed[0] = 9
	other, _ := meshcore.LocalIdentityFromSeed(otherSeed)
	pkt, _ := meshcore.BuildAdvert(other, time.Now(), &meshcore.AdvertData{Type: meshcore.AdvTypeChat, Name: "x"})
	otherRaw, _ := pkt.MarshalBinary()
	b.Publish(bus.FrameSent{Relay: "mc", At: time.Now(), Raw: otherRaw})
	// And a shadow emission never happened at all.
	b.Publish(bus.FrameSent{Relay: "mc", At: time.Now(), Raw: raw, Shadow: true})
	time.Sleep(50 * time.Millisecond)
	if rec.count() != 1 {
		t.Fatalf("count = %d, want the self advert alone", rec.count())
	}
	if n := counters(); n.Filtered != 1 {
		t.Errorf("filtered = %d, want the stranger's frame counted", n.Filtered)
	}
}

func TestObserverTypeFilterAndRawTopic(t *testing.T) {
	raw, _ := advertFrame(t)
	rec, b, counters := observerRig(t, Config{
		Instance: "t", Relay: "mc", RX: true, Packets: true, Raw: true,
		Types: []string{"REQ"}, OriginID: "feed",
	})
	b.Publish(bus.FrameHeard{Relay: "mc", At: time.Now(), Raw: raw}) // an ADVERT: declined
	time.Sleep(50 * time.Millisecond)
	if rec.count() != 0 || counters().Filtered != 1 {
		t.Fatalf("the filter leaked: %d published, %+v", rec.count(), counters())
	}
	// Widen the filter and both topics ride the same frame.
	rec2, b2, counters2 := observerRig(t, Config{
		Instance: "t", Relay: "mc", RX: true, Packets: true, Raw: true, OriginID: "feed",
	})
	b2.Publish(bus.FrameHeard{Relay: "mc", At: time.Now(), Raw: raw})
	settle(t, rec2, 2)
	topics := []string{rec2.msgs[0].topic, rec2.msgs[1].topic}
	if topics[0] != "meshcore/PAR/feed/packets" || topics[1] != "meshcore/PAR/feed/raw" {
		t.Errorf("topics = %v", topics)
	}
	var rawMsg map[string]any
	_ = json.Unmarshal(rec2.msgs[1].payload, &rawMsg)
	if rawMsg["type"] != "RAW" || rawMsg["data"] == "" {
		t.Errorf("raw message = %v", rawMsg)
	}
	if counters2().Published != 2 {
		t.Errorf("counters = %+v", counters2())
	}
}

func TestObserverHeartbeat(t *testing.T) {
	up := 5
	rec, _, counters := observerRig(t, Config{
		Instance: "t", Relay: "mc", Status: true, StatusInterval: 20 * time.Millisecond,
		OriginID: "feed", Model: "sx126x-spi", Firmware: "1.0", Client: "lotor 1.0",
		Radio:  RadioString(869_618_000, 62_500, 8, 8),
		Health: func() Health { return Health{Repeat: "on", UptimeSecs: &up} },
	})
	settle(t, rec, 2) // the immediate beat, then the ticker's
	if rec.msgs[0].topic != "meshcore/PAR/feed/status" || rec.msgs[0].qos != 1 {
		t.Errorf("status rode %q qos %d", rec.msgs[0].topic, rec.msgs[0].qos)
	}
	var m map[string]any
	_ = json.Unmarshal(rec.msgs[0].payload, &m)
	if m["status"] != "online" || m["repeat"] != "on" {
		t.Errorf("heartbeat = %v", m)
	}
	if counters().Published < 2 {
		t.Errorf("counters = %+v", counters())
	}
}

func TestObserverCountsWhatABrokerRefuses(t *testing.T) {
	raw, _ := advertFrame(t)
	rec, b, counters := observerRig(t, Config{
		Instance: "t", Relay: "mc", RX: true, Packets: true, OriginID: "feed",
	})
	rec.mu.Lock()
	rec.fail = true
	rec.mu.Unlock()
	b.Publish(bus.FrameHeard{Relay: "mc", At: time.Now(), Raw: raw})
	deadline := time.After(2 * time.Second)
	for counters().PublishErrors == 0 {
		select {
		case <-deadline:
			t.Fatal("the refusal was never counted")
		case <-time.After(5 * time.Millisecond):
		}
	}
}
