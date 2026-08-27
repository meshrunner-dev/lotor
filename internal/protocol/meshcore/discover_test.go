package meshcore

import (
	"testing"
	"time"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/txn"
)

func TestRateLimiterKeepsTheReferenceShape(t *testing.T) {
	// Fixed window: so many from the window's first event, denial
	// until it expires — RateLimiter.h, quirk and all.
	r := rateLimiter{max: 4, window: 2 * time.Minute}
	now := time.Now()
	for i := range 4 {
		if !r.allow(now.Add(time.Duration(i) * time.Second)) {
			t.Fatalf("event %d refused under the cap", i+1)
		}
	}
	if r.allow(now.Add(5 * time.Second)) {
		t.Fatal("fifth event allowed inside the window")
	}
	if !r.allow(now.Add(2*time.Minute + time.Second)) {
		t.Fatal("window expired, still refusing")
	}
}

// scanFrame marshals a neighbourhood scan as a companion sends it.
func scanFrame(t *testing.T, req meshcore.DiscoverReq) radio.Frame {
	t.Helper()
	pkt, err := meshcore.BuildDiscoverReq(req)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := pkt.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return radio.Frame{Payload: raw, At: time.Now(), SNR: 9.5, RSSI: -70}
}

func TestScanGetsAnAnswer(t *testing.T) {
	e, dev, sub, _ := txRig(t, "on-air")
	runEngine(t, e, dev)
	dev.frames <- scanFrame(t, meshcore.DiscoverReq{
		Filter: meshcore.RepeaterFilter(), Tag: 0xDEADBEEF,
	})

	sent := awaitSent(t, sub)
	if sent.Kind != "discover-resp" {
		t.Fatalf("sent = %+v, want discover-resp", sent)
	}
	raw := <-dev.sent
	pkt, err := meshcore.ParsePacket(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !pkt.IsRouteDirect() || pkt.PathHashCount() != 0 {
		t.Fatal("the answer must be zero-hop, like the question")
	}
	resp, err := meshcore.ParseDiscoverResp(pkt)
	if err != nil {
		t.Fatal(err)
	}
	if resp.NodeType != meshcore.AdvTypeRepeater || resp.Tag != 0xDEADBEEF ||
		resp.SNR != 9.5 || len(resp.PubKey) != meshcore.PubKeySize ||
		resp.PubKey[0] != e.id.PubKey[0] {
		t.Fatalf("resp = %+v", resp)
	}
}

func TestScanRespectsPrefixOnly(t *testing.T) {
	e, dev, _, _ := txRig(t, "shadow")
	e.queue.depth = 8
	pkt, err := meshcore.BuildDiscoverReq(meshcore.DiscoverReq{
		Filter: meshcore.RepeaterFilter(), Tag: 7, PrefixOnly: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	e.respondDiscover(dev, pkt, txn.New(), 8)
	if len(e.queue.entries) != 1 {
		t.Fatalf("%d entries queued", len(e.queue.entries))
	}
	resp, err := meshcore.ParseDiscoverResp(e.queue.entries[0].pkt)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.PubKey) != 8 {
		t.Fatalf("prefix-only answer carries %d key bytes, want 8", len(resp.PubKey))
	}
}

func TestScanForOthersStaysUnanswered(t *testing.T) {
	e, dev, _, _ := txRig(t, "shadow")
	e.queue.depth = 8
	chatOnly := meshcore.AdvTypeFilter(1 << meshcore.AdvTypeChat)
	pkt, err := meshcore.BuildDiscoverReq(meshcore.DiscoverReq{Filter: chatOnly, Tag: 7})
	if err != nil {
		t.Fatal(err)
	}
	e.respondDiscover(dev, pkt, txn.New(), 8)

	future, err := meshcore.BuildDiscoverReq(meshcore.DiscoverReq{
		Filter: meshcore.RepeaterFilter(), Tag: 8,
		Since: uint32(time.Now().Add(time.Hour).Unix()),
	})
	if err != nil {
		t.Fatal(err)
	}
	e.respondDiscover(dev, future, txn.New(), 8)

	if n := len(e.queue.entries); n != 0 {
		t.Fatalf("%d answers queued — the filter or the since gate leaked", n)
	}
}

func TestScanFloodIsRateLimited(t *testing.T) {
	e, dev, sub, _ := txRig(t, "shadow")
	e.queue.depth = 16
	for i := range 6 {
		pkt, err := meshcore.BuildDiscoverReq(meshcore.DiscoverReq{
			Filter: meshcore.RepeaterFilter(), Tag: uint32(i),
		})
		if err != nil {
			t.Fatal(err)
		}
		e.respondDiscover(dev, pkt, txn.New(), 8)
	}
	if n := len(e.queue.entries); n != discoverLimitMax {
		t.Fatalf("%d answers queued, want the cap %d", n, discoverLimitMax)
	}
	dropped := 0
	for done := false; !done; {
		select {
		case ev := <-sub.C:
			if d, ok := ev.(bus.TxDropped); ok {
				if d.Reason != "rate-limited" {
					t.Fatalf("dropped for %q, want rate-limited", d.Reason)
				}
				dropped++
			}
		default:
			done = true
		}
	}
	if dropped != 2 {
		t.Fatalf("%d refusals published, want 2", dropped)
	}
}

func TestAdvertsWaitForAPlausibleClock(t *testing.T) {
	// A Pi without an RTC boots into the past; the advert stamps the
	// wall clock into a signed payload, so it waits for the network.
	e, dev, _, _ := txRig(t, "on-air")
	past := time.Date(2005, time.March, 1, 0, 0, 0, 0, time.UTC)
	e.nextFloodAdvert, e.nextLocalAdvert = past, past

	e.dueAdverts(dev, past.Add(time.Hour))
	if len(e.queue.entries) != 0 {
		t.Fatal("an advert went out with a 2005 clock")
	}
	if !e.nextFloodAdvert.After(past) || !e.nextLocalAdvert.After(past) {
		t.Fatal("the clocks were not deferred")
	}

	now := time.Now()
	e.nextFloodAdvert, e.nextLocalAdvert = now, now
	e.dueAdverts(dev, now)
	if len(e.queue.entries) != 1 {
		t.Fatalf("%d adverts queued with a sane clock, want 1 (never two a pass)", len(e.queue.entries))
	}
}

func TestANameSurvivesAScanThatCarriesNone(t *testing.T) {
	// An advert names a node; a discovery answer never does. A scan of
	// a neighbourhood we already know must not strip it back to keys.
	nt := newNeighbourTable()
	var key [meshcore.PubKeySize]byte
	key[0] = 0x88
	now := time.Now()
	nt.put(key, "quatre-vingt-huit", 11.5, now.Add(-time.Minute))
	nt.put(key, "", 12.25, now) // as a scan answer refreshes it

	got := nt.get(key)
	if got.Name != "quatre-vingt-huit" {
		t.Fatalf("name = %q — the scan erased what an advert taught", got.Name)
	}
	if got.SNR != 12.25 || !got.Heard.Equal(now) {
		t.Fatalf("the fresher reading was lost: %+v", got)
	}
	// And a later advert may rename it.
	nt.put(key, "88", 12.25, now)
	if nt.get(key).Name != "88" {
		t.Fatalf("a node that renamed itself was not followed")
	}
}
