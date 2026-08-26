package meshcore

import (
	"crypto/rand"
	"strings"
	"testing"
	"time"

	"meshrunner.dev/pkg/meshcore"
)

func signedAdvert(t *testing.T, name string) []byte {
	t.Helper()
	id, err := meshcore.NewLocalIdentity(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pkt, err := meshcore.BuildAdvert(id, time.Now(), &meshcore.AdvertData{
		Type: meshcore.AdvTypeRepeater,
		Name: name,
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := pkt.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestAdvertsAreNamedAndVerified(t *testing.T) {
	e, sub := testEngine(t)

	e.judge(newFakeDevice(), frame(signedAdvert(t, "Wanadoo")))

	judged := drainJudged(t, sub)
	if len(judged) != 1 {
		t.Fatalf("judged %d frames", len(judged))
	}
	j := judged[0]
	if j.Node != "Wanadoo" || j.Detail != "repeater" {
		t.Errorf("node = %q, detail = %q", j.Node, j.Detail)
	}
	if len(j.PubKey) != keyPrefixLen {
		t.Errorf("pubkey prefix = %q", j.PubKey)
	}
	if j.Verdict != "would-relay-flood" {
		t.Errorf("verdict = %q", j.Verdict)
	}
}

func TestTamperedAdvertIsCalledOut(t *testing.T) {
	e, sub := testEngine(t)

	raw := signedAdvert(t, "Wanadoo")
	raw[len(raw)-1] ^= 0xFF // corrupt the signed region

	e.judge(newFakeDevice(), frame(raw))

	judged := drainJudged(t, sub)
	if len(judged) != 1 {
		t.Fatalf("judged %d frames", len(judged))
	}
	j := judged[0]
	if j.Detail != "advert signature invalid" {
		t.Errorf("detail = %q", j.Detail)
	}
	if j.Node != "" {
		t.Errorf("a tampered advert must name nobody, got %q", j.Node)
	}
}

func TestDiscoveryRequestIsDescribed(t *testing.T) {
	e, sub := testEngine(t)

	pkt, err := meshcore.BuildDiscoverReq(meshcore.DiscoverReq{
		Filter: meshcore.RepeaterFilter(),
		Tag:    0x14CABA78,
	})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := pkt.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	e.judge(newFakeDevice(), frame(raw))

	judged := drainJudged(t, sub)
	if len(judged) != 1 {
		t.Fatalf("judged %d frames", len(judged))
	}
	j := judged[0]
	if !strings.Contains(j.Detail, "discovery request") ||
		!strings.Contains(j.Detail, "repeaters") {
		t.Errorf("detail = %q", j.Detail)
	}
	if j.Verdict != "heard-zero-hop" {
		t.Errorf("verdict = %q", j.Verdict)
	}
}
