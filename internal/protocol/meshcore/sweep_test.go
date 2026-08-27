package meshcore

import (
	"strings"
	"testing"
	"time"

	"meshrunner.dev/pkg/meshcore"
)

func TestScanningTheNeighbourhood(t *testing.T) {
	// The question goes out zero-hop asking for repeaters, and an
	// answer bearing our own tag becomes a neighbour.
	e, dev, sub, peer := txRig(t, "on-air")
	runEngine(t, e, dev)

	found, _, err := e.Discover()
	if err != nil {
		t.Fatal(err)
	}
	if sent := awaitSent(t, sub); sent.Kind != "discover-req" {
		t.Fatalf("sent = %+v, want the scan", sent)
	}
	asked, err := meshcore.ParsePacket(<-dev.sent)
	if err != nil {
		t.Fatal(err)
	}
	req, err := meshcore.ParseDiscoverReq(asked)
	if err != nil {
		t.Fatal(err)
	}
	if !asked.IsRouteDirect() || asked.PathHashCount() != 0 {
		t.Fatalf("a scan must be zero hop, got %v/%d", asked.Route(), asked.PathHashCount())
	}
	if !req.Filter.Includes(meshcore.AdvTypeRepeater) || req.PrefixOnly {
		t.Fatalf("scan asks %+v, want whole keys from repeaters", req)
	}

	answer := func(tag uint32, key [meshcore.PubKeySize]byte) {
		resp, err := meshcore.BuildDiscoverResp(meshcore.DiscoverResp{
			NodeType: meshcore.AdvTypeRepeater, SNR: 6, Tag: tag, PubKey: key[:],
		}, false)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := resp.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}
		dev.frames <- frame(raw)
	}
	// An answer to somebody else's scan is not ours to keep.
	answer(req.Tag^0xFFFF, peer.PubKey)
	// Ours is.
	answer(req.Tag, peer.PubKey)

	select {
	case n := <-found:
		if n.PubKey != peer.PubKey {
			t.Fatalf("harvested %x", n.PubKey[:4])
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the answer never reached the scan")
	}
	if got := e.Neighbours(); len(got) != 1 || got[0].PubKey != peer.PubKey {
		t.Fatalf("neighbourhood = %+v, want the answering node", got)
	}
}

func TestOurOwnAnswerIsNotANeighbour(t *testing.T) {
	e := armedEngine(t, "shadow")
	e.pendingSweep = &sweep{
		tag: 7, until: time.Now().Add(time.Minute),
		found: make(chan Neighbour, 1),
		seen:  map[[meshcore.PubKeySize]byte]bool{},
	}
	resp, err := meshcore.BuildDiscoverResp(meshcore.DiscoverResp{
		NodeType: meshcore.AdvTypeRepeater, SNR: 6, Tag: 7, PubKey: e.id.PubKey[:],
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, handled := e.sweepAnswer(rxOf(e, resp)); !handled {
		t.Fatal("our own answer was not consumed")
	}
	if len(e.Neighbours()) != 0 {
		t.Fatal("this node became its own neighbour")
	}
}

func TestASecondScanIsRefusedNotSilentlyEmptied(t *testing.T) {
	e, dev, _, _ := txRig(t, "on-air")
	runEngine(t, e, dev)

	found, until, err := e.Discover()
	if err != nil {
		t.Fatalf("first scan: %v", err)
	}
	if found == nil || time.Until(until) < sweepWindow-askWait {
		t.Fatalf("window = %s, want about %s from when the question went out",
			time.Until(until), sweepWindow)
	}

	// A second one inside the first's window must say why it is not
	// running. Closing its channel would read as an empty room.
	_, _, err = e.Discover()
	if err == nil {
		t.Fatal("a second scan opened inside the first's window")
	}
	if !strings.Contains(err.Error(), "already listening") {
		t.Fatalf("refusal said %q", err)
	}
}

func TestADryRelayRefusesToScan(t *testing.T) {
	e, dev, _, _ := txRig(t, "dry")
	runEngine(t, e, dev)
	if _, _, err := e.Discover(); err == nil {
		t.Fatal("a dry relay asked the neighbourhood anyway")
	}
}
