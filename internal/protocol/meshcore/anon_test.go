package meshcore

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/bus"
	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/txn"
)

// ownerReq builds the companion's "request name": an ephemeral-keyed
// ANON_REQ of type owner, zero-hop direct, with an empty return path.
func ownerReq(t *testing.T, self, peer *meshcore.LocalIdentity, ts uint32) (radio.Frame, []byte) {
	t.Helper()
	secret, err := peer.SharedSecret(self.PubKey[:])
	if err != nil {
		t.Fatal(err)
	}
	plain := binary.LittleEndian.AppendUint32(nil, ts)
	plain = append(plain, anonReqTypeOwner, 0) // type, then an empty return path
	pkt, err := meshcore.BuildAnonDatagram(self.PubKey[:1], peer.PubKey[:], secret, plain)
	if err != nil {
		t.Fatal(err)
	}
	pkt.Header = meshcore.MakeHeader(meshcore.RouteDirect,
		meshcore.PayloadTypeAnonReq, meshcore.PayloadVer1)
	raw, err := pkt.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return radio.Frame{Payload: raw, At: time.Now(), SNR: 8, RSSI: -70}, secret
}

func TestRequestNameGetsTheName(t *testing.T) {
	e, dev, sub, peer := txRig(t, "on-air")
	runEngine(t, e, dev)
	frame, secret := ownerReq(t, e.id, peer, 0x11223344)
	dev.frames <- frame

	sent := awaitSent(t, sub)
	if sent.Kind != "anon-resp" || sent.Shadow {
		t.Fatalf("sent = %+v, want a real anon-resp", sent)
	}
	pkt, err := meshcore.ParsePacket(<-dev.sent)
	if err != nil {
		t.Fatal(err)
	}
	if pkt.PayloadType() != meshcore.PayloadTypeResponse ||
		!pkt.IsRouteDirect() || pkt.PathHashCount() != 0 {
		t.Fatalf("reply shape: type %v route %v hops %d", pkt.PayloadType(), pkt.Route(), pkt.PathHashCount())
	}
	dg, err := meshcore.ParseDatagram(pkt.Payload)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := dg.Open(secret)
	if err != nil {
		t.Fatalf("only the asker should read it: %v", err)
	}
	ts, rest, err := meshcore.UnframeAdmin(plain)
	if err != nil || ts != 0x11223344 {
		t.Fatalf("echoed tag = %08x (%v), want 11223344", ts, err)
	}
	// The cipher pads to its block; a client reads to the NUL, as C
	// strings always did.
	if len(rest) < 5 || string(bytes.TrimRight(rest[4:], "\x00")) != "test\n" {
		t.Fatalf("reply text = %q, want the node's name", rest[4:])
	}
}

func TestAnonRepliesAreRateLimited(t *testing.T) {
	e, dev, sub, peer := txRig(t, "shadow")
	e.queue.depth = 16
	for i := range anonLimitMax + 2 {
		frame, _ := ownerReq(t, e.id, peer, uint32(i))
		pkt, err := meshcore.ParsePacket(frame.Payload)
		if err != nil {
			t.Fatal(err)
		}
		e.respondAnon(dev, pkt, txn.New())
	}
	if n := len(e.queue.entries); n != anonLimitMax {
		t.Fatalf("%d replies queued, want the cap %d", n, anonLimitMax)
	}
	dropped := 0
	for done := false; !done; {
		select {
		case ev := <-sub.C:
			if d, ok := ev.(bus.TxDropped); ok && d.Reason == "rate-limited" {
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

func TestUnreadableAnonTrafficRoutesOn(t *testing.T) {
	// A request for someone else — or one whose MAC fails — is plain
	// traffic: the reference forwards what it could not read.
	e, _, _, peer := txRig(t, "shadow")
	other, err := meshcore.LocalIdentityFromSeed(append(make([]byte, 31), 9))
	if err != nil {
		t.Fatal(err)
	}
	frame, _ := ownerReq(t, other, peer, 7) // addressed to another node
	pkt, err := meshcore.ParsePacket(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	pkt.Header = meshcore.MakeHeader(meshcore.RouteFlood,
		meshcore.PayloadTypeAnonReq, meshcore.PayloadVer1)
	if v, _ := e.verdict(pkt, false, false); v != verdictDropFloodType && v != verdictRelayFlood {
		t.Fatalf("verdict = %q, want plain flood routing", v)
	}

	// Addressed to us but flooded: consumed — read, never answered,
	// never relayed (the reference marks it do-not-retransmit).
	frame, _ = ownerReq(t, e.id, peer, 8)
	pkt, err = meshcore.ParsePacket(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	pkt.Header = meshcore.MakeHeader(meshcore.RouteFlood,
		meshcore.PayloadTypeAnonReq, meshcore.PayloadVer1)
	if v, _ := e.verdict(pkt, false, false); v != verdictAnon {
		t.Fatalf("verdict = %q, want anon-request", v)
	}
	before := len(e.queue.entries)
	e.respondAnon(newFakeDevice(), pkt, txn.New())
	if len(e.queue.entries) != before {
		t.Fatal("a flooded owner request was answered — the reference gates on direct")
	}
}
