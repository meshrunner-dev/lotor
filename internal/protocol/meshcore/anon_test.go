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

// anonAsk builds a companion's anonymous question: an ANON_REQ sealed to
// our key, zero-hop direct, with an empty return path.
func anonAsk(t *testing.T, self, peer *meshcore.LocalIdentity, ts uint32, reqType byte) (radio.Frame, []byte) {
	t.Helper()
	secret, err := peer.SharedSecret(self.PubKey[:])
	if err != nil {
		t.Fatal(err)
	}
	plain := binary.LittleEndian.AppendUint32(nil, ts)
	plain = append(plain, reqType, 0) // type, then an empty return path
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
	frame, secret := anonAsk(t, e.id, peer, 0x11223344, meshcore.AnonReqOwner)
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
	e, _, sub, peer := txRig(t, "shadow")
	e.queue.depth = 16
	for i := range anonLimitMax + 2 {
		frame, _ := anonAsk(t, e.id, peer, uint32(i), meshcore.AnonReqOwner)
		pkt, err := meshcore.ParsePacket(frame.Payload)
		if err != nil {
			t.Fatal(err)
		}
		e.respondAnon(rxOf(e, pkt), txn.New())
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
	frame, _ := anonAsk(t, other, peer, 7, meshcore.AnonReqOwner) // addressed to another node
	pkt, err := meshcore.ParsePacket(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	pkt.Header = meshcore.MakeHeader(meshcore.RouteFlood,
		meshcore.PayloadTypeAnonReq, meshcore.PayloadVer1)
	if v, _ := e.verdict(rxOf(e, pkt)); v != verdictDropFloodType && v != verdictRelayFlood {
		t.Fatalf("verdict = %q, want plain flood routing", v)
	}

	// Addressed to us but flooded: consumed — read, never answered,
	// never relayed (the reference marks it do-not-retransmit).
	frame, _ = anonAsk(t, e.id, peer, 8, meshcore.AnonReqOwner)
	pkt, err = meshcore.ParsePacket(frame.Payload)
	if err != nil {
		t.Fatal(err)
	}
	pkt.Header = meshcore.MakeHeader(meshcore.RouteFlood,
		meshcore.PayloadTypeAnonReq, meshcore.PayloadVer1)
	if v, _ := e.verdict(rxOf(e, pkt)); v != verdictAnon {
		t.Fatalf("verdict = %q, want anon-request", v)
	}
	before := len(e.queue.entries)
	e.respondAnon(rxOf(e, pkt), txn.New())
	if len(e.queue.entries) != before {
		t.Fatal("a flooded owner request was answered — the reference gates on direct")
	}
}

// askAndOpen drives one anonymous question through a running engine
// and returns the decrypted reply body after the echoed tag and clock.
func askAndOpen(t *testing.T, reqType byte) []byte {
	t.Helper()
	e, dev, sub, peer := txRig(t, "on-air")
	runEngine(t, e, dev)
	frame, secret := anonAsk(t, e.id, peer, 0xCAFE, reqType)
	dev.frames <- frame

	if sent := awaitSent(t, sub); sent.Kind != "anon-resp" {
		t.Fatalf("sent = %+v", sent)
	}
	pkt, err := meshcore.ParsePacket(<-dev.sent)
	if err != nil {
		t.Fatal(err)
	}
	dg, err := meshcore.ParseDatagram(pkt.Payload)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := dg.Open(secret)
	if err != nil {
		t.Fatal(err)
	}
	ts, rest, err := meshcore.UnframeAdmin(plain)
	if err != nil || ts != 0xCAFE {
		t.Fatalf("tag = %x (%v)", ts, err)
	}
	if len(rest) < 4 {
		t.Fatalf("no clock in the reply: %x", rest)
	}
	now := binary.LittleEndian.Uint32(rest[:4])
	if delta := int64(now) - time.Now().Unix(); delta < -5 || delta > 5 {
		t.Fatalf("clock off by %d s", delta)
	}
	return bytes.TrimRight(rest[4:], "\x00")
}

func TestClockRequestGetsTheClock(t *testing.T) {
	if text := askAndOpen(t, meshcore.AnonReqClock); len(text) != 0 {
		t.Fatalf("clock reply carries text %q — the clock alone is the answer", text)
	}
}

func TestScopesRequestNamesWhatWeCarry(t *testing.T) {
	// The answer is the reference's shape: the wildcard first when
	// plain floods are carried, then each scope with its hash stripped.
	if text := askAndOpen(t, meshcore.AnonReqScopes); string(text) != "*" {
		t.Fatalf("scopes = %q, want just the wildcard from a relay with none", text)
	}
}
