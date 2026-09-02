package meshcore

import (
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/correlation"
	"meshrunner.dev/lotor/internal/radio"
)

// teachPath builds the PATH a client sends to say how to reach it: a
// route return sealed to the server, carrying the client's own route
// home and no extra payload.
func teachPath(t *testing.T, self, peer *meshcore.LocalIdentity,
	pathLen uint8, path []byte,
) radio.Frame {
	t.Helper()
	secret, err := peer.SharedSecret(self.PubKey[:])
	if err != nil {
		t.Fatal(err)
	}
	pkt, err := meshcore.BuildPathReturn(self.PubKey[:meshcore.PathHashSize],
		peer.PubKey[:meshcore.PathHashSize], secret, pathLen, path, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	pkt.Header = meshcore.MakeHeader(meshcore.RouteDirect,
		meshcore.PayloadTypePath, meshcore.PayloadVer1)
	raw, err := pkt.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	return radio.Frame{Payload: raw, At: time.Now(), SNR: rxTestSNR}
}

// drive judges one frame on the caller's goroutine, the way the
// pipeline would. The engine is not running in these tests: the ACL
// belongs to the engine's goroutine, and a test that reads it must be
// the one that wrote it.
func drive(t *testing.T, e *engine, f radio.Frame) string {
	t.Helper()
	pkt, err := meshcore.ParsePacket(f.Payload)
	if err != nil {
		t.Fatal(err)
	}
	rx := rxOf(e, pkt)
	v, _ := e.verdict(rx)
	if v == verdictAnon {
		e.respondAnon(rx, correlation.New())
	}
	return v
}

// guestIn opens a session the way a companion does, and returns it.
func guestIn(t *testing.T, e *engine, peer *meshcore.LocalIdentity) *client {
	t.Helper()
	e.p.GuestAccess = guestOpen
	frame, _ := login(t, e.id, peer, nowTS(100), "", false)
	drive(t, e, frame)
	c := e.acl.Get(peer.PubKey[:])
	if c == nil {
		t.Fatal("the login opened no session")
	}
	return c
}

func TestClientRouteHomeIsLearnedAndUsed(t *testing.T) {
	e, dev, sub, peer := txRig(t, "on-air")
	e.p.GuestAccess = guestOpen
	runEngine(t, e, dev)

	frame, _ := login(t, e.id, peer, nowTS(100), "", false)
	dev.frames <- frame
	if sent := awaitSent(t, sub); sent.Kind != "login-resp" {
		t.Fatalf("sent = %+v", sent)
	}
	<-dev.sent

	// The client teaches us a two-hop route home, then asks again.
	dev.frames <- teachPath(t, e.id, peer, 2, []byte{0xAA, 0xBB})
	dev.frames <- request(t, e.id, peer, nowTS(101), []byte{meshcore.ReqGetStatus, 0, 0, 0, 0})

	if sent := awaitSent(t, sub); sent.Kind != "req-resp" {
		t.Fatalf("sent = %+v", sent)
	}
	pkt, err := meshcore.ParsePacket(<-dev.sent)
	if err != nil {
		t.Fatal(err)
	}
	if !pkt.IsRouteDirect() {
		t.Fatalf("the answer routed %v — a known route home earns a direct reply", pkt.Route())
	}
	if pkt.PathLen != 2 || string(pkt.Path) != string([]byte{0xAA, 0xBB}) {
		t.Fatalf("answer path: len %d, % x", pkt.PathLen, pkt.Path)
	}
}

func TestWithoutARouteHomeTheAnswerFloods(t *testing.T) {
	e, dev, sub, peer := txRig(t, "on-air")
	e.p.GuestAccess = guestOpen
	runEngine(t, e, dev)

	frame, _ := login(t, e.id, peer, nowTS(100), "", false)
	dev.frames <- frame
	awaitSent(t, sub)
	<-dev.sent

	dev.frames <- request(t, e.id, peer, nowTS(101), []byte{meshcore.ReqGetStatus, 0, 0, 0, 0})
	awaitSent(t, sub)
	pkt, err := meshcore.ParsePacket(<-dev.sent)
	if err != nil {
		t.Fatal(err)
	}
	if !pkt.IsRouteFlood() {
		t.Fatalf("the answer routed %v — with nowhere to send it, only a flood reaches both "+
			"the adjacent and the distant", pkt.Route())
	}
}

func TestRouteHomeIsLearnedAndTheNewestWins(t *testing.T) {
	e, _, _, peer := txRig(t, "shadow")
	c := guestIn(t, e, peer)
	if c.Out != nil {
		t.Fatal("a fresh session already knows a route home")
	}

	if v := drive(t, e, teachPath(t, e.id, peer, 2, []byte{0xAA, 0xBB})); v != verdictClientPath {
		t.Fatalf("verdict = %q, want the route to be learned", v)
	}
	if c.Out == nil || c.Out.PathLen != 2 || string(c.Out.Path) != string([]byte{0xAA, 0xBB}) {
		t.Fatalf("route home = %+v", c.Out)
	}
	view := e.Clients()
	if len(view.Sessions) != 1 || !view.Sessions[0].HasPath ||
		string(view.Sessions[0].Path) != string([]byte{0xAA, 0xBB}) {
		t.Fatalf("published route home = %+v", view)
	}
	view.Sessions[0].Path[0] = 0xEE
	if got := e.Clients().Sessions[0].Path[0]; got != 0xAA {
		t.Fatalf("a caller changed the immutable edition: %x", got)
	}

	// The client moved, and says so: the older route no longer reaches.
	if v := drive(t, e, teachPath(t, e.id, peer, 1, []byte{0xCC})); v != verdictClientPath {
		t.Fatalf("verdict = %q", v)
	}
	if c.Out.PathLen != 1 || string(c.Out.Path) != string([]byte{0xCC}) {
		t.Fatalf("route home = %+v, want the one it just taught us", c.Out)
	}
}

func TestRouteLearningLogKeepsTheFrameCorrelation(t *testing.T) {
	e, _, _, peer := txRig(t, "shadow")
	c := guestIn(t, e, peer)
	core, observed := observer.New(zapcore.DebugLevel)
	e.log = zap.New(core)

	f := teachPath(t, e.id, peer, 2, []byte{0xAA, 0xBB})
	pkt, err := meshcore.ParsePacket(f.Payload)
	if err != nil {
		t.Fatal(err)
	}
	rx := rxOf(e, pkt)
	if c.Out == nil {
		t.Fatal("the path verdict did not learn the route")
	}
	entries := observed.FilterMessage("a client taught us its route home").All()
	if len(entries) != 1 {
		t.Fatalf("route logs = %+v", observed.All())
	}
	if got := entries[0].ContextMap()["corr"]; got != rx.id.Short() {
		t.Errorf("corr field = %v, want %s", got, rx.id.Short())
	}
}

func TestARouteFromAStrangerIsNotLearned(t *testing.T) {
	e, _, _, peer := txRig(t, "shadow")
	// No login: nothing here opens it, and it is not ours to read.
	if v := drive(t, e, teachPath(t, e.id, peer, 2, []byte{0xAA, 0xBB})); v == verdictClientPath {
		t.Fatal("a stranger taught us a route home")
	}
	if e.acl.Get(peer.PubKey[:]) != nil {
		t.Fatal("a route home conjured a session out of nothing")
	}
}
