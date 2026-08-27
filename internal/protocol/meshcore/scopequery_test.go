package meshcore

import (
	"testing"
	"time"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/txn"
)

func TestAskingANeighbourItsScopes(t *testing.T) {
	// The whole exchange: we ask zero-hop, the neighbour answers
	// sealed to us, and the names come back.
	e, dev, sub, peer := txRig(t, "on-air")
	runEngine(t, e, dev)

	answer := make(chan []string, 1)
	go func() {
		names, err := e.AskScopes(peer.PubKey[:])
		if err != nil {
			t.Error(err)
		}
		answer <- names
	}()

	if sent := awaitSent(t, sub); sent.Kind != "scope-req" {
		t.Fatalf("sent = %+v, want the question", sent)
	}
	asked, err := meshcore.ParsePacket(<-dev.sent)
	if err != nil {
		t.Fatal(err)
	}
	// Zero hop and addressed to the neighbour's key.
	if !asked.IsRouteDirect() || asked.PathHashCount() != 0 ||
		asked.PayloadType() != meshcore.PayloadTypeAnonReq {
		t.Fatalf("question shape: route %v hops %d type %v",
			asked.Route(), asked.PathHashCount(), asked.PayloadType())
	}
	// The neighbour reads it and finds the scopes sub-type.
	secret, err := peer.SharedSecret(e.id.PubKey[:])
	if err != nil {
		t.Fatal(err)
	}
	d, err := meshcore.ParseAnonDatagram(asked.Payload)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := d.Open(secret)
	if err != nil {
		t.Fatal(err)
	}
	tag, body, err := meshcore.UnframeAdmin(plain)
	if err != nil || len(body) < 1 || body[0] != anonReqTypeScopes {
		t.Fatalf("question body = %x (%v)", body, err)
	}

	// The neighbour answers: our clock, then the names.
	reply := append([]byte{0, 0, 0, 0}, []byte("*,fr,be")...)
	resp, err := meshcore.BuildResponse(e.id.PubKey[:meshcore.PathHashSize],
		peer.PubKey[:meshcore.PathHashSize], secret, tag, reply)
	if err != nil {
		t.Fatal(err)
	}
	resp.Header = meshcore.MakeHeader(meshcore.RouteDirect,
		meshcore.PayloadTypeResponse, meshcore.PayloadVer1)
	raw, err := resp.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	dev.frames <- frame(raw)

	select {
	case names := <-answer:
		if len(names) != 3 || names[0] != "*" || names[1] != "fr" || names[2] != "be" {
			t.Fatalf("names = %v", names)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the answer never reached the asker")
	}
}

func TestOnlyOneQuestionInFlight(t *testing.T) {
	// Asking several neighbours at once makes them answer into the
	// same window, where the replies collide.
	e, dev, _, peer := txRig(t, "on-air")
	e.scopeAsk <- &scopeQuery{answer: make(chan []string, 1)}
	if _, err := e.AskScopes(peer.PubKey[:]); err == nil {
		t.Fatal("a second question was accepted")
	}
	_ = dev
}

func TestAStrangersAnswerIsNotOurs(t *testing.T) {
	// A response addressed to a key prefix like ours, that our secret
	// cannot open, routes on rather than resolving our question.
	e, _, _, peer := txRig(t, "shadow")
	secret, err := e.id.SharedSecret(peer.PubKey[:])
	if err != nil {
		t.Fatal(err)
	}
	e.pendingScope = &scopeQuery{
		peer: peer.PubKey, secret: secret, tag: 42,
		answer: make(chan []string, 1), asked: time.Now(),
	}
	resp, err := meshcore.BuildResponse(e.id.PubKey[:meshcore.PathHashSize],
		peer.PubKey[:meshcore.PathHashSize], secret, 999, []byte{0, 0, 0, 0})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, handled := e.scopeAnswer(rxOf(e, resp)); handled {
		t.Fatal("an answer to a question we did not ask was consumed")
	}
	if e.pendingScope == nil {
		t.Fatal("our own question was retired by a stranger's answer")
	}
	_ = txn.New()
}
