package meshcore

import (
	"strings"
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
	if err != nil || len(body) < 1 || body[0] != meshcore.AnonReqScopes {
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

func TestASecondQuestionIsRefusedNotSwallowed(t *testing.T) {
	// The channel empties the moment the first question is taken, so
	// a second one enqueues freely and the pipeline then finds one
	// already in flight. Returning silently there left the caller
	// waiting out the whole window to be told the neighbour had not
	// answered — for a question that never left.
	e, dev, _, peer := txRig(t, "on-air")
	e.queue.depth = 8
	// A question already in flight, as a first AskScopes would leave.
	e.pendingScope = &scopeQuery{
		peer: peer.PubKey, tag: 1, answer: make(chan []string, 1), asked: time.Now(),
	}
	q := &scopeQuery{
		peer: peer.PubKey, tag: 2, answer: make(chan []string, 1),
		asked: time.Now(), started: newAck(),
	}
	e.scopeAsk <- q
	e.drainScopeAsk(dev, time.Now())

	err := q.started.wait("scopes question")
	if err == nil {
		t.Fatal("the second question was taken while one was in flight")
	}
	if !strings.Contains(err.Error(), "in flight") {
		t.Errorf("refusal = %v", err)
	}
	if len(e.queue.entries) != 0 {
		t.Errorf("a refused question queued %d emissions", len(e.queue.entries))
	}
}

func TestAQuestionTheQueueRefusesSaysSo(t *testing.T) {
	e, dev, _, peer := txRig(t, "on-air")
	e.queue.depth = 0 // nothing may be scheduled
	secret, err := e.id.SharedSecret(peer.PubKey[:])
	if err != nil {
		t.Fatal(err)
	}
	q := &scopeQuery{
		peer: peer.PubKey, secret: secret, tag: 3, answer: make(chan []string, 1),
		asked: time.Now(), started: newAck(),
	}
	e.scopeAsk <- q
	e.drainScopeAsk(dev, time.Now())

	err = q.started.wait("scopes question")
	if err == nil {
		t.Fatal("a question the queue refused reported success")
	}
	if !strings.Contains(err.Error(), "never left") {
		t.Errorf("refusal = %v", err)
	}
	// And the slot is free again: a refused question must not block
	// the next one behind a window it never opened.
	if e.pendingScope != nil {
		t.Error("the refused question still holds the slot")
	}
}

func TestRegionNamesRefuseTheirOwnDelimiter(t *testing.T) {
	// The regions answer separates names with commas, so a name
	// holding one is served as a pair and read back at the other end
	// as two names nobody can derive a key for. The reference's own
	// grammar refuses it, along with the rest of the punctuation —
	// and the refusal now guards the command door, where names are
	// written since the region table replaced the config attributes.
	m := meshcore.NewRegionMap()
	for _, bad := range []string{"eu,lab", "eu lab", "eu.lab", "eu:lab", "eu/lab", "eu\x00lab", "eu\nlab"} {
		if _, err := m.Put(bad, 0); err == nil {
			t.Errorf("region %q accepted", bad)
		}
	}
	for _, good := range []string{"eu", "EU-868", "#lab", "lab2", "café"} {
		if _, err := m.Put(good, 0); err != nil {
			t.Errorf("region %q refused: %v", good, err)
		}
	}
	// Over the command door the same refusal answers in the CLI's own
	// words, and the private-region syntax is refused with its reason.
	e := regionRig(t)
	if reply, _ := e.serveRegionLine("admin", "region put eu,lab"); reply != "Err - unable to put" {
		t.Errorf("comma name = %q", reply)
	}
	if reply, _ := e.serveRegionLine("admin", "region put $secret"); !strings.Contains(reply, "not supported") {
		t.Errorf("private region = %q", reply)
	}
}
