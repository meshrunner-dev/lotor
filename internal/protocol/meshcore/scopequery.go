package meshcore

// Asking a neighbour which scopes it carries. The mesh has no gossip
// for this: a node learns another's scopes by asking that node
// directly, zero hop, one at a time. There is no propagation and no
// periodic exchange in the protocol — this is the whole mechanism.

import (
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/txn"
)

// scopeQueryWait bounds how long an answer may take. The responder
// pauses its own fixed beat before replying and then queues behind its
// own channel wait, which the reference bounds at four seconds; ours
// may wait as long before the question even leaves. Generous, because
// the cost of waiting is an operator's patience and the cost of giving
// up early is an answer arriving with nobody left to hear it.
const scopeQueryWait = 30 * time.Second

// scopeQuery is one question in flight.
//
// Exactly one at a time, and that is not an implementation shortcut:
// asking several neighbours at once makes them all answer into the
// same window, where the replies collide and none survives. The
// protocol has no way to stagger them, so the asker must.
type scopeQuery struct {
	peer   [meshcore.PubKeySize]byte
	secret []byte
	tag    uint32
	answer chan []string
	asked  time.Time
	// started says whether the pipeline took the question at all, and
	// why not when it did not. Without it every refusal read as the
	// silence of a neighbour that never answered: the caller waited
	// out the whole window and was told nobody replied, for a
	// question that never left.
	started *ack
}

// Neighbour resolves a key prefix against the neighbourhood: the
// console names a node by the few bytes an operator can read, and the
// answer must be sealed to the whole key.
func (e *engine) Neighbour(prefix []byte) ([]byte, error) {
	if e.neighbours == nil {
		return nil, errors.New("this relay keeps no neighbourhood")
	}
	var found []byte
	for _, n := range e.neighbours.snapshot() {
		if len(prefix) <= len(n.PubKey) && string(n.PubKey[:len(prefix)]) == string(prefix) {
			if found != nil {
				return nil, fmt.Errorf("%x names more than one neighbour", prefix)
			}
			key := n.PubKey
			found = key[:]
		}
	}
	if found == nil {
		return nil, fmt.Errorf("no neighbour starts with %x — ask one we hear directly", prefix)
	}
	return found, nil
}

// ErrNoAnswer says a question went out and its window closed empty —
// distinct from failing to ask, and callers report the two apart.
var ErrNoAnswer = errors.New("no answer — the neighbour is out of earshot, " +
	"rate-limiting us, or carries nothing it will admit to")

// AskScopes asks one neighbour which scopes it carries and waits for
// the answer. Safe from any goroutine; the question is served by the
// pipeline's own, like every other emission.
func (e *engine) AskScopes(peer []byte) ([]string, error) {
	if !e.txEnabled() {
		return nil, errors.New("the transmit gate is dry — nothing may be asked")
	}
	if e.id == nil {
		return nil, errors.New("asking needs a node identity — the answer is sealed to it")
	}
	if len(peer) != meshcore.PubKeySize {
		return nil, fmt.Errorf("a scopes question needs the neighbour's whole %d-byte key",
			meshcore.PubKeySize)
	}
	secret, err := e.id.SharedSecret(peer)
	if err != nil {
		return nil, err
	}
	q := &scopeQuery{
		secret:  secret,
		tag:     uint32(time.Now().Unix()),
		answer:  make(chan []string, 1),
		asked:   time.Now(),
		started: newAck(),
	}
	copy(q.peer[:], peer)

	select {
	case e.scopeAsk <- q:
	default:
		return nil, errors.New("a scopes question is already in flight — they are asked one at a time")
	}
	e.wakeReceiver()
	// Whether the question left at all comes back first, and only
	// then does the window mean anything: ErrNoAnswer is the word for
	// a neighbour that stayed quiet, never for a question this node
	// declined to ask.
	if err := q.started.wait("scopes question"); err != nil {
		return nil, err
	}
	select {
	case names := <-q.answer:
		return names, nil
	case <-time.After(scopeQueryWait):
		return nil, ErrNoAnswer
	}
}

// drainScopeAsk sends a question the operator asked for, and retires
// one whose answer never came.
func (e *engine) drainScopeAsk(dev radio.Device, now time.Time) {
	if e.pendingScope != nil && now.Sub(e.pendingScope.asked) > scopeQueryWait {
		e.pendingScope = nil
	}
	select {
	case q := <-e.scopeAsk:
		if !q.started.claim() {
			return // the operator gave up; nothing goes out now
		}
		if e.pendingScope != nil {
			// The channel emptied when the first question was taken,
			// so a second one enqueues freely; it is refused here, and
			// told so rather than left to time out as if a neighbour
			// had gone quiet.
			q.started.refused(errors.New(
				"another scopes question is still in flight — they are asked one at a time"))
			return
		}
		pkt, err := e.scopeRequest(q)
		if err != nil {
			e.log.Warn("scopes question build failed", zap.Error(err))
			q.started.refused(fmt.Errorf("the question could not be composed: %w", err))
			return
		}
		// The slot is taken only once the question is really queued:
		// holding it for a dropped question blocks the next one for a
		// window nothing ever opened.
		id := txn.New()
		if !e.enqueue(dev, pkt, "scope-req", id, prioDirect, 0) {
			q.started.refused(errors.New(
				"the outbound queue is full — the question never left"))
			return
		}
		e.pendingScope = q
		// Same quiet-channel clause as the sweep: the slot must free
		// on time even if nothing is heard, or the next question hits
		// "already in flight" until luck turns the loop.
		time.AfterFunc(scopeQueryWait+time.Second, e.wakeReceiver)
		q.started.taken()
		e.log.Info("asking a neighbour for its scopes",
			zap.String("txn", id.Short()), zap.String("peer", shortKey(q.peer[:])))
	default:
	}
}

// scopeRequest composes the question: the anonymous envelope, sealed
// to the neighbour's key, carrying the scopes sub-type and a reply
// path of zero hops — we are adjacent by definition, since a scoped
// mesh is learned one neighbour at a time.
func (e *engine) scopeRequest(q *scopeQuery) (*meshcore.Packet, error) {
	plain, err := meshcore.FrameAnonRequest(q.tag, meshcore.AnonReqScopes, 0, nil)
	if err != nil {
		return nil, err
	}
	pkt, err := meshcore.BuildAnonDatagram(
		q.peer[:meshcore.PathHashSize], e.id.PubKey[:], q.secret, plain)
	if err != nil {
		return nil, err
	}
	pkt.Header = meshcore.MakeHeader(meshcore.RouteDirect,
		meshcore.PayloadTypeAnonReq, meshcore.PayloadVer1)
	return pkt, nil
}

// scopeAnswer consumes a RESPONSE that answers our own question.
// Anything else routes on untouched — including a response addressed
// to somebody whose hash prefix happens to match ours, which the MAC
// is what tells apart.
func (e *engine) scopeAnswer(rx *reception) (verdict, why string, handled bool) {
	q := e.pendingScope
	if q == nil || e.id == nil {
		return "", "", false
	}
	d, err := meshcore.ParseDatagram(rx.pkt.Payload)
	if err != nil || d.DestHash[0] != e.id.PubKey[0] || d.SrcHash[0] != q.peer[0] {
		return "", "", false
	}
	plain, err := d.Open(q.secret)
	if err != nil {
		return "", "", false
	}
	tag, body, err := meshcore.UnframeAdmin(plain)
	if err != nil || tag != q.tag {
		return "", "", false // an answer to a question we did not ask
	}
	e.pendingScope = nil
	reply, err := meshcore.ParseAnonReply(body)
	if err != nil {
		return "", "", false
	}
	names := meshcore.ScopeNames(reply.Text)
	select {
	case q.answer <- names:
	default:
	}
	return verdictScopeAnswer, fmt.Sprintf("carries %d scopes", len(names)), true
}
