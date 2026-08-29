package meshcore

// Orders from outside the pipeline.
//
// Almost everything this engine knows — the duplicate table, the
// sessions, the advert clocks, the scan and question in flight — is
// owned by one goroutine and needs no lock at all, because nothing
// else writes it. What comes from elsewhere is not state but orders:
// a console asking for a scan, a web button asking for an advert,
// later a question arriving over the air. Those arrive on an ask
// channel, and the pipeline serves them on its own turn.
//
// The half that is easy to forget is the answer. An order that can be
// refused must be able to say so, or the caller reads a refusal as an
// empty result. That is what an ack is for: the pipeline closes it
// when the order went through, or sends the reason it did not.
//
// The pattern is what keeps the mutex count flat. A new caller is a
// new *caller*, not new shared state, so it costs nothing here.

import (
	"errors"
	"sync/atomic"
	"time"
)

// askWait bounds how long a caller waits to hear whether its order was
// taken. The pipeline answers on its next turn of the receive loop, so
// this is slack for a busy relay, not a duration anyone should notice.
const askWait = 3 * time.Second

// The three states an order passes through. Exactly one of the two
// terminal ones is ever reached, and whoever reaches it owns the
// outcome.
const (
	askPending   int32 = iota
	askClaimed         // the pipeline holds it and will answer
	askAbandoned       // the caller gave up; the order must not act
)

// ack is how the pipeline answers one order — and the arbiter of who
// gets to decide its fate.
//
// The caller's deadline and the pipeline's turn race, and without an
// arbitration between them a timed-out order simply stayed in its
// channel and ran later. An advert ordered while the radio sat in its
// retry backoff was refused to the operator's face and then emitted
// when the session came back; a permission change could land after its
// author had been told it had not. A caller that has walked away must
// leave nothing behind that can still happen.
type ack struct {
	done  chan error
	state atomic.Int32
}

func newAck() *ack { return &ack{done: make(chan error, 1)} }

// claim is the pipeline's first move on any order: it wins the right
// to act, and with it the duty to answer. False means the caller
// already gave up, and the order must have no effect whatever — it is
// not refused, it never happened.
func (a *ack) claim() bool {
	return a.state.CompareAndSwap(askPending, askClaimed)
}

// taken says the order went through. Closing rather than sending means
// a caller that has already given up costs nothing.
func (a *ack) taken() { close(a.done) }

// refused says why it did not.
func (a *ack) refused(err error) { a.done <- err }

// wait blocks until the pipeline answers, or gives up on a relay that
// is not turning. what names the order, so the timeout reads as
// something an operator can act on.
//
// Giving up is a transition the pipeline must lose: when it has
// already claimed the order, this waits for the real answer instead
// of reporting a failure the order is about to contradict.
func (a *ack) wait(what string) error {
	select {
	case err := <-a.done:
		return err
	case <-time.After(askWait):
	}
	if a.state.CompareAndSwap(askPending, askAbandoned) {
		return errors.New("the relay never picked the " + what +
			" up — see \"relay <name>\" for what it is doing")
	}
	return <-a.done
}
