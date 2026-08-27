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
	"time"
)

// askWait bounds how long a caller waits to hear whether its order was
// taken. The pipeline answers on its next turn of the receive loop, so
// this is slack for a busy relay, not a duration anyone should notice.
const askWait = 3 * time.Second

// ack is how the pipeline answers one order.
type ack chan error

func newAck() ack { return make(ack, 1) }

// taken says the order went through. Closing rather than sending means
// a caller that has already given up costs nothing.
func (a ack) taken() { close(a) }

// refused says why it did not.
func (a ack) refused(err error) { a <- err }

// wait blocks until the pipeline answers, or gives up on a relay that
// is not turning. what names the order, so the timeout reads as
// something an operator can act on.
func (a ack) wait(what string) error {
	select {
	case err := <-a:
		return err
	case <-time.After(askWait):
		return errors.New("the relay never picked the " + what +
			" up — see \"relay <name>\" for what it is doing")
	}
}
