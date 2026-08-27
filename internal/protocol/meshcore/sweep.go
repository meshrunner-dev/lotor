package meshcore

// Scanning the neighbourhood. The node asks who hears it, zero hop,
// and the repeaters in earshot answer with their key and the SNR they
// heard the question at. This is the same exchange this relay has
// answered since discovery landed, put the other way round.

import (
	"crypto/rand"
	// Four random bytes read as a number: the seam this package guards
	// is wire formats, and a tag nobody parses has no byte order to
	// get wrong.
	"encoding/binary" //nolint:depguard
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/txn"
)

// sweepWindow is how long answers are collected, the reference's own
// span. Answers do not arrive together: every responder spreads itself
// over a jitter four times a relay's, precisely so a broadcast
// question does not become a chorus, and the last of them lands late.
const sweepWindow = 60 * time.Second

// sweep is one scan in flight: the tag that marks its answers, when it
// closes, and where the answers go as they land.
//
// Unlike a scopes question, a scan is a broadcast — many answer, and
// that is the point. It is the responders that stagger themselves, so
// nothing here needs to.
type sweep struct {
	tag   uint32
	until time.Time
	found chan Neighbour
	seen  map[[meshcore.PubKeySize]byte]bool
	// started is how the pipeline answers the asker: closed once the
	// question is really on its way, or carrying the reason it is not.
	// Without it a scan the relay refused would look exactly like a
	// scan nobody answered — an empty room and a silent failure read
	// the same from the console, and only one of them is true.
	started chan error
}

// scanStartWait bounds how long an asker waits to hear whether its
// scan went out. The pipeline picks the question up on its next turn
// of the receive loop, so this is slack, not a duration anyone should
// notice.
const scanStartWait = 3 * time.Second

// Discover asks the neighbourhood who is there. The channel carries
// each answer as it lands and closes when the window ends; the
// deadline is returned so a caller can bound its own wait.
func (e *engine) Discover() (<-chan Neighbour, time.Time, error) {
	if !e.txEnabled() {
		return nil, time.Time{}, errors.New("the transmit gate is dry — nothing may be asked")
	}
	var raw [4]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return nil, time.Time{}, err
	}
	s := &sweep{
		tag:     binary.LittleEndian.Uint32(raw[:]),
		until:   time.Now().Add(sweepWindow),
		found:   make(chan Neighbour, maxNeighbours),
		seen:    map[[meshcore.PubKeySize]byte]bool{},
		started: make(chan error, 1),
	}
	select {
	case e.sweepAsk <- s:
	default:
		return nil, time.Time{}, errors.New("a scan is already listening")
	}
	e.wakeMu.Lock()
	if e.wakeRx != nil {
		e.wakeRx() // close the receive window: ask now
	}
	e.wakeMu.Unlock()

	// Wait to hear that it went out. The window returned is the one
	// the pipeline stamped when it did, not the one guessed here.
	select {
	case err := <-s.started:
		if err != nil {
			return nil, time.Time{}, err
		}
	case <-time.After(scanStartWait):
		return nil, time.Time{}, errors.New(
			"the relay never picked the scan up — see \"relay <name>\" for what it is doing")
	}
	return s.found, s.until, nil
}

// refuse tells the asker why its scan never went out, and closes the
// answers it will never receive.
func (s *sweep) refuse(err error) {
	s.started <- err
	close(s.found)
}

// drainSweepAsk sends a scan the operator asked for, and closes one
// whose window has run out.
func (e *engine) drainSweepAsk(dev radio.Device, now time.Time) {
	if e.pendingSweep != nil && now.After(e.pendingSweep.until) {
		close(e.pendingSweep.found)
		e.pendingSweep = nil
	}
	select {
	case s := <-e.sweepAsk:
		if e.pendingSweep != nil {
			// One window at a time: two scans at once make every
			// responder answer both, into the same jitter, and the
			// collisions cost more answers than the second scan buys.
			s.refuse(fmt.Errorf(
				"a scan is already listening, %s left of its window — "+
					"its answers join the neighbourhood either way",
				time.Until(e.pendingSweep.until).Round(time.Second)))
			return
		}
		pkt, err := meshcore.BuildDiscoverReq(meshcore.DiscoverReq{
			Filter: meshcore.RepeaterFilter(),
			Tag:    s.tag,
			// Since zero asks everyone, not only the nodes whose state
			// changed: an operator scanning wants the room, not the news.
			Since: 0,
			// The whole key, because a name we cannot seal a question
			// to is a name we cannot use.
			PrefixOnly: false,
		})
		if err != nil {
			s.refuse(fmt.Errorf("the scan could not be composed: %w", err))
			e.log.Warn("scan build failed", zap.Error(err))
			return
		}
		// The window starts when the question does, not when it was
		// typed: the wait above is short but it is not nothing.
		s.until = now.Add(sweepWindow)
		e.pendingSweep = s
		close(s.started)
		id := txn.New()
		e.log.Info("scanning the neighbourhood",
			zap.String("txn", id.Short()), zap.Duration("window", sweepWindow))
		e.enqueue(dev, pkt, "discover-req", id, prioDirect, 0)
	default:
	}
}

// sweepAnswer harvests a discovery answer that belongs to our own
// scan. The reference trusts one only on those terms — tag, window,
// and not our own key coming back at us — and anything else routes on.
func (e *engine) sweepAnswer(rx *reception) (verdict, why string, handled bool) {
	s := e.pendingSweep
	if s == nil || time.Now().After(s.until) {
		return "", "", false
	}
	resp, err := meshcore.ParseDiscoverResp(rx.pkt)
	if err != nil || resp.Tag != s.tag {
		return "", "", false
	}
	if resp.NodeType != meshcore.AdvTypeRepeater || len(resp.PubKey) != meshcore.PubKeySize {
		return verdictDiscoverAnswer, "an answer we cannot use", true
	}
	var key [meshcore.PubKeySize]byte
	copy(key[:], resp.PubKey)
	if e.id != nil && key == e.id.PubKey {
		return verdictDiscoverAnswer, "our own answer echoing back", true
	}
	if s.seen[key] {
		return verdictDiscoverAnswer, "already answered", true
	}
	s.seen[key] = true

	// The SNR we record is ours — how well WE hear THEM — while the
	// one they sent is how well they hear us. Both are worth knowing
	// and they are not the same number.
	n := Neighbour{PubKey: key, SNR: rx.frame.SNR, Heard: rx.frame.At}
	e.neighbours.put(key, rx.frame.SNR, rx.frame.At)
	select {
	case s.found <- n:
	default:
	}
	return verdictDiscoverAnswer, "a neighbour answering our scan", true
}
