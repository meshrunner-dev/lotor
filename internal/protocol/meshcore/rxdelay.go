package meshcore

// The score-staggered hold on flood reception — rx_delay_base. A
// flood heard by several repeaters at once is about to be relayed by
// all of them; holding each copy for a delay that shrinks as the
// reception improves lets the best-placed repeater speak first, and
// by the time the others' holds expire, its relay has already been
// witnessed and their copies judge as duplicates. The reference ships
// the knob at 0 — off — and so does this engine; everything below
// only runs for a site that turned it on.

import (
	"math"
	"time"

	"meshrunner.dev/pkg/meshcore"

	"meshrunner.dev/lotor/internal/radio"
	"meshrunner.dev/lotor/internal/txn"
)

// snrThresholds is the least SNR at which each spreading factor —
// 7 through 12 — still demodulates, the reference's table from the
// Semtech datasheets. Below the threshold the score is zero: we
// barely heard it ourselves, so nobody is waiting on us to hold back.
var snrThresholds = [6]float64{-7.5, -10, -12.5, -15, -17.5, -20}

// packetScore says how comfortably a frame was received, 0..1: SNR
// margin over the spreading factor's threshold, discounted by the
// frame's length — a long frame is a long window for a collision.
// The reference's packetScoreInt, which real radios feed the actual
// spreading factor.
func packetScore(snr float64, sf, packetLen int) float64 {
	if sf < 7 || sf > 12 {
		return 0
	}
	threshold := snrThresholds[sf-7]
	if snr < threshold {
		return 0
	}
	successRate := (snr - threshold) / 10
	collisionPenalty := 1 - float64(packetLen)/256
	return max(0, min(1, successRate*collisionPenalty))
}

// The reference dispatcher's own figures around the curve: a hold
// shorter than the floor is not worth queueing, and no score holds a
// frame past the cap.
const (
	rxDelayFloor = 50 * time.Millisecond
	rxDelayCap   = 32 * time.Second
	// rxScorePivot is where the curve crosses zero: a score this good
	// waits for nobody.
	rxScorePivot = 0.85
)

// rxDelay is how long one flood reception is held before judgement;
// zero judges it now.
func (e *engine) rxDelay(frame radio.Frame) time.Duration {
	base := e.p.RxDelayBase
	if base <= 0 {
		return 0
	}
	score := packetScore(frame.SNR, e.p.SpreadingFactor, len(frame.Payload))
	d := time.Duration((math.Pow(base, rxScorePivot-score) - 1) * float64(frame.Airtime))
	if d < rxDelayFloor {
		return 0
	}
	return min(d, rxDelayCap)
}

// heldRx is one flood waiting out its score delay, parsed already —
// a frame that cannot be read is not worth a place in the queue.
type heldRx struct {
	pkt   *meshcore.Packet
	frame radio.Frame
	id    txn.ID
	due   time.Time
}

// drainHeld judges every held flood whose delay expired. Engine
// goroutine only, like the queue it drains.
func (e *engine) drainHeld(dev radio.Device, now time.Time) {
	kept := e.held[:0]
	for _, h := range e.held {
		if now.Before(h.due) {
			kept = append(kept, h)
			continue
		}
		e.process(dev, h.pkt, h.frame, h.id)
	}
	e.held = kept
}

// heldWait is how long until the earliest held flood is due; ok is
// false while nothing is held.
func (e *engine) heldWait(now time.Time) (time.Duration, bool) {
	var soonest time.Time
	for _, h := range e.held {
		if soonest.IsZero() || h.due.Before(soonest) {
			soonest = h.due
		}
	}
	if soonest.IsZero() {
		return 0, false
	}
	return max(0, soonest.Sub(now)), true
}
