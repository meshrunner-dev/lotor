package sx126x

import (
	"math"
	"slices"
	"sync/atomic"
	"time"

	"meshrunner.dev/lotor/internal/radio"
)

// Noise-floor policy: batches of idle RSSI samples reduced to their
// median — the robust estimator ambient-noise practice uses, which an
// impulsive burst cannot drag the way it drags a mean — acceptance
// gated near the known floor so a transmission the preamble detector
// missed cannot contaminate the batch, a low clamp, and a pause
// between batches. The spread between the batch's 90th percentile and
// its median rides along as the site's impulsiveness.
const (
	// floorSamples per batch; the first batch is the calibration.
	floorSamples = 64
	// floorGateDB rejects samples this far above the known floor.
	floorGateDB = 14
	// floorClampDBm bounds the floor below; a quieter reading is the
	// frontend mismeasuring, not a quieter site.
	floorClampDBm = -120
	// floorRestEvery separates batch starts: the floor refreshes at
	// this cadence when the channel is idle, slower when it is busy.
	floorRestEvery = 2 * time.Second
	// floorBatchMaxAge bounds one batch's span — a freshness filter,
	// not a survival deadline: an abandoned batch restarts at once and
	// retries its luck, so only batches this coherent ever publish. A
	// batch left to stretch would median across epochs and describe a
	// moment that never existed. The bound covers the p99 collection
	// time at 70% channel occupancy — own transmit at a saturated
	// duty cycle plus dense traffic; beyond that the floor refreshes
	// intermittently and the starved counter names the condition.
	floorBatchMaxAge = 10 * time.Second
)

// floorTracker turns accepted idle samples into a published floor. Its
// accumulation belongs to the radio's owning goroutine; the published
// value and the starvation counter may be read from anywhere.
//
// What starves is the measurement ATTEMPT, not the batch. An attempt
// opens at the first collection opportunity after the rest — whether
// that opportunity yields a sample, is rejected by the carrier gate,
// or is denied outright by a reception in progress — and every
// attempt window that expires without a published floor counts. A
// model that only aged batches-with-samples was blind to the very
// conditions the counter exists to report: a carrier already strong
// when the window opened never started a batch, and continuous LoRa
// reception never even reached the tracker.
type floorTracker struct {
	samples []float64
	attempt time.Time // when this measurement attempt opened; zero = none
	rest    time.Time // no new attempt before this instant
	floor   atomic.Value
	starved atomic.Uint64
}

// tick advances the attempt for one collection opportunity, taken or
// missed: it opens the attempt when none is running, and expires it —
// counted, samples dropped, a fresh attempt opened at once — when it
// has run a whole window without converging.
func (t *floorTracker) tick(now time.Time) {
	if t.attempt.IsZero() {
		if now.Before(t.rest) {
			return // resting: no opportunity is being missed
		}
		t.attempt = now
		return
	}
	if now.Sub(t.attempt) > floorBatchMaxAge {
		t.starved.Add(1)
		t.samples = t.samples[:0]
		t.attempt = now
	}
}

// denied records a collection opportunity the channel took away — a
// reception in progress, a radio busy transmitting. The sample never
// existed, but the attempt did, and starvation is precisely the story
// of attempts that got nothing.
func (t *floorTracker) denied(now time.Time) { t.tick(now) }

// sample feeds one idle RSSI reading; the caller has already
// established that nothing is arriving and the radio is receiving. It
// reports whether this sample completed a batch — a fresh floor.
func (t *floorTracker) sample(rssi float64, now time.Time) (converged bool) {
	t.tick(now)
	if t.attempt.IsZero() {
		return false // resting
	}
	if nf, ok := t.value(); ok && rssi >= nf.DBm+floorGateDB {
		return false
	}
	t.samples = append(t.samples, rssi)
	if len(t.samples) < floorSamples {
		return false
	}
	slices.Sort(t.samples)
	median := percentile(t.samples, 0.5)
	t.floor.Store(radio.NoiseFloor{
		DBm:      max(median, floorClampDBm),
		SpreadDB: percentile(t.samples, 0.9) - median,
		At:       now,
	})
	t.samples = t.samples[:0]
	t.attempt = time.Time{}
	t.rest = now.Add(floorRestEvery)
	return true
}

// value reports the last published floor; ok is false until the first
// batch converges.
func (t *floorTracker) value() (radio.NoiseFloor, bool) {
	nf, ok := t.floor.Load().(radio.NoiseFloor)
	return nf, ok
}

// starvedCount reports how many batches were abandoned, cumulatively.
func (t *floorTracker) starvedCount() uint64 { return t.starved.Load() }

// collecting reports whether the tracker wants opportunities now: an
// attempt in progress, or the rest between attempts elapsed. The
// receive loop's two phases — sampling ticks versus pure edge sleep —
// follow this.
func (t *floorTracker) collecting(now time.Time) bool {
	return !t.attempt.IsZero() || !now.Before(t.rest)
}

// restLeft is how long until sampling resumes.
func (t *floorTracker) restLeft(now time.Time) time.Duration {
	return t.rest.Sub(now)
}

// percentile reads the p-th percentile off sorted samples, by the
// nearest-rank method.
func percentile(sorted []float64, p float64) float64 {
	i := int(math.Ceil(p*float64(len(sorted)))) - 1
	return sorted[max(0, min(i, len(sorted)-1))]
}
