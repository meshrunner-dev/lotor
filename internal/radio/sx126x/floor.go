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
type floorTracker struct {
	samples []float64
	began   time.Time // the current batch's first sample
	rest    time.Time // no new batch before this instant
	floor   atomic.Value
	starved atomic.Uint64
}

// sample feeds one idle RSSI reading; the caller has already
// established that nothing is arriving and the radio is receiving. It
// reports whether this sample completed a batch — a fresh floor.
func (t *floorTracker) sample(rssi float64, now time.Time) (converged bool) {
	if len(t.samples) == 0 && now.Before(t.rest) {
		return false
	}
	if nf, ok := t.value(); ok && rssi >= nf.DBm+floorGateDB {
		return false
	}
	if len(t.samples) > 0 && now.Sub(t.began) > floorBatchMaxAge {
		t.samples = t.samples[:0]
		t.starved.Add(1)
	}
	if len(t.samples) == 0 {
		t.began = now
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

// percentile reads the p-th percentile off sorted samples, by the
// nearest-rank method.
func percentile(sorted []float64, p float64) float64 {
	i := int(math.Ceil(p*float64(len(sorted)))) - 1
	return sorted[max(0, min(i, len(sorted)-1))]
}
