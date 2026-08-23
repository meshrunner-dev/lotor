package sx126x

import (
	"sync/atomic"
	"time"

	"meshrunner.dev/lotor/internal/radio"
)

// Noise-floor policy, mirroring the reference implementation: batches
// of idle RSSI samples averaged into a floor, acceptance gated near
// the known floor so a transmission the preamble detector missed
// cannot drag the floor up, a low clamp, and a pause between batches.
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
)

// floorTracker turns accepted idle samples into a published floor. Its
// accumulation belongs to the radio's owning goroutine; the published
// value may be read from anywhere.
type floorTracker struct {
	sum   float64
	n     int
	rest  time.Time // no new batch before this instant
	floor atomic.Value
}

// sample feeds one idle RSSI reading; the caller has already
// established that nothing is arriving and the radio is receiving. It
// reports whether this sample completed a batch — a fresh floor.
func (t *floorTracker) sample(rssi float64, now time.Time) (converged bool) {
	if t.n == 0 && now.Before(t.rest) {
		return false
	}
	if nf, ok := t.value(); ok && rssi >= nf.DBm+floorGateDB {
		return false
	}
	t.sum += rssi
	t.n++
	if t.n < floorSamples {
		return false
	}
	t.floor.Store(radio.NoiseFloor{
		DBm: max(t.sum/floorSamples, floorClampDBm), At: now,
	})
	t.sum, t.n = 0, 0
	t.rest = now.Add(floorRestEvery)
	return true
}

// value reports the last published floor; ok is false until the first
// batch converges.
func (t *floorTracker) value() (radio.NoiseFloor, bool) {
	nf, ok := t.floor.Load().(radio.NoiseFloor)
	return nf, ok
}
