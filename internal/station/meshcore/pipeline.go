package meshcore

// The origination pipeline is shared with every producer that is not a
// relay — meshrunner.dev/lotor/internal/origin. This package keeps its
// own spellings for the pipeline's names so the companion code reads
// as it always did.

import (
	"meshrunner.dev/lotor/internal/origin"
	"meshrunner.dev/lotor/internal/station"
)

type (
	emission      = origin.Emission
	emissionQueue = origin.Queue
)

// stationLBTBound is how long a frame may wait for the channel before
// the exhausted policy applies — the pipeline's default, the
// reference companion's four seconds.
const stationLBTBound = origin.DefaultLBTBound

// originPolicy is this station's gate in the pipeline's terms.
func originPolicy(p station.TXPolicy) origin.Policy {
	return origin.Policy{Mode: p.Mode, LBTThresholdDB: p.LBTThresholdDB, LBTExhausted: p.LBTExhausted, CAD: p.CAD}
}
