package meshcore

import (
	"context"
	"sync"
	"time"
)

type queuedEmission struct {
	item     emission
	sequence uint64
}

// emissionQueue is the reference companion's bounded dispatcher queue:
// eligible packets with the smallest numeric priority leave first, while
// equal priorities preserve submission order. A future packet never blocks a
// later packet that is already due.
type emissionQueue struct {
	mu       sync.Mutex
	items    []queuedEmission
	capacity int
	next     uint64
	changed  chan struct{}
}

func newEmissionQueue(capacity int) *emissionQueue {
	return &emissionQueue{capacity: capacity, changed: make(chan struct{})}
}

func (q *emissionQueue) offer(item emission) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) >= q.capacity {
		return false
	}
	q.next++
	q.items = append(q.items, queuedEmission{item: item, sequence: q.next})
	q.notifyLocked()
	return true
}

func (q *emissionQueue) takeUntil(ctx context.Context, deadline time.Time) (emission, bool) {
	for {
		now := time.Now()
		q.mu.Lock()
		best, wakeAt := q.bestLocked(now)
		if best >= 0 {
			item := q.items[best].item
			q.items = append(q.items[:best], q.items[best+1:]...)
			q.mu.Unlock()
			return item, true
		}
		changed := q.changed
		q.mu.Unlock()

		if wakeAt.IsZero() || deadline.Before(wakeAt) {
			wakeAt = deadline
		}
		timer := time.NewTimer(max(time.Duration(0), time.Until(wakeAt)))
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return emission{}, false
		case <-changed:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
			if !deadline.After(time.Now()) {
				return emission{}, false
			}
		}
	}
}

func (q *emissionQueue) bestLocked(now time.Time) (int, time.Time) {
	best := -1
	var wakeAt time.Time
	for i := range q.items {
		candidate := q.items[i]
		if candidate.item.notBefore.After(now) {
			if wakeAt.IsZero() || candidate.item.notBefore.Before(wakeAt) {
				wakeAt = candidate.item.notBefore
			}
			continue
		}
		if best < 0 || candidate.item.priority < q.items[best].item.priority ||
			(candidate.item.priority == q.items[best].item.priority &&
				candidate.sequence < q.items[best].sequence) {
			best = i
		}
	}
	return best, wakeAt
}

func (q *emissionQueue) drain() []emission {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]emission, len(q.items))
	for i := range q.items {
		out[i] = q.items[i].item
	}
	q.items = nil
	q.notifyLocked()
	return out
}

func (q *emissionQueue) len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

func (q *emissionQueue) notifyLocked() {
	close(q.changed)
	q.changed = make(chan struct{})
}
