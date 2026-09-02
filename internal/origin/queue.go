package origin

import (
	"context"
	"sync"
	"time"
)

type queued struct {
	item     Emission
	sequence uint64
}

// Queue is the reference companion's bounded dispatcher queue: eligible
// frames with the smallest numeric priority leave first, while equal
// priorities preserve submission order. A future frame never blocks a
// later frame that is already due.
type Queue struct {
	mu       sync.Mutex
	items    []queued
	capacity int
	next     uint64
	changed  chan struct{}
}

// NewQueue makes a queue that refuses the newcomer past capacity.
func NewQueue(capacity int) *Queue {
	return &Queue{capacity: capacity, changed: make(chan struct{})}
}

// Offer adds an emission; false when the queue is full, and nothing is
// evicted to make room.
func (q *Queue) Offer(item Emission) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) >= q.capacity {
		return false
	}
	q.next++
	q.items = append(q.items, queued{item: item, sequence: q.next})
	q.notifyLocked()
	return true
}

// TakeUntil yields the best due emission, waiting for one to become
// due until the deadline or ctx ends; false when neither happened.
func (q *Queue) TakeUntil(ctx context.Context, deadline time.Time) (Emission, bool) {
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
			return Emission{}, false
		case <-changed:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
			if !deadline.After(time.Now()) {
				return Emission{}, false
			}
		}
	}
}

func (q *Queue) bestLocked(now time.Time) (int, time.Time) {
	best := -1
	var wakeAt time.Time
	for i := range q.items {
		candidate := q.items[i]
		if candidate.item.NotBefore.After(now) {
			if wakeAt.IsZero() || candidate.item.NotBefore.Before(wakeAt) {
				wakeAt = candidate.item.NotBefore
			}
			continue
		}
		if best < 0 || candidate.item.Priority < q.items[best].item.Priority ||
			(candidate.item.Priority == q.items[best].item.Priority &&
				candidate.sequence < q.items[best].sequence) {
			best = i
		}
	}
	return best, wakeAt
}

// Drain empties the queue and returns what it held, for the owner to
// account as dropped.
func (q *Queue) Drain() []Emission {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]Emission, len(q.items))
	for i := range q.items {
		out[i] = q.items[i].item
	}
	q.items = nil
	q.notifyLocked()
	return out
}

// Len reports the backlog.
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

func (q *Queue) notifyLocked() {
	close(q.changed)
	q.changed = make(chan struct{})
}
