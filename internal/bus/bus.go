// Package bus is the daemon's spine: an in-process publish/subscribe
// channel for typed events with provenance. Everything that happens —
// a frame heard, a relay decision, a radio state change — is published
// here, and every consumer (journal, metrics, SSE, CLI, and later
// bridges or alerting) is a subscriber. Publishing never blocks: a
// subscriber that cannot keep up loses events and the loss is counted,
// because the radio path must never wait on a reader.
package bus

import (
	"sync"
	"sync/atomic"
)

// Event is any published value; concrete types live in events.go.
type Event any

// Bus fans events out to subscribers.
type Bus struct {
	mu   sync.RWMutex
	subs map[*Subscription]struct{}
}

// Subscription receives events on C until Close.
type Subscription struct {
	C       <-chan Event
	c       chan Event
	bus     *Bus
	dropped atomic.Uint64
}

// New returns an empty bus.
func New() *Bus {
	return &Bus{subs: make(map[*Subscription]struct{})}
}

// Subscribe registers a subscriber with the given channel capacity.
func (b *Bus) Subscribe(buffer int) *Subscription {
	c := make(chan Event, buffer)
	s := &Subscription{C: c, c: c, bus: b}
	b.mu.Lock()
	b.subs[s] = struct{}{}
	b.mu.Unlock()
	return s
}

// Publish delivers the event to every subscriber that has room, and
// counts a drop for each one that does not.
func (b *Bus) Publish(ev Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for s := range b.subs {
		select {
		case s.c <- ev:
		default:
			s.dropped.Add(1)
		}
	}
}

// Dropped reports how many events this subscriber has lost.
func (s *Subscription) Dropped() uint64 { return s.dropped.Load() }

// Close unregisters the subscriber and closes its channel.
func (s *Subscription) Close() {
	s.bus.mu.Lock()
	if _, ok := s.bus.subs[s]; ok {
		delete(s.bus.subs, s)
		close(s.c)
	}
	s.bus.mu.Unlock()
}
