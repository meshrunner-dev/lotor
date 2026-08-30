package radio

import (
	"fmt"
	"slices"
	"sync"
	"time"
)

// AirtimeStamp is one past real or shadow emission that still occupies a
// shared radio's regulatory window.
type AirtimeStamp struct {
	At      time.Time
	Airtime time.Duration
}

// AirtimeLedger accounts for every producer sharing one physical radio. A
// shadow emission consumes this ledger deliberately: shadow models the exact
// capacity an on-air decision would have spent, while telemetry keeps the two
// kinds distinct.
type AirtimeLedger struct {
	mu           sync.Mutex
	budget       time.Duration
	window       []AirtimeStamp
	reservations map[*AirtimeReservation]time.Duration
}

// AirtimeReservation holds room in one physical radio's duty window while a
// producer assesses the channel and keys the radio. Commit turns the estimate
// into measured airtime; Cancel releases it when nothing reached the air.
// Both operations are idempotent.
type AirtimeReservation struct {
	ledger *AirtimeLedger
	active bool
}

// NewAirtimeLedger creates one sliding-hour account and restores its recent
// history. Stamps are sorted because pruning relies on time order.
func NewAirtimeLedger(budget time.Duration, spent []AirtimeStamp) *AirtimeLedger {
	ledger := &AirtimeLedger{
		budget: budget, window: slices.Clone(spent),
		reservations: make(map[*AirtimeReservation]time.Duration),
	}
	slices.SortFunc(ledger.window, func(a, b AirtimeStamp) int { return a.At.Compare(b.At) })
	ledger.prune(time.Now())
	return ledger
}

// Budget is the maximum airtime in one sliding hour. Zero means unbudgeted.
func (l *AirtimeLedger) Budget() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.budget
}

// RequireBudget refuses consumers that describe the same radio with a
// different regulatory ceiling. Picking either value silently would make one
// configuration lie; sharing is valid only when the budget agreement matches.
func (l *AirtimeLedger) RequireBudget(budget time.Duration) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.budget != budget {
		return fmt.Errorf("shared radio duty budget differs: existing %s, requested %s",
			l.budget, budget)
	}
	return nil
}

// SetBudget changes the ceiling without laundering the existing window. The
// owner must first prove every consumer of the radio agrees on the new value.
func (l *AirtimeLedger) SetBudget(budget time.Duration) {
	l.mu.Lock()
	l.budget = budget
	l.mu.Unlock()
}

// prune drops stamps older than the window; the caller holds mu.
func (l *AirtimeLedger) prune(now time.Time) time.Duration {
	cut := now.Add(-time.Hour)
	i := 0
	for i < len(l.window) && l.window[i].At.Before(cut) {
		i++
	}
	l.window = l.window[i:]
	var sum time.Duration
	for _, stamp := range l.window {
		sum += stamp.Airtime
	}
	return sum
}

// Usage is the sliding hour's spent airtime as of now, pruned first.
func (l *AirtimeLedger) Usage(now time.Time) time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.prune(now)
}

func (l *AirtimeLedger) occupiedLocked(now time.Time) time.Duration {
	used := l.prune(now)
	for _, airtime := range l.reservations {
		used += airtime
	}
	return used
}

// Admit reports whether airtime fits now. When it does not, freeAt is the
// earliest expiry that frees enough budget; never is true when one emission
// can never fit this ledger.
func (l *AirtimeLedger) Admit(now time.Time, airtime time.Duration) (ok bool, freeAt time.Time, never bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.admitLocked(now, airtime)
}

func (l *AirtimeLedger) admitLocked(now time.Time, airtime time.Duration) (ok bool, freeAt time.Time, never bool) {
	if l.budget <= 0 {
		return true, time.Time{}, false
	}
	if airtime > l.budget {
		return false, time.Time{}, true
	}
	used := l.occupiedLocked(now)
	if used+airtime <= l.budget {
		return true, time.Time{}, false
	}
	need := used + airtime - l.budget
	var freed time.Duration
	for _, stamp := range l.window {
		freed += stamp.Airtime
		if freed >= need {
			return false, stamp.At.Add(time.Hour), false
		}
	}
	// Outstanding reservations have no regulatory expiry: they resolve when
	// their in-flight radio attempt commits or cancels. Poll promptly instead
	// of pretending they occupy a full hour.
	return false, now.Add(100 * time.Millisecond), false
}

// Reserve atomically admits and holds one candidate emission. Callers must
// Commit or Cancel every non-nil reservation.
func (l *AirtimeLedger) Reserve(now time.Time, airtime time.Duration) (
	reservation *AirtimeReservation, freeAt time.Time, never bool,
) {
	l.mu.Lock()
	defer l.mu.Unlock()
	ok, freeAt, never := l.admitLocked(now, airtime)
	if !ok {
		return nil, freeAt, never
	}
	reservation = &AirtimeReservation{ledger: l, active: true}
	if l.budget > 0 {
		l.reservations[reservation] = airtime
	}
	return reservation, time.Time{}, false
}

// Cancel releases an admitted candidate when it did not reach the air.
func (r *AirtimeReservation) Cancel() {
	if r == nil || r.ledger == nil {
		return
	}
	l := r.ledger
	l.mu.Lock()
	defer l.mu.Unlock()
	if !r.active {
		return
	}
	delete(l.reservations, r)
	r.active = false
}

// Commit replaces the estimate with the actual real or shadow emission.
func (r *AirtimeReservation) Commit(at time.Time, airtime time.Duration) {
	if r == nil || r.ledger == nil {
		return
	}
	l := r.ledger
	l.mu.Lock()
	defer l.mu.Unlock()
	if !r.active {
		return
	}
	delete(l.reservations, r)
	r.active = false
	l.insertLocked(at, airtime)
	l.prune(at)
}

// Record spends airtime from the shared window.
func (l *AirtimeLedger) Record(at time.Time, airtime time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.insertLocked(at, airtime)
	l.prune(at)
}

func (l *AirtimeLedger) insertLocked(at time.Time, airtime time.Duration) {
	stamp := AirtimeStamp{At: at, Airtime: airtime}
	i := len(l.window)
	if i > 0 && at.Before(l.window[i-1].At) {
		i = slices.IndexFunc(l.window, func(existing AirtimeStamp) bool {
			return !existing.At.Before(at)
		})
	}
	l.window = slices.Insert(l.window, i, stamp)
}
