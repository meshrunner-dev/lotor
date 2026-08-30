package radio

import (
	"testing"
	"time"
)

func TestAirtimeLedgerRestoresOrdersAndSharesWindow(t *testing.T) {
	now := time.Now()
	ledger := NewAirtimeLedger(10*time.Second, []AirtimeStamp{
		{At: now.Add(-10 * time.Minute), Airtime: 3 * time.Second},
		{At: now.Add(-2 * time.Hour), Airtime: 8 * time.Second},
		{At: now.Add(-20 * time.Minute), Airtime: 4 * time.Second},
	})
	if got := ledger.Usage(now); got != 7*time.Second {
		t.Fatalf("restored usage = %s", got)
	}
	if ok, freeAt, never := ledger.Admit(now, 4*time.Second); ok || never ||
		!freeAt.Equal(now.Add(40*time.Minute)) {
		t.Fatalf("admit = %v, %s, %v", ok, freeAt, never)
	}
	ledger.Record(now, 2*time.Second)
	if got := ledger.Usage(now); got != 9*time.Second {
		t.Fatalf("shared usage = %s", got)
	}
	if err := ledger.RequireBudget(10 * time.Second); err != nil {
		t.Fatal(err)
	}
	if err := ledger.RequireBudget(time.Second); err == nil {
		t.Fatal("different consumers supplied different budgets")
	}
}

func TestAirtimeLedgerKeepsConcurrentRecordsOrdered(t *testing.T) {
	now := time.Now()
	ledger := NewAirtimeLedger(10*time.Second, nil)
	ledger.Record(now.Add(-10*time.Minute), 7*time.Second)
	ledger.Record(now.Add(-50*time.Minute), 2*time.Second)

	if ok, freeAt, never := ledger.Admit(now, 2*time.Second); ok || never ||
		!freeAt.Equal(now.Add(10*time.Minute)) {
		t.Fatalf("admit = %v, %s, %v", ok, freeAt, never)
	}
}

func TestAirtimeLedgerReservationsCloseTheAdmissionRace(t *testing.T) {
	now := time.Now()
	ledger := NewAirtimeLedger(10*time.Second, nil)
	first, _, never := ledger.Reserve(now, 7*time.Second)
	if first == nil || never {
		t.Fatal("first candidate was not reserved")
	}
	if second, _, never := ledger.Reserve(now, 4*time.Second); second != nil || never {
		t.Fatalf("overlapping candidate = %#v, never = %v", second, never)
	}
	if got := ledger.Usage(now); got != 0 {
		t.Fatalf("an estimate appeared as spent airtime: %s", got)
	}
	first.Cancel()
	second, _, never := ledger.Reserve(now, 4*time.Second)
	if second == nil || never {
		t.Fatal("cancelled capacity was not released")
	}
	second.Commit(now, 5*time.Second)
	if got := ledger.Usage(now); got != 5*time.Second {
		t.Fatalf("committed usage = %s", got)
	}
	second.Commit(now, time.Second)
	if got := ledger.Usage(now); got != 5*time.Second {
		t.Fatalf("second commit changed usage to %s", got)
	}
}
