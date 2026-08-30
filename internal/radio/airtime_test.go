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
