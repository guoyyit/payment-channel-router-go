package settlement

import (
	"fmt"
	"sync"
	"testing"
)

// TestConcurrentNoOversell fires many payments in parallel over the same
// channels. Every accepted payment must settle atomically; total money
// conserved; no directional balance may go negative and no two payments may
// spend the same funds.
func TestConcurrentNoOversell(t *testing.T) {
	e := newEnv(t)
	e.mgr.opt.MaxConcurrent = 0 // unlimited; capacity is the real constraint
	const n = 60
	var wg sync.WaitGroup
	var succ, fail int64
	var mu sync.Mutex
	start := make(chan struct{})
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			id := fmt.Sprintf("p-%02d", i)
			err := e.mgr.Send(id, "A", "C", 10, fmt.Sprintf("pre-%d", i))
			mu.Lock()
			if err == nil {
				succ++
			} else {
				fail++
			}
			mu.Unlock()
		}(i)
	}
	close(start)
	wg.Wait()

	// Each successful payment moves 10 on each channel plus 10 fee to B.
	// Capacity constraints: ab A-side 1000 -> at most floor(1000/20)=50;
	// bc B-side 1000 -> at most floor(1000/10)=100; fee total adds to the
	// ab constraint, so success is bounded by 50.
	if succ+fail != n {
		t.Fatalf("counts = %d success + %d fail, want %d", succ, fail, n)
	}
	if succ != 50 {
		t.Fatalf("successful payments = %d, want exactly 50 (no oversell)", succ)
	}

	cab, _ := e.g.GetChannel("ab")
	cbc, _ := e.g.GetChannel("bc")
	if cab.BalanceAB < 0 || cab.BalanceBA < 0 || cab.BalanceAB+cab.BalanceBA != 1000 {
		t.Fatalf("ab not conserved: %+v", cab)
	}
	if cbc.BalanceAB < 0 || cbc.BalanceBA < 0 || cbc.BalanceAB+cbc.BalanceBA != 1000 {
		t.Fatalf("bc not conserved: %+v", cbc)
	}
	// Every successful payment: ab loses 20 (10 payment + 10 fee to B),
	// bc loses 10; and no holds remain.
	if cab.BalanceAB != 0 || cab.BalanceBA != 1000 {
		t.Fatalf("ab = %d/%d, want 0/1000", cab.BalanceAB, cab.BalanceBA)
	}
	if cbc.BalanceAB != 500 || cbc.BalanceBA != 500 {
		t.Fatalf("bc = %d/%d, want 500/500", cbc.BalanceAB, cbc.BalanceBA)
	}
	if e.g.HoldAmount("ab", "A", "B") != 0 || e.g.HoldAmount("bc", "B", "C") != 0 {
		t.Fatal("residual holds after concurrent run")
	}
	if e.mgr.Active() != 0 {
		t.Fatalf("active = %d, want 0", e.mgr.Active())
	}
}
