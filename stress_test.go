package pchr

import (
	"sync"
	"testing"
)

// Diamond network: A -> {B,C} -> D plus a direct channel, with random-ish
// sender/receiver pairs hammered concurrently. Total channel funds per
// channel pair must be conserved and locks must always return to zero.
func TestConcurrentStressConservation(t *testing.T) {
	net := NewNetwork(nil)
	registerNodes(t, net, "A", "B", "C", "D")
	chans := []Channel{
		{ID: "ab", PeerA: "A", PeerB: "B", BalanceA: 5000, BalanceB: 5000, FeePolicy: FeePolicy{BaseFee: 1}},
		{ID: "ac", PeerA: "A", PeerB: "C", BalanceA: 5000, BalanceB: 5000, FeePolicy: FeePolicy{BaseFee: 1}},
		{ID: "bd", PeerA: "B", PeerB: "D", BalanceA: 5000, BalanceB: 5000, FeePolicy: FeePolicy{BaseFee: 1}},
		{ID: "cd", PeerA: "C", PeerB: "D", BalanceA: 5000, BalanceB: 5000, FeePolicy: FeePolicy{BaseFee: 1}},
		{ID: "bc", PeerA: "B", PeerB: "C", BalanceA: 5000, BalanceB: 5000, FeePolicy: FeePolicy{BaseFee: 1}},
	}
	for _, c := range chans {
		if err := net.OpenChannel(c); err != nil {
			t.Fatal(err)
		}
	}
	cfg := DefaultRoutingConfig()
	cfg.MaxConcurrent = 0 // unlimited
	mgr := NewManager(net, nil, nil, cfg)

	pairs := []struct{ s, r NodeID }{
		{"A", "D"}, {"A", "B"}, {"A", "C"},
		{"B", "D"}, {"C", "D"}, {"B", "C"},
	}
	const workers = 16
	const each = 40
	var wg sync.WaitGroup
	var ok, fail int64
	var mu sync.Mutex
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for k := 0; k < each; k++ {
				pr := pairs[(w*7+k)%len(pairs)]
				amt := int64(1 + (w*13+k*7)%200)
				_, err := mgr.SendPayment(pr.s, pr.r, amt, newPreimage())
				mu.Lock()
				if err == nil {
					ok++
				} else {
					fail++
				}
				mu.Unlock()
			}
		}(w)
	}
	wg.Wait()
	t.Logf("succeeded=%d failed=%d", ok, fail)

	if mgr.InFlight() != 0 {
		t.Fatalf("in flight = %d", mgr.InFlight())
	}
	for _, c := range chans {
		ch, err := net.GetChannel(c.ID)
		if err != nil {
			t.Fatal(err)
		}
		if ch.LockedA != 0 || ch.LockedB != 0 {
			t.Fatalf("channel %s retains locks: %+v", c.ID, ch)
		}
		// Within a channel every settle moves the full HTLC amount from one
		// side to the other, so the sum of the two balances is invariant for
		// that channel; fees emerge across an intermediary's two channels.
		if ch.BalanceA < 0 || ch.BalanceB < 0 {
			t.Fatalf("negative balance on %s: %+v", c.ID, ch)
		}
		if ch.BalanceA+ch.BalanceB != 10000 {
			t.Fatalf("funds not conserved on %s: %d+%d", c.ID, ch.BalanceA, ch.BalanceB)
		}
	}
}
