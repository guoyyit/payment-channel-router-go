package pchr

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestMultiHopSuccessBalances(t *testing.T) {
	net, mgr := buildLinearNet(t, nil)
	p, err := mgr.SendPayment("A", "D", 100, newPreimage())
	if err != nil {
		t.Fatal(err)
	}
	if p.Status != PaymentSucceeded || p.SettledHops != 3 {
		t.Fatalf("payment %+v", p)
	}
	// Route carries 103 (ab), 102 (bc), 100 (cd). Each channel moves its
	// full HTLC amount; fees emerge as intermediary net positions.
	ab, _ := net.GetChannel("ab")
	if ab.BalanceA != 897 || ab.BalanceB != 1103 || ab.LockedA != 0 || ab.LockedB != 0 {
		t.Fatalf("ab %+v", ab)
	}
	bc, _ := net.GetChannel("bc")
	// B receives 103 on ab and forwards 102 on bc: B nets the 1-fee.
	if bc.BalanceA != 898 || bc.BalanceB != 1102 || bc.LockedA != 0 {
		t.Fatalf("bc %+v", bc)
	}
	cd, _ := net.GetChannel("cd")
	// C receives 102 on bc and forwards 100 on cd: C nets the 2-fee.
	if cd.BalanceA != 900 || cd.BalanceB != 1100 || cd.LockedA != 0 {
		t.Fatalf("cd %+v", cd)
	}
}

func TestConcurrentNoDoubleSpend(t *testing.T) {
	net := NewNetwork(nil)
	registerNodes(t, net, "A", "B")
	// A has exactly 100 spendable towards B.
	if err := net.OpenChannel(Channel{ID: "ab", PeerA: "A", PeerB: "B", BalanceA: 100, BalanceB: 0}); err != nil {
		t.Fatal(err)
	}
	cfg := DefaultRoutingConfig()
	mgr := NewManager(net, nil, nil, cfg)

	const n = 20
	var wg sync.WaitGroup
	var ok, fail int32
	var mu sync.Mutex
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := mgr.SendPayment("A", "B", 100, newPreimage())
			mu.Lock()
			if err == nil {
				ok++
			} else {
				fail++
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	if ok != 1 || fail != n-1 {
		t.Fatalf("ok=%d fail=%d", ok, fail)
	}
	ab, _ := net.GetChannel("ab")
	if ab.BalanceA != 0 || ab.BalanceB != 100 || ab.LockedA != 0 {
		t.Fatalf("channel after concurrent payments: %+v", ab)
	}
	if mgr.InFlight() != 0 {
		t.Fatalf("in flight = %d", mgr.InFlight())
	}
}

func TestMidPathFailureReleasesReserves(t *testing.T) {
	net := NewNetwork(nil)
	registerNodes(t, net, "A", "B", "C", "D")
	chans := []Channel{
		{ID: "ab", PeerA: "A", PeerB: "B", BalanceA: 1000, BalanceB: 1000},
		{ID: "bc", PeerA: "B", PeerB: "C", BalanceA: 100, BalanceB: 1000},
		{ID: "cd", PeerA: "C", PeerB: "D", BalanceA: 1000, BalanceB: 1000},
	}
	for _, c := range chans {
		if err := net.OpenChannel(c); err != nil {
			t.Fatal(err)
		}
	}
	// First payment locks 60 of B->C, leaving 40; the second payment needs
	// 100 on that hop and must fail while leaving all other channels intact.
	cfg := DefaultRoutingConfig()
	mgr := NewManager(net, nil, nil, cfg)
	locked, err := mgr.LockPayment("A", "D", 60, newPreimage())
	if err != nil {
		t.Fatal(err)
	}
	if locked.Status != PaymentLocked {
		t.Fatalf("status %s", locked.Status)
	}
	bc, _ := net.GetChannel("bc")
	if bc.LockedA != 60 {
		t.Fatalf("bc locked=%d", bc.LockedA)
	}

	// The 60-unit lock consumes B->C capacity, so a 100-unit payment has no
	// feasible route and must be rejected without mutating any channel.
	if _, err := mgr.SendPayment("A", "D", 100, newPreimage()); !errors.Is(err, ErrNoRoute) {
		t.Fatalf("want no route, got %v", err)
	}
	// Only the legitimate locks of the first payment (ab and bc, 60 each)
	// may remain; the failed second payment must not leak any reserve.
	want := map[ChannelID]int64{"ab": 60, "bc": 60, "cd": 60}
	for _, id := range []ChannelID{"ab", "bc", "cd"} {
		ch, _ := net.GetChannel(id)
		if ch.LockedA != want[id] {
			t.Fatalf("channel %s locked=%d want %d: %+v", id, ch.LockedA, want[id], ch)
		}
	}
	ab, _ := net.GetChannel("ab")
	if ab.BalanceA != 1000 || ab.BalanceB != 1000 {
		t.Fatalf("ab balances changed after failed payment: %+v", ab)
	}
}

func TestFeeCapAndMaxHopsAndConcurrencyLimits(t *testing.T) {
	net, _ := buildLinearNet(t, nil)

	feeCfg := DefaultRoutingConfig()
	feeCfg.MaxTotalFee = 2 // route A->D forwarding fees are 1+2=3
	mgr := NewManager(net, NewRouter(net, feeCfg), nil, feeCfg)
	if _, err := mgr.SendPayment("A", "D", 100, newPreimage()); !errors.Is(err, ErrFeeExceeded) {
		t.Fatalf("fee cap: %v", err)
	}
	// No balances moved.
	ab, _ := net.GetChannel("ab")
	if ab.BalanceA != 1000 || ab.LockedA != 0 {
		t.Fatalf("state changed on fee rejection: %+v", ab)
	}

	hopCfg := DefaultRoutingConfig()
	hopCfg.MaxHops = 2
	mgr2 := NewManager(net, NewRouter(net, hopCfg), nil, hopCfg)
	if _, err := mgr2.SendPayment("A", "D", 100, newPreimage()); !errors.Is(err, ErrNoRoute) {
		t.Fatalf("max hops: %v", err)
	}

	concCfg := DefaultRoutingConfig()
	concCfg.MaxConcurrent = 1
	mgr3 := NewManager(net, NewRouter(net, concCfg), nil, concCfg)
	if _, err := mgr3.LockPayment("A", "B", 10, newPreimage()); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr3.LockPayment("A", "B", 10, newPreimage()); !errors.Is(err, ErrTooManyConcurrent) {
		t.Fatalf("concurrency: %v", err)
	}
}

func TestTimeoutRefund(t *testing.T) {
	net := NewNetwork(nil)
	registerNodes(t, net, "A", "B", "C")
	for _, c := range []Channel{
		{ID: "ab", PeerA: "A", PeerB: "B", BalanceA: 1000, BalanceB: 1000},
		{ID: "bc", PeerA: "B", PeerB: "C", BalanceA: 1000, BalanceB: 1000},
	} {
		if err := net.OpenChannel(c); err != nil {
			t.Fatal(err)
		}
	}
	clock := new(fakeClock)
	clock.t = time.Unix(1_000_000, 0)
	cfg := DefaultRoutingConfig()
	cfg.LockDuration = time.Minute
	mgr := NewManager(net, nil, nil, cfg)
	mgr.SetClock(clock.now)

	p, err := mgr.LockPayment("A", "C", 100, newPreimage())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.RefundPayment(p.ID); !errors.Is(err, ErrPaymentWrongState) {
		t.Fatalf("early refund should be rejected: %v", err)
	}
	clock.advance(61 * time.Second)
	if _, err := mgr.RefundPayment(p.ID); err != nil {
		t.Fatalf("refund after expiry: %v", err)
	}
	got, _ := mgr.GetPayment(p.ID)
	if got.Status != PaymentRefunded {
		t.Fatalf("status %s", got.Status)
	}
	for _, id := range []ChannelID{"ab", "bc"} {
		ch, _ := net.GetChannel(id)
		if ch.BalanceA != 1000 || ch.BalanceB != 1000 || ch.LockedA != 0 || ch.LockedB != 0 {
			t.Fatalf("channel %s not restored: %+v", id, ch)
		}
	}
	// Refund is idempotent.
	if _, err := mgr.RefundPayment(p.ID); err != nil {
		t.Fatalf("idempotent refund: %v", err)
	}
	if mgr.InFlight() != 0 {
		t.Fatalf("inflight %d", mgr.InFlight())
	}
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// reserveMidPathFailNet builds A-B-C-D where B->C carries less than the
// requested amount, so routing rejects the payment without touching state.
func TestReserveFailureRollsBackPartialReserves(t *testing.T) {
	net := NewNetwork(nil)
	registerNodes(t, net, "A", "B", "C", "D")
	for _, c := range []Channel{
		{ID: "ab", PeerA: "A", PeerB: "B", BalanceA: 1000, BalanceB: 1000},
		{ID: "bc", PeerA: "B", PeerB: "C", BalanceA: 50, BalanceB: 1000},
		{ID: "cd", PeerA: "C", PeerB: "D", BalanceA: 1000, BalanceB: 1000},
	} {
		if err := net.OpenChannel(c); err != nil {
			t.Fatal(err)
		}
	}
	mgr := NewManager(net, nil, nil, DefaultRoutingConfig())
	if _, err := mgr.SendPayment("A", "D", 100, newPreimage()); !errors.Is(err, ErrNoRoute) {
		t.Fatalf("want no route, got %v", err)
	}
	for _, id := range []ChannelID{"ab", "bc", "cd"} {
		ch, _ := net.GetChannel(id)
		if ch.LockedA != 0 || ch.LockedB != 0 {
			t.Fatalf("channel %s leaked lock: %+v", id, ch)
		}
	}
	ab, _ := net.GetChannel("ab")
	if ab.BalanceA != 1000 || ab.BalanceB != 1000 {
		t.Fatalf("ab balances mutated on failed payment: %+v", ab)
	}
}

// Two senders race: both route while funds are free, then reserve; one wins
// the middle hop, the loser must release its first-hop reservation.
func TestConcurrentRacePartialReserveRollback(t *testing.T) {
	net := NewNetwork(nil)
	registerNodes(t, net, "A", "B", "C", "D")
	for _, c := range []Channel{
		{ID: "ab", PeerA: "A", PeerB: "B", BalanceA: 1000, BalanceB: 1000},
		{ID: "bc", PeerA: "B", PeerB: "C", BalanceA: 80, BalanceB: 1000},
		{ID: "cd", PeerA: "C", PeerB: "D", BalanceA: 1000, BalanceB: 1000},
	} {
		if err := net.OpenChannel(c); err != nil {
			t.Fatal(err)
		}
	}
	mgr := NewManager(net, nil, nil, DefaultRoutingConfig())

	start := make(chan struct{})
	var wg sync.WaitGroup
	var ok, failed int32
	var mu sync.Mutex
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := mgr.SendPayment("A", "D", 50, newPreimage())
			mu.Lock()
			if err == nil {
				ok++
			} else {
				failed++
			}
			mu.Unlock()
		}()
	}
	close(start)
	wg.Wait()
	if ok != 1 || failed != 4 {
		t.Fatalf("ok=%d failed=%d", ok, failed)
	}
	for _, id := range []ChannelID{"ab", "bc", "cd"} {
		ch, _ := net.GetChannel(id)
		if ch.LockedA != 0 || ch.LockedB != 0 {
			t.Fatalf("channel %s leaked lock: %+v", id, ch)
		}
	}
	if mgr.InFlight() != 0 {
		t.Fatalf("inflight %d", mgr.InFlight())
	}
}
