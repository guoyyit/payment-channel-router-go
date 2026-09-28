package settlement

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/guoyyit/payment-channel-router-go/internal/graph"
	"github.com/guoyyit/payment-channel-router-go/internal/model"
	"github.com/guoyyit/payment-channel-router-go/internal/router"
	"github.com/guoyyit/payment-channel-router-go/internal/store"
)

type fakeClock struct{ t int64 }

func (c *fakeClock) NowUnix() int64 { return c.t }

type env struct {
	dir string
	g   *graph.Graph
	r   *router.Router
	st  *store.Store
	mgr *Manager
	clk *fakeClock
}

func newEnv(t *testing.T) *env {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "wal")
	st, err := store.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	g := graph.New()
	// A(sender) - B - C(receiver). B charges base fee 10. Topology is also
	// journaled so a reopened manager can rebuild it from the WAL.
	for _, n := range []model.Node{
		{ID: "A", Policy: model.FeePolicy{}},
		{ID: "B", Policy: model.FeePolicy{BaseFee: 10}},
		{ID: "C", Policy: model.FeePolicy{}},
	} {
		if _, err := st.Append(store.KindNodeRegistered, model.NodeRegistered{Node: n}); err != nil {
			t.Fatal(err)
		}
		_ = g.AddNode(n)
	}
	for _, c := range []model.Channel{
		{ID: "ab", NodeA: "A", NodeB: "B", Capacity: 1000, BalanceAB: 1000, BalanceBA: 0},
		{ID: "bc", NodeA: "B", NodeB: "C", Capacity: 1000, BalanceAB: 1000, BalanceBA: 0},
	} {
		if _, err := st.Append(store.KindChannelOpened, model.ChannelOpened{Channel: c}); err != nil {
			t.Fatal(err)
		}
		_ = g.AddChannel(c)
	}
	r := router.New(g, router.Options{Strategy: router.LowestFee, MaxHops: 5, MaxFee: 1000})
	clk := &fakeClock{t: 1000}
	mgr := New(g, r, st, clk, Options{
		MaxHops: 5, MaxFee: 1000, MaxConcurrent: 10,
		LockDuration: 100, HopGrace: 1,
	})
	t.Cleanup(func() { st.Close() })
	return &env{dir: dir, g: g, r: r, st: st, mgr: mgr, clk: clk}
}

func mustNode(g *graph.Graph, id string, base, ppm int64) {
	if err := g.AddNode(model.Node{ID: id, Policy: model.FeePolicy{BaseFee: base, FeeRate: ppm}}); err != nil {
		panic(err)
	}
}

func mustChan(g *graph.Graph, id, a, b string, cap, ab int64) {
	if err := g.AddChannel(model.Channel{
		ID: id, NodeA: a, NodeB: b, Capacity: cap, BalanceAB: ab, BalanceBA: cap - ab,
	}); err != nil {
		panic(err)
	}
}

func balances(g *graph.Graph, id string) (ab, ba int64) {
	c, _ := g.GetChannel(id)
	return c.BalanceAB, c.BalanceBA
}

func TestMultiHopSuccess(t *testing.T) {
	e := newEnv(t)
	pre := "secret-preimage"
	if err := e.mgr.Send("pay1", "A", "C", 100, pre); err != nil {
		t.Fatal(err)
	}
	pay, ok := e.mgr.GetPayment("pay1")
	if !ok || pay.Phase != PhaseCompleted {
		t.Fatalf("payment = %+v ok=%v", pay, ok)
	}
	// Amounts: A sends 110 on ab; B sends 100 on bc; B keeps 10.
	if ab, ba := balances(e.g, "ab"); ab != 890 || ba != 110 {
		t.Fatalf("ab after settle = %d/%d, want 890/110", ab, ba)
	}
	if ab, ba := balances(e.g, "bc"); ab != 900 || ba != 100 {
		t.Fatalf("bc after settle = %d/%d, want 900/100", ab, ba)
	}
	// No holds remain.
	if e.g.HoldAmount("ab", "A", "B") != 0 || e.g.HoldAmount("bc", "B", "C") != 0 {
		t.Fatal("residual holds after settlement")
	}
}

func TestSettleIdempotentWrongPreimage(t *testing.T) {
	e := newEnv(t)
	if err := e.mgr.Lock("pay1", "A", "C", 100, Hash("right")); err != nil {
		t.Fatal(err)
	}
	if err := e.mgr.SettleWithPreimage("pay1", "wrong"); !errors.Is(err, ErrBadPreimage) {
		t.Fatalf("want ErrBadPreimage, got %v", err)
	}
	// Still locked, balances still reserved.
	if ab, _ := balances(e.g, "ab"); ab != 890 {
		t.Fatalf("ab after failed settle = %d, want 890", ab)
	}
	if err := e.mgr.SettleWithPreimage("pay1", "right"); err != nil {
		t.Fatal(err)
	}
	// Re-settle is a no-op success.
	if err := e.mgr.SettleWithPreimage("pay1", "right"); err != nil {
		t.Fatal(err)
	}
	if ab, ba := balances(e.g, "ab"); ab != 890 || ba != 110 {
		t.Fatalf("ab = %d/%d", ab, ba)
	}
}

func TestMidPathFailureReleasesAll(t *testing.T) {
	e := newEnv(t)
	// The route A->B->C is feasible at search time. Between hop 0 and hop 1
	// a competing payment grabs almost all B->C capacity, so the second
	// reservation fails AFTER hop 0 already locked 110 on A->B.
	e.mgr.SetBeforeReserveHook(func(paymentID string, hop int) {
		if paymentID == "pay1" && hop == 1 {
			if err := e.g.Reserve(graph.ReserveParams{
				SegmentID: "racer", PaymentID: "racer", ChannelID: "bc",
				From: "B", To: "C", Amount: 950,
			}); err != nil {
				t.Errorf("inject race: %v", err)
			}
		}
	})
	err := e.mgr.Lock("pay1", "A", "C", 100, Hash("x"))
	if !errors.Is(err, graph.ErrInsufficient) {
		t.Fatalf("want ErrInsufficient, got %v", err)
	}
	// Hop 0's reservation (110 on A->B) must have been released fully.
	if ab, ba := balances(e.g, "ab"); ab != 1000 || ba != 0 {
		t.Fatalf("ab = %d/%d, want 1000/0 after rollback", ab, ba)
	}
	if e.g.HoldAmount("ab", "A", "B") != 0 {
		t.Fatal("leaked reservation on hop 0 after failed path")
	}
	if e.mgr.Active() != 0 {
		t.Fatalf("active = %d, want 0", e.mgr.Active())
	}
	// Racing hold is independent and remains (it is not our payment).
	if e.g.HoldAmount("bc", "B", "C") != 950 {
		t.Fatal("competing hold must remain untouched")
	}
}

func TestInfeasibleRouteLeavesStateUnchanged(t *testing.T) {
	e := newEnv(t)
	// A->B cannot cover the debit (110): routing rejects before reserving.
	if err := e.g.MoveBalance("ab", "A", "B", 940); err != nil {
		t.Fatal(err)
	}
	err := e.mgr.Lock("pay1", "A", "C", 100, Hash("x"))
	if !errors.Is(err, ErrRouteNotFound) {
		t.Fatalf("want ErrRouteNotFound, got %v", err)
	}
	if ab, ba := balances(e.g, "ab"); ab != 60 || ba != 940 {
		t.Fatalf("ab = %d/%d, want unchanged 60/940", ab, ba)
	}
	if e.g.HoldAmount("ab", "A", "B") != 0 {
		t.Fatal("state changed after rejected request")
	}
}

func TestExpiryReleasesInOrder(t *testing.T) {
	e := newEnv(t)
	if err := e.mgr.Lock("pay1", "A", "C", 100, Hash("x")); err != nil {
		t.Fatal(err)
	}
	// Before deadline: cannot expire.
	if err := e.mgr.Expire("pay1"); !errors.Is(err, ErrNotExpired) {
		t.Fatalf("want ErrNotExpired, got %v", err)
	}
	e.clk.t = 2000
	n, err := e.mgr.Sweep(e.clk.NowUnix())
	if err != nil || n != 1 {
		t.Fatalf("sweep n=%d err=%v", n, err)
	}
	if ab, ba := balances(e.g, "ab"); ab != 1000 || ba != 0 {
		t.Fatalf("ab after expiry = %d/%d, want 1000/0", ab, ba)
	}
	if ab, ba := balances(e.g, "bc"); ab != 1000 || ba != 0 {
		t.Fatalf("bc after expiry = %d/%d, want 1000/0", ab, ba)
	}
	pay, _ := e.mgr.GetPayment("pay1")
	if pay.Phase != PhaseAborted {
		t.Fatalf("phase = %v", pay.Phase)
	}
	// Expiring again is idempotent.
	if err := e.mgr.Expire("pay1"); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentLimit(t *testing.T) {
	e := newEnv(t)
	e.mgr.opt.MaxConcurrent = 1
	if err := e.mgr.Lock("p1", "A", "C", 10, Hash("a")); err != nil {
		t.Fatal(err)
	}
	if err := e.mgr.Lock("p2", "A", "C", 10, Hash("b")); !errors.Is(err, ErrConcurrentLimit) {
		t.Fatalf("want ErrConcurrentLimit, got %v", err)
	}
}
