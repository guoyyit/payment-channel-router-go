package settlement

import (
	"path/filepath"
	"testing"

	"github.com/guoyyit/payment-channel-router-go/internal/graph"
	"github.com/guoyyit/payment-channel-router-go/internal/model"
	"github.com/guoyyit/payment-channel-router-go/internal/router"
	"github.com/guoyyit/payment-channel-router-go/internal/store"
)

// reopen simulates a node crash and restart: a brand new graph/manager is
// built from the same WAL dir, events replayed, then Recover() runs.
func reopen(t *testing.T, dir string, clk *fakeClock) *env {
	t.Helper()
	st, err := store.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	g := graph.New()
	r := router.New(g, router.Options{Strategy: router.LowestFee, MaxHops: 5, MaxFee: 1000})
	mgr := New(g, r, st, clk, Options{
		MaxHops: 5, MaxFee: 1000, MaxConcurrent: 10,
		LockDuration: 100, HopGrace: 1,
	})
	mgr.BeginReplay()
	if err := st.Replay(mgr.ApplyEvent); err != nil {
		t.Fatal(err)
	}
	mgr.EndReplay()
	if err := mgr.Recover(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return &env{g: g, r: r, st: st, mgr: mgr, clk: clk}
}

func TestRestartResumesMidSettlement(t *testing.T) {
	e := newEnv(t)
	// Settle hop 1 (last) then "crash" before hop 0 settles. Because
	// settlement runs reverse order, the hook after hop index 1 panics out
	// of the call; the reveal + last-settle events are already durable.
	crash := errCrash{}
	e.mgr.SetAfterSettleHook(func(paymentID string, hop int) {
		if paymentID == "pay1" && hop == 1 {
			panic(crash)
		}
	})
	dir := e.dir
	func() {
		defer func() { _ = recover() }()
		if err := e.mgr.Send("pay1", "A", "C", 100, "right"); err != nil {
			t.Fatalf("unexpected err %v", err)
		}
	}()
	e.st.Close()

	// Restart: recovery must complete hop 0 exactly once.
	e2 := reopen(t, dir, &fakeClock{t: 1000})
	pay, ok := e2.mgr.GetPayment("pay1")
	if !ok || pay.Phase != PhaseCompleted {
		t.Fatalf("payment phase after recover = %v ok=%v", pay.Phase, ok)
	}
	if ab, ba := balances(e2.g, "ab"); ab != 890 || ba != 110 {
		t.Fatalf("ab after resume = %d/%d, want 890/110", ab, ba)
	}
	if ab, ba := balances(e2.g, "bc"); ab != 900 || ba != 100 {
		t.Fatalf("bc after resume = %d/%d, want 900/100", ab, ba)
	}
	// No holds, and settling again is a harmless no-op.
	if e2.g.HoldAmount("ab", "A", "B") != 0 {
		t.Fatal("residual hold after resume")
	}
	if err := e2.mgr.SettleWithPreimage("pay1", "right"); err != nil {
		t.Fatal(err)
	}
	if ab, ba := balances(e2.g, "ab"); ab != 890 || ba != 110 {
		t.Fatalf("ab changed on re-settle: %d/%d", ab, ba)
	}
}

func TestRestartExpiresTimedOutLockedPayment(t *testing.T) {
	e := newEnv(t)
	if err := e.mgr.Lock("pay1", "A", "C", 100, Hash("x")); err != nil {
		t.Fatal(err)
	}
	dir := e.dir
	e.st.Close()

	// Restart AFTER the deadline: locked payment must be safely rolled back.
	e2 := reopen(t, dir, &fakeClock{t: 5000})
	pay, _ := e2.mgr.GetPayment("pay1")
	if pay.Phase != PhaseAborted {
		t.Fatalf("phase = %v, want aborted", pay.Phase)
	}
	if ab, ba := balances(e2.g, "ab"); ab != 1000 || ba != 0 {
		t.Fatalf("ab = %d/%d, want 1000/0", ab, ba)
	}
	if ab, ba := balances(e2.g, "bc"); ab != 1000 || ba != 0 {
		t.Fatalf("bc = %d/%d, want 1000/0", ab, ba)
	}
}

func TestRestartKeepsStillLockedPayment(t *testing.T) {
	e := newEnv(t)
	if err := e.mgr.Lock("pay1", "A", "C", 100, Hash("x")); err != nil {
		t.Fatal(err)
	}
	dir := e.dir
	e.st.Close()

	// Restart still within deadline: reserves must be intact, payment stays
	// locked and can then be settled.
	e2 := reopen(t, dir, &fakeClock{t: 1001})
	pay, _ := e2.mgr.GetPayment("pay1")
	if pay.Phase != PhaseLocked {
		t.Fatalf("phase = %v, want locked", pay.Phase)
	}
	if e2.g.HoldAmount("ab", "A", "B") != 110 {
		t.Fatalf("ab hold = %d, want 110", e2.g.HoldAmount("ab", "A", "B"))
	}
	if err := e2.mgr.SettleWithPreimage("pay1", "x"); err != nil {
		t.Fatal(err)
	}
	if ab, ba := balances(e2.g, "ab"); ab != 890 || ba != 110 {
		t.Fatalf("ab after delayed settle = %d/%d", ab, ba)
	}
}

func TestCompletedPaymentNotSettledTwiceAfterRestart(t *testing.T) {
	e := newEnv(t)
	if err := e.mgr.Send("pay1", "A", "C", 100, "right"); err != nil {
		t.Fatal(err)
	}
	dir := e.dir
	e.st.Close()

	e2 := reopen(t, dir, &fakeClock{t: 1000})
	pay, _ := e2.mgr.GetPayment("pay1")
	if pay.Phase != PhaseCompleted {
		t.Fatalf("phase = %v", pay.Phase)
	}
	if e2.mgr.Active() != 0 {
		t.Fatalf("active = %d, want 0", e2.mgr.Active())
	}
	// Idempotent, balances unchanged.
	if err := e2.mgr.SettleWithPreimage("pay1", "right"); err != nil {
		t.Fatal(err)
	}
	if ab, ba := balances(e2.g, "ab"); ab != 890 || ba != 110 {
		t.Fatalf("ab = %d/%d", ab, ba)
	}
}

type errCrash struct{}

func (errCrash) Error() string { return "simulated crash" }

func TestRestartReleasesOrphanReserves(t *testing.T) {
	// Journal topology plus ONE segment reserve but NO PaymentReserved
	// header (crash mid-Lock after hop 0). Recovery must unwind the orphan
	// hold and not leave funds stranded or an active payment.
	dir := testWalDir(t)
	st, err := store.New(dir)
	if err != nil {
		t.Fatal(err)
	}
	g := graph.New()
	for _, n := range modelNode("A", "B", "C") {
		_, _ = st.Append(store.KindNodeRegistered, model.NodeRegistered{Node: n})
		_ = g.AddNode(n)
	}
	for _, c := range []model.Channel{
		{ID: "ab", NodeA: "A", NodeB: "B", Capacity: 1000, BalanceAB: 1000, BalanceBA: 0},
		{ID: "bc", NodeA: "B", NodeB: "C", Capacity: 1000, BalanceAB: 1000, BalanceBA: 0},
	} {
		_, _ = st.Append(store.KindChannelOpened, model.ChannelOpened{Channel: c})
		_ = g.AddChannel(c)
	}
	_, _ = st.Append(store.KindSegmentReserved, model.SegmentReserved{
		SegmentID: "orph:seg:0", PaymentID: "orph", ChannelID: "ab",
		From: "A", To: "B", Amount: 110, Deadline: 5000,
	})
	st.Close()

	e := reopen(t, dir, &fakeClock{t: 1000})
	if e.g.HoldAmount("ab", "A", "B") != 0 {
		t.Fatalf("orphan hold = %d, want 0", e.g.HoldAmount("ab", "A", "B"))
	}
	if ab, ba := balances(e.g, "ab"); ab != 1000 || ba != 0 {
		t.Fatalf("ab = %d/%d, want restored 1000/0", ab, ba)
	}
	if _, ok := e.mgr.GetPayment("orph"); ok {
		t.Fatal("orphan payment placeholder must be dropped")
	}
	if e.mgr.Active() != 0 {
		t.Fatalf("active = %d, want 0", e.mgr.Active())
	}
}

func testWalDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "wal")
	return dir
}

// modelNode builds the three topology nodes used by tests.
func modelNode(ids ...string) []model.Node {
	out := make([]model.Node, 0, len(ids))
	for _, id := range ids {
		pol := model.FeePolicy{}
		if id == "B" {
			pol = model.FeePolicy{BaseFee: 10}
		}
		out = append(out, model.Node{ID: id, Policy: pol})
	}
	return out
}
