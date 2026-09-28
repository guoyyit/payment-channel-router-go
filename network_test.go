package pctr

import (
	"errors"
	"testing"

	"github.com/guoyyit/payment-channel-router-go/internal/model"
	"github.com/guoyyit/payment-channel-router-go/internal/router"
	"github.com/guoyyit/payment-channel-router-go/internal/settlement"
)

type clk struct{ t int64 }

func (c *clk) NowUnix() int64 { return c.t }

func seed(t *testing.T, dir string, maxFee int64) *Network {
	t.Helper()
	n, err := Open(Config{
		DataDir: dir, Strategy: router.LowestFee, MaxHops: 5,
		MaxFee: maxFee, MaxConcurrent: 10, LockDuration: 3600,
	})
	if err != nil {
		t.Fatal(err)
	}
	register := func(id string, base int64) {
		if err := n.RegisterNode(id, model.FeePolicy{BaseFee: base}); err != nil {
			t.Fatal(err)
		}
	}
	register("A", 0)
	register("B", 10) // B charges 10 base fee
	register("C", 0)
	if err := n.OpenChannel("ab", "A", "B", 1000, 1000); err != nil {
		t.Fatal(err)
	}
	if err := n.OpenChannel("bc", "B", "C", 1000, 1000); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestRegisterQueryAndDuplicates(t *testing.T) {
	n := seed(t, t.TempDir(), 1000)
	defer n.Close()
	if _, ok := n.Graph().GetNode("B"); !ok {
		t.Fatal("node B missing")
	}
	if _, ok := n.Graph().ChannelBetween("B", "A"); !ok {
		t.Fatal("channel A-B missing (reverse lookup)")
	}
	if err := n.RegisterNode("A", model.FeePolicy{}); err == nil {
		t.Fatal("duplicate node must be rejected")
	}
	if err := n.OpenChannel("dup", "A", "B", 10, 5); err == nil {
		t.Fatal("duplicate channel pair must be rejected")
	}
}

func TestFeeCapRejectedLeavesBalances(t *testing.T) {
	n := seed(t, t.TempDir(), 5) // fee on A->B->C is 10 > cap 5
	defer n.Close()
	err := n.Manager().Send("p1", "A", "C", 100, settlement.GeneratePreimage())
	if !errors.Is(err, settlement.ErrFeeBudget) {
		t.Fatalf("want ErrFeeBudget, got %v", err)
	}
	c, _ := n.Graph().GetChannel("ab")
	if c.BalanceAB != 1000 || c.BalanceBA != 0 {
		t.Fatalf("ab changed after rejected payment: %+v", c)
	}
}

func TestSendPersistRestartComplete(t *testing.T) {
	dir := t.TempDir()
	c := &clk{t: 1000}
	n, err := Open(Config{
		DataDir: dir, Strategy: router.LowestFee, MaxHops: 5,
		MaxFee: 1000, MaxConcurrent: 10, LockDuration: 3600, Clock: c,
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = n.RegisterNode("A", model.FeePolicy{})
	_ = n.RegisterNode("B", model.FeePolicy{BaseFee: 10})
	_ = n.RegisterNode("C", model.FeePolicy{})
	_ = n.OpenChannel("ab", "A", "B", 1000, 1000)
	_ = n.OpenChannel("bc", "B", "C", 1000, 1000)

	// Lock only, then "crash" by closing.
	if err := n.Manager().Lock("p1", "A", "C", 100, settlement.Hash("secret")); err != nil {
		t.Fatal(err)
	}
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen (still within deadline) and complete settlement.
	n2, err := Open(Config{
		DataDir: dir, Strategy: router.LowestFee, MaxHops: 5,
		MaxFee: 1000, MaxConcurrent: 10, LockDuration: 3600, Clock: c,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer n2.Close()
	pay, ok := n2.Manager().GetPayment("p1")
	if !ok || pay.Phase != settlement.PhaseLocked {
		t.Fatalf("reopened payment = %+v ok=%v", pay, ok)
	}
	if err := n2.Manager().SettleWithPreimage("p1", "secret"); err != nil {
		t.Fatal(err)
	}
	cab, _ := n2.Graph().GetChannel("ab")
	cbc, _ := n2.Graph().GetChannel("bc")
	if cab.BalanceAB != 890 || cab.BalanceBA != 110 {
		t.Fatalf("ab = %d/%d, want 890/110", cab.BalanceAB, cab.BalanceBA)
	}
	if cbc.BalanceAB != 900 || cbc.BalanceBA != 100 {
		t.Fatalf("bc = %d/%d, want 900/100", cbc.BalanceAB, cbc.BalanceBA)
	}
}

func TestRejectInvalidRequests(t *testing.T) {
	n := seed(t, t.TempDir(), 1000)
	defer n.Close()
	if err := n.Manager().Send("", "A", "C", 100, "x"); err == nil {
		t.Fatal("empty payment id must be rejected")
	}
	if err := n.Manager().Send("p2", "A", "A", 100, "x"); err == nil {
		t.Fatal("sender==receiver must be rejected")
	}
	if err := n.Manager().Send("p3", "A", "C", 0, "x"); err == nil {
		t.Fatal("zero amount must be rejected")
	}
	if err := n.Manager().Send("p4", "A", "Z", 10, "x"); err == nil {
		t.Fatal("unknown receiver must be rejected")
	}
}
