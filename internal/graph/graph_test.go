package graph

import (
	"errors"
	"testing"

	"github.com/guoyyit/payment-channel-router-go/internal/model"
)

func seedGraph(t *testing.T) *Graph {
	t.Helper()
	g := New()
	if err := g.AddNode(model.Node{ID: "A"}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode(model.Node{ID: "B"}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode(model.Node{ID: "C"}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddChannel(model.Channel{
		ID: "cAB", NodeA: "A", NodeB: "B", Capacity: 100, BalanceAB: 60, BalanceBA: 40,
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddChannel(model.Channel{
		ID: "cBC", NodeA: "B", NodeB: "C", Capacity: 100, BalanceAB: 50, BalanceBA: 50,
	}); err != nil {
		t.Fatal(err)
	}
	return g
}

func TestMoveBalance(t *testing.T) {
	g := seedGraph(t)
	if err := g.MoveBalance("cAB", "A", "B", 30); err != nil {
		t.Fatal(err)
	}
	ch, _ := g.GetChannel("cAB")
	if ch.BalanceAB != 30 || ch.BalanceBA != 70 || ch.BalanceAB+ch.BalanceBA != 100 {
		t.Fatalf("balances = %d/%d", ch.BalanceAB, ch.BalanceBA)
	}
	if err := g.MoveBalance("cAB", "A", "B", 31); !errors.Is(err, ErrInsufficient) {
		t.Fatalf("want ErrInsufficient, got %v", err)
	}
}

func TestDuplicateChannelPairRejected(t *testing.T) {
	g := seedGraph(t)
	err := g.AddChannel(model.Channel{
		ID: "other", NodeA: "B", NodeB: "A", Capacity: 10, BalanceAB: 5, BalanceBA: 5,
	})
	if !errors.Is(err, ErrChannelExists) {
		t.Fatalf("want ErrChannelExists, got %v", err)
	}
}

func TestReserveSettleRelease(t *testing.T) {
	g := seedGraph(t)
	// Reserve 40 of A->B (balance 60).
	if err := g.Reserve(ReserveParams{
		SegmentID: "s1", PaymentID: "p", ChannelID: "cAB",
		From: "A", To: "B", Amount: 40,
	}); err != nil {
		t.Fatal(err)
	}
	ch, _ := g.GetChannel("cAB")
	if ch.BalanceAB != 20 {
		t.Fatalf("after reserve A->B = %d, want 20", ch.BalanceAB)
	}
	if g.HoldAmount("cAB", "A", "B") != 40 {
		t.Fatal("hold amount wrong")
	}
	// Oversell against the locked amount must fail.
	if err := g.Reserve(ReserveParams{
		SegmentID: "s2", PaymentID: "p", ChannelID: "cAB",
		From: "A", To: "B", Amount: 21,
	}); !errors.Is(err, ErrInsufficient) {
		t.Fatalf("want insufficient, got %v", err)
	}
	// Settle: B keeps the full locked 40; balances stay at 20/80 and the
	// hold is removed.
	if err := g.SettleReserve("s1"); err != nil {
		t.Fatal(err)
	}
	ch, _ = g.GetChannel("cAB")
	if ch.BalanceAB != 20 || ch.BalanceBA != 80 {
		t.Fatalf("after settle balances = %d/%d, want 20/80", ch.BalanceAB, ch.BalanceBA)
	}
	if g.HoldAmount("cAB", "A", "B") != 0 {
		t.Fatal("settled hold must be cleared")
	}

	// New reserve then release restores the settled state (20/80).
	if err := g.Reserve(ReserveParams{
		SegmentID: "s3", PaymentID: "p", ChannelID: "cAB",
		From: "A", To: "B", Amount: 20,
	}); err != nil {
		t.Fatal(err)
	}
	if err := g.ReleaseReserve("s3"); err != nil {
		t.Fatal(err)
	}
	ch, _ = g.GetChannel("cAB")
	if ch.BalanceAB != 20 || ch.BalanceBA != 80 {
		t.Fatalf("after release balances = %d/%d", ch.BalanceAB, ch.BalanceBA)
	}
}

func TestEdgesSorted(t *testing.T) {
	g := seedGraph(t)
	// B connects to A and C; edges must be deterministically sorted.
	e1 := g.EdgesFrom("B")
	e2 := g.EdgesFrom("B")
	if len(e1) != 2 || e1[0].To != "A" || e1[1].To != "C" {
		t.Fatalf("edges = %v", e1)
	}
	for i := range e1 {
		if e1[i] != e2[i] {
			t.Fatal("edges not deterministic")
		}
	}
}
