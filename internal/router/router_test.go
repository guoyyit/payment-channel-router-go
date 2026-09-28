package router

import (
	"errors"
	"testing"

	"github.com/guoyyit/payment-channel-router-go/internal/graph"
	"github.com/guoyyit/payment-channel-router-go/internal/model"
)

func addNode(g *graph.Graph, id string, base, ppm int64) {
	if err := g.AddNode(model.Node{ID: id, Policy: model.FeePolicy{BaseFee: base, FeeRate: ppm}}); err != nil {
		panic(err)
	}
}

func addChan(g *graph.Graph, id, a, b string, cap, ab int64) {
	if err := g.AddChannel(model.Channel{
		ID: id, NodeA: a, NodeB: b, Capacity: cap, BalanceAB: ab, BalanceBA: cap - ab,
	}); err != nil {
		panic(err)
	}
}

func TestDirectRouteAndFee(t *testing.T) {
	g := graph.New()
	addNode(g, "A", 0, 0)
	addNode(g, "B", 10, 0)
	addChan(g, "c", "A", "B", 100, 100)
	r := New(g, Options{Strategy: LowestFee, MaxHops: 3, MaxFee: 100})
	rt, err := r.FindRoute("A", "B", 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(rt.Hops) != 1 || rt.TotalFee != 0 || rt.TotalDebit != 50 {
		t.Fatalf("direct route = %+v", rt)
	}
	if rt.Hops[0].Fee != 0 {
		t.Fatal("sender must not charge fee")
	}
}

func TestMultiHopFeeBackwardPricing(t *testing.T) {
	// A -> B -> C -> D; B fee base 10, C fee 10% (100000ppm).
	g := graph.New()
	addNode(g, "A", 0, 0)
	addNode(g, "B", 10, 0)
	addNode(g, "C", 0, 100000)
	addNode(g, "D", 0, 0)
	addChan(g, "ab", "A", "B", 1000, 1000)
	addChan(g, "bc", "B", "C", 1000, 1000)
	addChan(g, "cd", "C", "D", 1000, 1000)
	r := New(g, Options{Strategy: LowestFee, MaxHops: 5, MaxFee: 1000})
	rt, err := r.FindRoute("A", "D", 100)
	if err != nil {
		t.Fatal(err)
	}
	// C must deliver 100: charges 10 -> B must send 110.
	// B must deliver 110: charges 10 -> A must send 120.
	if rt.TotalDebit != 120 || rt.TotalFee != 20 {
		t.Fatalf("debit=%d fee=%d", rt.TotalDebit, rt.TotalFee)
	}
	if rt.Hops[0].AmountToForward != 120 || rt.Hops[1].AmountToForward != 110 ||
		rt.Hops[2].AmountToForward != 100 {
		t.Fatalf("hop amounts = %d %d %d",
			rt.Hops[0].AmountToForward, rt.Hops[1].AmountToForward, rt.Hops[2].AmountToForward)
	}
	if rt.Hops[0].Fee != 10 || rt.Hops[1].Fee != 10 || rt.Hops[2].Fee != 0 {
		t.Fatalf("hop fees = %d %d %d, want 10 10 0", rt.Hops[0].Fee, rt.Hops[1].Fee, rt.Hops[2].Fee)
	}
}

func TestFeeCapRejected(t *testing.T) {
	g := graph.New()
	addNode(g, "A", 0, 0)
	addNode(g, "B", 10, 0)
	addNode(g, "C", 0, 0)
	addChan(g, "ab", "A", "B", 1000, 1000)
	addChan(g, "bc", "B", "C", 1000, 1000)
	r := New(g, Options{Strategy: LowestFee, MaxHops: 3, MaxFee: 5})
	_, err := r.FindRoute("A", "C", 100)
	if !errors.Is(err, ErrFeeExceeded) {
		t.Fatalf("want ErrFeeExceeded, got %v", err)
	}
}

func TestMaxHopsRejected(t *testing.T) {
	g := graph.New()
	for _, id := range []string{"A", "B", "C", "D"} {
		addNode(g, id, 0, 0)
	}
	addChan(g, "ab", "A", "B", 1000, 1000)
	addChan(g, "bc", "B", "C", 1000, 1000)
	addChan(g, "cd", "C", "D", 1000, 1000)
	r := New(g, Options{Strategy: LowestFee, MaxHops: 2, MaxFee: 1000})
	if _, err := r.FindRoute("A", "D", 10); !errors.Is(err, ErrNoRoute) {
		t.Fatalf("want ErrNoRoute, got %v", err)
	}
}

func TestInsufficientDirectionalBalance(t *testing.T) {
	g := graph.New()
	addNode(g, "A", 0, 0)
	addNode(g, "B", 0, 0)
	// A can only send 30 to B.
	addChan(g, "ab", "A", "B", 100, 30)
	r := New(g, Options{Strategy: LowestFee, MaxHops: 3, MaxFee: 100})
	if _, err := r.FindRoute("A", "B", 31); !errors.Is(err, ErrNoRoute) {
		t.Fatalf("want ErrNoRoute, got %v", err)
	}
}

func TestDeterministicTieBreak(t *testing.T) {
	// Two equal-cost paths A->B->D and A->C->D: lexicographically smaller
	// path (B before C) must be chosen, repeatedly.
	g := graph.New()
	for _, id := range []string{"A", "B", "C", "D"} {
		addNode(g, id, 0, 0)
	}
	addChan(g, "ab", "A", "B", 1000, 1000)
	addChan(g, "ac", "A", "C", 1000, 1000)
	addChan(g, "bd", "B", "D", 1000, 1000)
	addChan(g, "cd", "C", "D", 1000, 1000)
	r := New(g, Options{Strategy: ShortestPath, MaxHops: 4, MaxFee: 1000})
	for i := 0; i < 5; i++ {
		rt, err := r.FindRoute("A", "D", 10)
		if err != nil {
			t.Fatal(err)
		}
		if rt.Path[1] != "B" {
			t.Fatalf("nondeterministic path: %v", rt.Path)
		}
	}
}
