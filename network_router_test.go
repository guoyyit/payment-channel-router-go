package pchr

import (
	"crypto/rand"
	"errors"
	"testing"
)

func newPreimage() [32]byte {
	var p [32]byte
	_, _ = rand.Read(p[:])
	return p
}

func registerNodes(t *testing.T, net *Network, ids ...NodeID) {
	t.Helper()
	for _, id := range ids {
		if err := net.RegisterNode(id, "addr-"+string(id), nil); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
	}
}

func buildLinearNet(t *testing.T, store Store) (*Network, *Manager) {
	t.Helper()
	net := NewNetwork(store)
	registerNodes(t, net, "A", "B", "C", "D")
	cfg := DefaultRoutingConfig()
	chans := []Channel{
		{ID: "ab", PeerA: "A", PeerB: "B", BalanceA: 1000, BalanceB: 1000, FeePolicy: FeePolicy{BaseFee: 1}},
		{ID: "bc", PeerA: "B", PeerB: "C", BalanceA: 1000, BalanceB: 1000, FeePolicy: FeePolicy{BaseFee: 2}},
		{ID: "cd", PeerA: "C", PeerB: "D", BalanceA: 1000, BalanceB: 1000, FeePolicy: FeePolicy{BaseFee: 3}},
	}
	for _, c := range chans {
		if err := net.OpenChannel(c); err != nil {
			t.Fatal(err)
		}
	}
	mgr := NewManager(net, nil, store, cfg)
	return net, mgr
}

func TestNodeAndChannelRegistration(t *testing.T) {
	net := NewNetwork(nil)
	if err := net.RegisterNode("", "a", nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty id: %v", err)
	}
	registerNodes(t, net, "A", "B")
	if err := net.RegisterNode("A", "dup", nil); !errors.Is(err, ErrNodeExists) {
		t.Fatalf("dup node: %v", err)
	}
	if _, err := net.GetNode("X"); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("missing node: %v", err)
	}
	ch := Channel{ID: "ab", PeerA: "A", PeerB: "B", BalanceA: 50, BalanceB: 60}
	if err := net.OpenChannel(ch); err != nil {
		t.Fatal(err)
	}
	if err := net.OpenChannel(ch); !errors.Is(err, ErrChannelExists) {
		t.Fatalf("dup channel: %v", err)
	}
	if err := net.OpenChannel(Channel{ID: "xx", PeerA: "A", PeerB: "Z", BalanceA: 1, BalanceB: 1}); !errors.Is(err, ErrNodeNotFound) {
		t.Fatalf("unknown peer: %v", err)
	}
	got, err := net.GetChannel("ab")
	if err != nil {
		t.Fatal(err)
	}
	if got.BalanceA != 50 || got.BalanceB != 60 || !got.Active {
		t.Fatalf("channel %+v", got)
	}
	spend, locked, ok := got.Balances("A")
	if !ok || spend != 50 || locked != 0 {
		t.Fatalf("balances %d %d %v", spend, locked, ok)
	}
	if ids := net.ChannelsBetween("A", "B"); len(ids) != 1 || ids[0] != "ab" {
		t.Fatalf("channels between: %v", ids)
	}
}

func TestFeePolicy(t *testing.T) {
	p := FeePolicy{BaseFee: 10, FeeRateMillionths: 1000} // 0.1% + 10
	if got := p.Fee(1000); got != 11 {
		t.Fatalf("fee(1000)=%d want 11", got)
	}
	if got := p.Fee(1); got != 11 { // ceil rounding: 10 + 1
		t.Fatalf("fee(1)=%d want 11", got)
	}
}

func TestRoutingCheapestAndShortest(t *testing.T) {
	net := NewNetwork(nil)
	registerNodes(t, net, "S", "X", "Y", "R")
	// Two same-length paths S->X->R and S->Y->R differ in their FORWARDING
	// node fees (the receiver edge is always free).
	chans := []Channel{
		{ID: "c-sx", PeerA: "S", PeerB: "X", BalanceA: 10000, BalanceB: 10000, FeePolicy: FeePolicy{BaseFee: 50}},
		{ID: "c-xr", PeerA: "X", PeerB: "R", BalanceA: 10000, BalanceB: 10000, FeePolicy: FeePolicy{BaseFee: 1}},
		{ID: "c-sy", PeerA: "S", PeerB: "Y", BalanceA: 10000, BalanceB: 10000, FeePolicy: FeePolicy{BaseFee: 1}},
		{ID: "c-yr", PeerA: "Y", PeerB: "R", BalanceA: 10000, BalanceB: 10000, FeePolicy: FeePolicy{BaseFee: 1}},
	}
	for _, c := range chans {
		if err := net.OpenChannel(c); err != nil {
			t.Fatal(err)
		}
	}

	cheap := NewRouter(net, RoutingConfig{Strategy: StrategyCheapestFee, MaxHops: 5})
	route, err := cheap.FindRoute("S", "R", 1000)
	if err != nil {
		t.Fatal(err)
	}
	// Via Y: Y's forwarding fee is charged on S-Y (sy fee 1), Y-R free.
	if len(route.Hops) != 2 || route.TotalFee != 1 {
		t.Fatalf("cheapest route hops=%d fee=%d", len(route.Hops), route.TotalFee)
	}
	if route.AmountToSend != 1001 || route.AmountReceived != 1000 {
		t.Fatalf("amounts %+v", route)
	}
	if route.Hops[0].ChannelID != "c-sy" {
		t.Fatalf("expected cheap path via Y, got %s", route.Hops[0].ChannelID)
	}

	// Same hop count: shortest strategy still breaks ties by fee.
	short := NewRouter(net, RoutingConfig{Strategy: StrategyShortest, MaxHops: 5})
	route2, err := short.FindRoute("S", "R", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(route2.Hops) != 2 || route2.Hops[0].ChannelID != "c-sy" {
		t.Fatalf("shortest(2-hop tie) route: %+v", route2.Hops)
	}
}

func TestRoutingRespectsDirectionalBalanceAndMaxHops(t *testing.T) {
	net := NewNetwork(nil)
	registerNodes(t, net, "A", "B", "C")
	// A can send 5 towards B; B can send 200 towards C via directional funds.
	if err := net.OpenChannel(Channel{ID: "ab", PeerA: "A", PeerB: "B", BalanceA: 5, BalanceB: 1000}); err != nil {
		t.Fatal(err)
	}
	if err := net.OpenChannel(Channel{ID: "bc", PeerA: "B", PeerB: "C", BalanceA: 1000, BalanceB: 0}); err != nil {
		t.Fatal(err)
	}
	r := NewRouter(net, RoutingConfig{Strategy: StrategyCheapestFee, MaxHops: 5})
	if _, err := r.FindRoute("A", "C", 100); !errors.Is(err, ErrNoRoute) {
		t.Fatalf("insufficient directional balance: %v", err)
	}
	// No direct path A->C under a 1-hop limit.
	r1 := NewRouter(net, RoutingConfig{Strategy: StrategyCheapestFee, MaxHops: 1})
	if _, err := r1.FindRoute("A", "C", 1); !errors.Is(err, ErrNoRoute) {
		t.Fatalf("max hops: %v", err)
	}
}

func TestRoutingDeterministic(t *testing.T) {
	net := NewNetwork(nil)
	registerNodes(t, net, "S", "X", "Y", "R")
	cfg := RoutingConfig{Strategy: StrategyCheapestFee, MaxHops: 5}
	chans := []Channel{
		{ID: "c-sy", PeerA: "S", PeerB: "Y", BalanceA: 10000, BalanceB: 10000, FeePolicy: FeePolicy{BaseFee: 1}},
		{ID: "c-sx", PeerA: "S", PeerB: "X", BalanceA: 10000, BalanceB: 10000, FeePolicy: FeePolicy{BaseFee: 1}},
		{ID: "c-xr", PeerA: "X", PeerB: "R", BalanceA: 10000, BalanceB: 10000, FeePolicy: FeePolicy{BaseFee: 1}},
		{ID: "c-yr", PeerA: "Y", PeerB: "R", BalanceA: 10000, BalanceB: 10000, FeePolicy: FeePolicy{BaseFee: 1}},
	}
	for _, c := range chans {
		if err := net.OpenChannel(c); err != nil {
			t.Fatal(err)
		}
	}
	r := NewRouter(net, cfg)
	var first []ChannelID
	for i := 0; i < 20; i++ {
		route, err := r.FindRoute("S", "R", 100)
		if err != nil {
			t.Fatal(err)
		}
		ids := make([]ChannelID, len(route.Hops))
		for j, h := range route.Hops {
			ids[j] = h.ChannelID
		}
		if first == nil {
			first = ids
		} else if len(ids) != len(first) {
			t.Fatal("unstable route length")
		} else {
			for j := range ids {
				if ids[j] != first[j] {
					t.Fatalf("non-deterministic route: %v vs %v", ids, first)
				}
			}
		}
	}
	// c-sx < c-sy lexicographically, so the cheapest tie resolves via X.
	if len(first) != 2 || first[0] != "c-sx" || first[1] != "c-xr" {
		t.Fatalf("unexpected tie-broken route: %v", first)
	}
}

func TestShortestStrategyPrefersFewerHops(t *testing.T) {
	net := NewNetwork(nil)
	registerNodes(t, net, "S", "M", "R")
	for _, c := range []Channel{
		{ID: "c-direct", PeerA: "S", PeerB: "R", BalanceA: 10000, BalanceB: 10000, FeePolicy: FeePolicy{BaseFee: 0}},
		{ID: "c-sm", PeerA: "S", PeerB: "M", BalanceA: 10000, BalanceB: 10000, FeePolicy: FeePolicy{BaseFee: 0}},
		{ID: "c-mr", PeerA: "M", PeerB: "R", BalanceA: 10000, BalanceB: 10000, FeePolicy: FeePolicy{BaseFee: 0}},
	} {
		if err := net.OpenChannel(c); err != nil {
			t.Fatal(err)
		}
	}
	// Equal fees: cheapest falls back to fewer hops; shortest obviously too.
	// Differentiate by giving the direct edge a receiver-side zero fee which
	// is always free, while 2-hop is also zero. Both prefer 1 hop.
	for _, strat := range []RouteStrategy{StrategyCheapestFee, StrategyShortest} {
		r := NewRouter(net, RoutingConfig{Strategy: strat, MaxHops: 5})
		route, err := r.FindRoute("S", "R", 500)
		if err != nil {
			t.Fatal(err)
		}
		if len(route.Hops) != 1 || route.Hops[0].ChannelID != "c-direct" {
			t.Fatalf("strategy %s picked %+v", strat, route.Hops)
		}
	}
}
