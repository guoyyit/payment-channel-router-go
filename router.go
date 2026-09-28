package pchr

import (
	"container/heap"
)

// Router performs deterministic multi-hop path search over a Network.
type Router struct {
	net    *Network
	config RoutingConfig
}

// NewRouter builds a router with the given configuration.
func NewRouter(net *Network, cfg RoutingConfig) *Router {
	return &Router{net: net, config: cfg}
}

// Config returns the router configuration.
func (r *Router) Config() RoutingConfig { return r.config }

// FindRoute finds the best route from sender to receiver so that the receiver
// gets exactly amount. Fees are accumulated backwards: each hop's fee is
// computed on the amount that hop actually forwards.
//
// Determinism: a node's channels are traversed in channel-id order and ties
// are resolved by the lexicographically smallest channel-id path.
func (r *Router) FindRoute(sender, receiver NodeID, amount int64) (*Route, error) {
	if amount <= 0 || sender == "" || receiver == "" {
		return nil, ErrInvalidArgument
	}
	if sender == receiver {
		return nil, ErrInvalidArgument
	}
	if _, err := r.net.GetNode(sender); err != nil {
		return nil, err
	}
	if _, err := r.net.GetNode(receiver); err != nil {
		return nil, err
	}
	maxHops := r.config.MaxHops
	if maxHops <= 0 {
		maxHops = DefaultRoutingConfig().MaxHops
	}

	start := &routeLabel{
		node:         receiver,
		pathNodes:    []NodeID{receiver},
		pathChannels: nil,
		amounts:      []int64{amount},
		amountNeeded: amount,
		hops:         0,
	}

	pq := &labelHeap{strategy: r.config.Strategy}
	heap.Init(pq)
	heap.Push(pq, start)
	seen := map[string]bool{pathKey(start): true}

	for pq.Len() > 0 {
		cur := heap.Pop(pq).(*routeLabel)
		if cur.node == sender {
			return r.buildRoute(cur, amount)
		}
		if cur.hops >= maxHops {
			continue
		}
		r.expand(cur, seen, pq)
	}
	return nil, ErrNoRoute
}

func (r *Router) expand(cur *routeLabel, seen map[string]bool, pq *labelHeap) {
	visited := make(map[NodeID]bool, len(cur.pathNodes))
	for _, n := range cur.pathNodes {
		visited[n] = true
	}

	for _, cid := range r.net.ChannelsFrom(cur.node) {
		ch, err := r.net.GetChannel(cid)
		if err != nil || !ch.Active {
			continue
		}
		prev := ch.otherPeer(cur.node)
		if visited[prev] {
			continue // simple paths only
		}
		spendable, _, ok := ch.Balances(prev)
		if !ok {
			continue
		}
		fee := ch.FeePolicy.Fee(cur.amountNeeded)
		// The receiver edge carries no forwarding fee: the terminal node
		// does not forward. A channel's fee is its receiving (forwarding)
		// node's revenue, realized on the NEXT hop, so it is added only
		// when extending an edge whose receiver forwards further.
		if cur.hops == 0 {
			fee = 0
		}
		needed := cur.amountNeeded + fee
		if needed < cur.amountNeeded { // int64 overflow guard
			continue
		}
		if spendable < needed {
			continue
		}
		amounts := append(append([]int64{}, cur.amounts...), needed)
		nl := &routeLabel{
			node:         prev,
			pathNodes:    append(append([]NodeID{}, cur.pathNodes...), prev),
			pathChannels: append(append([]ChannelID{}, cur.pathChannels...), ch.ID),
			amounts:      amounts,
			amountNeeded: needed,
			hops:         cur.hops + 1,
		}
		key := pathKey(nl)
		if seen[key] {
			continue
		}
		seen[key] = true
		heap.Push(pq, nl)
	}
}

func (r *Router) buildRoute(l *routeLabel, amount int64) (*Route, error) {
	hops := l.hops
	fwdNodes := make([]NodeID, 0, hops+1)
	fwdChans := make([]ChannelID, 0, hops)
	for i := hops; i >= 0; i-- {
		fwdNodes = append(fwdNodes, l.pathNodes[i])
	}
	for i := hops - 1; i >= 0; i-- {
		fwdChans = append(fwdChans, l.pathChannels[i])
	}

	out := &Route{
		Hops:           make([]Hop, 0, hops),
		AmountToSend:   l.amountNeeded,
		AmountReceived: amount,
		TotalFee:       l.amountNeeded - amount,
	}
	for i := 0; i < hops; i++ {
		amountIn := l.amountAt(i)
		amountOut := l.amountAt(i + 1)
		out.Hops = append(out.Hops, Hop{
			ChannelID: fwdChans[i],
			From:      fwdNodes[i],
			To:        fwdNodes[i+1],
			AmountIn:  amountIn,
			AmountOut: amountOut,
			Fee:       amountIn - amountOut,
		})
	}
	return out, nil
}

// routeLabel is an internal backward-search state.
type routeLabel struct {
	node         NodeID
	pathNodes    []NodeID // receiver -> current node
	pathChannels []ChannelID
	amounts      []int64 // aligned with pathNodes (receiver side first)
	amountNeeded int64   // amount this node must forward towards receiver
	hops         int
	index        int
}

// amountAt returns the forward amount at forward index i. i==0 is the amount
// the sender forwards; i==hops is the amount delivered to the receiver.
// amounts is stored aligned with pathNodes (receiver first), hence the
// reverse indexing.
func (l *routeLabel) amountAt(i int) int64 {
	return l.amounts[l.hops-i]
}

// ---------------------------------------------------------------------------
// Label ordering and dominance
// ---------------------------------------------------------------------------

type labelHeap struct {
	items    []*routeLabel
	strategy RouteStrategy
}

func (h *labelHeap) Len() int { return len(h.items) }

func (h *labelHeap) Less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	switch h.strategy {
	case StrategyShortest:
		if a.hops != b.hops {
			return a.hops < b.hops
		}
		if a.amountNeeded != b.amountNeeded {
			return a.amountNeeded < b.amountNeeded
		}
	default: // StrategyCheapestFee
		if a.amountNeeded != b.amountNeeded {
			return a.amountNeeded < b.amountNeeded
		}
		if a.hops != b.hops {
			return a.hops < b.hops
		}
	}
	return pathKey(a) < pathKey(b)
}

func (h *labelHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].index = i
	h.items[j].index = j
}

func (h *labelHeap) Push(x any) {
	l := x.(*routeLabel)
	l.index = len(h.items)
	h.items = append(h.items, l)
}

func (h *labelHeap) Pop() any {
	n := len(h.items)
	l := h.items[n-1]
	h.items[n-1] = nil
	h.items = h.items[:n-1]
	return l
}

func pathKey(l *routeLabel) string {
	var b []byte
	for _, c := range l.pathChannels {
		b = append(b, c...)
		b = append(b, '/')
	}
	return string(b)
}

// dominates reports whether a is at least as good as b on both metrics and
// strictly better on at least one. This Pareto criterion is valid for both
// strategies; the heap ordering decides which non-dominated label is picked
// first for the configured strategy.
func dominates(a, b *routeLabel, strategy RouteStrategy) bool {
	_ = strategy
	return a.amountNeeded <= b.amountNeeded && a.hops <= b.hops &&
		(a.amountNeeded < b.amountNeeded || a.hops < b.hops)
}
