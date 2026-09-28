// Package router finds multi-hop payment paths over the channel graph.
//
// Path pricing. A route sender=n0 -> n1 -> ... -> nk=receiver consists of k
// channel segments with amounts a_0..a_(k-1), where a_(k-1) equals the
// payment amount. Segment i crosses channel (ni, n(i+1)) carrying a_i.
// The node that RECEIVES a segment and forwards it onward (n(i+1), i<k-1)
// charges its fee:
//
//	a_i = a_(i+1) + fee_(i+1)(a_(i+1)),
//	fee = base + ceil(amount*rate/1e6).
//
// The sender n0 charges nothing. The search therefore runs BACKWARDS from
// the receiver (required amount = payment amount) toward the sender, and at
// each extension the CURRENT node's own fee is added.
//
// A segment is usable only when the sender-directional channel balance
// covers its amount. Results are deterministic: ties are broken per the
// configured strategy and finally by the lexicographically smallest path.
package router

import (
	"container/heap"
	"sort"

	"github.com/guoyyit/payment-channel-router-go/internal/graph"
	"github.com/guoyyit/payment-channel-router-go/internal/model"
)

// Strategy selects the path ranking policy.
type Strategy int

const (
	// LowestFee minimizes the total forwarding fee, then hop count, then path.
	LowestFee Strategy = iota
	// ShortestPath minimizes the hop count, then fee, then path.
	ShortestPath
)

// Options configures a Router.
type Options struct {
	Strategy Strategy
	MaxHops  int
	MaxFee   model.Amount
}

// Hop describes one traversed channel and the amount that must enter it.
type Hop struct {
	ChannelID string
	From      string
	To        string
	// AmountToForward is the amount leaving this hop's sender side.
	AmountToForward model.Amount
	// Fee charged by the From node for forwarding (0 on the first hop).
	Fee model.Amount
}

// Route is a fully priced path from sender to receiver.
type Route struct {
	Path []string
	Hops []Hop
	// TotalFee is the sum of per-hop forwarding fees.
	TotalFee model.Amount
	// TotalDebit is amount + TotalFee charged to the sender.
	TotalDebit model.Amount
}

// Router searches routes.
type Router struct {
	g *graph.Graph
	o Options
}

// New constructs a Router.
func New(g *graph.Graph, opts Options) *Router {
	if opts.MaxHops <= 0 {
		opts.MaxHops = 20
	}
	return &Router{g: g, o: opts}
}

// fee computes the forwarding fee charged for delivering amount onward.
func fee(p model.FeePolicy, amount model.Amount) model.Amount {
	f := p.BaseFee
	if p.FeeRate > 0 {
		f += (amount*p.FeeRate + 1_000_000 - 1) / 1_000_000
	}
	return f
}

type edgeEx struct {
	from   string
	chanID string
	bal    model.Amount
}

type state struct {
	node string
	hops int // segments from this node to the receiver
}

type label struct {
	need  model.Amount // amount this node's predecessor must send
	path  []string     // node ... receiver (forward order)
	chans []string     // channel ids in forward order (len == hops)
}

type pqItem struct {
	st  state
	lab label
}

type pq struct {
	items []pqItem
	r     *Router
}

func (p *pq) Len() int { return len(p.items) }
func (p *pq) Less(i, j int) bool {
	a, b := p.items[i], p.items[j]
	return p.r.labelLess(a.lab, b.lab)
}
func (p *pq) Swap(i, j int) { p.items[i], p.items[j] = p.items[j], p.items[i] }
func (p *pq) Push(x any)    { p.items = append(p.items, x.(pqItem)) }
func (p *pq) Pop() any {
	old := p.items
	n := len(old)
	x := old[n-1]
	p.items = old[:n-1]
	return x
}

// labelLess gives a total deterministic order over labels under the strategy.
// For any node the receiver amount is fixed, so a smaller required amount
// equals strictly smaller accumulated downstream fees.
func (r *Router) labelLess(a, b label) bool {
	switch r.o.Strategy {
	case ShortestPath:
		if len(a.chans) != len(b.chans) {
			return len(a.chans) < len(b.chans)
		}
		if a.need != b.need {
			return a.need < b.need
		}
	default: // LowestFee
		if a.need != b.need {
			return a.need < b.need
		}
		if len(a.chans) != len(b.chans) {
			return len(a.chans) < len(b.chans)
		}
	}
	return lexLess(a.path, b.path)
}

func lexLess(a, b []string) bool {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] != b[i] {
			return a[i] < b[i]
		}
	}
	return len(a) < len(b)
}

// FindRoute finds a deterministic feasible route carrying amount from sender
// to receiver, respecting max hops and fee cap.
func (r *Router) FindRoute(sender, receiver string, amount model.Amount) (*Route, error) {
	if amount <= 0 {
		return nil, ErrInvalidAmount
	}
	if sender == receiver {
		return nil, ErrSameNode
	}
	if _, ok := r.g.GetNode(sender); !ok {
		return nil, graph.ErrNodeNotFound
	}
	if _, ok := r.g.GetNode(receiver); !ok {
		return nil, graph.ErrNodeNotFound
	}

	// Build reverse adjacency: node -> incoming edges (x -> node), sorted
	// by predecessor id for deterministic tie handling.
	rev := map[string][]edgeEx{}
	for _, ch := range r.g.Channels() {
		directions := [][2]string{{ch.NodeA, ch.NodeB}, {ch.NodeB, ch.NodeA}}
		for _, d := range directions {
			from, to := d[0], d[1]
			bal, _ := ch.ForwardBalance(from, to)
			rev[to] = append(rev[to], edgeEx{
				from: from, chanID: ch.ID, bal: bal,
			})
		}
	}
	for k := range rev {
		sort.Slice(rev[k], func(i, j int) bool { return rev[k][i].from < rev[k][j].from })
	}

	best := map[state]label{}
	start := state{node: receiver, hops: 0}
	startLab := label{need: amount, path: []string{receiver}}
	best[start] = startLab
	q := &pq{r: r, items: []pqItem{{st: start, lab: startLab}}}
	heap.Init(q)

	// bestSender holds the best label reaching the sender (any hop count).
	var bestSender *label
	consider := func(lab label) {
		if bestSender == nil || r.labelLess(lab, *bestSender) {
			cp := lab
			bestSender = &cp
		}
	}

	for q.Len() > 0 {
		it := heap.Pop(q).(pqItem)
		if cur, ok := best[it.st]; !ok || !sameLabel(cur, it.lab) {
			continue
		}
		if it.st.node == sender {
			consider(it.lab)
			continue
		}
		if it.st.hops >= r.o.MaxHops {
			continue
		}
		for _, e := range rev[it.st.node] {
			// Extend backwards across (e.from -> curNode). The amount the
			// current node receives on the new segment must cover what it
			// sends onward PLUS its own forwarding fee (the receiver itself
			// charges no forwarding fee).
			var need model.Amount
			curNode, _ := r.g.GetNode(it.st.node)
			if it.st.node == receiver {
				need = it.lab.need
			} else {
				need = it.lab.need + fee(curNode.Policy, it.lab.need)
			}
			if need > e.bal {
				continue
			}
			// The fee cap is enforced on the FINAL route only: a longer
			// backward prefix may exceed the cap even though the best path
			// to the sender later does not.
			nst := state{node: e.from, hops: it.st.hops + 1}
			nlab := label{
				need:  need,
				path:  append([]string{e.from}, it.lab.path...),
				chans: append(append([]string{}, e.chanID), it.lab.chans...),
			}
			if old, ok := best[nst]; ok && !r.labelLess(nlab, old) {
				continue
			}
			best[nst] = nlab
			heap.Push(q, pqItem{st: nst, lab: nlab})
		}
	}

	if bestSender == nil {
		return nil, ErrNoRoute
	}

	lab := *bestSender
	totalDebit := lab.need
	totalFee := totalDebit - amount
	if totalFee > r.o.MaxFee {
		return nil, ErrFeeExceeded
	}

	// Build forward hops with per-hop amounts and per-hop fees.
	needed := make([]model.Amount, len(lab.path))
	needed[len(needed)-1] = amount
	for i := len(lab.path) - 2; i >= 0; i-- {
		// node i+1 receives needed[i+1] on segment i and charges its fee
		// before forwarding needed[i+1] on segment i+1.
		n, _ := r.g.GetNode(lab.path[i+1])
		needed[i] = needed[i+1] + fee(n.Policy, needed[i+1])
	}
	hops := make([]Hop, len(lab.chans))
	for i := 0; i < len(lab.chans); i++ {
		var f model.Amount
		if i < len(lab.chans)-1 {
			f = needed[i] - needed[i+1]
		}
		hops[i] = Hop{
			ChannelID:       lab.chans[i],
			From:            lab.path[i],
			To:              lab.path[i+1],
			AmountToForward: needed[i],
			Fee:             f,
		}
	}
	return &Route{
		Path:       append([]string{}, lab.path...),
		Hops:       hops,
		TotalFee:   totalFee,
		TotalDebit: totalDebit,
	}, nil
}

func sameLabel(a, b label) bool {
	if a.need != b.need || len(a.path) != len(b.path) || len(a.chans) != len(b.chans) {
		return false
	}
	for i := range a.path {
		if a.path[i] != b.path[i] {
			return false
		}
	}
	for i := range a.chans {
		if a.chans[i] != b.chans[i] {
			return false
		}
	}
	return true
}
