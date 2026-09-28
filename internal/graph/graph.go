// Package graph holds the in-memory network topology: nodes, channels and
// their balances. Mutating primitives are in-memory only; the owning
// component (settlement.Manager / Network) durably appends the corresponding
// WAL event first and then invokes the Apply* method, which makes each
// channel state change both atomic (under one write lock) and recoverable.
package graph

import (
	"sort"
	"sync"

	"github.com/guoyyit/payment-channel-router-go/internal/model"
)

// Errors returned by graph operations.
var (
	ErrNodeExists      = graphError("graph: node already exists")
	ErrNodeNotFound    = graphError("graph: node not found")
	ErrChannelExists   = graphError("graph: channel already exists")
	ErrChannelNotFound = graphError("graph: channel not found")
	ErrInsufficient    = graphError("graph: insufficient directional balance")
	ErrBadChannel      = graphError("graph: invalid channel parameters")
	ErrSameNode        = graphError("graph: channel endpoints must differ")
)

type graphError string

func (e graphError) Error() string { return string(e) }

// Edge is one directed view of a channel, used for path finding.
// Fees are charged by the *from* node (the node forwarding funds onward).
type Edge struct {
	ChannelID string
	From      string
	To        string
	Balance   model.Amount
	BaseFee   model.Amount
	FeeRate   int64
}

// reserve is an HTLC hold on one channel direction. A reservation has ALREADY
// moved amount out of From's directional balance (into a locked hold);
// settling it moves the receiver-side fee back to From, releasing it returns
// the whole amount to From. Each reserve is keyed by a unique segment id.
type reserve struct {
	ID        string
	PaymentID string
	channelID string
	From      string
	To        string
	Amount    model.Amount
}

// Graph is the in-memory channel graph.
type Graph struct {
	mu       sync.RWMutex
	nodes    map[string]*model.Node
	channels map[string]*model.Channel
	reserves map[string]*reserve // segment id -> reserve
	// nodeID -> peerID -> channelID (one channel per unordered pair).
	adj map[string]map[string]string
}

// New creates an empty graph.
func New() *Graph {
	return &Graph{
		nodes:    map[string]*model.Node{},
		channels: map[string]*model.Channel{},
		reserves: map[string]*reserve{},
		adj:      map[string]map[string]string{},
	}
}

// AddNode registers a node in memory. Caller is responsible for persistence.
func (g *Graph) AddNode(n model.Node) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if n.ID == "" {
		return ErrBadChannel
	}
	if _, ok := g.nodes[n.ID]; ok {
		return ErrNodeExists
	}
	cp := n
	g.nodes[n.ID] = &cp
	g.adj[n.ID] = map[string]string{}
	return nil
}

// AddChannel registers a channel in memory. Both endpoints must exist and
// exactly one channel per node pair is allowed.
func (g *Graph) AddChannel(c model.Channel) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.addChannelLocked(c)
}

func (g *Graph) addChannelLocked(c model.Channel) error {
	if c.ID == "" || c.NodeA == "" || c.NodeB == "" {
		return ErrBadChannel
	}
	if c.NodeA == c.NodeB {
		return ErrSameNode
	}
	if _, ok := g.channels[c.ID]; ok {
		return ErrChannelExists
	}
	if _, ok := g.nodes[c.NodeA]; !ok {
		return ErrNodeNotFound
	}
	if _, ok := g.nodes[c.NodeB]; !ok {
		return ErrNodeNotFound
	}
	if c.Capacity < 0 || c.BalanceAB < 0 || c.BalanceBA < 0 ||
		c.BalanceAB+c.BalanceBA != c.Capacity {
		return ErrBadChannel
	}
	// canonical pair ordering to keep the pair map consistent
	lo, hi := c.NodeA, c.NodeB
	if lo > hi {
		lo, hi = hi, lo
	}
	if _, ok := g.adj[lo][hi]; ok {
		return ErrChannelExists
	}
	cp := c
	g.channels[c.ID] = &cp
	g.adj[c.NodeA][c.NodeB] = c.ID
	g.adj[c.NodeB][c.NodeA] = c.ID
	return nil
}

// MoveBalance moves delta units from->to inside one channel, in memory.
// Returns ErrInsufficient when the directional balance is too low.
// Total capacity is preserved.
func (g *Graph) MoveBalance(channelID, from, to string, delta model.Amount) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.moveBalanceLocked(channelID, from, to, delta)
}

func (g *Graph) moveBalanceLocked(channelID, from, to string, delta model.Amount) error {
	if delta < 0 {
		return ErrBadChannel
	}
	if delta == 0 {
		return nil
	}
	c, ok := g.channels[channelID]
	if !ok {
		return ErrChannelNotFound
	}
	switch {
	case from == c.NodeA && to == c.NodeB:
		if c.BalanceAB < delta {
			return ErrInsufficient
		}
		c.BalanceAB -= delta
		c.BalanceBA += delta
	case from == c.NodeB && to == c.NodeA:
		if c.BalanceBA < delta {
			return ErrInsufficient
		}
		c.BalanceBA -= delta
		c.BalanceAB += delta
	default:
		return ErrBadChannel
	}
	return nil
}

// GetNode returns a copy of a node.
func (g *Graph) GetNode(id string) (model.Node, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	n, ok := g.nodes[id]
	if !ok {
		return model.Node{}, false
	}
	return *n, true
}

// GetChannel returns a copy of a channel.
func (g *Graph) GetChannel(id string) (model.Channel, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	c, ok := g.channels[id]
	if !ok {
		return model.Channel{}, false
	}
	return *c, true
}

// ChannelBetween returns the channel connecting a and b, if any.
func (g *Graph) ChannelBetween(a, b string) (model.Channel, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	lo, hi := a, b
	if lo > hi {
		lo, hi = hi, lo
	}
	id, ok := g.adj[lo][hi]
	if !ok {
		return model.Channel{}, false
	}
	return *g.channels[id], true
}

// Nodes returns all node IDs in deterministic (sorted) order.
func (g *Graph) Nodes() []string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	ids := make([]string, 0, len(g.nodes))
	for id := range g.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Channels returns all channels sorted by ID.
func (g *Graph) Channels() []model.Channel {
	g.mu.RLock()
	defer g.mu.RUnlock()
	out := make([]model.Channel, 0, len(g.channels))
	for _, c := range g.channels {
		out = append(out, *c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// EdgesFrom returns directed outgoing edges of a node, sorted by peer id so
// path search expansion is deterministic.
func (g *Graph) EdgesFrom(node string) []Edge {
	g.mu.RLock()
	defer g.mu.RUnlock()
	n, ok := g.nodes[node]
	if !ok {
		return nil
	}
	peers := g.adj[node]
	peerIDs := make([]string, 0, len(peers))
	for p := range peers {
		peerIDs = append(peerIDs, p)
	}
	sort.Strings(peerIDs)
	edges := make([]Edge, 0, len(peerIDs))
	for _, p := range peerIDs {
		ch := g.channels[peers[p]]
		bal, ok := ch.ForwardBalance(node, p)
		if !ok {
			continue
		}
		edges = append(edges, Edge{
			ChannelID: ch.ID,
			From:      node,
			To:        p,
			Balance:   bal,
			BaseFee:   n.Policy.BaseFee,
			FeeRate:   n.Policy.FeeRate,
		})
	}
	return edges
}

// ApplyNodeRegistered is used during WAL replay.
func (g *Graph) ApplyNodeRegistered(p model.NodeRegistered) {
	_ = g.AddNode(p.Node) // replay is idempotent-ish; node identity never changes
}

// ApplyChannelOpened is used during WAL replay.
func (g *Graph) ApplyChannelOpened(p model.ChannelOpened) {
	_ = g.AddChannel(p.Channel)
}

// ApplyBalanceChanged moves delta from->to inside one channel. During replay
// the event was previously validated, so it must succeed.
func (g *Graph) ApplyBalanceChanged(p model.ChannelBalanceChanged) error {
	return g.MoveBalance(p.ChannelID, p.From, p.To, p.Delta)
}
