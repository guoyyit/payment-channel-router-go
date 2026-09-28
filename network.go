// Package pctr is the entry point of the local payment channel network
// simulator. It wires topology management, routing and HTLC settlement
// together with durable local persistence (an append-only write-ahead log on
// disk). No external blockchain or network service is required.
package pctr

import (
	"sync"

	"github.com/guoyyit/payment-channel-router-go/internal/graph"
	"github.com/guoyyit/payment-channel-router-go/internal/model"
	"github.com/guoyyit/payment-channel-router-go/internal/router"
	"github.com/guoyyit/payment-channel-router-go/internal/settlement"
	"github.com/guoyyit/payment-channel-router-go/internal/store"
)

// Config configures a Network.
type Config struct {
	DataDir       string
	Strategy      router.Strategy
	MaxHops       int
	MaxFee        int64
	MaxConcurrent int
	LockDuration  int64 // seconds
	HopGrace      int64 // seconds per downstream hop
	// Clock optionally overrides wall time (nil = system clock).
	Clock settlement.Clock
}

// Network is the payment channel network simulator.
type Network struct {
	mu     sync.Mutex
	store  *store.Store
	graph  *graph.Graph
	router *router.Router
	mgr    *settlement.Manager
}

// Open opens a network, replaying persisted state from cfg.DataDir and
// resuming or safely rolling back any payment interrupted by a crash.
func Open(cfg Config) (*Network, error) {
	st, err := store.New(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	g := graph.New()
	r := router.New(g, router.Options{
		Strategy: cfg.Strategy,
		MaxHops:  cfg.MaxHops,
		MaxFee:   cfg.MaxFee,
	})
	mgr := settlement.New(g, r, st, cfg.Clock, settlement.Options{
		MaxHops:       cfg.MaxHops,
		MaxFee:        cfg.MaxFee,
		MaxConcurrent: cfg.MaxConcurrent,
		LockDuration:  cfg.LockDuration,
		HopGrace:      cfg.HopGrace,
	})

	// Replay: build graph and payments from the log without new appends.
	mgr.BeginReplay()
	err = st.Replay(func(ev store.Event) error {
		return mgr.ApplyEvent(ev)
	})
	mgr.EndReplay()
	if err != nil {
		_ = st.Close()
		return nil, err
	}
	// Resume/revert interrupted payments.
	if err := mgr.Recover(); err != nil {
		_ = st.Close()
		return nil, err
	}
	return &Network{store: st, graph: g, router: r, mgr: mgr}, nil
}

// Close flushes the log and releases resources.
func (n *Network) Close() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.store == nil {
		return nil
	}
	err := n.store.Close()
	n.store = nil
	return err
}

// RegisterNode registers a node and durably persists it.
func (n *Network) RegisterNode(id string, policy model.FeePolicy) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	node := model.Node{ID: id, Policy: policy}
	if _, err := n.store.Append(store.KindNodeRegistered,
		model.NodeRegistered{Node: node}); err != nil {
		return err
	}
	return n.graph.AddNode(node)
}

// OpenChannel opens a channel with an initial directional balance balanceAB
// (A->B); balanceBA = capacity - balanceAB. Persisted atomically.
func (n *Network) OpenChannel(id, a, b string, capacity, balanceAB int64) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	ch := model.Channel{
		ID: id, NodeA: a, NodeB: b, Capacity: capacity,
		BalanceAB: balanceAB, BalanceBA: capacity - balanceAB,
	}
	if _, err := n.store.Append(store.KindChannelOpened,
		model.ChannelOpened{Channel: ch}); err != nil {
		return err
	}
	return n.graph.AddChannel(ch)
}

// Graph exposes the topology for queries.
func (n *Network) Graph() *graph.Graph { return n.graph }

// Router exposes the path finder.
func (n *Network) Router() *router.Router { return n.router }

// Manager exposes the settlement manager.
func (n *Network) Manager() *settlement.Manager { return n.mgr }

// Store exposes the WAL (mostly for tests/tooling).
func (n *Network) Store() *store.Store { return n.store }
