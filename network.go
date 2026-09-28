package pchr

import (
	"sort"
	"sync"
)

// Network manages registered nodes, channels and their balances.
//
// Every channel state mutation is serialized per channel; channels are
// always locked in ascending id order to avoid lock-ordering deadlocks.
// Each mutation is persisted to the WAL (as a full channel-state record)
// before it becomes visible, so channel state changes are atomic and
// recoverable.
type Network struct {
	mu       sync.RWMutex
	nodes    map[NodeID]*Node
	channels map[ChannelID]*Channel
	// edges indexes channels by unordered endpoint pair.
	edges map[nodePair][]ChannelID
	locks map[ChannelID]*sync.RWMutex
	store Store
}

type nodePair struct{ a, b NodeID }

func makePair(a, b NodeID) nodePair {
	if a <= b {
		return nodePair{a, b}
	}
	return nodePair{b, a}
}

// NewNetwork creates a network backed by store (nil means ephemeral).
func NewNetwork(store Store) *Network {
	return &Network{
		nodes:    map[NodeID]*Node{},
		channels: map[ChannelID]*Channel{},
		edges:    map[nodePair][]ChannelID{},
		locks:    map[ChannelID]*sync.RWMutex{},
		store:    store,
	}
}

// RegisterNode adds a node. It is an error to register the same id twice.
func (n *Network) RegisterNode(id NodeID, address string, meta map[string]string) error {
	if id == "" {
		return ErrInvalidArgument
	}
	node := &Node{ID: id, Address: address, Meta: cloneMeta(meta)}

	n.mu.Lock()
	if _, exists := n.nodes[id]; exists {
		n.mu.Unlock()
		return ErrNodeExists
	}
	n.nodes[id] = node
	if err := n.append(RecNodeRegistered, NodeRegistered{Node: *node}); err != nil {
		delete(n.nodes, id)
		n.mu.Unlock()
		return err
	}
	n.mu.Unlock()
	return nil
}

func cloneMeta(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// GetNode returns a copy of a registered node.
func (n *Network) GetNode(id NodeID) (*Node, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	node, ok := n.nodes[id]
	if !ok {
		return nil, ErrNodeNotFound
	}
	cp := *node
	return &cp, nil
}

// ListNodes returns all registered nodes ordered by id for determinism.
func (n *Network) ListNodes() []Node {
	n.mu.RLock()
	out := make([]Node, 0, len(n.nodes))
	for _, node := range n.nodes {
		out = append(out, *node)
	}
	n.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// OpenChannel registers a channel between two registered nodes.
func (n *Network) OpenChannel(ch Channel) error {
	if ch.ID == "" || ch.PeerA == "" || ch.PeerB == "" {
		return ErrInvalidArgument
	}
	if ch.PeerA == ch.PeerB {
		return ErrInvalidArgument
	}
	if ch.BalanceA < 0 || ch.BalanceB < 0 {
		return ErrInvalidArgument
	}
	if ch.FeePolicy.BaseFee < 0 || ch.FeePolicy.FeeRateMillionths < 0 {
		return ErrInvalidArgument
	}
	n.mu.Lock()
	defer n.mu.Unlock()

	if _, exists := n.channels[ch.ID]; exists {
		return ErrChannelExists
	}
	if _, ok := n.nodes[ch.PeerA]; !ok {
		return ErrNodeNotFound
	}
	if _, ok := n.nodes[ch.PeerB]; !ok {
		return ErrNodeNotFound
	}
	cp := ch
	cp.Active = true
	cp.LockedA, cp.LockedB = 0, 0

	if err := n.appendLocked(RecChannelOpened, ChannelOpened{Channel: cp}); err != nil {
		return err
	}
	n.channels[ch.ID] = &cp
	pair := makePair(ch.PeerA, ch.PeerB)
	n.edges[pair] = append(n.edges[pair], ch.ID)
	n.locks[ch.ID] = &sync.RWMutex{}
	return nil
}

// GetChannel returns a consistent snapshot copy of a channel.
func (n *Network) GetChannel(id ChannelID) (*Channel, error) {
	n.mu.RLock()
	ch, ok := n.channels[id]
	l := n.locks[id]
	n.mu.RUnlock()
	if !ok {
		return nil, ErrChannelNotFound
	}
	l.RLock()
	cp := *ch
	l.RUnlock()
	return &cp, nil
}

// ListChannels returns every channel ordered by id.
func (n *Network) ListChannels() []Channel {
	n.mu.RLock()
	out := make([]Channel, 0, len(n.channels))
	for _, ch := range n.channels {
		out = append(out, *ch)
	}
	n.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// ChannelsBetween lists active channel ids connecting two nodes, sorted.
func (n *Network) ChannelsBetween(a, b NodeID) []ChannelID {
	n.mu.RLock()
	defer n.mu.RUnlock()
	ids := n.edges[makePair(a, b)]
	out := make([]ChannelID, 0, len(ids))
	for _, id := range ids {
		if n.channels[id].Active {
			out = append(out, id)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// ChannelsFrom returns all active channel ids incident to node, sorted.
func (n *Network) ChannelsFrom(node NodeID) []ChannelID {
	n.mu.RLock()
	var out []ChannelID
	for pair, ids := range n.edges {
		if pair.a != node && pair.b != node {
			continue
		}
		for _, id := range ids {
			if n.channels[id].Active {
				out = append(out, id)
			}
		}
	}
	n.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// reserve moves amount from spendable to locked in the direction from->to.
// It mutates in-memory state only; durability is the caller's responsibility
// (the payment manager writes one atomic WAL record covering every reserve).
// The caller MUST hold the per-channel lock.
func (n *Network) reserve(id ChannelID, from NodeID, amount int64) error {
	if amount <= 0 {
		return ErrInvalidArgument
	}
	ch, err := n.getChannelPtr(id)
	if err != nil {
		return err
	}
	spendable, _, ok := ch.Balances(from)
	if !ok {
		return ErrInvalidArgument
	}
	if spendable < amount {
		return ErrInsufficientFunds
	}
	if from == ch.PeerA {
		ch.LockedA += amount
	} else {
		ch.LockedB += amount
	}
	return nil
}

// settle commits a previously reserved hop: the full HTLC amount moves from
// the sender side to the receiver side within the channel (balances and
// locks). A forwarding node's fee is NOT deducted inside this channel; it is
// the difference between the amount it receives on its incoming channel and
// the amount it forwards on its outgoing channel, so it emerges across the
// pair of channels. The fee argument is validated for consistency only.
// It mutates in-memory state only; the caller persists one atomic WAL record
// containing the resulting channel snapshot.
// The caller MUST hold the per-channel lock.
func (n *Network) settle(id ChannelID, from NodeID, amount int64, fee int64) error {
	if amount <= 0 || fee < 0 || fee > amount {
		return ErrInvalidArgument
	}
	_ = fee
	ch, err := n.getChannelPtr(id)
	if err != nil {
		return err
	}
	switch from {
	case ch.PeerA:
		if ch.LockedA < amount {
			return ErrPaymentWrongState
		}
		ch.LockedA -= amount
		ch.BalanceA -= amount
		ch.BalanceB += amount
	case ch.PeerB:
		if ch.LockedB < amount {
			return ErrPaymentWrongState
		}
		ch.LockedB -= amount
		ch.BalanceB -= amount
		ch.BalanceA += amount
	default:
		return ErrInvalidArgument
	}
	return nil
}

// refundReserve releases a previously reserved amount back to spendable
// without changing either party's settled balance. In-memory state only.
// The caller MUST hold the per-channel lock.
func (n *Network) refundReserve(id ChannelID, from NodeID, amount int64) error {
	if amount <= 0 {
		return ErrInvalidArgument
	}
	ch, err := n.getChannelPtr(id)
	if err != nil {
		return err
	}
	switch from {
	case ch.PeerA:
		if ch.LockedA < amount {
			return ErrPaymentWrongState
		}
		ch.LockedA -= amount
	case ch.PeerB:
		if ch.LockedB < amount {
			return ErrPaymentWrongState
		}
		ch.LockedB -= amount
	default:
		return ErrInvalidArgument
	}
	return nil
}

func (n *Network) getChannelPtr(id ChannelID) (*Channel, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	ch, ok := n.channels[id]
	if !ok {
		return nil, ErrChannelNotFound
	}
	return ch, nil
}

func (n *Network) append(recType RecordType, payload any) error {
	if n.store == nil {
		return nil
	}
	_, err := n.store.Append(recType, payload)
	return err
}

// appendLocked must be called while holding n.mu.
func (n *Network) appendLocked(recType RecordType, payload any) error {
	if n.store == nil {
		return nil
	}
	_, err := n.store.Append(recType, payload)
	return err
}

// snapshotChannelsLocked returns copies of the given channels. The caller
// MUST hold the per-channel locks so the snapshots are a consistent state
// captured together with an in-memory mutation.
func (n *Network) snapshotChannelsLocked(ids []ChannelID) []Channel {
	out := make([]Channel, 0, len(ids))
	for _, id := range uniqueSorted(ids) {
		if ch, err := n.getChannelPtr(id); err == nil {
			out = append(out, *ch)
		}
	}
	return out
}

// lockChannels locks all given channels in ascending id order. Duplicate ids
// are locked once. The returned function releases them in reverse order.
func (n *Network) lockChannels(ids []ChannelID) (func(), error) {
	uniq := uniqueSorted(ids)

	n.mu.RLock()
	locks := make([]*sync.RWMutex, 0, len(uniq))
	for _, id := range uniq {
		l, ok := n.locks[id]
		if !ok {
			n.mu.RUnlock()
			return nil, ErrChannelNotFound
		}
		locks = append(locks, l)
	}
	n.mu.RUnlock()

	for _, l := range locks {
		l.Lock()
	}
	return func() {
		for i := len(locks) - 1; i >= 0; i-- {
			locks[i].Unlock()
		}
	}, nil
}

func uniqueSorted(ids []ChannelID) []ChannelID {
	if len(ids) == 0 {
		return nil
	}
	cp := append([]ChannelID(nil), ids...)
	sort.Slice(cp, func(i, j int) bool { return cp[i] < cp[j] })
	out := cp[:0]
	for _, id := range cp {
		if len(out) == 0 || out[len(out)-1] != id {
			out = append(out, id)
		}
	}
	return out
}

// restoreChannel is used during WAL replay to install channel state.
func (n *Network) restoreChannel(ch Channel) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, exists := n.channels[ch.ID]; !exists {
		n.locks[ch.ID] = &sync.RWMutex{}
		pair := makePair(ch.PeerA, ch.PeerB)
		n.edges[pair] = append(n.edges[pair], ch.ID)
	}
	cp := ch
	n.channels[ch.ID] = &cp
}

// restoreNode is used during WAL replay.
func (n *Network) restoreNode(node Node) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, exists := n.nodes[node.ID]; !exists {
		n.nodes[node.ID] = &node
	}
}
