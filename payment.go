package pchr

import (
	"crypto/sha256"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Manager coordinates multi-hop atomic payments over a Network.
//
// Settlement model (hash-time-lock, locally simulated):
//
//  1. The sender chooses a preimage; H = SHA256(preimage).
//  2. Every hop along the route reserves its incoming amount in the channel
//     (balance moves from spendable to locked). Reserves are acquired with
//     all channel locks held in id order, so concurrent payments cannot
//     double-spend or oversell a channel.
//  3. Once every hop is reserved the payment is "locked" and durably recorded.
//     At this point no node has settled both legs, so no intermediate node
//     has advanced funds it cannot recover.
//  4. Settle: after verifying the preimage matches H, hops settle forward
//     (0 -> n). Each hop is a separate durable hop_settled record.
//  5. On timeout (before the preimage is revealed) every reserve is released
//     in reverse hop order (n-1 -> 0), each as a durable hop_refunded record.
//
// Both forward settlement and reverse refunding are resumable from the WAL
// after a crash: hop_settled / hop_refunded records mark exactly which hops
// already moved, so a hop never settles twice and a released reserve is
// never released twice.
type Manager struct {
	mu       sync.Mutex
	net      *Network
	router   *Router
	store    Store
	cfg      RoutingConfig
	payments map[PaymentID]*Payment
	byHash   map[Hash]PaymentID
	inFlight int
	counter  int64
	now      func() time.Time
	// crashAfterHop, when > 0, simulates a process crash immediately after
	// the hop with that 1-based index has been durably settled. The hook is
	// one-shot: it clears itself before panicking. Test only.
	crashAfterHop int
	// crashAfterRefundK simulates a crash after the k-th reverse-order
	// refund (0-based ordinal within the refund loop). One-shot. Test only.
	crashAfterRefundK int
}

// NewManager creates a payment manager.
func NewManager(net *Network, router *Router, store Store, cfg RoutingConfig) *Manager {
	if router == nil {
		router = NewRouter(net, cfg)
	}
	return &Manager{
		net:               net,
		router:            router,
		store:             store,
		cfg:               cfg,
		payments:          map[PaymentID]*Payment{},
		byHash:            map[Hash]PaymentID{},
		now:               time.Now,
		crashAfterRefundK: -1,
	}
}

// SetClock overrides the time source (mainly for tests).
func (m *Manager) SetClock(now func() time.Time) {
	m.mu.Lock()
	m.now = now
	m.mu.Unlock()
}

func (m *Manager) hashOf(preimage [32]byte) Hash {
	return sha256.Sum256(preimage[:])
}

// SendPayment finds a route, atomically reserves every hop and settles the
// payment by revealing the preimage. On any failure before the payment
// becomes "locked" all partial reserves are released and channel state is
// left untouched otherwise.
func (m *Manager) SendPayment(sender, receiver NodeID, amount int64, preimage [32]byte) (*Payment, error) {
	if amount <= 0 {
		return nil, ErrInvalidArgument
	}

	route, err := m.router.FindRoute(sender, receiver, amount)
	if err != nil {
		return nil, err
	}
	if m.cfg.MaxHops > 0 && len(route.Hops) > m.cfg.MaxHops {
		return nil, ErrTooManyHops
	}
	if m.cfg.MaxTotalFee > 0 && route.TotalFee > m.cfg.MaxTotalFee {
		return nil, ErrFeeExceeded
	}
	if sha256.Sum256(preimage[:]) == (Hash{}) {
		return nil, ErrInvalidArgument
	}

	p, err := m.lockPaymentLocked(sender, receiver, amount, route, preimage)
	if err != nil {
		return nil, err
	}
	if err := m.settlePayment(p, preimage); err != nil {
		return nil, err
	}
	return m.GetPayment(p.ID)
}

// LockPayment finds a route and atomically reserves every hop, leaving the
// payment in the "locked" state without settling it. It is used when the
// hash lock reveal must happen separately from reservation (for example to
// test or exercise timeout refunds); normal callers use SendPayment.
func (m *Manager) LockPayment(sender, receiver NodeID, amount int64, preimage [32]byte) (*Payment, error) {
	if amount <= 0 {
		return nil, ErrInvalidArgument
	}
	route, err := m.router.FindRoute(sender, receiver, amount)
	if err != nil {
		return nil, err
	}
	if m.cfg.MaxHops > 0 && len(route.Hops) > m.cfg.MaxHops {
		return nil, ErrTooManyHops
	}
	if m.cfg.MaxTotalFee > 0 && route.TotalFee > m.cfg.MaxTotalFee {
		return nil, ErrFeeExceeded
	}
	if m.hashOf(preimage) == (Hash{}) {
		return nil, ErrInvalidArgument
	}
	p, err := m.lockPaymentLocked(sender, receiver, amount, route, preimage)
	if err != nil {
		return nil, err
	}
	return m.GetPayment(p.ID)
}

// lockPaymentLocked creates the payment record and reserves all hops.
// m.mu is released before channel locks are taken so that this path and the
// settlement path (channel locks first, m.mu only briefly) can never form an
// AB-BA deadlock.
func (m *Manager) lockPaymentLocked(sender, receiver NodeID, amount int64, route *Route, preimage [32]byte) (*Payment, error) {
	m.mu.Lock()
	if m.cfg.MaxConcurrent > 0 && m.inFlight >= m.cfg.MaxConcurrent {
		m.mu.Unlock()
		return nil, ErrTooManyConcurrent
	}
	m.counter++
	id := PaymentID(fmt.Sprintf("pay-%06d", m.counter))
	now := m.now()
	p := &Payment{
		ID:          id,
		Sender:      sender,
		Receiver:    receiver,
		Amount:      amount,
		Status:      PaymentPending,
		Route:       *route,
		PaymentHash: m.hashOf(preimage),
		ExpiresAt:   now.Add(m.cfg.LockDuration),
		CreatedAt:   now,
	}
	m.payments[id] = p
	m.byHash[p.PaymentHash] = id
	m.inFlight++
	m.mu.Unlock()

	// Reserve every hop atomically (all channel locks held in id order).
	chanIDs := make([]ChannelID, len(route.Hops))
	for i, h := range route.Hops {
		chanIDs[i] = h.ChannelID
	}
	unlock, err := m.net.lockChannels(chanIDs)
	if err != nil {
		m.failPayment(p)
		return nil, err
	}
	reserved := 0
	for _, h := range route.Hops {
		if err := m.net.reserve(h.ChannelID, h.From, h.AmountIn); err != nil {
			for j := reserved - 1; j >= 0; j-- {
				rh := route.Hops[j]
				_ = m.net.refundReserve(rh.ChannelID, rh.From, rh.AmountIn)
			}
			unlock()
			m.failPayment(p)
			return nil, err
		}
		reserved++
	}

	// All reserves are in memory. Persist payment + every post-reserve
	// channel snapshot as a SINGLE atomic record: a crash can never leave
	// reserved balances without their payment (lost funds) or a payment
	// without reserves (oversell on recovery).
	snapshots := m.net.snapshotChannelsLocked(chanIDs)
	m.mu.Lock()
	p.Status = PaymentLocked
	err = m.append(RecPaymentLocked, PaymentLockedPayload{Payment: *p, Snapshots: snapshots})
	m.mu.Unlock()
	unlock()
	if err != nil {
		m.releaseAll(p)
		m.failPayment(p)
		return nil, err
	}
	return p, nil
}

// SettlePayment starts or resumes forward settlement of a locked payment
// using the revealed preimage. Used to complete a payment that crashed while
// in the "locked" or "settling" state.
func (m *Manager) SettlePayment(id PaymentID, preimage [32]byte) (*Payment, error) {
	m.mu.Lock()
	p := m.payments[id]
	m.mu.Unlock()
	if p == nil {
		return nil, ErrPaymentNotFound
	}
	if m.hashOf(preimage) != p.PaymentHash {
		return nil, ErrPreimageMismatch
	}
	if err := m.settlePayment(p, preimage); err != nil {
		return nil, err
	}
	return m.GetPayment(id)
}

// RefundPayment cancels an expired payment that is still fully locked
// (preimage not yet revealed), releasing every reserve in reverse hop
// order. Once settlement has started the payment can only be completed
// forward, so a settling payment cannot be refunded.
func (m *Manager) RefundPayment(id PaymentID) (*Payment, error) {
	m.mu.Lock()
	p := m.payments[id]
	now := m.now()
	m.mu.Unlock()
	if p == nil {
		return nil, ErrPaymentNotFound
	}
	if p.Status == PaymentRefunded {
		return m.GetPayment(id) // idempotent
	}
	if p.Status != PaymentLocked && p.Status != PaymentRefunding {
		return nil, ErrPaymentWrongState
	}
	// An already-started refund (crash mid-refund) is always finished even if
	// the restart clock disagrees about expiry; a fresh refund needs expiry.
	if p.RefundedCount == 0 && now.Before(p.ExpiresAt) {
		return nil, ErrPaymentWrongState
	}
	if err := m.refundPayment(p); err != nil {
		return nil, err
	}
	return m.GetPayment(id)
}

// GetPayment returns a copy of a payment.
func (m *Manager) GetPayment(id PaymentID) (*Payment, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.payments[id]
	if !ok {
		return nil, ErrPaymentNotFound
	}
	cp := *p
	return &cp, nil
}

// ListPayments returns all known payments ordered by id.
func (m *Manager) ListPayments() []Payment {
	m.mu.Lock()
	out := make([]Payment, 0, len(m.payments))
	for _, p := range m.payments {
		out = append(out, *p)
	}
	m.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// InFlight returns the number of payments not in a terminal state.
func (m *Manager) InFlight() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.inFlight
}

func (m *Manager) append(recType RecordType, payload any) error {
	if m.store == nil {
		return nil
	}
	_, err := m.store.Append(recType, payload)
	return err
}

// settlePayment performs forward settlement for hops [SettledHops..end).
// Safe to call repeatedly: already-settled hops are skipped using the
// durable SettledHops cursor.
func (m *Manager) settlePayment(p *Payment, preimage [32]byte) error {
	// Phase 1: validate and durably publish the preimage. This record changes
	// no channel state, so it needs no channel locks.
	m.mu.Lock()
	if m.hashOf(preimage) != p.PaymentHash {
		m.mu.Unlock()
		return ErrPreimageMismatch
	}
	switch p.Status {
	case PaymentSucceeded:
		m.mu.Unlock()
		return nil
	case PaymentLocked:
		if !m.now().Before(p.ExpiresAt) {
			m.mu.Unlock()
			return ErrPaymentExpired
		}
		p.Status = PaymentSettling
		p.Preimage = append([]byte(nil), preimage[:]...)
		if err := m.append(RecPaymentSettling, PaymentSettlingPayload{
			PaymentID: p.ID, Preimage: append([]byte(nil), preimage[:]...),
		}); err != nil {
			p.Status = PaymentLocked
			p.Preimage = nil
			m.mu.Unlock()
			return err
		}
	case PaymentSettling:
		// resuming after restart: preimage was already published
	default:
		m.mu.Unlock()
		return ErrPaymentWrongState
	}
	hops := append([]Hop(nil), p.Route.Hops...)
	from := p.SettledHops
	m.mu.Unlock()

	// Phase 2: settle each hop forward. For every hop the in-memory mutation
	// and the durable (cursor + channel snapshot) record are published as one
	// unit while both locks are held, so a crash can never observe one
	// without the other: a hop settles at most once.
	chanIDs := make([]ChannelID, 0, len(hops)-from)
	for i := from; i < len(hops); i++ {
		chanIDs = append(chanIDs, hops[i].ChannelID)
	}
	unlock, err := m.net.lockChannels(chanIDs)
	if err != nil {
		return err
	}
	defer unlock()

	for i := from; i < len(hops); i++ {
		h := hops[i]
		if err := m.net.settle(h.ChannelID, h.From, h.AmountIn, h.Fee); err != nil {
			return err
		}
		ch, err := m.net.getChannelPtr(h.ChannelID)
		if err != nil {
			return err
		}
		snapshot := *ch
		m.mu.Lock()
		p.SettledHops = i + 1
		err = m.append(RecHopSettled, HopSettledPayload{
			PaymentID: p.ID, HopIndex: i + 1, Snapshot: snapshot,
		})
		m.mu.Unlock()
		if err != nil {
			return err
		}
		if m.crashAfterHop == i+1 {
			m.crashAfterHop = 0
			panic(simulatedCrash)
		}
	}

	m.mu.Lock()
	p.Status = PaymentSucceeded
	m.inFlight--
	err = m.append(RecPaymentSucceeded, PaymentSucceededPayload{PaymentID: p.ID})
	m.mu.Unlock()
	return err
}

// simulatedCrash is the panic value used by crash injection tests.
type simulatedCrashType struct{}

func (simulatedCrashType) String() string { return "simulated node crash" }

var simulatedCrash = simulatedCrashType{}

// refundPayment releases all reserves in reverse hop order. Each release and
// its durable (cursor + channel snapshot) record are one atomic unit, so a
// crashed refund resumes exactly at the first still-reserved hop.
func (m *Manager) refundPayment(p *Payment) error {
	m.mu.Lock()
	switch p.Status {
	case PaymentRefunded, PaymentFailed:
		// Idempotent: a completed refund is reported as success so both
		// callers and crash recovery can retry safely.
		m.mu.Unlock()
		return nil
	case PaymentSucceeded, PaymentSettling:
		// Settlement already revealed the preimage and possibly moved funds;
		// the payment can only be completed forward.
		m.mu.Unlock()
		return ErrPaymentWrongState
	case PaymentLocked, PaymentRefunding:
	default:
		m.mu.Unlock()
		return ErrPaymentWrongState
	}
	hops := append([]Hop(nil), p.Route.Hops...)
	released := p.RefundedCount
	m.mu.Unlock()

	// Hops are refunded from the last index backwards. Hops
	// [len-released, len-1] are already released durably.
	for k := released; k < len(hops); k++ {
		i := len(hops) - 1 - k
		h := hops[i]
		unlock, err := m.net.lockChannels([]ChannelID{h.ChannelID})
		if err != nil {
			return err
		}
		if err := m.net.refundReserve(h.ChannelID, h.From, h.AmountIn); err != nil {
			unlock()
			return err
		}
		ch, err := m.net.getChannelPtr(h.ChannelID)
		if err != nil {
			unlock()
			return err
		}
		snapshot := *ch
		m.mu.Lock()
		p.RefundedCount = k + 1
		err = m.append(RecHopRefunded, HopRefundedPayload{
			PaymentID: p.ID, HopIndex: i + 1, Count: k + 1, Snapshot: snapshot,
		})
		m.mu.Unlock()
		unlock()
		if err != nil {
			return err
		}
		if m.crashAfterRefundK == k {
			m.crashAfterRefundK = -1
			panic("simulated node crash")
		}
	}

	m.mu.Lock()
	p.Status = PaymentRefunded
	m.inFlight--
	err := m.append(RecPaymentRefunded, PaymentRefundedPayload{PaymentID: p.ID})
	m.mu.Unlock()
	return err
}

// releaseAll releases every hop reserve in reverse order; used on internal
// failures (it does not move payment state on its own).
func (m *Manager) releaseAll(p *Payment) {
	hops := p.Route.Hops
	for i := len(hops) - 1; i >= 0; i-- {
		h := hops[i]
		unlock, err := m.net.lockChannels([]ChannelID{h.ChannelID})
		if err != nil {
			continue
		}
		_ = m.net.refundReserve(h.ChannelID, h.From, h.AmountIn)
		unlock()
	}
}

// failPayment moves a never-locked payment to the failed terminal state.
func (m *Manager) failPayment(p *Payment) {
	m.mu.Lock()
	if p.Status != PaymentFailed {
		p.Status = PaymentFailed
		m.inFlight--
		_ = m.append(RecPaymentFailed, PaymentFailedPayload{PaymentID: p.ID})
	}
	m.mu.Unlock()
}

// restorePayment applies one WAL record to manager state during replay.
func (m *Manager) restorePayment(rec Record) error {
	switch rec.Type {
	case RecPaymentLocked:
		var pl PaymentLockedPayload
		if err := decode(rec.Payload, &pl); err != nil {
			return err
		}
		cp := pl.Payment
		m.payments[cp.ID] = &cp
		m.byHash[cp.PaymentHash] = cp.ID
		if !isTerminal(cp.Status) {
			m.inFlight++
		}
		if n := paymentSeqNum(cp.ID); n > m.counter {
			m.counter = n
		}
	case RecPaymentSettling:
		var pl PaymentSettlingPayload
		if err := decode(rec.Payload, &pl); err != nil {
			return err
		}
		if p := m.payments[pl.PaymentID]; p != nil {
			p.Status = PaymentSettling
			p.Preimage = append([]byte(nil), pl.Preimage...)
		}
	case RecHopSettled:
		var pl HopSettledPayload
		if err := decode(rec.Payload, &pl); err != nil {
			return err
		}
		if p := m.payments[pl.PaymentID]; p != nil && pl.HopIndex > p.SettledHops {
			p.SettledHops = pl.HopIndex
		}
	case RecPaymentSucceeded:
		var pl PaymentSucceededPayload
		if err := decode(rec.Payload, &pl); err != nil {
			return err
		}
		if p := m.payments[pl.PaymentID]; p != nil {
			p.Status = PaymentSucceeded
			p.SettledHops = len(p.Route.Hops)
		}
	case RecHopRefunded:
		var pl HopRefundedPayload
		if err := decode(rec.Payload, &pl); err != nil {
			return err
		}
		if p := m.payments[pl.PaymentID]; p != nil && pl.Count > p.RefundedCount {
			p.RefundedCount = pl.Count
		}
	case RecPaymentRefunded:
		var pl PaymentRefundedPayload
		if err := decode(rec.Payload, &pl); err != nil {
			return err
		}
		if p := m.payments[pl.PaymentID]; p != nil {
			p.Status = PaymentRefunded
		}
	case RecPaymentFailed:
		var pl PaymentFailedPayload
		if err := decode(rec.Payload, &pl); err != nil {
			return err
		}
		if p := m.payments[pl.PaymentID]; p != nil {
			if !isTerminal(p.Status) {
				m.inFlight--
			}
			p.Status = PaymentFailed
		}
	}
	return nil
}

func isTerminal(s PaymentStatus) bool {
	return s == PaymentSucceeded || s == PaymentRefunded || s == PaymentFailed
}
