// Package settlement implements cross-channel atomic payments with hash
// time-locked contracts (HTLCs), simulated entirely on local state.
//
// Lifecycle of a payment:
//
//	Lock:      funds are reserved hop by hop along the route. Each segment
//	           atomically moves its amount out of the sender-directional
//	           balance of its channel into a locked hold (WAL event first,
//	           then memory). If any hop cannot be reserved, every earlier
//	           hop is released before the call returns, so a failed path
//	           never leaves reservations behind and balances are unchanged.
//	Settle:    the receiver reveals the SHA-256 preimage. Segments are
//	           settled in REVERSE order (receiver hop first); each forwarder
//	           receives its fee back and the payee keeps the rest. The
//	           reveal itself is persisted before any settlement, so a crash
//	           mid-settlement is resumable after restart.
//	Expire:    if the deadline passes without a reveal, segments are released
//	           in FORWARD order (sender hop first) and all holds unwind.
//
// A single manager mutex serializes payment transitions; individual channel
// reserve primitives are themselves atomic, so concurrent payments can never
// double-spend or oversell a directional balance.
package settlement

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/guoyyit/payment-channel-router-go/internal/graph"
	"github.com/guoyyit/payment-channel-router-go/internal/model"
	"github.com/guoyyit/payment-channel-router-go/internal/router"
	"github.com/guoyyit/payment-channel-router-go/internal/store"
)

// Hash computes the HTLC hash of a preimage (SHA-256, hex encoded).
func Hash(preimage string) string {
	sum := sha256.Sum256([]byte(preimage))
	return hex.EncodeToString(sum[:])
}

// GeneratePreimage returns a random 32-byte hex preimage.
func GeneratePreimage() string {
	var b [32]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Clock abstracts wall-clock time.
type Clock interface{ NowUnix() int64 }

type wallClock struct{}

func (wallClock) NowUnix() int64 { return time.Now().Unix() }

// Phase is a payment lifecycle phase.
type Phase int

func (p Phase) String() string {
	switch p {
	case PhaseLocked:
		return "locked"
	case PhaseSettling:
		return "settling"
	case PhaseCompleted:
		return "completed"
	case PhaseAborted:
		return "aborted"
	default:
		return "unknown"
	}
}

const (
	PhaseLocked Phase = iota
	PhaseSettling
	PhaseCompleted
	PhaseAborted
)

// Segment is one per-channel HTLC leg of a payment.
type Segment struct {
	ID        string
	PaymentID string
	ChannelID string
	From      string
	To        string
	Amount    model.Amount
	Fee       model.Amount // fee earned by From on settlement
	Deadline  int64
	Settled   bool
	Released  bool
}

// Payment is an in-flight or finished atomic payment.
type Payment struct {
	ID           string
	Sender       string
	Receiver     string
	Amount       model.Amount
	FeeBudget    model.Amount
	Path         []string
	Hash         string
	Preimage     string
	LockDeadline int64
	Phase        Phase
	Reason       string
	Segments     []*Segment
}

// Options configures the settlement manager.
type Options struct {
	MaxHops       int
	MaxFee        model.Amount
	MaxConcurrent int
	LockDuration  int64 // seconds from now for the first (sender) hop
	HopGrace      int64 // seconds each downstream deadline is shortened
}

// Manager executes and tracks atomic payments.
type Manager struct {
	mu       sync.Mutex
	g        *graph.Graph
	r        *router.Router
	st       *store.Store
	clock    Clock
	opt      Options
	payments map[string]*Payment
	active   int
	// during replay, events only mutate memory (no new appends)
	replaying bool
	// beforeReserveHop, if set, is invoked just before reserving hop idx
	// (manager mutex held). Tests use it to inject a balance race.
	beforeReserveHop func(paymentID string, idx int)
	// afterSettleHop, if set, is invoked right after hop idx settles (manager
	// mutex held). Tests use it to simulate a crash mid-settlement.
	afterSettleHop func(paymentID string, idx int)
}

// New constructs a Manager.
func New(g *graph.Graph, r *router.Router, st *store.Store, clock Clock, opt Options) *Manager {
	if clock == nil {
		clock = wallClock{}
	}
	if opt.LockDuration <= 0 {
		opt.LockDuration = 3600
	}
	if opt.HopGrace < 0 {
		opt.HopGrace = 0
	}
	return &Manager{
		g: g, r: r, st: st, clock: clock, opt: opt,
		payments: map[string]*Payment{},
	}
}

// SetClock replaces the time source (mainly for tests).
func (m *Manager) SetClock(c Clock) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c != nil {
		m.clock = c
	}
}

// SetBeforeReserveHook installs a callback invoked before each hop
// reservation (manager lock held). Intended for tests to simulate a balance
// race between routing and reservation.
func (m *Manager) SetBeforeReserveHook(fn func(paymentID string, hop int)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.beforeReserveHop = fn
}

// SetAfterSettleHook installs a callback invoked after each hop settles.
// Intended for tests to simulate a crash mid-settlement.
func (m *Manager) SetAfterSettleHook(fn func(paymentID string, hop int)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.afterSettleHop = fn
}

func segID(paymentID string, idx int) string {
	return fmt.Sprintf("%s:seg:%d", paymentID, idx)
}

// Send performs the common happy path: reserve all hops and immediately
// settle with the revealed preimage. Either the payment fully completes or
// balances are left exactly as before.
func (m *Manager) Send(paymentID, sender, receiver string, amount model.Amount, preimage string) error {
	if preimage == "" {
		return ErrBadPreimage
	}
	if err := m.Lock(paymentID, sender, receiver, amount, Hash(preimage)); err != nil {
		return err
	}
	return m.SettleWithPreimage(paymentID, preimage)
}

// Lock reserves all segments of a payment without settling. The receiver
// later reveals the preimage via SettleWithPreimage, or the payment expires.
func (m *Manager) Lock(paymentID, sender, receiver string, amount model.Amount, hash string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if paymentID == "" || hash == "" {
		return ErrWrongPhase
	}
	if _, exists := m.payments[paymentID]; exists {
		return ErrPaymentExists
	}
	if amount <= 0 {
		return router.ErrInvalidAmount
	}
	if m.opt.MaxConcurrent > 0 && m.active >= m.opt.MaxConcurrent {
		return ErrConcurrentLimit
	}
	route, err := m.r.FindRoute(sender, receiver, amount)
	if err != nil {
		if errors.Is(err, router.ErrFeeExceeded) {
			return ErrFeeBudget
		}
		if errors.Is(err, router.ErrNoRoute) {
			return ErrRouteNotFound
		}
		return err
	}
	if route.TotalFee > m.opt.MaxFee {
		return ErrFeeBudget
	}
	if m.opt.MaxHops > 0 && len(route.Hops) > m.opt.MaxHops {
		return ErrRouteNotFound
	}

	now := m.clock.NowUnix()
	firstDeadline := now + m.opt.LockDuration

	// Reserve each hop in forward order. Persist each segment event BEFORE
	// applying it; on any failure release everything reserved so far.
	segs := make([]*Segment, 0, len(route.Hops))
	reserved := make([]string, 0, len(route.Hops))
	for i, h := range route.Hops {
		if m.beforeReserveHop != nil {
			m.beforeReserveHop(paymentID, i)
		}
		sid := segID(paymentID, i)
		deadline := firstDeadline - m.opt.HopGrace*int64(i)
		payload := model.SegmentReserved{
			SegmentID: sid, PaymentID: paymentID, ChannelID: h.ChannelID,
			From: h.From, To: h.To, Amount: h.AmountToForward, Deadline: deadline,
		}
		if err := m.appendApply(store.KindSegmentReserved, payload, func() error {
			return m.g.Reserve(graph.ReserveParams{
				SegmentID: sid, PaymentID: paymentID, ChannelID: h.ChannelID,
				From: h.From, To: h.To, Amount: h.AmountToForward,
			})
		}); err != nil {
			m.rollbackReserved(paymentID, reserved)
			return err
		}
		segs = append(segs, &Segment{
			ID: sid, PaymentID: paymentID, ChannelID: h.ChannelID,
			From: h.From, To: h.To, Amount: h.AmountToForward,
			Fee: h.Fee, Deadline: deadline,
		})
		reserved = append(reserved, sid)
	}

	pay := &Payment{
		ID: paymentID, Sender: sender, Receiver: receiver, Amount: amount,
		FeeBudget: m.opt.MaxFee, Path: route.Path, Hash: hash,
		LockDeadline: firstDeadline, Phase: PhaseLocked, Segments: segs,
	}
	payload := model.PaymentReserved{
		PaymentID: paymentID, Sender: sender, Receiver: receiver, Amount: amount,
		FeeBudget: m.opt.MaxFee, Path: append([]string{}, route.Path...),
		Hash: hash, LockDeadline: firstDeadline, SegmentIDs: append([]string{}, reserved...),
	}
	if err := m.appendApply(store.KindPaymentReserved, payload, func() error {
		m.payments[paymentID] = pay
		m.active++
		return nil
	}); err != nil {
		m.rollbackReserved(paymentID, reserved)
		return err
	}
	return nil
}

// rollbackReserved releases already held segments in REVERSE reservation
// order and records durable release events for each.
func (m *Manager) rollbackReserved(paymentID string, segIDs []string) {
	for i := len(segIDs) - 1; i >= 0; i-- {
		sid := segIDs[i]
		_ = m.appendApply(store.KindSegmentReleased,
			model.SegmentReleased{SegmentID: sid, PaymentID: paymentID},
			func() error { return m.g.ReleaseReserve(sid) })
	}
}

// SettleWithPreimage settles a locked or partially settled payment when the
// preimage matches the hash. Segments settle in reverse hop order.
func (m *Manager) SettleWithPreimage(paymentID, preimage string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	pay, ok := m.payments[paymentID]
	if !ok {
		return ErrPaymentNotFound
	}
	if pay.Phase == PhaseCompleted {
		return nil // idempotent
	}
	if pay.Phase == PhaseAborted {
		return ErrWrongPhase
	}
	if Hash(preimage) != pay.Hash {
		return ErrBadPreimage
	}
	// Persist the reveal first so a crash here is resumable on restart.
	if pay.Preimage == "" {
		if err := m.appendApply(store.KindPreimageRevealed,
			model.PreimageRevealed{PaymentID: paymentID, Preimage: preimage},
			func() error {
				pay.Preimage = preimage
				pay.Phase = PhaseSettling
				return nil
			}); err != nil {
			return err
		}
	}
	for i := len(pay.Segments) - 1; i >= 0; i-- {
		s := pay.Segments[i]
		if s.Settled {
			continue
		}
		sid := s.ID
		// forwarder keeps its fee: net move To->From equals Fee.
		if err := m.appendApply(store.KindSegmentSettled,
			model.SegmentSettled{SegmentID: sid, PaymentID: paymentID},
			func() error { return m.g.SettleReserve(sid) }); err != nil {
			return err
		}
		s.Settled = true
		if m.afterSettleHop != nil {
			m.afterSettleHop(paymentID, i)
		}
	}
	if err := m.appendApply(store.KindPaymentCompleted,
		model.PaymentCompleted{PaymentID: paymentID}, func() error {
			pay.Phase = PhaseCompleted
			m.active--
			return nil
		}); err != nil {
		return err
	}
	return nil
}

// Expire aborts an unsettled payment whose deadline passed and releases all
// still-reserved segments in forward order.
func (m *Manager) Expire(paymentID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.expireLocked(paymentID, m.clock.NowUnix())
}

func (m *Manager) expireLocked(paymentID string, now int64) error {
	pay, ok := m.payments[paymentID]
	if !ok {
		return ErrPaymentNotFound
	}
	if pay.Phase == PhaseCompleted {
		return ErrAlreadySettled
	}
	if pay.Phase == PhaseAborted {
		return nil
	}
	if pay.Preimage != "" {
		return ErrWrongPhase // settlement is in progress; finish it instead
	}
	if now < pay.LockDeadline {
		return ErrNotExpired
	}
	for _, s := range pay.Segments {
		if s.Released || s.Settled {
			continue
		}
		sid := s.ID
		if err := m.appendApply(store.KindSegmentReleased,
			model.SegmentReleased{SegmentID: sid, PaymentID: paymentID},
			func() error { return m.g.ReleaseReserve(sid) }); err != nil {
			return err
		}
		s.Released = true
	}
	return m.appendApply(store.KindPaymentAborted,
		model.PaymentAborted{PaymentID: paymentID, Reason: "deadline expired"},
		func() error {
			pay.Phase = PhaseAborted
			pay.Reason = "deadline expired"
			m.active--
			return nil
		})
}

// Sweep expires every locked, unrevealed payment whose deadline passed.
func (m *Manager) Sweep(now int64) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]string, 0)
	for id, p := range m.payments {
		if p.Phase == PhaseLocked && p.Preimage == "" && p.LockDeadline <= now {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	n := 0
	for _, id := range ids {
		if err := m.expireLocked(id, now); err == nil {
			n++
		}
	}
	return n, nil
}

// GetPayment returns a deep copy of a payment snapshot.
func (m *Manager) GetPayment(id string) (*Payment, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	p, ok := m.payments[id]
	if !ok {
		return nil, false
	}
	cp := *p
	cp.Path = append([]string{}, p.Path...)
	cp.Segments = make([]*Segment, len(p.Segments))
	for i, s := range p.Segments {
		sc := *s
		cp.Segments[i] = &sc
	}
	return &cp, true
}

// Payments returns all payment IDs sorted.
func (m *Manager) Payments() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	ids := make([]string, 0, len(m.payments))
	for id := range m.payments {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Active returns the number of payments still reserving funds.
func (m *Manager) Active() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.active
}

// appendApply persists an event (unless replaying) and then applies it. The
// apply function MUST succeed: it repeats a transition already validated and
// journaled. If persistence fails nothing is applied.
func (m *Manager) appendApply(kind store.Kind, payload any, apply func() error) error {
	if !m.replaying {
		if _, err := m.st.Append(kind, payload); err != nil {
			return err
		}
	}
	if apply != nil {
		if err := apply(); err != nil {
			return fmt.Errorf("settlement: journaled apply failed: %w", err)
		}
	}
	return nil
}
