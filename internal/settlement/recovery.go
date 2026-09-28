package settlement

import (
	"github.com/guoyyit/payment-channel-router-go/internal/graph"
	"github.com/guoyyit/payment-channel-router-go/internal/model"
	"github.com/guoyyit/payment-channel-router-go/internal/store"
)

// BeginReplay switches the manager into replay mode: Apply* events mutate
// memory only, without appending new WAL records.
func (m *Manager) BeginReplay() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.replaying = true
}

// EndReplay leaves replay mode.
func (m *Manager) EndReplay() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.replaying = false
}

// ApplyEvent routes one replayed WAL event to the right state mutation.
// Graph-affecting events are applied to the graph too, so a component can
// replay with only the manager as entry point.
func (m *Manager) ApplyEvent(ev store.Event) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch ev.Kind {
	case store.KindNodeRegistered:
		p := ev.Payload.(*model.NodeRegistered)
		m.g.ApplyNodeRegistered(*p)
	case store.KindChannelOpened:
		p := ev.Payload.(*model.ChannelOpened)
		m.g.ApplyChannelOpened(*p)
	case store.KindBalanceChanged:
		p := ev.Payload.(*model.ChannelBalanceChanged)
		if err := m.g.ApplyBalanceChanged(*p); err != nil {
			return err
		}
	case store.KindSegmentReserved:
		m.applySegmentReservedLocked(*ev.Payload.(*model.SegmentReserved))
	case store.KindPaymentReserved:
		m.applyPaymentReservedLocked(*ev.Payload.(*model.PaymentReserved))
	case store.KindSegmentSettled:
		m.applySegmentSettledLocked(*ev.Payload.(*model.SegmentSettled))
	case store.KindSegmentReleased:
		m.applySegmentReleasedLocked(*ev.Payload.(*model.SegmentReleased))
	case store.KindPreimageRevealed:
		m.applyPreimageRevealedLocked(*ev.Payload.(*model.PreimageRevealed))
	case store.KindPaymentCompleted:
		m.applyPaymentCompletedLocked(*ev.Payload.(*model.PaymentCompleted))
	case store.KindPaymentAborted:
		m.applyPaymentAbortedLocked(*ev.Payload.(*model.PaymentAborted))
	}
	return nil
}

func (m *Manager) ensurePayment(id string) *Payment {
	p, ok := m.payments[id]
	if !ok {
		p = &Payment{ID: id, Phase: PhaseLocked}
		m.payments[id] = p
	}
	return p
}

// applySegmentReservedLocked journal order: segments may appear BEFORE the
// PaymentReserved event when a crash happened after some reserves but before
// the payment header. We reconstruct graph holds and a provisional payment;
// Recover later rolls such orphan reservations back.
func (m *Manager) applySegmentReservedLocked(p model.SegmentReserved) {
	// Recreate the graph hold. If it already exists (shouldn't on a fresh
	// process) ignore: replay is idempotent.
	if !m.g.ReserveExists(p.SegmentID) {
		_ = m.g.Reserve(graph.ReserveParams{
			SegmentID: p.SegmentID, PaymentID: p.PaymentID, ChannelID: p.ChannelID,
			From: p.From, To: p.To, Amount: p.Amount,
		})
	}
	pay := m.ensurePayment(p.PaymentID)
	pay.Segments = append(pay.Segments, &Segment{
		ID: p.SegmentID, PaymentID: p.PaymentID, ChannelID: p.ChannelID,
		From: p.From, To: p.To, Amount: p.Amount, Deadline: p.Deadline,
	})
}

func (m *Manager) applyPaymentReservedLocked(p model.PaymentReserved) {
	pay := m.ensurePayment(p.PaymentID)
	pay.Sender = p.Sender
	pay.Receiver = p.Receiver
	pay.Amount = p.Amount
	pay.FeeBudget = p.FeeBudget
	pay.Path = append([]string{}, p.Path...)
	pay.Hash = p.Hash
	pay.LockDeadline = p.LockDeadline
	pay.Phase = PhaseLocked
	sortSegmentsByIndex(pay)
	m.active++
}

// sortSegmentsByIndex orders replayed segments by their hop index suffix.
func sortSegmentsByIndex(pay *Payment) {
	// segment ids are "<payment>:seg:<i>"; order via suffix parse-free sort:
	// ids happen to sort lexicographically correctly only up to 9, so do a
	// stable insertion based on channel/path ordering is unnecessary; use a
	// simple numeric extraction.
	for i := 1; i < len(pay.Segments); i++ {
		for j := i; j > 0 && segIndex(pay.Segments[j].ID) < segIndex(pay.Segments[j-1].ID); j-- {
			pay.Segments[j], pay.Segments[j-1] = pay.Segments[j-1], pay.Segments[j]
		}
	}
}

func (m *Manager) applySegmentSettledLocked(p model.SegmentSettled) {
	pay, ok := m.payments[p.PaymentID]
	if !ok {
		return
	}
	for _, s := range pay.Segments {
		if s.ID == p.SegmentID && !s.Settled {
			// Fee is the Hop fee recorded at reservation time; during replay
			// the per-segment fee is recoverable from amounts of adjacent
			// segments. Populated by fillFees during Recover.
			_ = m.g.SettleReserve(p.SegmentID)
			s.Settled = true
			return
		}
	}
}

func (m *Manager) applySegmentReleasedLocked(p model.SegmentReleased) {
	if m.g.ReserveExists(p.SegmentID) {
		_ = m.g.ReleaseReserve(p.SegmentID)
	}
	if pay, ok := m.payments[p.PaymentID]; ok {
		for _, s := range pay.Segments {
			if s.ID == p.SegmentID {
				s.Released = true
			}
		}
	}
}

func (m *Manager) applyPreimageRevealedLocked(p model.PreimageRevealed) {
	pay := m.ensurePayment(p.PaymentID)
	pay.Preimage = p.Preimage
	pay.Phase = PhaseSettling
}

func (m *Manager) applyPaymentCompletedLocked(p model.PaymentCompleted) {
	pay := m.ensurePayment(p.PaymentID)
	if pay.Phase != PhaseCompleted {
		m.active--
	}
	pay.Phase = PhaseCompleted
	for _, s := range pay.Segments {
		s.Settled = true
	}
}

func (m *Manager) applyPaymentAbortedLocked(p model.PaymentAborted) {
	pay := m.ensurePayment(p.PaymentID)
	wasActive := pay.Hash != "" && pay.Phase != PhaseCompleted && pay.Phase != PhaseAborted
	pay.Phase = PhaseAborted
	pay.Reason = p.Reason
	for _, s := range pay.Segments {
		s.Released = true
	}
	// Only payments that had a PaymentReserved header were counted as
	// active; failed-lock rollbacks append Abort without ever being active.
	if wasActive {
		m.active--
	}
}

// Recover finishes or rolls back payments left in-flight by a crash:
//   - payments whose preimage was revealed (settling) are completed;
//   - fully locked payments past their deadline are released (aborted);
//   - fully locked payments still within deadline remain locked and wait;
//   - orphaned reservations (crash before PaymentReserved) are released.
//
// No payment is lost and no segment is settled twice: each transition is
// journaled and graph primitives drop already-finalized holds.
func (m *Manager) Recover() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.replaying = false
	now := m.clock.NowUnix()
	ids := make([]string, 0, len(m.payments))
	for id := range m.payments {
		ids = append(ids, id)
	}
	// deterministic recovery order
	for i := 1; i < len(ids); i++ {
		for j := i; j > 0 && ids[j] < ids[j-1]; j-- {
			ids[j], ids[j-1] = ids[j-1], ids[j]
		}
	}
	for _, id := range ids {
		pay := m.payments[id]
		m.fillReplayFeesLocked(pay)
		switch {
		case pay.Hash == "":
			// orphan: no PaymentReserved ever landed
			m.rollbackReservedLocked(pay)
			delete(m.payments, id)
		case pay.Phase == PhaseSettling || (pay.Preimage != "" && !allSettled(pay)):
			if err := m.finishSettlementLocked(pay); err != nil {
				return err
			}
		case pay.Phase == PhaseLocked && pay.LockDeadline <= now:
			if err := m.expireLocked(id, now); err != nil {
				return err
			}
		}
	}
	return nil
}

func allSettled(pay *Payment) bool {
	for _, s := range pay.Segments {
		if !s.Settled && !s.Released {
			return false
		}
	}
	return true
}

// fillReplayFeesLocked recovers each segment's forwarding fee from adjacent
// segment amounts. Segment i carries amount a_i; node n(i+1) keeps the
// difference a_i - a(i+1) as its fee, and the final segment's receiver keeps
// the full amount (fee field 0). The fee is only stored for reporting:
// settlement itself moves no funds per channel.
func (m *Manager) fillReplayFeesLocked(pay *Payment) {
	if len(pay.Segments) == 0 {
		return
	}
	sortSegmentsByIndex(pay)
	for i, s := range pay.Segments {
		if i == len(pay.Segments)-1 {
			s.Fee = 0
		} else {
			s.Fee = s.Amount - pay.Segments[i+1].Amount
			if s.Fee < 0 {
				s.Fee = 0
			}
		}
	}
}

// rollbackReservedLocked is the replay-mode variant: it must not append.
func (m *Manager) rollbackReservedLocked(pay *Payment) {
	was := m.replaying
	m.replaying = true
	defer func() { m.replaying = was }()
	for i := len(pay.Segments) - 1; i >= 0; i-- {
		sid := pay.Segments[i].ID
		_ = m.appendApply(store.KindSegmentReleased,
			model.SegmentReleased{SegmentID: sid, PaymentID: pay.ID},
			func() error { return m.g.ReleaseReserve(sid) })
	}
}

// finishSettlementLocked settles remaining segments in reverse order.
func (m *Manager) finishSettlementLocked(pay *Payment) error {
	for i := len(pay.Segments) - 1; i >= 0; i-- {
		s := pay.Segments[i]
		if s.Settled {
			continue
		}
		sid := s.ID
		if err := m.appendApply(store.KindSegmentSettled,
			model.SegmentSettled{SegmentID: sid, PaymentID: pay.ID},
			func() error { return m.g.SettleReserve(sid) }); err != nil {
			return err
		}
		s.Settled = true
	}
	pay.Phase = PhaseCompleted
	m.active--
	return m.appendApply(store.KindPaymentCompleted,
		model.PaymentCompleted{PaymentID: pay.ID}, nil)
}
