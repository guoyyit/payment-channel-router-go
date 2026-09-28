package pchr

// Recover replays the durable WAL into network and manager, rebuilding all
// node, channel and payment state, then resumes interrupted payments:
//
//   - payments left in "settling" (preimage revealed, some hops settled) are
//     settled forward to completion — a hop that has a hop_settled record is
//     never settled twice;
//   - payments that are only "locked" and already past their timelock are
//     safely refunded in reverse hop order;
//   - still-valid "locked" payments wait for SettlePayment / RefundPayment.
//
// Channel state is taken entirely from channel_opened/channel_updated
// records, so locked balances reserved by interrupted payments reappear
// automatically and are never double-counted.
func Recover(store Store, net *Network, mgr *Manager) error {
	// Replay channel and node state first.
	if err := store.Replay(func(rec Record) error {
		switch rec.Type {
		case RecNodeRegistered:
			var pl NodeRegistered
			if err := decode(rec.Payload, &pl); err != nil {
				return err
			}
			net.restoreNode(pl.Node)
		case RecChannelOpened:
			var pl ChannelOpened
			if err := decode(rec.Payload, &pl); err != nil {
				return err
			}
			net.restoreChannel(pl.Channel)
		case RecChannelUpdated:
			var pl ChannelUpdated
			if err := decode(rec.Payload, &pl); err != nil {
				return err
			}
			net.restoreChannel(pl.Channel)
		case RecPaymentLocked:
			var pl PaymentLockedPayload
			if err := decode(rec.Payload, &pl); err != nil {
				return err
			}
			// All post-reservation snapshots land as one commit unit.
			for i := range pl.Snapshots {
				net.restoreChannel(pl.Snapshots[i])
			}
		case RecHopSettled:
			var pl HopSettledPayload
			if err := decode(rec.Payload, &pl); err != nil {
				return err
			}
			net.restoreChannel(pl.Snapshot)
		case RecHopRefunded:
			var pl HopRefundedPayload
			if err := decode(rec.Payload, &pl); err != nil {
				return err
			}
			net.restoreChannel(pl.Snapshot)
		}
		return nil
	}); err != nil {
		return err
	}

	// Replay payment lifecycle records.
	if err := store.Replay(func(rec Record) error {
		return mgr.restorePayment(rec)
	}); err != nil {
		return err
	}

	return resumeInterrupted(mgr)
}

// resumeInterrupted finishes payments interrupted by a crash.
func resumeInterrupted(mgr *Manager) error {
	mgr.mu.Lock()
	var ids []PaymentID
	for id, p := range mgr.payments {
		switch {
		case p.Status == PaymentSettling:
			ids = append(ids, id)
		case p.Status == PaymentLocked && p.RefundedCount > 0:
			// A refund had already been durably started: finish it.
			ids = append(ids, id)
		case p.Status == PaymentLocked && !mgr.now().Before(p.ExpiresAt):
			ids = append(ids, id)
		}
	}
	// Complete forward settlements first, then finish refunds.
	var settleIDs []PaymentID
	var refundIDs []PaymentID
	for _, id := range ids {
		if mgr.payments[id].Status == PaymentSettling {
			settleIDs = append(settleIDs, id)
		} else {
			refundIDs = append(refundIDs, id)
		}
	}
	mgr.mu.Unlock()

	for _, id := range settleIDs {
		mgr.mu.Lock()
		var preimage [32]byte
		copy(preimage[:], mgr.payments[id].Preimage)
		mgr.mu.Unlock()
		if _, err := mgr.SettlePayment(id, preimage); err != nil && err != ErrPaymentWrongState {
			return err
		}
	}
	for _, id := range refundIDs {
		if _, err := mgr.RefundPayment(id); err != nil && err != ErrPaymentWrongState {
			return err
		}
	}
	return nil
}
