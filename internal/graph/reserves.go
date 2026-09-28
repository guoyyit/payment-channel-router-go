package graph

import (
	"github.com/guoyyit/payment-channel-router-go/internal/model"
)

// ReserveParams describes one HTLC reservation to create.
type ReserveParams struct {
	SegmentID string
	PaymentID string
	ChannelID string
	From      string
	To        string
	Amount    model.Amount
}

// Reserve atomically locks Amount on the From->To direction of a channel:
// the directional spendable balance is reduced by Amount (the funds become
// unspendable until Settle or Release). It fails without effect when the
// balance is insufficient or the segment id is already in use.
func (g *Graph) Reserve(p ReserveParams) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if p.Amount <= 0 {
		return ErrBadChannel
	}
	if _, dup := g.reserves[p.SegmentID]; dup {
		return ErrChannelExists
	}
	if err := g.moveBalanceLocked(p.ChannelID, p.From, p.To, p.Amount); err != nil {
		return err
	}
	g.reserves[p.SegmentID] = &reserve{
		ID: p.SegmentID, PaymentID: p.PaymentID, channelID: p.ChannelID,
		From: p.From, To: p.To, Amount: p.Amount,
	}
	return nil
}

// SettleReserve finalizes an HTLC: the To side keeps the full locked amount
// (the funds were moved out of From's balance at reserve time). A
// forwarding node's profit is the difference between the amount it received
// on the previous segment and the amount it sent on the next one, so no
// extra movement is needed inside a single channel. The reservation is
// removed.
func (g *Graph) SettleReserve(segmentID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.reserves[segmentID]; !ok {
		return ErrChannelNotFound
	}
	delete(g.reserves, segmentID)
	return nil
}

// ReleaseReserve unlocks an HTLC and returns the full Amount to the From
// side, restoring the pre-reservation balances. The reservation is removed.
func (g *Graph) ReleaseReserve(segmentID string) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	rv, ok := g.reserves[segmentID]
	if !ok {
		return ErrChannelNotFound
	}
	if err := g.moveBalanceLocked(rv.channelID, rv.To, rv.From, rv.Amount); err != nil {
		return err
	}
	delete(g.reserves, segmentID)
	return nil
}

// ReserveExists reports whether a segment reservation is present.
func (g *Graph) ReserveExists(segmentID string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, ok := g.reserves[segmentID]
	return ok
}

// PaymentReserves returns the segment ids held by a payment.
func (g *Graph) PaymentReserves(paymentID string) []string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	var ids []string
	for id, rv := range g.reserves {
		if rv.PaymentID == paymentID {
			ids = append(ids, id)
		}
	}
	return ids
}

// HoldAmount reports how much of a channel direction is currently locked.
func (g *Graph) HoldAmount(channelID, from, to string) model.Amount {
	g.mu.RLock()
	defer g.mu.RUnlock()
	var sum model.Amount
	for _, rv := range g.reserves {
		if rv.channelID == channelID && rv.From == from && rv.To == to {
			sum += rv.Amount
		}
	}
	return sum
}

// SpendableBalance reports the currently spendable balance on the
// from->to direction: the settled directional balance. Reservations move
// funds into holds at reserve time, so the directional balance already
// excludes locked amounts; this accessor is provided for clarity/queries.
func (g *Graph) SpendableBalance(channelID, from, to string) (model.Amount, bool) {
	c, ok := g.GetChannel(channelID)
	if !ok {
		return 0, false
	}
	bal, ok := c.ForwardBalance(from, to)
	return bal, ok
}

// channelID field is set when the reserve is created.
var _ = model.Amount(0)
