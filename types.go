// Package pchr implements an in-process, locally simulated payment channel
// network with multi-hop routing and hash-time-locked atomic settlement.
package pchr

import (
	"math"
	"time"
)

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

type sentinelError string

func (e sentinelError) Error() string { return string(e) }

const (
	ErrNodeExists        sentinelError = "node already exists"
	ErrNodeNotFound      sentinelError = "node not found"
	ErrChannelExists     sentinelError = "channel already exists"
	ErrChannelNotFound   sentinelError = "channel not found"
	ErrInsufficientFunds sentinelError = "insufficient channel balance"
	ErrInvalidArgument   sentinelError = "invalid argument"
	ErrNoRoute           sentinelError = "no route available"
	ErrPaymentNotFound   sentinelError = "payment not found"
	ErrPaymentWrongState sentinelError = "payment is in an unexpected state"
	ErrPreimageMismatch  sentinelError = "preimage does not match hash lock"
	ErrFeeExceeded       sentinelError = "total fee exceeds the configured cap"
	ErrTooManyHops       sentinelError = "route exceeds the maximum hop count"
	ErrTooManyConcurrent sentinelError = "too many concurrent payments in flight"
	ErrPaymentExpired    sentinelError = "payment timelock has expired"
)

// Hash is a 32-byte hash lock (SHA-256 of the payment preimage).
type Hash [32]byte

// ---------------------------------------------------------------------------
// Nodes and channels
// ---------------------------------------------------------------------------

// NodeID identifies a network node.
type NodeID string

// ChannelID identifies a payment channel.
type ChannelID string

// FeePolicy charges a proportional fee plus a flat base fee.
type FeePolicy struct {
	BaseFee           int64 `json:"base_fee"`
	FeeRateMillionths int64 `json:"fee_rate_millionths"`
}

// Fee computes the forwarding fee for an amount carried on a channel.
func (p FeePolicy) Fee(amount int64) int64 {
	if amount > 0 && p.FeeRateMillionths > 0 &&
		amount > (math.MaxInt64-p.BaseFee)/p.FeeRateMillionths {
		// Saturate at MaxInt64; the caller's overflow guard then rejects the
		// edge rather than wrapping to a negative fee.
		return math.MaxInt64
	}
	return p.BaseFee + (amount*p.FeeRateMillionths+999_999)/1_000_000
}

// Node is a registered network participant.
type Node = struct {
	ID      NodeID            `json:"id"`
	Address string            `json:"address"`
	Meta    map[string]string `json:"meta,omitempty"`
}

// Channel is a bidirectional payment channel between two peers.
//
// Balances are stored from the perspective of BalanceA: BalanceA is the
// amount spendable by PeerA towards PeerB and vice versa.
type Channel struct {
	ID       ChannelID `json:"id"`
	PeerA    NodeID    `json:"peer_a"`
	PeerB    NodeID    `json:"peer_b"`
	BalanceA int64     `json:"balance_a"`
	BalanceB int64     `json:"balance_b"`
	// LockedA / LockedB are amounts reserved by in-flight payments and not
	// currently spendable. LockedX counts reserves on BalanceX.
	LockedA   int64     `json:"locked_a"`
	LockedB   int64     `json:"locked_b"`
	Active    bool      `json:"active"`
	FeePolicy FeePolicy `json:"fee_policy"`
}

// Balances returns (spendable, locked) for a direction of the channel.
//
// "from" must be one of the two channel peers; otherwise ok is false.
// Spendable is the balance that is not currently reserved by in-flight
// payments.
func (c *Channel) Balances(from NodeID) (spendable int64, locked int64, ok bool) {
	switch from {
	case c.PeerA:
		return c.BalanceA - c.LockedA, c.LockedA, true
	case c.PeerB:
		return c.BalanceB - c.LockedB, c.LockedB, true
	default:
		return 0, 0, false
	}
}

// otherPeer returns the counterparty of the channel.
func (c *Channel) otherPeer(p NodeID) NodeID {
	if p == c.PeerA {
		return c.PeerB
	}
	return c.PeerA
}

// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------
// Routing
// ---------------------------------------------------------------------------

type RouteStrategy string

const (
	// StrategyCheapestFee minimizes total forwarding fee, then hop count,
	// then lexicographic ids for deterministic tie breaking.
	StrategyCheapestFee RouteStrategy = "cheapest_fee"
	// StrategyShortest minimizes hop count, then total fee.
	StrategyShortest RouteStrategy = "shortest"
)

type Hop struct {
	ChannelID ChannelID `json:"channel_id"`
	From      NodeID    `json:"from"`
	To        NodeID    `json:"to"`
	// AmountIn is the amount the sender of this hop must forward.
	AmountIn int64 `json:"amount_in"`
	// AmountOut is the amount that reaches the receiver of this hop.
	AmountOut int64 `json:"amount_out"`
	Fee       int64 `json:"fee"`
}

// Route is a complete source-to-destination payment path.
type Route struct {
	Hops     []Hop `json:"hops"`
	TotalFee int64 `json:"total_fee"`
	// AmountToSend is the amount debited from the payer.
	AmountToSend int64 `json:"amount_to_send"`
	// AmountReceived is the amount credited to the payee.
	AmountReceived int64 `json:"amount_received"`
}

// RoutingConfig configures path finding and request limits.
type RoutingConfig struct {
	Strategy      RouteStrategy `json:"strategy"`
	MaxHops       int           `json:"max_hops"`
	MaxTotalFee   int64         `json:"max_total_fee"`
	MaxConcurrent int           `json:"max_concurrent"`
	// LockDuration is added to the start time of every new payment.
	LockDuration time.Duration `json:"lock_duration"`
}

// DefaultRoutingConfig returns conservative default limits.
func DefaultRoutingConfig() RoutingConfig {
	return RoutingConfig{
		Strategy:      StrategyCheapestFee,
		MaxHops:       5,
		MaxTotalFee:   1_000_000,
		MaxConcurrent: 64,
		LockDuration:  time.Hour,
	}
}

// ---------------------------------------------------------------------------
// Payments
// ---------------------------------------------------------------------------

type PaymentID string

type PaymentStatus string

const (
	// PaymentPending: reserves not yet acquired on all hops.
	PaymentPending PaymentStatus = "pending"
	// PaymentLocked: every hop has reserved funds; awaiting settlement.
	PaymentLocked PaymentStatus = "locked"
	// PaymentSettling: hash lock released, hop settlements in progress.
	PaymentSettling PaymentStatus = "settling"
	// PaymentSucceeded: all hops settled forward.
	PaymentSucceeded PaymentStatus = "succeeded"
	// PaymentRefunding: timeout path; hop reserves released in reverse.
	PaymentRefunding PaymentStatus = "refunding"
	// PaymentRefunded: all reserves released, balances untouched.
	PaymentRefunded PaymentStatus = "refunded"
	// PaymentFailed: payment never locked; all partial reserves released.
	PaymentFailed PaymentStatus = "failed"
)

// Payment is the durable view of an end-to-end payment.
type Payment struct {
	ID          PaymentID     `json:"id"`
	Sender      NodeID        `json:"sender"`
	Receiver    NodeID        `json:"receiver"`
	Amount      int64         `json:"amount"`
	Status      PaymentStatus `json:"status"`
	Route       Route         `json:"route"`
	PaymentHash Hash          `json:"payment_hash"`
	// Preimage is recorded once revealed (at settlement start) so that a
	// restart during forward settlement can resume deterministically.
	Preimage    []byte    `json:"preimage,omitempty"`
	ExpiresAt   time.Time `json:"expires_at"`
	CreatedAt   time.Time `json:"created_at"`
	SettledHops int       `json:"settled_hops"`
	// RefundedCount is the number of hops released so far in reverse order
	// (last hop first). A resumed refund continues at hop index
	// len(hops)-1-RefundedCount.
	RefundedCount int `json:"refunded_count"`
}
