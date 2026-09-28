// Package model defines core domain types for the payment channel network:
// nodes, channels and fee policies.
package model

// Amount is a monetary amount in the smallest indivisible unit.
type Amount = int64

// FeePolicy describes how a node charges forwarding fees.
// fee = BaseFee + amount*FeeRate/1_000_000 (integer arithmetic, rounded up).
type FeePolicy struct {
	BaseFee Amount `json:"base_fee"`
	FeeRate int64  `json:"fee_rate_ppm"`
}

// Node is a participant in the payment channel network.
type Node struct {
	ID     string    `json:"id"`
	Policy FeePolicy `json:"policy"`
}

// Channel is a payment channel between two peers with directional balances.
// A->B capacity is BalanceAB; B->A capacity is BalanceBA.
// BalanceAB + BalanceBA == Capacity always holds.
type Channel struct {
	ID        string `json:"id"`
	NodeA     string `json:"node_a"`
	NodeB     string `json:"node_b"`
	Capacity  Amount `json:"capacity"`
	BalanceAB Amount `json:"balance_a_b"`
	BalanceBA Amount `json:"balance_b_a"`
}

// Other returns the counterparty of the channel for the given node.
func (c *Channel) Other(node string) (string, bool) {
	switch node {
	case c.NodeA:
		return c.NodeB, true
	case c.NodeB:
		return c.NodeA, true
	default:
		return "", false
	}
}

// ForwardBalance returns the balance spendable when sending from -> to.
func (c *Channel) ForwardBalance(from, to string) (Amount, bool) {
	switch {
	case from == c.NodeA && to == c.NodeB:
		return c.BalanceAB, true
	case from == c.NodeB && to == c.NodeA:
		return c.BalanceBA, true
	default:
		return 0, false
	}
}
