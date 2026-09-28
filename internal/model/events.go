package model

// Event payloads persisted in the write-ahead log.
// Each payload describes one atomic state transition.

type NodeRegistered struct {
	Node Node `json:"node"`
}

type ChannelOpened struct {
	Channel Channel `json:"channel"`
}

// ChannelBalanceChanged describes an atomic movement of an amount inside a
// single channel: the balance from->to decreases by AmountDelta and the
// reverse balance increases by the same delta (total capacity preserved).
type ChannelBalanceChanged struct {
	ChannelID string `json:"channel_id"`
	From      string `json:"from"`
	To        string `json:"to"`
	Delta     Amount `json:"delta"`
}

type PaymentReserved struct {
	PaymentID    string   `json:"payment_id"`
	Sender       string   `json:"sender"`
	Receiver     string   `json:"receiver"`
	Amount       Amount   `json:"amount"`
	FeeBudget    Amount   `json:"fee_budget"`
	Path         []string `json:"path"`
	Hash         string   `json:"hash"`
	LockDeadline int64    `json:"lock_deadline_unix"`
	SegmentIDs   []string `json:"segment_ids"`
}

type SegmentReserved struct {
	SegmentID string `json:"segment_id"`
	PaymentID string `json:"payment_id"`
	ChannelID string `json:"channel_id"`
	From      string `json:"from"`
	To        string `json:"to"`
	Amount    Amount `json:"amount"`
	Deadline  int64  `json:"deadline_unix"`
}

type SegmentSettled struct {
	SegmentID string `json:"segment_id"`
	PaymentID string `json:"payment_id"`
}

type SegmentReleased struct {
	SegmentID string `json:"segment_id"`
	PaymentID string `json:"payment_id"`
}

// PreimageRevealed records the secret revealed by the receiver; it makes an
// in-progress settlement resumable after a restart.
type PreimageRevealed struct {
	PaymentID string `json:"payment_id"`
	Preimage  string `json:"preimage"`
}

type PaymentCompleted struct {
	PaymentID string `json:"payment_id"`
}

type PaymentAborted struct {
	PaymentID string `json:"payment_id"`
	Reason    string `json:"reason"`
}
