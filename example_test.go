package pctr_test

import (
	"fmt"
	"os"
	"path/filepath"

	pctr "github.com/guoyyit/payment-channel-router-go"
	"github.com/guoyyit/payment-channel-router-go/internal/model"
	"github.com/guoyyit/payment-channel-router-go/internal/router"
)

func Example() {
	// Use a throwaway directory so the example leaves nothing on disk.
	dir, err := os.MkdirTemp("", "pctr-example-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)

	// Open a network: A -> B -> C, B charges 10 per forwarded payment.
	n, err := pctr.Open(pctr.Config{
		DataDir: filepath.Join(dir, "wal"), Strategy: router.LowestFee,
		MaxHops: 5, MaxFee: 100, MaxConcurrent: 10,
		LockDuration: 3600,
	})
	if err != nil {
		panic(err)
	}
	defer func() { _ = n.Close() }()

	_ = n.RegisterNode("A", model.FeePolicy{})
	_ = n.RegisterNode("B", model.FeePolicy{BaseFee: 10})
	_ = n.RegisterNode("C", model.FeePolicy{})
	_ = n.OpenChannel("ab", "A", "B", 1000, 1000) // A funds 1000 A->B
	_ = n.OpenChannel("bc", "B", "C", 1000, 1000) // B funds 1000 B->C

	// Inspect the priced route for a 100-unit payment A -> C.
	rt, err := n.Router().FindRoute("A", "C", 100)
	if err != nil {
		panic(err)
	}
	fmt.Println("path:", rt.Path, "fee:", rt.TotalFee, "debit:", rt.TotalDebit)

	// Pay atomically with a preimage whose SHA-256 hash is the HTLC hash.
	if err := n.Manager().Send("pay-1", "A", "C", 100, "demo-secret"); err != nil {
		panic(err)
	}
	ab, _ := n.Graph().GetChannel("ab")
	bc, _ := n.Graph().GetChannel("bc")
	fmt.Println("ab:", ab.BalanceAB, ab.BalanceBA)
	fmt.Println("bc:", bc.BalanceAB, bc.BalanceBA)

	// Output:
	// path: [A B C] fee: 10 debit: 110
	// ab: 890 110
	// bc: 900 100
}
