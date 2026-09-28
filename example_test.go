package pchr_test

import (
	"crypto/rand"
	"fmt"

	pchr "github.com/guoyyit/payment-channel-router-go"
)

func Example() {
	net := pchr.NewNetwork(nil)
	_ = net.RegisterNode("A", "a", nil)
	_ = net.RegisterNode("B", "b", nil)
	_ = net.RegisterNode("C", "c", nil)
	_ = net.OpenChannel(pchr.Channel{ID: "ab", PeerA: "A", PeerB: "B", BalanceA: 1000, BalanceB: 1000,
		FeePolicy: pchr.FeePolicy{BaseFee: 1}})
	_ = net.OpenChannel(pchr.Channel{ID: "bc", PeerA: "B", PeerB: "C", BalanceA: 1000, BalanceB: 1000,
		FeePolicy: pchr.FeePolicy{BaseFee: 1}})

	cfg := pchr.DefaultRoutingConfig()
	mgr := pchr.NewManager(net, nil, nil, cfg)

	var preimage [32]byte
	_, _ = rand.Read(preimage[:])
	pay, err := mgr.SendPayment("A", "C", 100, preimage)
	if err != nil {
		fmt.Println("error:", err)
		return
	}
	fmt.Println(pay.Status, pay.Route.TotalFee, pay.SettledHops)
	// Output: succeeded 1 2
}
