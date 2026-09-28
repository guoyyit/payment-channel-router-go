package pchr

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

func tempWALPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "wal.jsonl")
}

func reopenAndRecover(t *testing.T, path string, cfg RoutingConfig) (*Network, *Manager, *FileStore) {
	t.Helper()
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	net := NewNetwork(store)
	mgr := NewManager(net, nil, store, cfg)
	if err := Recover(store, net, mgr); err != nil {
		t.Fatal(err)
	}
	return net, mgr, store
}

func TestWALReplaysTopologyAndChannelState(t *testing.T) {
	path := tempWALPath(t)
	cfg := DefaultRoutingConfig()

	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	net := NewNetwork(store)
	registerNodes(t, net, "A", "B", "C", "D")
	for _, c := range []Channel{
		{ID: "ab", PeerA: "A", PeerB: "B", BalanceA: 1000, BalanceB: 1000, FeePolicy: FeePolicy{BaseFee: 1}},
		{ID: "bc", PeerA: "B", PeerB: "C", BalanceA: 1000, BalanceB: 1000, FeePolicy: FeePolicy{BaseFee: 2}},
		{ID: "cd", PeerA: "C", PeerB: "D", BalanceA: 1000, BalanceB: 1000, FeePolicy: FeePolicy{BaseFee: 3}},
	} {
		if err := net.OpenChannel(c); err != nil {
			t.Fatal(err)
		}
	}
	mgr := NewManager(net, nil, store, cfg)
	p, err := mgr.SendPayment("A", "D", 100, newPreimage())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	net2, mgr2, store2 := reopenAndRecover(t, path, cfg)
	defer store2.Close()

	if nodes := net2.ListNodes(); len(nodes) != 4 {
		t.Fatalf("nodes: %d", len(nodes))
	}
	if chans := net2.ListChannels(); len(chans) != 3 {
		t.Fatalf("channels: %d", len(chans))
	}
	got, err := mgr2.GetPayment(p.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != PaymentSucceeded || got.SettledHops != 3 {
		t.Fatalf("payment recovered as %+v", got)
	}
	ab, _ := net2.GetChannel("ab")
	if ab.BalanceA != 897 || ab.BalanceB != 1103 || ab.LockedA != 0 {
		t.Fatalf("ab after restart: %+v", ab)
	}
}

func TestCrashDuringSettlementResumesOnce(t *testing.T) {
	path := tempWALPath(t)
	cfg := DefaultRoutingConfig()

	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	net := NewNetwork(store)
	registerNodes(t, net, "A", "B", "C", "D")
	for _, c := range []Channel{
		{ID: "ab", PeerA: "A", PeerB: "B", BalanceA: 1000, BalanceB: 1000, FeePolicy: FeePolicy{BaseFee: 1}},
		{ID: "bc", PeerA: "B", PeerB: "C", BalanceA: 1000, BalanceB: 1000, FeePolicy: FeePolicy{BaseFee: 2}},
		{ID: "cd", PeerA: "C", PeerB: "D", BalanceA: 1000, BalanceB: 1000, FeePolicy: FeePolicy{BaseFee: 3}},
	} {
		if err := net.OpenChannel(c); err != nil {
			t.Fatal(err)
		}
	}
	mgr := NewManager(net, nil, store, cfg)
	preimage := newPreimage()
	locked, err := mgr.LockPayment("A", "D", 100, preimage)
	if err != nil {
		t.Fatal(err)
	}
	mgr.crashAfterHop = 2 // crash right after hop 2's durable settle

	func() {
		defer func() { _ = recover() }()
		_, _ = mgr.SettlePayment(locked.ID, preimage)
	}()
	// Simulate abrupt process exit: drop all in-memory state.
	_ = store.Close()

	net2, mgr2, store2 := reopenAndRecover(t, path, cfg)
	defer store2.Close()

	p2, err := mgr2.GetPayment(locked.ID)
	if err != nil {
		t.Fatal(err)
	}
	if p2.Status != PaymentSucceeded || p2.SettledHops != 3 {
		t.Fatalf("payment not resumed to success: %+v", p2)
	}
	// Exactly-once channel effects (same numbers as a clean 3-hop payment).
	ab, _ := net2.GetChannel("ab")
	if ab.BalanceA != 897 || ab.BalanceB != 1103 || ab.LockedA != 0 {
		t.Fatalf("ab: %+v", ab)
	}
	bc, _ := net2.GetChannel("bc")
	if bc.BalanceA != 898 || bc.BalanceB != 1102 || bc.LockedA != 0 {
		t.Fatalf("bc: %+v", bc)
	}
	cd, _ := net2.GetChannel("cd")
	if cd.BalanceA != 900 || cd.BalanceB != 1100 || cd.LockedA != 0 {
		t.Fatalf("cd: %+v", cd)
	}
	// Calling settle again is a harmless no-op.
	p3, err := mgr2.SettlePayment(locked.ID, preimage)
	if err != nil {
		t.Fatalf("resettle: %v", err)
	}
	if p3.Status != PaymentSucceeded {
		t.Fatalf("resettle status %s", p3.Status)
	}
}

func TestRestartRefundsExpiredLockedPayment(t *testing.T) {
	path := tempWALPath(t)
	cfg := DefaultRoutingConfig()
	cfg.LockDuration = time.Minute

	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	net := NewNetwork(store)
	registerNodes(t, net, "A", "B", "C")
	for _, c := range []Channel{
		{ID: "ab", PeerA: "A", PeerB: "B", BalanceA: 1000, BalanceB: 1000},
		{ID: "bc", PeerA: "B", PeerB: "C", BalanceA: 1000, BalanceB: 1000},
	} {
		if err := net.OpenChannel(c); err != nil {
			t.Fatal(err)
		}
	}
	clock := new(fakeClock)
	clock.t = time.Unix(1_000_000, 0)
	mgr := NewManager(net, nil, store, cfg)
	mgr.SetClock(clock.now)
	preimage := newPreimage()
	locked, err := mgr.LockPayment("A", "C", 100, preimage)
	if err != nil {
		t.Fatal(err)
	}
	_ = store.Close()

	// Restart well past expiry: recovery must refund automatically.
	clock.advance(time.Hour)
	store2, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	net2 := NewNetwork(store2)
	mgr2 := NewManager(net2, nil, store2, cfg)
	mgr2.SetClock(clock.now)
	if err := Recover(store2, net2, mgr2); err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	p2, _ := mgr2.GetPayment(locked.ID)
	if p2.Status != PaymentRefunded {
		t.Fatalf("status %s", p2.Status)
	}
	for _, id := range []ChannelID{"ab", "bc"} {
		ch, _ := net2.GetChannel(id)
		if ch.BalanceA != 1000 || ch.LockedA != 0 {
			t.Fatalf("channel %s: %+v", id, ch)
		}
	}
}

func TestRestartKeepsUnexpiredLockedPayment(t *testing.T) {
	path := tempWALPath(t)
	cfg := DefaultRoutingConfig()
	cfg.LockDuration = time.Hour

	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	net := NewNetwork(store)
	registerNodes(t, net, "A", "B")
	if err := net.OpenChannel(Channel{ID: "ab", PeerA: "A", PeerB: "B", BalanceA: 1000, BalanceB: 1000}); err != nil {
		t.Fatal(err)
	}
	mgr := NewManager(net, nil, store, cfg)
	preimage := newPreimage()
	locked, err := mgr.LockPayment("A", "B", 100, preimage)
	if err != nil {
		t.Fatal(err)
	}
	_ = store.Close()

	net2, mgr2, store2 := reopenAndRecover(t, path, cfg)
	defer store2.Close()
	p2, _ := mgr2.GetPayment(locked.ID)
	if p2.Status != PaymentLocked {
		t.Fatalf("status %s", p2.Status)
	}
	// Reserves survived the restart and settlement can still complete.
	p3, err := mgr2.SettlePayment(locked.ID, preimage)
	if err != nil {
		t.Fatal(err)
	}
	if p3.Status != PaymentSucceeded {
		t.Fatalf("status %s", p3.Status)
	}
	ch, _ := net2.GetChannel("ab")
	if ch.BalanceA != 900 || ch.BalanceB != 1100 || ch.LockedA != 0 {
		t.Fatalf("ab after resumed settlement: %+v", ch)
	}
}

func TestWALWrongPreimageRejectedAfterRestart(t *testing.T) {
	path := tempWALPath(t)
	cfg := DefaultRoutingConfig()
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	net := NewNetwork(store)
	registerNodes(t, net, "A", "B")
	if err := net.OpenChannel(Channel{ID: "ab", PeerA: "A", PeerB: "B", BalanceA: 1000, BalanceB: 1000}); err != nil {
		t.Fatal(err)
	}
	mgr := NewManager(net, nil, store, cfg)
	locked, err := mgr.LockPayment("A", "B", 100, newPreimage())
	if err != nil {
		t.Fatal(err)
	}
	_ = store.Close()

	net2, mgr2, store2 := reopenAndRecover(t, path, cfg)
	defer store2.Close()
	if _, err := mgr2.SettlePayment(locked.ID, newPreimage()); !errors.Is(err, ErrPreimageMismatch) {
		t.Fatalf("want preimage mismatch, got %v", err)
	}
	p2, _ := mgr2.GetPayment(locked.ID)
	if p2.Status != PaymentLocked {
		t.Fatalf("payment state changed on bad preimage: %s", p2.Status)
	}
	ch, _ := net2.GetChannel("ab")
	if ch.LockedA != 100 {
		t.Fatalf("reserve changed on bad preimage: %+v", ch)
	}
}

func TestCrashDuringRefundFinishesReverseRollback(t *testing.T) {
	path := tempWALPath(t)
	cfg := DefaultRoutingConfig()
	cfg.LockDuration = time.Minute

	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	net := NewNetwork(store)
	registerNodes(t, net, "A", "B", "C", "D")
	for _, c := range []Channel{
		{ID: "ab", PeerA: "A", PeerB: "B", BalanceA: 1000, BalanceB: 1000},
		{ID: "bc", PeerA: "B", PeerB: "C", BalanceA: 1000, BalanceB: 1000},
		{ID: "cd", PeerA: "C", PeerB: "D", BalanceA: 1000, BalanceB: 1000},
	} {
		if err := net.OpenChannel(c); err != nil {
			t.Fatal(err)
		}
	}
	clock := new(fakeClock)
	clock.t = time.Unix(1_000_000, 0)
	mgr := NewManager(net, nil, store, cfg)
	mgr.SetClock(clock.now)
	preimage := newPreimage()
	locked, err := mgr.LockPayment("A", "D", 100, preimage)
	if err != nil {
		t.Fatal(err)
	}
	clock.advance(2 * time.Minute)
	// Refund order is cd (index 2), bc (index 1), ab (index 0). Crash after
	// the first reverse release (cd) is durable.
	mgr.crashAfterRefundK = 0 // crash after first reverse release (cd)
	func() {
		defer func() { _ = recover() }()
		_, _ = mgr.RefundPayment(locked.ID)
	}()
	_ = store.Close()

	net2, mgr2, store2 := reopenAndRecoverWithClock(t, path, cfg, clock)
	defer store2.Close()

	p2, _ := mgr2.GetPayment(locked.ID)
	if p2.Status != PaymentRefunded || p2.RefundedCount != 3 {
		t.Fatalf("payment not fully refunded: %+v", p2)
	}
	for _, id := range []ChannelID{"ab", "bc", "cd"} {
		ch, _ := net2.GetChannel(id)
		if ch.BalanceA != 1000 || ch.BalanceB != 1000 || ch.LockedA != 0 || ch.LockedB != 0 {
			t.Fatalf("channel %s not restored: %+v", id, ch)
		}
	}
}

func reopenAndRecoverWithClock(t *testing.T, path string, cfg RoutingConfig, clock *fakeClock) (*Network, *Manager, *FileStore) {
	t.Helper()
	store, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	net := NewNetwork(store)
	mgr := NewManager(net, nil, store, cfg)
	mgr.SetClock(clock.now)
	if err := Recover(store, net, mgr); err != nil {
		t.Fatal(err)
	}
	return net, mgr, store
}
