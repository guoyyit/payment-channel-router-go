package store

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/guoyyit/payment-channel-router-go/internal/model"
)

func TestAppendReplayRoundTrip(t *testing.T) {
	dir := t.TempDir()
	st, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 3; i++ {
		seq, err := st.Append(KindNodeRegistered, model.NodeRegistered{
			Node: model.Node{ID: "n"},
		})
		if err != nil {
			t.Fatal(err)
		}
		if seq != int64(i) {
			t.Fatalf("seq = %d, want %d", seq, i)
		}
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	st2, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	if err := st2.Replay(func(ev Event) error {
		if ev.Kind != KindNodeRegistered {
			t.Fatalf("unexpected kind %s", ev.Kind)
		}
		if _, ok := ev.Payload.(*model.NodeRegistered); !ok {
			t.Fatalf("bad payload type %T", ev.Payload)
		}
		count++
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Fatalf("replayed %d events, want 3", count)
	}
	if st2.LastSeq() != 3 {
		t.Fatalf("last seq %d", st2.LastSeq())
	}
}

func TestTornTailRecovery(t *testing.T) {
	dir := t.TempDir()
	st, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = st.Append(KindPaymentCompleted, model.PaymentCompleted{PaymentID: "p1"})
	_, _ = st.Append(KindPaymentCompleted, model.PaymentCompleted{PaymentID: "p2"})
	st.Close()

	// Append a partial (torn) line, simulating a crash mid-write.
	path := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"seq":3,"kind":"payment_completed","paylo`); err != nil {
		t.Fatal(err)
	}
	f.Sync()
	f.Close()

	st2, err := New(dir)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	err = st2.Replay(func(ev Event) error { count++; return nil })
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("replayed %d events after torn tail, want 2", count)
	}
	// New appends continue at seq 3.
	seq, err := st2.Append(KindPaymentAborted, model.PaymentAborted{PaymentID: "p3"})
	if err != nil {
		t.Fatal(err)
	}
	if seq != 3 {
		t.Fatalf("seq after recovery = %d, want 3", seq)
	}
}
