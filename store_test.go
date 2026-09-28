package pchr

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestFileStoreAppendAndReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.jsonl")
	s, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	seq1, err := s.Append(RecNodeRegistered, NodeRegistered{Node: Node{ID: "A"}})
	if err != nil {
		t.Fatal(err)
	}
	seq2, err := s.Append(RecNodeRegistered, NodeRegistered{Node: Node{ID: "B"}})
	if err != nil {
		t.Fatal(err)
	}
	if seq1 != 1 || seq2 != 2 {
		t.Fatalf("seqs %d %d", seq1, seq2)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: sequence counter must continue past the replayed records.
	s2, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	var ids []NodeID
	if err := s2.Replay(func(rec Record) error {
		if rec.Type != RecNodeRegistered {
			t.Fatalf("unexpected type %s", rec.Type)
		}
		var pl NodeRegistered
		if err := decode(rec.Payload, &pl); err != nil {
			return err
		}
		ids = append(ids, pl.Node.ID)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != "A" || ids[1] != "B" {
		t.Fatalf("replay ids %v", ids)
	}
	seq3, err := s2.Append(RecNodeRegistered, NodeRegistered{Node: Node{ID: "C"}})
	if err != nil {
		t.Fatal(err)
	}
	if seq3 != 3 {
		t.Fatalf("seq after reopen %d", seq3)
	}
	_ = s2.Close()
}

func TestFileStoreIgnoresTornTrailingRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.jsonl")
	s, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Append(RecNodeRegistered, NodeRegistered{Node: Node{ID: "A"}}); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Simulate a crash mid-write: append an incomplete JSON line.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(`{"seq":2,"type":"node_regi`); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	s2, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	if err := s2.Replay(func(rec Record) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("torn tail should be tolerated: %v", err)
	}
	if count != 1 {
		t.Fatalf("replayed %d records, want 1", count)
	}
	// The torn line must not appear in new committed records.
	var types []string
	_ = s2.Replay(func(rec Record) error {
		types = append(types, string(rec.Type))
		return nil
	})
	if len(types) != 1 || !strings.HasPrefix(types[0], "node_registered") {
		t.Fatalf("types %v", types)
	}
	// A post-repair append must form its own clean record (no line joining).
	seq, err := s2.Append(RecChannelOpened, ChannelOpened{Channel: Channel{ID: "c1"}})
	if err != nil {
		t.Fatal(err)
	}
	if seq != 2 {
		t.Fatalf("seq after repair %d", seq)
	}
	var rt []RecordType
	if err := s2.Replay(func(rec Record) error {
		rt = append(rt, rec.Type)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(rt) != 2 || rt[0] != RecNodeRegistered || rt[1] != RecChannelOpened {
		t.Fatalf("records after repair: %v", rt)
	}
	_ = s2.Close()
}
