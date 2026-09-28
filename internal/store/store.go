// Package store provides an append-only write-ahead log used to durably
// persist every state mutation before it is applied in memory.
// On restart the log is replayed to reconstruct the full state.
//
// On-disk format: one JSON record per line. A crash in the middle of an
// append leaves a truncated trailing line; on open the log is scanned and
// truncated back to the last fully written record, so a torn write can
// never corrupt committed state.
package store

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/guoyyit/payment-channel-router-go/internal/model"
)

// Kind discriminates payload types inside an Event.
type Kind string

// Supported event kinds.
const (
	KindNodeRegistered   Kind = "node_registered"
	KindChannelOpened    Kind = "channel_opened"
	KindBalanceChanged   Kind = "balance_changed"
	KindPaymentReserved  Kind = "payment_reserved"
	KindSegmentReserved  Kind = "segment_reserved"
	KindSegmentSettled   Kind = "segment_settled"
	KindSegmentReleased  Kind = "segment_released"
	KindPreimageRevealed Kind = "preimage_revealed"
	KindPaymentCompleted Kind = "payment_completed"
	KindPaymentAborted   Kind = "payment_aborted"
)

// Event is one durable log record with a decoded payload.
type Event struct {
	Seq     int64
	Kind    Kind
	Payload any
}

// record is the on-disk JSON envelope.
type record struct {
	Seq     int64           `json:"seq"`
	Kind    Kind            `json:"kind"`
	Payload json.RawMessage `json:"payload"`
}

// Store is the durable append-only event log.
type Store struct {
	mu      sync.Mutex
	path    string
	f       *os.File
	lastSeq int64
}

// New opens (or creates) a WAL in dir, recovering torn trailing writes.
func New(dir string) (*Store, error) {
	if dir == "" {
		return nil, errors.New("store: empty data dir")
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("store: mkdir: %w", err)
	}
	path := filepath.Join(dir, "wal.log")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("store: open: %w", err)
	}
	s := &Store{path: path, f: f}
	if err := s.recoverTornTail(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return s, nil
}

// recoverTornTail scans every line; validates JSON; truncates after the last
// valid record. A final line without a newline (partial write) or invalid
// JSON is discarded.
func (s *Store) recoverTornTail() error {
	data, err := io.ReadAll(s.f)
	if err != nil {
		return fmt.Errorf("store: read: %w", err)
	}
	goodEnd := int64(0)
	var lastSeq int64
	offset := int64(0)
	r := bufio.NewReader(bytes.NewReader(data))
	for {
		line, err := r.ReadBytes('\n')
		consumed := int64(len(line))
		if len(bytes.TrimSpace(line)) > 0 {
			var rec record
			if json.Unmarshal(line, &rec) == nil && rec.Seq > 0 {
				goodEnd = offset + consumed
				lastSeq = rec.Seq
			}
		}
		offset += consumed
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
	}
	if goodEnd != int64(len(data)) {
		if err := s.f.Truncate(goodEnd); err != nil {
			return fmt.Errorf("store: truncate torn tail: %w", err)
		}
	}
	if _, err := s.f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	s.lastSeq = lastSeq
	return nil
}

// Append durably persists one event (fsync) and returns its sequence number.
func (s *Store) Append(kind Kind, payload any) (int64, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return 0, fmt.Errorf("store: marshal payload: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	seq := s.lastSeq + 1
	rec := record{Seq: seq, Kind: kind, Payload: raw}
	line, err := json.Marshal(rec)
	if err != nil {
		return 0, fmt.Errorf("store: marshal record: %w", err)
	}
	line = append(line, '\n')
	if _, err := s.f.Write(line); err != nil {
		return 0, fmt.Errorf("store: write: %w", err)
	}
	if err := s.f.Sync(); err != nil {
		return 0, fmt.Errorf("store: sync: %w", err)
	}
	s.lastSeq = seq
	return seq, nil
}

// newPayload allocates the concrete payload for a kind.
func newPayload(kind Kind) (any, error) {
	switch kind {
	case KindNodeRegistered:
		return &model.NodeRegistered{}, nil
	case KindChannelOpened:
		return &model.ChannelOpened{}, nil
	case KindBalanceChanged:
		return &model.ChannelBalanceChanged{}, nil
	case KindPaymentReserved:
		return &model.PaymentReserved{}, nil
	case KindSegmentReserved:
		return &model.SegmentReserved{}, nil
	case KindSegmentSettled:
		return &model.SegmentSettled{}, nil
	case KindSegmentReleased:
		return &model.SegmentReleased{}, nil
	case KindPreimageRevealed:
		return &model.PreimageRevealed{}, nil
	case KindPaymentCompleted:
		return &model.PaymentCompleted{}, nil
	case KindPaymentAborted:
		return &model.PaymentAborted{}, nil
	default:
		return nil, fmt.Errorf("store: unknown event kind %q", kind)
	}
}

// Replay replays all events in order, invoking fn for each.
func (s *Store) Replay(fn func(Event) error) error {
	f, err := os.Open(s.path)
	if err != nil {
		return fmt.Errorf("store: reopen for replay: %w", err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 16*1024*1024)
	for sc.Scan() {
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		var rec record
		if err := json.Unmarshal(line, &rec); err != nil {
			return fmt.Errorf("store: corrupt record: %w", err)
		}
		p, err := newPayload(rec.Kind)
		if err != nil {
			return err
		}
		if err := json.Unmarshal(rec.Payload, p); err != nil {
			return fmt.Errorf("store: corrupt payload: %w", err)
		}
		if err := fn(Event{Seq: rec.Seq, Kind: rec.Kind, Payload: p}); err != nil {
			return err
		}
	}
	return sc.Err()
}

// Close flushes and releases file handles.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}

// LastSeq returns the highest persisted sequence number (0 if empty).
func (s *Store) LastSeq() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastSeq
}

// Dir returns the directory containing the WAL file.
func (s *Store) Dir() string { return filepath.Dir(s.path) }
