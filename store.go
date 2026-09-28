package pchr

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
)

// RecordType tags durable WAL records.
type RecordType string

const (
	RecNodeRegistered   RecordType = "node_registered"
	RecChannelOpened    RecordType = "channel_opened"
	RecChannelUpdated   RecordType = "channel_updated"
	RecPaymentLocked    RecordType = "payment_locked"
	RecPaymentSettling  RecordType = "payment_settling"
	RecHopSettled       RecordType = "hop_settled"
	RecPaymentSucceeded RecordType = "payment_succeeded"
	RecHopRefunded      RecordType = "hop_refunded"
	RecPaymentRefunded  RecordType = "payment_refunded"
	RecPaymentFailed    RecordType = "payment_failed"
)

// Record is one durable write-ahead log entry.
type Record struct {
	Seq     int64           `json:"seq"`
	Type    RecordType      `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

// Record payloads.
type NodeRegistered struct {
	Node Node `json:"node"`
}

type ChannelOpened struct {
	Channel Channel `json:"channel"`
}

// ChannelUpdated is an atomic full-state replacement of a channel.
type ChannelUpdated struct {
	Channel Channel `json:"channel"`
}

// PaymentLockedPayload is one atomic record: the payment plus the complete
// post-reservation snapshot of every channel it touches.
type PaymentLockedPayload struct {
	Payment   Payment   `json:"payment"`
	Snapshots []Channel `json:"snapshots"`
}

// PaymentSettlingPayload durably records the revealed preimage, so forward
// settlement can resume after a restart.
type PaymentSettlingPayload struct {
	PaymentID PaymentID `json:"payment_id"`
	Preimage  []byte    `json:"preimage"`
}

// HopSettledPayload atomically pairs the settled-hop cursor with the full
// snapshot of the channel that hop mutated.
type HopSettledPayload struct {
	PaymentID PaymentID `json:"payment_id"`
	HopIndex  int       `json:"hop_index"`
	Snapshot  Channel   `json:"snapshot"`
}

type PaymentSucceededPayload struct {
	PaymentID PaymentID `json:"payment_id"`
}

type HopRefundedPayload struct {
	PaymentID PaymentID `json:"payment_id"`
	HopIndex  int       `json:"hop_index"`
	// Count is the cumulative number of hops released in reverse order.
	Count    int     `json:"count"`
	Snapshot Channel `json:"snapshot"`
}

type PaymentRefundedPayload struct {
	PaymentID PaymentID `json:"payment_id"`
}

type PaymentFailedPayload struct {
	PaymentID PaymentID `json:"payment_id"`
}

// Store is the durable write-ahead log abstraction.
type Store interface {
	// Append atomically persists one record and returns its sequence number.
	Append(recType RecordType, payload any) (int64, error)
	// Replay replays every committed record in order.
	Replay(handle func(Record) error) error
	io.Closer
}

// FileStore is a JSON-lines backed WAL. Every Append is followed by fsync so
// that a committed record survives process crash or machine restart.
type FileStore struct {
	mu   sync.Mutex
	f    *os.File
	path string
	seq  int64
}

// NewFileStore opens (creating if needed) the WAL file at path and rebuilds
// its sequence counter from any existing committed records.
func NewFileStore(path string) (*FileStore, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	s := &FileStore{f: f, path: path}
	var validBytes int64
	if err := s.Replay(func(rec Record) error {
		if rec.Seq > s.seq {
			s.seq = rec.Seq
		}
		return nil
	}); err != nil {
		_ = f.Close()
		return nil, err
	}
	// Locate the byte offset just past the last complete record so any torn
	// trailing line left by a crash mid-write is truncated away before new
	// records are appended.
	if err := s.measureValidLength(&validBytes); err != nil {
		_ = f.Close()
		return nil, err
	}
	if fi, err := f.Stat(); err == nil && fi.Size() != validBytes {
		if err := f.Truncate(validBytes); err != nil {
			_ = f.Close()
			return nil, err
		}
		if _, err := f.Seek(validBytes, io.SeekStart); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	return s, nil
}

// measureValidLength returns the file offset immediately after the final
// parseable newline-terminated record.
func (s *FileStore) measureValidLength(valid *int64) error {
	f, err := os.Open(s.path)
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	var off int64
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			off++
			continue
		}
		var rec Record
		if err := json.Unmarshal(line, &rec); err != nil {
			break
		}
		off += int64(len(line)) + 1 // include trailing newline
	}
	*valid = off
	return sc.Err()
}

// Append encodes and fsyncs a single record.
func (s *FileStore) Append(recType RecordType, payload any) (int64, error) {
	raw, err := json.Marshal(payload)
	if err != nil {
		return 0, err
	}
	rec := Record{Type: recType, Payload: raw}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.seq++
	rec.Seq = s.seq
	line, err := json.Marshal(rec)
	if err != nil {
		s.seq--
		return 0, err
	}
	line = append(line, '\n')
	if _, err := s.f.Write(append([]byte(nil), line...)); err != nil {
		s.seq--
		return 0, err
	}
	if err := s.f.Sync(); err != nil {
		s.seq--
		return 0, err
	}
	return rec.Seq, nil
}

// Replay decodes every committed record in sequence order.
func (s *FileStore) Replay(handle func(Record) error) error {
	f, err := os.Open(s.path)
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		var rec Record
		if err := json.Unmarshal(line, &rec); err != nil {
			// A torn trailing record (crash mid-write) is ignored; fsync +
			// line framing guarantees all earlier records are complete.
			if errors.Is(err, io.ErrUnexpectedEOF) || isJSONSyntax(err) {
				break
			}
			return err
		}
		if err := handle(rec); err != nil {
			return err
		}
	}
	return sc.Err()
}

func isJSONSyntax(err error) bool {
	var se *json.SyntaxError
	return errors.As(err, &se)
}

// Close flushes and closes the underlying WAL file.
func (s *FileStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.f.Close()
}
