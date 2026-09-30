package store

import (
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/shrec21/engram/internal/clock"
	pb "github.com/shrec21/engram/proto/engrampb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Store is the core deterministic state machine backed by Pebble.
type Store struct {
	db    *pebble.DB
	clock clock.Clock
	mu    sync.Mutex // serializes Apply calls; replaced by Raft log in Week 2
}

// New opens or creates a Pebble database at the given path.
func New(path string, clk clock.Clock) (*Store, error) {
	db, err := pebble.Open(path, &pebble.Options{})
	if err != nil {
		return nil, fmt.Errorf("pebble open: %w", err)
	}
	return &Store{db: db, clock: clk}, nil
}

// Close closes the underlying Pebble database.
func (s *Store) Close() error {
	return s.db.Close()
}

// Apply executes a deterministic command against the store.
// No wall-clock reads — all timestamps come from cmd.Timestamp.
// The mutex serializes calls for single-node CAS correctness.
func (s *Store) Apply(cmd *pb.Command) (*pb.CommandResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch p := cmd.Payload.(type) {
	case *pb.Command_Put:
		return s.applyPut(cmd.Timestamp, p.Put)
	case *pb.Command_CompareAndSet:
		return s.applyCAS(cmd.Timestamp, p.CompareAndSet)
	case *pb.Command_Delete:
		return s.applyDelete(p.Delete)
	default:
		return nil, fmt.Errorf("unknown command payload type: %T", cmd.Payload)
	}
}

func (s *Store) applyPut(ts *timestamppb.Timestamp, cmd *pb.PutCommand) (*pb.CommandResult, error) {
	pkey := EncodeKey(cmd.Namespace, cmd.Key)

	var existing pb.MemoryRecord
	var version uint64

	data, closer, err := s.db.Get(pkey)
	if err == nil {
		defer closer.Close()
		if err := proto.Unmarshal(data, &existing); err != nil {
			return nil, fmt.Errorf("unmarshal existing: %w", err)
		}
		version = existing.Version
	} else if err != pebble.ErrNotFound {
		return nil, fmt.Errorf("pebble get: %w", err)
	}

	version++
	rec := &pb.MemoryRecord{
		Namespace:  cmd.Namespace,
		Key:        cmd.Key,
		Value:      cmd.Value,
		Version:    version,
		CreatedBy:  cmd.CreatedBy,
		Source:     cmd.Source,
		Confidence: cmd.Confidence,
		UpdatedAt:  ts,
		ExpiresAt:  cmd.ExpiresAt,
	}

	if existing.CreatedAt != nil {
		rec.CreatedAt = existing.CreatedAt
	} else {
		rec.CreatedAt = ts
	}

	// Preserve access metadata across puts.
	rec.LastAccessedAt = existing.LastAccessedAt
	rec.AccessCount = existing.AccessCount

	encoded, err := proto.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}

	if err := s.db.Set(pkey, encoded, pebble.Sync); err != nil {
		return nil, fmt.Errorf("pebble set: %w", err)
	}

	return &pb.CommandResult{
		Result: &pb.CommandResult_Mutation{
			Mutation: &pb.MutationResult{Version: version},
		},
	}, nil
}

func (s *Store) applyCAS(ts *timestamppb.Timestamp, cmd *pb.CompareAndSetCommand) (*pb.CommandResult, error) {
	pkey := EncodeKey(cmd.Namespace, cmd.Key)

	data, closer, err := s.db.Get(pkey)

	if cmd.ExpectedVersion == 0 {
		// Create-only: key must NOT exist.
		if err == nil {
			closer.Close()
			var rec pb.MemoryRecord
			_ = proto.Unmarshal(data, &rec)
			return &pb.CommandResult{
				Result: &pb.CommandResult_CasConflict{
					CasConflict: &pb.CASConflict{CurrentVersion: rec.Version},
				},
			}, nil
		}
		if err != pebble.ErrNotFound {
			return nil, fmt.Errorf("pebble get: %w", err)
		}
	} else {
		// Update: key must exist with matching version.
		if err == pebble.ErrNotFound {
			return &pb.CommandResult{
				Result: &pb.CommandResult_CasConflict{
					CasConflict: &pb.CASConflict{CurrentVersion: 0},
				},
			}, nil
		}
		if err != nil {
			return nil, fmt.Errorf("pebble get: %w", err)
		}
		closer.Close()

		var rec pb.MemoryRecord
		if err := proto.Unmarshal(data, &rec); err != nil {
			return nil, fmt.Errorf("unmarshal: %w", err)
		}
		if rec.Version != cmd.ExpectedVersion {
			return &pb.CommandResult{
				Result: &pb.CommandResult_CasConflict{
					CasConflict: &pb.CASConflict{CurrentVersion: rec.Version},
				},
			}, nil
		}
	}

	var version uint64
	if cmd.ExpectedVersion == 0 {
		version = 1
	} else {
		version = cmd.ExpectedVersion + 1
	}

	var createdAt *timestamppb.Timestamp
	if cmd.ExpectedVersion > 0 {
		// Updating: preserve original created_at by re-reading.
		// We already read and validated above, safe to re-read.
		d, c, err := s.db.Get(pkey)
		if err == nil {
			c.Close()
			var old pb.MemoryRecord
			_ = proto.Unmarshal(d, &old)
			createdAt = old.CreatedAt
		}
	}
	if createdAt == nil {
		createdAt = ts
	}

	rec := &pb.MemoryRecord{
		Namespace:  cmd.Namespace,
		Key:        cmd.Key,
		Value:      cmd.Value,
		Version:    version,
		CreatedBy:  cmd.CreatedBy,
		Source:     cmd.Source,
		Confidence: cmd.Confidence,
		CreatedAt:  createdAt,
		UpdatedAt:  ts,
		ExpiresAt:  cmd.ExpiresAt,
	}

	encoded, err := proto.Marshal(rec)
	if err != nil {
		return nil, fmt.Errorf("marshal: %w", err)
	}

	if err := s.db.Set(pkey, encoded, pebble.Sync); err != nil {
		return nil, fmt.Errorf("pebble set: %w", err)
	}

	return &pb.CommandResult{
		Result: &pb.CommandResult_Mutation{
			Mutation: &pb.MutationResult{Version: version},
		},
	}, nil
}

func (s *Store) applyDelete(cmd *pb.DeleteCommand) (*pb.CommandResult, error) {
	pkey := EncodeKey(cmd.Namespace, cmd.Key)

	if cmd.ExpectedVersion != 0 {
		data, closer, err := s.db.Get(pkey)
		if err == pebble.ErrNotFound {
			return &pb.CommandResult{
				Result: &pb.CommandResult_CasConflict{
					CasConflict: &pb.CASConflict{CurrentVersion: 0},
				},
			}, nil
		}
		if err != nil {
			return nil, fmt.Errorf("pebble get: %w", err)
		}
		closer.Close()

		var rec pb.MemoryRecord
		if err := proto.Unmarshal(data, &rec); err != nil {
			return nil, fmt.Errorf("unmarshal: %w", err)
		}
		if rec.Version != cmd.ExpectedVersion {
			return &pb.CommandResult{
				Result: &pb.CommandResult_CasConflict{
					CasConflict: &pb.CASConflict{CurrentVersion: rec.Version},
				},
			}, nil
		}
	}

	if err := s.db.Delete(pkey, pebble.Sync); err != nil {
		return nil, fmt.Errorf("pebble delete: %w", err)
	}

	return &pb.CommandResult{
		Result: &pb.CommandResult_DeleteResult{
			DeleteResult: &pb.DeleteResult{},
		},
	}, nil
}

// Get retrieves a record and updates access metadata.
// Returns (nil, nil) if the key does not exist or is expired.
// Access metadata write-back holds the mutex to prevent clobbering concurrent Apply writes.
func (s *Store) Get(namespace, key string) (*pb.MemoryRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pkey := EncodeKey(namespace, key)

	data, closer, err := s.db.Get(pkey)
	if err == pebble.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("pebble get: %w", err)
	}
	defer closer.Close()

	var rec pb.MemoryRecord
	if err := proto.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}

	now := s.clock.Now()

	// Lazy TTL: expired records are invisible.
	if rec.ExpiresAt != nil && rec.ExpiresAt.AsTime().Before(now) {
		return nil, nil
	}

	// Update access metadata (non-deterministic side-effect, outside Apply).
	rec.LastAccessedAt = timestamppb.New(now)
	rec.AccessCount++

	updated, err := proto.Marshal(&rec)
	if err != nil {
		return nil, fmt.Errorf("marshal access update: %w", err)
	}
	_ = s.db.Set(pkey, updated, pebble.NoSync)

	return &rec, nil
}

// List returns records in a namespace matching an optional key prefix, with pagination.
func (s *Store) List(namespace, keyPrefix string, limit int, pageToken string) ([]*pb.MemoryRecord, string, error) {
	prefix := NamespacePrefixWithKey(namespace, keyPrefix)
	upper := prefixUpperBound(prefix)

	iterOpts := &pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: upper,
	}

	iter, err := s.db.NewIter(iterOpts)
	if err != nil {
		return nil, "", fmt.Errorf("new iter: %w", err)
	}
	defer iter.Close()

	if pageToken != "" {
		startKey := decodePageToken(pageToken)
		iter.SeekGE(startKey)
		if iter.Valid() {
			// Skip the token key (it was the last key returned previously).
			iter.Next()
		}
	} else {
		iter.First()
	}

	now := s.clock.Now()
	var records []*pb.MemoryRecord
	var lastKey []byte

	for iter.Valid() && (limit <= 0 || len(records) < limit) {
		var rec pb.MemoryRecord
		if err := proto.Unmarshal(iter.Value(), &rec); err != nil {
			iter.Next()
			continue
		}

		// Lazy TTL: skip expired records.
		if rec.ExpiresAt != nil && rec.ExpiresAt.AsTime().Before(now) {
			iter.Next()
			continue
		}

		records = append(records, &rec)
		lastKey = append(lastKey[:0], iter.Key()...)
		iter.Next()
	}

	var nextPageToken string
	if iter.Valid() && len(records) == limit {
		nextPageToken = encodePageToken(lastKey)
	}

	return records, nextPageToken, nil
}

// ExpiredEntry identifies a record that has passed its TTL.
type ExpiredEntry struct {
	Namespace string
	Key       string
}

// ScanExpired returns all records whose expires_at is before the given time.
func (s *Store) ScanExpired(before time.Time) ([]ExpiredEntry, error) {
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var expired []ExpiredEntry
	for iter.First(); iter.Valid(); iter.Next() {
		var rec pb.MemoryRecord
		if err := proto.Unmarshal(iter.Value(), &rec); err != nil {
			continue
		}
		if rec.ExpiresAt != nil && rec.ExpiresAt.AsTime().Before(before) {
			ns, key := DecodeKey(iter.Key())
			expired = append(expired, ExpiredEntry{Namespace: ns, Key: key})
		}
	}
	return expired, iter.Error()
}
