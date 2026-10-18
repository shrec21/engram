package raft

import (
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/shrec21/engram/internal/store"
	pb "github.com/shrec21/engram/proto/engrampb"
	"google.golang.org/protobuf/proto"
)

// DedupEntry is stored in Pebble under the dedup key prefix.
type DedupEntry struct {
	Seq          uint64
	CachedResult []byte // marshalled CommandResult
	LastSeen     int64  // unix nanos
}

// DedupTable provides exactly-once semantics by tracking the last processed
// sequence number per client_id. Entries are stored in Pebble so they survive
// snapshots naturally.
type DedupTable struct {
	db *pebble.DB
}

// NewDedupTable creates a dedup table backed by the given Pebble instance.
func NewDedupTable(db *pebble.DB) *DedupTable {
	return &DedupTable{db: db}
}

// Check returns (cachedResult, true) if the command is a duplicate (seq <= stored seq).
// Returns (nil, false) if this is a new command.
func (d *DedupTable) Check(clientID string, seq uint64) (*pb.CommandResult, bool) {
	if clientID == "" {
		return nil, false
	}

	key := store.EncodeDedupKey(clientID)
	data, closer, err := d.db.Get(key)
	if err != nil {
		return nil, false
	}
	defer closer.Close()

	entry, err := unmarshalDedupEntry(data)
	if err != nil {
		return nil, false
	}

	if seq <= entry.Seq {
		// Duplicate — return cached result.
		if len(entry.CachedResult) == 0 {
			return nil, true
		}
		var result pb.CommandResult
		if err := proto.Unmarshal(entry.CachedResult, &result); err != nil {
			return nil, true
		}
		return &result, true
	}
	return nil, false
}

// Record stores the result for a client_id + seq pair.
func (d *DedupTable) Record(clientID string, seq uint64, result *pb.CommandResult, now time.Time) error {
	if clientID == "" {
		return nil
	}

	var cached []byte
	if result != nil {
		var err error
		cached, err = proto.Marshal(result)
		if err != nil {
			return err
		}
	}

	entry := &DedupEntry{
		Seq:          seq,
		CachedResult: cached,
		LastSeen:     now.UnixNano(),
	}

	key := store.EncodeDedupKey(clientID)
	data := marshalDedupEntry(entry)
	return d.db.Set(key, data, pebble.Sync)
}

// Prune removes dedup entries that haven't been seen since before the cutoff time.
func (d *DedupTable) Prune(cutoff time.Time) (int, error) {
	iter, err := d.db.NewIter(&pebble.IterOptions{
		LowerBound: store.DedupKeyPrefix,
		UpperBound: store.EncodeDedupKey("\xFF"), // beyond any client_id
	})
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	var toDelete [][]byte
	cutoffNanos := cutoff.UnixNano()

	for iter.First(); iter.Valid(); iter.Next() {
		entry, err := unmarshalDedupEntry(iter.Value())
		if err != nil {
			continue
		}
		if entry.LastSeen < cutoffNanos {
			key := make([]byte, len(iter.Key()))
			copy(key, iter.Key())
			toDelete = append(toDelete, key)
		}
	}
	if err := iter.Error(); err != nil {
		return 0, err
	}

	for _, key := range toDelete {
		if err := d.db.Delete(key, pebble.Sync); err != nil {
			return 0, err
		}
	}
	return len(toDelete), nil
}

// Simple binary encoding for DedupEntry: seq(8) + lastSeen(8) + cachedResult(rest)
func marshalDedupEntry(e *DedupEntry) []byte {
	buf := make([]byte, 16+len(e.CachedResult))
	buf[0] = byte(e.Seq >> 56)
	buf[1] = byte(e.Seq >> 48)
	buf[2] = byte(e.Seq >> 40)
	buf[3] = byte(e.Seq >> 32)
	buf[4] = byte(e.Seq >> 24)
	buf[5] = byte(e.Seq >> 16)
	buf[6] = byte(e.Seq >> 8)
	buf[7] = byte(e.Seq)
	buf[8] = byte(e.LastSeen >> 56)
	buf[9] = byte(e.LastSeen >> 48)
	buf[10] = byte(e.LastSeen >> 40)
	buf[11] = byte(e.LastSeen >> 32)
	buf[12] = byte(e.LastSeen >> 24)
	buf[13] = byte(e.LastSeen >> 16)
	buf[14] = byte(e.LastSeen >> 8)
	buf[15] = byte(e.LastSeen)
	copy(buf[16:], e.CachedResult)
	return buf
}

func unmarshalDedupEntry(data []byte) (*DedupEntry, error) {
	if len(data) < 16 {
		return nil, pebble.ErrNotFound
	}
	e := &DedupEntry{
		Seq: uint64(data[0])<<56 | uint64(data[1])<<48 | uint64(data[2])<<40 | uint64(data[3])<<32 |
			uint64(data[4])<<24 | uint64(data[5])<<16 | uint64(data[6])<<8 | uint64(data[7]),
		LastSeen: int64(data[8])<<56 | int64(data[9])<<48 | int64(data[10])<<40 | int64(data[11])<<32 |
			int64(data[12])<<24 | int64(data[13])<<16 | int64(data[14])<<8 | int64(data[15]),
	}
	if len(data) > 16 {
		e.CachedResult = data[16:]
	}
	return e, nil
}
