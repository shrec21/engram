package store

import (
	"encoding/binary"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/shrec21/engram/internal/clock"
	pb "github.com/shrec21/engram/proto/engrampb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var baseTime = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

func newTestStore(t *testing.T) (*Store, *clock.TestClock) {
	t.Helper()
	clk := clock.NewTestClock(baseTime)
	s, err := New(t.TempDir(), clk)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s, clk
}

func putCmd(ns, key string, value []byte, ts time.Time) *pb.Command {
	return &pb.Command{
		Timestamp: timestamppb.New(ts),
		Payload: &pb.Command_Put{
			Put: &pb.PutCommand{
				Namespace: ns,
				Key:       key,
				Value:     value,
				CreatedBy: "test-agent",
				Source:    "test",
			},
		},
	}
}

func putCmdWithTTL(ns, key string, value []byte, ts time.Time, expiresAt time.Time) *pb.Command {
	cmd := putCmd(ns, key, value, ts)
	cmd.GetPut().ExpiresAt = timestamppb.New(expiresAt)
	return cmd
}

func casCmd(ns, key string, value []byte, expectedVersion uint64, ts time.Time) *pb.Command {
	return &pb.Command{
		Timestamp: timestamppb.New(ts),
		Payload: &pb.Command_CompareAndSet{
			CompareAndSet: &pb.CompareAndSetCommand{
				Namespace:       ns,
				Key:             key,
				Value:           value,
				ExpectedVersion: expectedVersion,
				CreatedBy:       "test-agent",
				Source:          "test",
			},
		},
	}
}

func deleteCmd(ns, key string, expectedVersion uint64, ts time.Time) *pb.Command {
	return &pb.Command{
		Timestamp: timestamppb.New(ts),
		Payload: &pb.Command_Delete{
			Delete: &pb.DeleteCommand{
				Namespace:       ns,
				Key:             key,
				ExpectedVersion: expectedVersion,
			},
		},
	}
}

// --- Put tests ---

func TestPutAndGet(t *testing.T) {
	s, _ := newTestStore(t)

	res, err := s.Apply(putCmd("ns", "key1", []byte("hello"), baseTime))
	require.NoError(t, err)
	require.NotNil(t, res.GetMutation())
	assert.Equal(t, uint64(1), res.GetMutation().Version)

	rec, err := s.Get("ns", "key1")
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, "ns", rec.Namespace)
	assert.Equal(t, "key1", rec.Key)
	assert.Equal(t, []byte("hello"), rec.Value)
	assert.Equal(t, uint64(1), rec.Version)
	assert.Equal(t, "test-agent", rec.CreatedBy)
	assert.Equal(t, "test", rec.Source)
}

func TestPutUpdatesVersion(t *testing.T) {
	s, _ := newTestStore(t)

	res1, err := s.Apply(putCmd("ns", "k", []byte("v1"), baseTime))
	require.NoError(t, err)
	assert.Equal(t, uint64(1), res1.GetMutation().Version)

	res2, err := s.Apply(putCmd("ns", "k", []byte("v2"), baseTime.Add(time.Second)))
	require.NoError(t, err)
	assert.Equal(t, uint64(2), res2.GetMutation().Version)

	rec, err := s.Get("ns", "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), rec.Value)
	assert.Equal(t, uint64(2), rec.Version)
}

func TestPutPreservesCreatedAt(t *testing.T) {
	s, _ := newTestStore(t)

	t1 := baseTime
	t2 := baseTime.Add(time.Hour)

	_, err := s.Apply(putCmd("ns", "k", []byte("v1"), t1))
	require.NoError(t, err)

	_, err = s.Apply(putCmd("ns", "k", []byte("v2"), t2))
	require.NoError(t, err)

	rec, err := s.Get("ns", "k")
	require.NoError(t, err)
	assert.Equal(t, t1.Unix(), rec.CreatedAt.AsTime().Unix(), "created_at should be preserved")
	assert.Equal(t, t2.Unix(), rec.UpdatedAt.AsTime().Unix(), "updated_at should reflect second put")
}

// --- CAS tests ---

func TestCASCreateOnly(t *testing.T) {
	s, _ := newTestStore(t)

	// Create with expected_version=0 should succeed.
	res, err := s.Apply(casCmd("ns", "k", []byte("v1"), 0, baseTime))
	require.NoError(t, err)
	require.NotNil(t, res.GetMutation())
	assert.Equal(t, uint64(1), res.GetMutation().Version)

	// Second create attempt should conflict.
	res2, err := s.Apply(casCmd("ns", "k", []byte("v2"), 0, baseTime))
	require.NoError(t, err)
	require.NotNil(t, res2.GetCasConflict())
	assert.Equal(t, uint64(1), res2.GetCasConflict().CurrentVersion)
}

func TestCASUpdate(t *testing.T) {
	s, _ := newTestStore(t)

	_, err := s.Apply(putCmd("ns", "k", []byte("v1"), baseTime))
	require.NoError(t, err)

	res, err := s.Apply(casCmd("ns", "k", []byte("v2"), 1, baseTime.Add(time.Second)))
	require.NoError(t, err)
	require.NotNil(t, res.GetMutation())
	assert.Equal(t, uint64(2), res.GetMutation().Version)

	rec, err := s.Get("ns", "k")
	require.NoError(t, err)
	assert.Equal(t, []byte("v2"), rec.Value)
}

func TestCASVersionMismatch(t *testing.T) {
	s, _ := newTestStore(t)

	_, err := s.Apply(putCmd("ns", "k", []byte("v1"), baseTime))
	require.NoError(t, err)

	res, err := s.Apply(casCmd("ns", "k", []byte("v2"), 5, baseTime))
	require.NoError(t, err)
	require.NotNil(t, res.GetCasConflict())
	assert.Equal(t, uint64(1), res.GetCasConflict().CurrentVersion)
}

func TestCASNotFound(t *testing.T) {
	s, _ := newTestStore(t)

	res, err := s.Apply(casCmd("ns", "k", []byte("v1"), 3, baseTime))
	require.NoError(t, err)
	require.NotNil(t, res.GetCasConflict())
	assert.Equal(t, uint64(0), res.GetCasConflict().CurrentVersion)
}

// --- Delete tests ---

func TestDeleteUnconditional(t *testing.T) {
	s, _ := newTestStore(t)

	_, err := s.Apply(putCmd("ns", "k", []byte("v1"), baseTime))
	require.NoError(t, err)

	res, err := s.Apply(deleteCmd("ns", "k", 0, baseTime))
	require.NoError(t, err)
	require.NotNil(t, res.GetDeleteResult())

	rec, err := s.Get("ns", "k")
	require.NoError(t, err)
	assert.Nil(t, rec)
}

func TestDeleteConditional(t *testing.T) {
	s, _ := newTestStore(t)

	_, err := s.Apply(putCmd("ns", "k", []byte("v1"), baseTime))
	require.NoError(t, err)

	res, err := s.Apply(deleteCmd("ns", "k", 1, baseTime))
	require.NoError(t, err)
	require.NotNil(t, res.GetDeleteResult())

	rec, err := s.Get("ns", "k")
	require.NoError(t, err)
	assert.Nil(t, rec)
}

func TestDeleteVersionMismatch(t *testing.T) {
	s, _ := newTestStore(t)

	_, err := s.Apply(putCmd("ns", "k", []byte("v1"), baseTime))
	require.NoError(t, err)

	res, err := s.Apply(deleteCmd("ns", "k", 5, baseTime))
	require.NoError(t, err)
	require.NotNil(t, res.GetCasConflict())
	assert.Equal(t, uint64(1), res.GetCasConflict().CurrentVersion)

	// Key should still exist.
	rec, err := s.Get("ns", "k")
	require.NoError(t, err)
	assert.NotNil(t, rec)
}

// --- TTL tests ---

func TestGetLazyTTLExpiry(t *testing.T) {
	s, clk := newTestStore(t)

	expiresAt := baseTime.Add(time.Hour)
	_, err := s.Apply(putCmdWithTTL("ns", "k", []byte("ephemeral"), baseTime, expiresAt))
	require.NoError(t, err)

	// Before expiry: visible.
	rec, err := s.Get("ns", "k")
	require.NoError(t, err)
	require.NotNil(t, rec)

	// Advance past expiry.
	clk.Advance(2 * time.Hour)

	rec, err = s.Get("ns", "k")
	require.NoError(t, err)
	assert.Nil(t, rec, "expired record should be invisible")
}

// --- Access metadata tests ---

func TestGetUpdatesAccessMetadata(t *testing.T) {
	s, clk := newTestStore(t)

	_, err := s.Apply(putCmd("ns", "k", []byte("v"), baseTime))
	require.NoError(t, err)

	clk.Advance(time.Minute)
	rec1, err := s.Get("ns", "k")
	require.NoError(t, err)
	assert.Equal(t, uint64(1), rec1.AccessCount)
	assert.Equal(t, baseTime.Add(time.Minute).Unix(), rec1.LastAccessedAt.AsTime().Unix())

	clk.Advance(time.Minute)
	rec2, err := s.Get("ns", "k")
	require.NoError(t, err)
	assert.Equal(t, uint64(2), rec2.AccessCount)
	assert.Equal(t, baseTime.Add(2*time.Minute).Unix(), rec2.LastAccessedAt.AsTime().Unix())
}

// --- List tests ---

func TestListNamespaceIsolation(t *testing.T) {
	s, _ := newTestStore(t)

	_, err := s.Apply(putCmd("alpha", "k1", []byte("a"), baseTime))
	require.NoError(t, err)
	_, err = s.Apply(putCmd("beta", "k1", []byte("b"), baseTime))
	require.NoError(t, err)

	recs, _, err := s.List("alpha", "", 100, "")
	require.NoError(t, err)
	require.Len(t, recs, 1)
	assert.Equal(t, "alpha", recs[0].Namespace)
	assert.Equal(t, []byte("a"), recs[0].Value)
}

func TestListPagination(t *testing.T) {
	s, _ := newTestStore(t)

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key-%02d", i)
		_, err := s.Apply(putCmd("ns", key, []byte(fmt.Sprintf("v%d", i)), baseTime))
		require.NoError(t, err)
	}

	// Page 1: first 3.
	recs1, token1, err := s.List("ns", "", 3, "")
	require.NoError(t, err)
	assert.Len(t, recs1, 3)
	assert.NotEmpty(t, token1)
	assert.Equal(t, "key-00", recs1[0].Key)
	assert.Equal(t, "key-02", recs1[2].Key)

	// Page 2: next 3.
	recs2, token2, err := s.List("ns", "", 3, token1)
	require.NoError(t, err)
	assert.Len(t, recs2, 3)
	assert.NotEmpty(t, token2)
	assert.Equal(t, "key-03", recs2[0].Key)

	// Page 3: next 3.
	recs3, token3, err := s.List("ns", "", 3, token2)
	require.NoError(t, err)
	assert.Len(t, recs3, 3)
	assert.NotEmpty(t, token3)
	assert.Equal(t, "key-06", recs3[0].Key)

	// Page 4: last 1.
	recs4, token4, err := s.List("ns", "", 3, token3)
	require.NoError(t, err)
	assert.Len(t, recs4, 1)
	assert.Empty(t, token4)
	assert.Equal(t, "key-09", recs4[0].Key)
}

func TestListSkipsExpired(t *testing.T) {
	s, clk := newTestStore(t)

	_, err := s.Apply(putCmd("ns", "live", []byte("ok"), baseTime))
	require.NoError(t, err)
	_, err = s.Apply(putCmdWithTTL("ns", "dead", []byte("gone"), baseTime, baseTime.Add(time.Hour)))
	require.NoError(t, err)

	clk.Advance(2 * time.Hour)

	recs, _, err := s.List("ns", "", 100, "")
	require.NoError(t, err)
	require.Len(t, recs, 1)
	assert.Equal(t, "live", recs[0].Key)
}

// --- Concurrency tests ---

func TestCASConcurrency(t *testing.T) {
	s, _ := newTestStore(t)

	// Seed the counter at 0.
	_, err := s.Apply(putCmd("ns", "counter", encodeUint64(0), baseTime))
	require.NoError(t, err)

	const goroutines = 50
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				rec, err := s.Get("ns", "counter")
				if err != nil {
					t.Errorf("get: %v", err)
					return
				}
				current := decodeUint64(rec.Value)
				next := current + 1

				res, err := s.Apply(casCmd("ns", "counter", encodeUint64(next), rec.Version, baseTime))
				if err != nil {
					t.Errorf("apply: %v", err)
					return
				}
				if res.GetMutation() != nil {
					return // success
				}
				// CAS conflict — retry.
			}
		}()
	}

	wg.Wait()

	rec, err := s.Get("ns", "counter")
	require.NoError(t, err)
	assert.Equal(t, uint64(goroutines), decodeUint64(rec.Value),
		"final counter must equal the number of goroutines")
}

func TestPutLosesUpdates(t *testing.T) {
	s, _ := newTestStore(t)

	// Seed the counter at 0.
	_, err := s.Apply(putCmd("ns", "counter", encodeUint64(0), baseTime))
	require.NoError(t, err)

	const goroutines = 50
	var wg sync.WaitGroup

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec, err := s.Get("ns", "counter")
			if err != nil {
				t.Errorf("get: %v", err)
				return
			}
			current := decodeUint64(rec.Value)
			next := current + 1
			_, err = s.Apply(putCmd("ns", "counter", encodeUint64(next), baseTime))
			if err != nil {
				t.Errorf("apply: %v", err)
			}
		}()
	}

	wg.Wait()

	rec, err := s.Get("ns", "counter")
	require.NoError(t, err)
	final := decodeUint64(rec.Value)
	assert.Less(t, final, uint64(goroutines),
		"plain Put should lose updates: got %d, expected less than %d", final, goroutines)
}

// --- Restart durability test ---

func TestRestartDurability(t *testing.T) {
	dir := t.TempDir()
	clk := clock.NewTestClock(baseTime)

	s1, err := New(dir, clk)
	require.NoError(t, err)

	_, err = s1.Apply(putCmd("ns", "persist", []byte("durable"), baseTime))
	require.NoError(t, err)

	require.NoError(t, s1.Close())

	// Reopen.
	s2, err := New(dir, clk)
	require.NoError(t, err)
	defer s2.Close()

	rec, err := s2.Get("ns", "persist")
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, []byte("durable"), rec.Value)
	assert.Equal(t, uint64(1), rec.Version)
}

// --- Key encoding tests ---

func TestEncodeDecodeKey(t *testing.T) {
	ns, key := "my-namespace", "my-key"
	encoded := EncodeKey(ns, key)
	gotNs, gotKey := DecodeKey(encoded)
	assert.Equal(t, ns, gotNs)
	assert.Equal(t, key, gotKey)
}

func TestGetNonexistent(t *testing.T) {
	s, _ := newTestStore(t)
	rec, err := s.Get("ns", "missing")
	require.NoError(t, err)
	assert.Nil(t, rec)
}

// --- Helpers ---

func encodeUint64(v uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	return buf
}

func decodeUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
