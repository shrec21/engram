package decay

import (
	"context"
	"testing"
	"time"

	"github.com/shrec21/engram/internal/clock"
	"github.com/shrec21/engram/internal/store"
	pb "github.com/shrec21/engram/proto/engrampb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var baseTime = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

// localProposer wraps store.Apply for single-node testing.
type localProposer struct {
	store *store.Store
}

func (p *localProposer) Propose(_ context.Context, cmd *pb.Command) (*pb.CommandResult, uint64, error) {
	result, err := p.store.Apply(cmd)
	return result, 0, err
}

func putWithTTL(t *testing.T, s *store.Store, ns, key string, value []byte, ts, expiresAt time.Time) {
	t.Helper()
	cmd := &pb.Command{
		Timestamp: timestamppb.New(ts),
		Payload: &pb.Command_Put{
			Put: &pb.PutCommand{
				Namespace: ns,
				Key:       key,
				Value:     value,
				ExpiresAt: timestamppb.New(expiresAt),
			},
		},
	}
	_, err := s.Apply(cmd)
	require.NoError(t, err)
}

func put(t *testing.T, s *store.Store, ns, key string, value []byte, ts time.Time) {
	t.Helper()
	cmd := &pb.Command{
		Timestamp: timestamppb.New(ts),
		Payload: &pb.Command_Put{
			Put: &pb.PutCommand{
				Namespace: ns,
				Key:       key,
				Value:     value,
			},
		},
	}
	_, err := s.Apply(cmd)
	require.NoError(t, err)
}

func TestSweeperDeletesExpiredRecords(t *testing.T) {
	clk := clock.NewTestClock(baseTime)
	s, err := store.New(t.TempDir(), clk)
	require.NoError(t, err)
	defer s.Close()

	putWithTTL(t, s, "ns", "ephemeral", []byte("temp"), baseTime, baseTime.Add(time.Hour))
	put(t, s, "ns", "permanent", []byte("keep"), baseTime)

	// Before expiry: both visible.
	rec, err := s.Get("ns", "ephemeral")
	require.NoError(t, err)
	require.NotNil(t, rec)

	// Advance past expiry.
	clk.Advance(2 * time.Hour)

	// Sweep.
	sw := NewSweeper(s, &localProposer{store: s}, clk, time.Minute)
	sw.SweepOnce()

	// Expired record is gone (deleted from Pebble, not just invisible).
	expired, err := s.ScanExpired(clk.Now())
	require.NoError(t, err)
	assert.Empty(t, expired, "sweeper should have deleted expired records")

	// Permanent record is untouched.
	rec, err = s.Get("ns", "permanent")
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, []byte("keep"), rec.Value)
}

func TestSweeperLeavesNonExpiredAlone(t *testing.T) {
	clk := clock.NewTestClock(baseTime)
	s, err := store.New(t.TempDir(), clk)
	require.NoError(t, err)
	defer s.Close()

	// TTL in the future.
	putWithTTL(t, s, "ns", "future", []byte("ok"), baseTime, baseTime.Add(24*time.Hour))

	sw := NewSweeper(s, &localProposer{store: s}, clk, time.Minute)
	sw.SweepOnce()

	rec, err := s.Get("ns", "future")
	require.NoError(t, err)
	require.NotNil(t, rec)
}

func TestSweeperMultipleExpired(t *testing.T) {
	clk := clock.NewTestClock(baseTime)
	s, err := store.New(t.TempDir(), clk)
	require.NoError(t, err)
	defer s.Close()

	for i := 0; i < 10; i++ {
		putWithTTL(t, s, "ns", string(rune('a'+i)), []byte("data"), baseTime, baseTime.Add(time.Duration(i+1)*time.Minute))
	}

	// Advance past all TTLs.
	clk.Advance(time.Hour)

	sw := NewSweeper(s, &localProposer{store: s}, clk, time.Minute)
	sw.SweepOnce()

	expired, err := s.ScanExpired(clk.Now())
	require.NoError(t, err)
	assert.Empty(t, expired)
}

func TestTouchFlusher(t *testing.T) {
	clk := clock.NewTestClock(baseTime)
	s, err := store.New(t.TempDir(), clk)
	require.NoError(t, err)
	defer s.Close()

	put(t, s, "ns", "k", []byte("v"), baseTime)

	tracker := store.NewAccessTracker()
	t1 := baseTime.Add(time.Minute)
	tracker.Track("ns", "k", t1)
	tracker.Track("ns", "k", t1.Add(time.Second))

	flusher := NewTouchFlusher(tracker, &localProposer{store: s}, time.Second)
	flusher.FlushOnce()

	rec, err := s.Get("ns", "k")
	require.NoError(t, err)
	require.NotNil(t, rec)
	assert.Equal(t, uint64(2), rec.AccessCount)
}
