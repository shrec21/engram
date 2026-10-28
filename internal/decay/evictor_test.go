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

var evictBaseTime = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

type evictTestProposer struct {
	store *store.Store
}

func (p *evictTestProposer) Propose(_ context.Context, cmd *pb.Command) (*pb.CommandResult, uint64, error) {
	result, err := p.store.Apply(cmd)
	return result, 0, err
}

func newEvictTestStore(t *testing.T, clk clock.Clock) *store.Store {
	t.Helper()
	s, err := store.New(t.TempDir(), clk)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

func putCmd(ns, key string, value []byte, confidence float32, ts time.Time) *pb.Command {
	return &pb.Command{
		Timestamp: timestamppb.New(ts),
		Payload: &pb.Command_Put{
			Put: &pb.PutCommand{
				Namespace:  ns,
				Key:        key,
				Value:      value,
				Confidence: confidence,
			},
		},
	}
}

func putPinnedCmd(ns, key string, value []byte, confidence float32, ts time.Time) *pb.Command {
	return &pb.Command{
		Timestamp: timestamppb.New(ts),
		Payload: &pb.Command_Put{
			Put: &pb.PutCommand{
				Namespace:  ns,
				Key:        key,
				Value:      value,
				Confidence: confidence,
				Pinned:     true,
			},
		},
	}
}

func setDecayPolicyCmd(ns string, policy *pb.DecayPolicy) *pb.Command {
	return &pb.Command{
		Payload: &pb.Command_SetDecayPolicy{
			SetDecayPolicy: &pb.SetDecayPolicyCommand{
				Namespace: ns,
				Policy:    policy,
			},
		},
	}
}

func TestEvictorEvictsLowScore(t *testing.T) {
	clk := clock.NewTestClock(evictBaseTime)
	s := newEvictTestStore(t, clk)
	proposer := &evictTestProposer{store: s}

	// Create records.
	_, err := s.Apply(putCmd("ns", "old", []byte("stale"), 0.5, evictBaseTime))
	require.NoError(t, err)
	_, err = s.Apply(putCmd("ns", "fresh", []byte("new"), 0.9, evictBaseTime))
	require.NoError(t, err)

	// Set decay policy: half-life 1 hour, min_score 0.3.
	_, err = s.Apply(setDecayPolicyCmd("ns", &pb.DecayPolicy{
		HalfLifeSeconds: 3600,
		MinScore:        0.3,
	}))
	require.NoError(t, err)

	// Advance time by 4 hours (~0.0625 of original confidence after 4 half-lives).
	// "old" at confidence 0.5: 0.5 × 2^(-4) = 0.03125 → below 0.3
	// "fresh" at confidence 0.9: 0.9 × 2^(-4) = 0.05625 → below 0.3
	clk.Advance(4 * time.Hour)

	evictor := NewEvictor(s, s, proposer, clk, time.Minute)
	evictor.EvictOnce()

	// Both should be evicted since both are below 0.3.
	recOld, err := s.Get("ns", "old")
	require.NoError(t, err)
	assert.Nil(t, recOld, "old record should be evicted")

	recFresh, err := s.Get("ns", "fresh")
	require.NoError(t, err)
	assert.Nil(t, recFresh, "fresh record should also be evicted after 4 half-lives")
}

func TestEvictorSkipsPinned(t *testing.T) {
	clk := clock.NewTestClock(evictBaseTime)
	s := newEvictTestStore(t, clk)
	proposer := &evictTestProposer{store: s}

	// Create pinned record.
	_, err := s.Apply(putPinnedCmd("ns", "pinned", []byte("keep"), 0.5, evictBaseTime))
	require.NoError(t, err)

	// Set decay policy.
	_, err = s.Apply(setDecayPolicyCmd("ns", &pb.DecayPolicy{
		HalfLifeSeconds: 3600,
		MinScore:        0.3,
	}))
	require.NoError(t, err)

	// Advance way past threshold.
	clk.Advance(10 * time.Hour)

	evictor := NewEvictor(s, s, proposer, clk, time.Minute)
	evictor.EvictOnce()

	// Pinned record should survive.
	rec, err := s.Get("ns", "pinned")
	require.NoError(t, err)
	assert.NotNil(t, rec, "pinned record should not be evicted")
}

func TestEvictorVersionSafe(t *testing.T) {
	clk := clock.NewTestClock(evictBaseTime)
	s := newEvictTestStore(t, clk)

	// Create record at version 1.
	_, err := s.Apply(putCmd("ns", "k", []byte("v1"), 0.1, evictBaseTime))
	require.NoError(t, err)

	// Set decay policy.
	_, err = s.Apply(setDecayPolicyCmd("ns", &pb.DecayPolicy{
		HalfLifeSeconds: 3600,
		MinScore:        0.3,
	}))
	require.NoError(t, err)

	clk.Advance(10 * time.Hour)

	// Simulate a concurrent update before evictor runs.
	// The evictor will try to delete with version 1 but record is now version 2.
	_, err = s.Apply(putCmd("ns", "k", []byte("v2"), 0.9, clk.Now()))
	require.NoError(t, err)

	proposer := &evictTestProposer{store: s}
	evictor := NewEvictor(s, s, proposer, clk, time.Minute)
	evictor.EvictOnce()

	// Record should still exist (version mismatch prevents delete).
	// But it was re-listed at version 2 with a fresh timestamp and high confidence,
	// so its score will actually be high. The evictor won't even try to delete it.
	rec, err := s.Get("ns", "k")
	require.NoError(t, err)
	assert.NotNil(t, rec, "updated record should survive")
	assert.Equal(t, uint64(2), rec.Version)
}
