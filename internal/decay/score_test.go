package decay

import (
	"math"
	"testing"
	"time"

	pb "github.com/shrec21/engram/proto/engrampb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var t0 = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

func TestScoreNoPolicy(t *testing.T) {
	rec := &pb.MemoryRecord{Confidence: 0.8}
	assert.InDelta(t, 0.8, Score(rec, nil, t0), 0.001)
}

func TestScoreZeroConfidenceDefaultsToOne(t *testing.T) {
	rec := &pb.MemoryRecord{}
	assert.InDelta(t, 1.0, Score(rec, nil, t0), 0.001)
}

func TestScoreHalfLifeAccuracy(t *testing.T) {
	policy := &pb.DecayPolicy{HalfLifeSeconds: 3600} // 1 hour

	// At t=0 (just updated): score ≈ confidence.
	rec := &pb.MemoryRecord{
		Confidence: 1.0,
		UpdatedAt:  timestamppb.New(t0),
	}
	score0 := Score(rec, policy, t0)
	assert.InDelta(t, 1.0, score0, 0.001)

	// After exactly one half-life: score ≈ 0.5.
	score1 := Score(rec, policy, t0.Add(time.Hour))
	assert.InDelta(t, 0.5, score1, 0.001)

	// After two half-lives: score ≈ 0.25.
	score2 := Score(rec, policy, t0.Add(2*time.Hour))
	assert.InDelta(t, 0.25, score2, 0.001)
}

func TestScoreAccessBoost(t *testing.T) {
	policy := &pb.DecayPolicy{
		HalfLifeSeconds: 3600,
		AccessWeight:    0.5,
	}

	rec := &pb.MemoryRecord{
		Confidence:  1.0,
		UpdatedAt:   timestamppb.New(t0),
		AccessCount: 10,
	}

	scoreWithAccess := Score(rec, policy, t0)

	recNoAccess := &pb.MemoryRecord{
		Confidence:  1.0,
		UpdatedAt:   timestamppb.New(t0),
		AccessCount: 0,
	}
	scoreNoAccess := Score(recNoAccess, policy, t0)

	// The frequently accessed record should have a higher score.
	assert.Greater(t, scoreWithAccess, scoreNoAccess)

	// Verify the expected formula: 1.0 * 1.0 * (1 + 0.5*ln(11))
	expectedBoost := 1.0 + 0.5*math.Log1p(10)
	assert.InDelta(t, expectedBoost, scoreWithAccess, 0.001)
}

func TestScorePinnedStillScored(t *testing.T) {
	policy := &pb.DecayPolicy{HalfLifeSeconds: 3600}

	// Pinned records still get a score (pinned only prevents eviction).
	rec := &pb.MemoryRecord{
		Confidence: 0.9,
		UpdatedAt:  timestamppb.New(t0),
		Pinned:     true,
	}
	score := Score(rec, policy, t0.Add(2*time.Hour))
	assert.Greater(t, score, 0.0)
	assert.InDelta(t, 0.9*0.25, score, 0.001)
}

func TestCombinedScore(t *testing.T) {
	combined := CombinedScore(0.8, 0.6, 0.7)
	expected := 0.7*0.8 + 0.3*0.6
	assert.InDelta(t, expected, combined, 0.001)
}
