package decay

import (
	"math"
	"time"

	pb "github.com/shrec21/engram/proto/engrampb"
)

// Score computes the decay score for a record given a policy and the current time.
// Score = confidence × 2^(-age/half_life) × (1 + w·ln(1+count))
// If policy is nil or half_life is 0, returns confidence unchanged.
func Score(rec *pb.MemoryRecord, policy *pb.DecayPolicy, now time.Time) float64 {
	confidence := float64(rec.Confidence)
	if confidence == 0 {
		confidence = 1.0
	}

	if policy == nil || policy.HalfLifeSeconds <= 0 {
		return confidence
	}

	// Recency factor: exponential decay based on age.
	updatedAt := now
	if rec.UpdatedAt != nil {
		updatedAt = rec.UpdatedAt.AsTime()
	}
	age := now.Sub(updatedAt).Seconds()
	if age < 0 {
		age = 0
	}
	recency := math.Pow(2, -age/policy.HalfLifeSeconds)

	// Access boost: logarithmic boost based on access count.
	accessBoost := 1.0
	if policy.AccessWeight > 0 {
		accessBoost = 1.0 + policy.AccessWeight*math.Log1p(float64(rec.AccessCount))
	}

	return confidence * recency * accessBoost
}

// CombinedScore blends similarity and decay scores.
// combined = alpha * similarity + (1-alpha) * decayScore
func CombinedScore(similarity, decayScore, alpha float64) float64 {
	return alpha*similarity + (1-alpha)*decayScore
}
