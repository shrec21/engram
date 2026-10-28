package embedding

import (
	"context"
	"hash/fnv"
	"math"
)

// FakeEmbedder produces deterministic embeddings from FNV hashes of the input text.
// Useful for testing without an external model.
type FakeEmbedder struct {
	dim int
}

// NewFakeEmbedder creates a FakeEmbedder with the given dimensionality.
func NewFakeEmbedder(dim int) *FakeEmbedder {
	return &FakeEmbedder{dim: dim}
}

func (f *FakeEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	results := make([][]float32, len(texts))
	for i, text := range texts {
		results[i] = f.hashToVector(text)
	}
	return results, nil
}

func (f *FakeEmbedder) Dimension() int { return f.dim }
func (f *FakeEmbedder) Model() string  { return "fake-embed" }

func (f *FakeEmbedder) hashToVector(text string) []float32 {
	vec := make([]float32, f.dim)
	h := fnv.New64a()
	h.Write([]byte(text))
	seed := h.Sum64()

	// Generate deterministic pseudo-random components using xorshift.
	var norm float64
	for j := 0; j < f.dim; j++ {
		seed ^= seed << 13
		seed ^= seed >> 7
		seed ^= seed << 17
		// Map to [-1, 1].
		val := float64(int64(seed)) / float64(math.MaxInt64)
		vec[j] = float32(val)
		norm += val * val
	}

	// Normalize to unit vector.
	norm = math.Sqrt(norm)
	if norm > 0 {
		for j := range vec {
			vec[j] = float32(float64(vec[j]) / norm)
		}
	}
	return vec
}

var _ Embedder = (*FakeEmbedder)(nil)
