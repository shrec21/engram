package index

import (
	"testing"

	pb "github.com/shrec21/engram/proto/engrampb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSemanticIndexAddSearch(t *testing.T) {
	idx := NewSemanticIndex(3)

	idx.Add("ns", "k1", []float32{1, 0, 0})
	idx.Add("ns", "k2", []float32{0, 1, 0})
	idx.Add("ns", "k3", []float32{0.9, 0.1, 0})

	assert.Equal(t, 3, idx.Len())

	hits := idx.Search("ns", []float32{1, 0, 0}, 3)
	require.GreaterOrEqual(t, len(hits), 2)

	// k1 should be the most similar to [1,0,0].
	assert.Equal(t, "k1", hits[0].Key)
	assert.Greater(t, hits[0].Similarity, float32(0.9))

	// All results should be in "ns".
	for _, h := range hits {
		assert.Equal(t, "ns", h.Namespace)
	}
}

func TestSemanticIndexNamespaceFilter(t *testing.T) {
	idx := NewSemanticIndex(3)

	idx.Add("ns1", "k1", []float32{1, 0, 0})
	idx.Add("ns2", "k2", []float32{0.99, 0.01, 0})

	hits := idx.Search("ns1", []float32{1, 0, 0}, 10)
	require.Len(t, hits, 1)
	assert.Equal(t, "ns1", hits[0].Namespace)
	assert.Equal(t, "k1", hits[0].Key)
}

func TestSemanticIndexDelete(t *testing.T) {
	idx := NewSemanticIndex(3)

	idx.Add("ns", "k1", []float32{1, 0, 0})
	idx.Add("ns", "k2", []float32{0, 1, 0})
	assert.Equal(t, 2, idx.Len())

	idx.Delete("ns", "k1")
	assert.Equal(t, 1, idx.Len())

	hits := idx.Search("ns", []float32{1, 0, 0}, 10)
	require.Len(t, hits, 1)
	assert.Equal(t, "k2", hits[0].Key)
}

func TestSemanticIndexRebuildFrom(t *testing.T) {
	idx := NewSemanticIndex(3)

	// Add some initial data.
	idx.Add("ns", "old", []float32{1, 0, 0})

	records := []*pb.MemoryRecord{
		{Namespace: "ns", Key: "k1", Embedding: []float32{1, 0, 0}},
		{Namespace: "ns", Key: "k2", Embedding: []float32{0, 1, 0}},
		{Namespace: "ns", Key: "k3"}, // no embedding — skip
	}
	idx.RebuildFrom(records)

	assert.Equal(t, 2, idx.Len())

	// "old" should be gone after rebuild.
	hits := idx.Search("ns", []float32{1, 0, 0}, 10)
	for _, h := range hits {
		assert.NotEqual(t, "old", h.Key)
	}
}

func TestSemanticIndexClear(t *testing.T) {
	idx := NewSemanticIndex(3)
	idx.Add("ns", "k1", []float32{1, 0, 0})
	assert.Equal(t, 1, idx.Len())
	idx.Clear()
	assert.Equal(t, 0, idx.Len())
}

func TestSemanticIndexEmptySearch(t *testing.T) {
	idx := NewSemanticIndex(3)
	hits := idx.Search("ns", []float32{1, 0, 0}, 5)
	assert.Empty(t, hits)
}

func TestCompositeKey(t *testing.T) {
	ns, key := "myns", "mykey"
	ck := compositeKey(ns, key)
	gotNs, gotKey := splitCompositeKey(ck)
	assert.Equal(t, ns, gotNs)
	assert.Equal(t, key, gotKey)
}
