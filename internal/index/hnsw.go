package index

import (
	"sync"

	"github.com/coder/hnsw"
	pb "github.com/shrec21/engram/proto/engrampb"
)

// SearchHit is a result from the HNSW index.
type SearchHit struct {
	Namespace  string
	Key        string
	Similarity float32
}

// compositeKey encodes namespace+key for HNSW node keys.
func compositeKey(namespace, key string) string {
	return namespace + "\x00" + key
}

// splitCompositeKey decodes a composite key.
func splitCompositeKey(ck string) (namespace, key string) {
	for i := 0; i < len(ck); i++ {
		if ck[i] == 0x00 {
			return ck[:i], ck[i+1:]
		}
	}
	return ck, ""
}

// SemanticIndex wraps coder/hnsw for per-node vector search.
// Thread-safe. Runs on ALL nodes (it's a projection, not authoritative state).
type SemanticIndex struct {
	mu    sync.RWMutex
	graph *hnsw.Graph[string]
	dim   int
}

// NewSemanticIndex creates a new HNSW-backed index.
func NewSemanticIndex(dim int) *SemanticIndex {
	g := &hnsw.Graph[string]{
		M:        16,
		Ml:       0.25,
		EfSearch:  20,
		Distance: hnsw.CosineDistance,
	}
	return &SemanticIndex{
		graph: g,
		dim:   dim,
	}
}

// Add inserts or updates a vector for (namespace, key).
func (idx *SemanticIndex) Add(namespace, key string, embedding []float32) {
	if len(embedding) == 0 {
		return
	}
	ck := compositeKey(namespace, key)
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.graph.Add(hnsw.MakeNode(ck, embedding))
}

// Delete removes a vector for (namespace, key).
func (idx *SemanticIndex) Delete(namespace, key string) {
	ck := compositeKey(namespace, key)
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.graph.Delete(ck)
}

// Search returns up to k nearest neighbors in a given namespace.
// If namespace is empty, searches across all namespaces.
func (idx *SemanticIndex) Search(namespace string, query []float32, k int) []SearchHit {
	if len(query) == 0 || k <= 0 {
		return nil
	}
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Over-fetch to allow for namespace filtering.
	fetchK := k * 3
	if fetchK < 50 {
		fetchK = 50
	}

	results := idx.graph.Search(query, fetchK)

	var hits []SearchHit
	for _, node := range results {
		ns, key := splitCompositeKey(node.Key)
		if namespace != "" && ns != namespace {
			continue
		}
		// Similarity = 1 - cosine distance.
		dist := hnsw.CosineDistance(query, node.Value)
		similarity := 1.0 - dist
		hits = append(hits, SearchHit{
			Namespace:  ns,
			Key:        key,
			Similarity: similarity,
		})
		if len(hits) >= k {
			break
		}
	}
	return hits
}

// Clear removes all entries from the index.
func (idx *SemanticIndex) Clear() {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.graph = &hnsw.Graph[string]{
		M:        16,
		Ml:       0.25,
		EfSearch:  20,
		Distance: hnsw.CosineDistance,
	}
}

// RebuildFrom replaces the entire index from a set of records.
func (idx *SemanticIndex) RebuildFrom(records []*pb.MemoryRecord) {
	g := &hnsw.Graph[string]{
		M:        16,
		Ml:       0.25,
		EfSearch:  20,
		Distance: hnsw.CosineDistance,
	}

	for _, rec := range records {
		if len(rec.Embedding) == 0 {
			continue
		}
		ck := compositeKey(rec.Namespace, rec.Key)
		g.Add(hnsw.MakeNode(ck, rec.Embedding))
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()
	idx.graph = g
}

// Len returns the number of vectors in the index.
func (idx *SemanticIndex) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.graph.Len()
}

// Dimension returns the expected vector dimension.
func (idx *SemanticIndex) Dimension() int {
	return idx.dim
}
