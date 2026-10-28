package embedding

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

type testProposer struct {
	store *store.Store
}

func (p *testProposer) Propose(_ context.Context, cmd *pb.Command) (*pb.CommandResult, uint64, error) {
	result, err := p.store.Apply(cmd)
	return result, 0, err
}

func newTestStore(t *testing.T) *store.Store {
	t.Helper()
	clk := clock.NewTestClock(baseTime)
	s, err := store.New(t.TempDir(), clk)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })
	return s
}

func putRecord(t *testing.T, s *store.Store, ns, key, value string) {
	t.Helper()
	cmd := &pb.Command{
		Timestamp: timestamppb.New(baseTime),
		Payload: &pb.Command_Put{
			Put: &pb.PutCommand{
				Namespace: ns,
				Key:       key,
				Value:     []byte(value),
			},
		},
	}
	_, err := s.Apply(cmd)
	require.NoError(t, err)
}

func TestFakeEmbedderDeterministic(t *testing.T) {
	f := NewFakeEmbedder(64)
	ctx := context.Background()

	v1, err := f.Embed(ctx, []string{"hello world"})
	require.NoError(t, err)
	v2, err := f.Embed(ctx, []string{"hello world"})
	require.NoError(t, err)

	assert.Equal(t, v1[0], v2[0], "same input should produce same vector")
	assert.Equal(t, 64, len(v1[0]))
	assert.Equal(t, "fake-embed", f.Model())
	assert.Equal(t, 64, f.Dimension())
}

func TestFakeEmbedderDifferentInputs(t *testing.T) {
	f := NewFakeEmbedder(32)
	ctx := context.Background()

	vecs, err := f.Embed(ctx, []string{"hello", "world"})
	require.NoError(t, err)
	assert.Len(t, vecs, 2)
	assert.NotEqual(t, vecs[0], vecs[1], "different inputs should produce different vectors")
}

func TestEmbeddingWorkerBackfill(t *testing.T) {
	s := newTestStore(t)
	proposer := &testProposer{store: s}
	embedder := NewFakeEmbedder(32)

	// Insert records without embeddings.
	putRecord(t, s, "ns", "k1", "hello world")
	putRecord(t, s, "ns", "k2", "goodbye world")

	// Verify they need embedding.
	entries, err := s.ScanNeedingEmbedding(100)
	require.NoError(t, err)
	assert.Len(t, entries, 2)

	// Create and run worker briefly.
	w := NewEmbeddingWorker(s, embedder, proposer, 10, 100*time.Millisecond)
	w.Start()
	time.Sleep(500 * time.Millisecond)
	w.Stop()

	// Verify embeddings were set.
	rec1, err := s.Get("ns", "k1")
	require.NoError(t, err)
	require.NotNil(t, rec1)
	assert.Len(t, rec1.Embedding, 32)
	assert.Equal(t, "fake-embed", rec1.EmbeddingModel)
	assert.Equal(t, uint64(1), rec1.EmbeddedVersion)

	rec2, err := s.Get("ns", "k2")
	require.NoError(t, err)
	require.NotNil(t, rec2)
	assert.Len(t, rec2.Embedding, 32)

	// No more records needing embedding.
	entries, err = s.ScanNeedingEmbedding(100)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

func TestEmbeddingVersionRace(t *testing.T) {
	s := newTestStore(t)
	proposer := &testProposer{store: s}

	// Insert record at version 1.
	putRecord(t, s, "ns", "k1", "original")

	// Update it to version 2.
	putRecord(t, s, "ns", "k1", "updated")

	// Try to set embedding for version 1 — should not apply.
	cmd := &pb.Command{
		Timestamp: timestamppb.New(baseTime),
		Payload: &pb.Command_SetEmbedding{
			SetEmbedding: &pb.SetEmbeddingCommand{
				Namespace:       "ns",
				Key:             "k1",
				ExpectedVersion: 1,
				Embedding:       []float32{1, 2, 3},
				EmbeddingModel:  "fake",
			},
		},
	}
	result, _, err := proposer.Propose(context.Background(), cmd)
	require.NoError(t, err)
	assert.False(t, result.GetSetEmbeddingResult().Applied, "should not apply stale embedding")

	rec, err := s.Get("ns", "k1")
	require.NoError(t, err)
	assert.Empty(t, rec.Embedding, "embedding should not be set for stale version")
}
