package raft

import (
	"testing"
	"time"

	"github.com/cockroachdb/pebble"
	pb "github.com/shrec21/engram/proto/engrampb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestDB(t *testing.T) *pebble.DB {
	t.Helper()
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestDedupCheckMiss(t *testing.T) {
	d := NewDedupTable(newTestDB(t))
	_, dup := d.Check("client1", 1)
	assert.False(t, dup)
}

func TestDedupRecordAndCheck(t *testing.T) {
	d := NewDedupTable(newTestDB(t))
	now := time.Now()

	result := &pb.CommandResult{
		Result: &pb.CommandResult_Mutation{
			Mutation: &pb.MutationResult{Version: 42},
		},
	}

	require.NoError(t, d.Record("client1", 5, result, now))

	// Same seq → duplicate.
	cached, dup := d.Check("client1", 5)
	assert.True(t, dup)
	require.NotNil(t, cached)
	assert.Equal(t, uint64(42), cached.GetMutation().Version)

	// Lower seq → still duplicate.
	_, dup = d.Check("client1", 3)
	assert.True(t, dup)

	// Higher seq → not duplicate.
	_, dup = d.Check("client1", 6)
	assert.False(t, dup)
}

func TestDedupEmptyClientID(t *testing.T) {
	d := NewDedupTable(newTestDB(t))
	_, dup := d.Check("", 1)
	assert.False(t, dup, "empty client_id should never be a dup")

	require.NoError(t, d.Record("", 1, nil, time.Now()))
}

func TestDedupPrune(t *testing.T) {
	d := NewDedupTable(newTestDB(t))
	old := time.Now().Add(-10 * time.Minute)
	recent := time.Now()

	require.NoError(t, d.Record("old-client", 1, nil, old))
	require.NoError(t, d.Record("new-client", 1, nil, recent))

	pruned, err := d.Prune(time.Now().Add(-5 * time.Minute))
	require.NoError(t, err)
	assert.Equal(t, 1, pruned)

	// old-client should be gone.
	_, dup := d.Check("old-client", 1)
	assert.False(t, dup)

	// new-client should still be there.
	_, dup = d.Check("new-client", 1)
	assert.True(t, dup)
}
