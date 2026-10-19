package test

import (
	"context"
	"testing"
	"time"

	pb "github.com/shrec21/engram/proto/engrampb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClusterPutGetList(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster test in short mode")
	}

	cluster := newTestCluster(t, 3)
	client, conn := cluster.leaderClient(t)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Put.
	putResp, err := client.Put(ctx, &pb.PutRequest{
		Namespace: "ns",
		Key:       "k1",
		Value:     []byte("hello-cluster"),
		CreatedBy: "test",
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), putResp.Version)

	// Get from leader.
	getResp, err := client.Get(ctx, &pb.GetRequest{
		Namespace:   "ns",
		Key:         "k1",
		Consistency: pb.ConsistencyLevel_CONSISTENCY_LINEARIZABLE,
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("hello-cluster"), getResp.Record.Value)

	// List from leader.
	listResp, err := client.List(ctx, &pb.ListRequest{
		Namespace:   "ns",
		Consistency: pb.ConsistencyLevel_CONSISTENCY_LINEARIZABLE,
	})
	require.NoError(t, err)
	assert.Len(t, listResp.Records, 1)
}

func TestClusterWriteToFollowerReturnsUnavailable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster test in short mode")
	}

	cluster := newTestCluster(t, 3)
	cluster.waitForLeader(t, 10*time.Second)

	follower := cluster.followerNode(t)
	client, conn := cluster.anyClient(t, 0)
	defer conn.Close()

	// Find a non-leader node.
	for i, n := range cluster.nodes {
		if n.id == follower.id {
			client, conn = cluster.anyClient(t, i)
			defer conn.Close()
			break
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Put(ctx, &pb.PutRequest{
		Namespace: "ns",
		Key:       "k",
		Value:     []byte("v"),
	})
	require.Error(t, err)
	assert.Equal(t, codes.Unavailable, status.Code(err))
}

func TestClusterCAS(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster test in short mode")
	}

	cluster := newTestCluster(t, 3)
	client, conn := cluster.leaderClient(t)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Put initial.
	_, err := client.Put(ctx, &pb.PutRequest{
		Namespace: "ns", Key: "k", Value: []byte("v1"),
	})
	require.NoError(t, err)

	// CAS success.
	casResp, err := client.CompareAndSet(ctx, &pb.CompareAndSetRequest{
		Namespace:       "ns",
		Key:             "k",
		Value:           []byte("v2"),
		ExpectedVersion: 1,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(2), casResp.Version)

	// CAS conflict.
	_, err = client.CompareAndSet(ctx, &pb.CompareAndSetRequest{
		Namespace:       "ns",
		Key:             "k",
		Value:           []byte("v3"),
		ExpectedVersion: 1,
	})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestClusterSessionConsistency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping cluster test in short mode")
	}

	cluster := newTestCluster(t, 3)
	client, conn := cluster.leaderClient(t)
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Put a value and get the applied_index.
	putResp, err := client.Put(ctx, &pb.PutRequest{
		Namespace: "ns", Key: "k", Value: []byte("v"),
	})
	require.NoError(t, err)
	appliedIndex := putResp.AppliedIndex

	// Read with session consistency using the applied_index.
	getResp, err := client.Get(ctx, &pb.GetRequest{
		Namespace:    "ns",
		Key:          "k",
		Consistency:  pb.ConsistencyLevel_CONSISTENCY_SESSION,
		AppliedIndex: appliedIndex,
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("v"), getResp.Record.Value)
}
