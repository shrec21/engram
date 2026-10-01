package api

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/shrec21/engram/internal/clock"
	"github.com/shrec21/engram/internal/store"
	pb "github.com/shrec21/engram/proto/engrampb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var baseTime = time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC)

type testEnv struct {
	client pb.MemoryServiceClient
	clk    *clock.TestClock
	conn   *grpc.ClientConn
	srv    *grpc.Server
}

func setup(t *testing.T) *testEnv {
	t.Helper()

	clk := clock.NewTestClock(baseTime)
	s, err := store.New(t.TempDir(), clk)
	require.NoError(t, err)
	t.Cleanup(func() { s.Close() })

	srv := grpc.NewServer()
	pb.RegisterMemoryServiceServer(srv, NewServer(s, clk))

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	go srv.Serve(lis)
	t.Cleanup(func() { srv.Stop() })

	conn, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	return &testEnv{
		client: pb.NewMemoryServiceClient(conn),
		clk:    clk,
		conn:   conn,
		srv:    srv,
	}
}

func TestGRPCPutAndGet(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	resp, err := env.client.Put(ctx, &pb.PutRequest{
		Namespace:  "ns",
		Key:        "k1",
		Value:      []byte("hello"),
		CreatedBy:  "agent",
		Source:     "test",
		Confidence: 0.9,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), resp.Version)

	getResp, err := env.client.Get(ctx, &pb.GetRequest{Namespace: "ns", Key: "k1"})
	require.NoError(t, err)
	assert.Equal(t, []byte("hello"), getResp.Record.Value)
	assert.Equal(t, uint64(1), getResp.Record.Version)
	assert.Equal(t, "agent", getResp.Record.CreatedBy)
}

func TestGRPCGetNotFound(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	_, err := env.client.Get(ctx, &pb.GetRequest{Namespace: "ns", Key: "missing"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestGRPCCompareAndSet(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	// Put initial value.
	_, err := env.client.Put(ctx, &pb.PutRequest{
		Namespace: "ns", Key: "k", Value: []byte("v1"),
	})
	require.NoError(t, err)

	// CAS with correct version.
	casResp, err := env.client.CompareAndSet(ctx, &pb.CompareAndSetRequest{
		Namespace:       "ns",
		Key:             "k",
		Value:           []byte("v2"),
		ExpectedVersion: 1,
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(2), casResp.Version)

	// CAS with wrong version → FAILED_PRECONDITION.
	_, err = env.client.CompareAndSet(ctx, &pb.CompareAndSetRequest{
		Namespace:       "ns",
		Key:             "k",
		Value:           []byte("v3"),
		ExpectedVersion: 1,
	})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestGRPCDelete(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	_, err := env.client.Put(ctx, &pb.PutRequest{
		Namespace: "ns", Key: "k", Value: []byte("v"),
	})
	require.NoError(t, err)

	_, err = env.client.Delete(ctx, &pb.DeleteRequest{
		Namespace: "ns", Key: "k",
	})
	require.NoError(t, err)

	_, err = env.client.Get(ctx, &pb.GetRequest{Namespace: "ns", Key: "k"})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestGRPCDeleteVersionMismatch(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	_, err := env.client.Put(ctx, &pb.PutRequest{
		Namespace: "ns", Key: "k", Value: []byte("v"),
	})
	require.NoError(t, err)

	_, err = env.client.Delete(ctx, &pb.DeleteRequest{
		Namespace: "ns", Key: "k", ExpectedVersion: 99,
	})
	require.Error(t, err)
	assert.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestGRPCList(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		_, err := env.client.Put(ctx, &pb.PutRequest{
			Namespace: "ns",
			Key:       string(rune('a' + i)),
			Value:     []byte{byte(i)},
		})
		require.NoError(t, err)
	}

	resp, err := env.client.List(ctx, &pb.ListRequest{
		Namespace: "ns",
		Limit:     3,
	})
	require.NoError(t, err)
	assert.Len(t, resp.Records, 3)
	assert.NotEmpty(t, resp.NextPageToken)

	// Second page.
	resp2, err := env.client.List(ctx, &pb.ListRequest{
		Namespace: "ns",
		Limit:     3,
		PageToken: resp.NextPageToken,
	})
	require.NoError(t, err)
	assert.Len(t, resp2.Records, 2)
	assert.Empty(t, resp2.NextPageToken)
}

func TestGRPCTTLExpiry(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	_, err := env.client.Put(ctx, &pb.PutRequest{
		Namespace: "ns",
		Key:       "ttl-key",
		Value:     []byte("temp"),
		ExpiresAt: timestamppb.New(baseTime.Add(time.Hour)),
	})
	require.NoError(t, err)

	// Before expiry.
	_, err = env.client.Get(ctx, &pb.GetRequest{Namespace: "ns", Key: "ttl-key"})
	require.NoError(t, err)

	// Advance past expiry.
	env.clk.Advance(2 * time.Hour)

	_, err = env.client.Get(ctx, &pb.GetRequest{Namespace: "ns", Key: "ttl-key"})
	require.Error(t, err)
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestGRPCValidation(t *testing.T) {
	env := setup(t)
	ctx := context.Background()

	// Missing namespace.
	_, err := env.client.Put(ctx, &pb.PutRequest{Key: "k", Value: []byte("v")})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	// Missing key.
	_, err = env.client.Get(ctx, &pb.GetRequest{Namespace: "ns"})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	// Missing both.
	_, err = env.client.Delete(ctx, &pb.DeleteRequest{})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))
}
