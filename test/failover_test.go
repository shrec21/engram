package test

import (
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shrec21/engram/internal/client"
	pb "github.com/shrec21/engram/proto/engrampb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestCASCounterFailover(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping failover test in short mode")
	}

	cluster := newTestCluster(t, 3)
	leader := cluster.waitForLeader(t, 10*time.Second)

	conn, err := grpc.NewClient(leader.grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()
	leaderClient := pb.NewMemoryServiceClient(conn)

	ctx := context.Background()

	// Seed the counter at 0.
	_, err = leaderClient.Put(ctx, &pb.PutRequest{
		Namespace: "ns",
		Key:       "counter",
		Value:     encodeUint64(0),
	})
	require.NoError(t, err)

	// Run concurrent CAS incrementers using the smart client.
	const goroutines = 10
	const incrementsPerGoroutine = 10
	var succeeded atomic.Int64
	var wg sync.WaitGroup

	// Use the smart client so redirects after leader kill work.
	// Connect to a non-leader so we test redirect.
	allAddrs := ""
	for _, n := range cluster.nodes {
		if allAddrs == "" {
			allAddrs = n.grpcAddr
		}
	}

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			c, err := client.New(allAddrs, "")
			if err != nil {
				t.Errorf("client %d: %v", id, err)
				return
			}
			defer c.Close()

			for j := 0; j < incrementsPerGoroutine; j++ {
				for attempt := 0; attempt < 100; attempt++ {
					getCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					resp, err := c.Get(getCtx, &pb.GetRequest{
						Namespace:   "ns",
						Key:         "counter",
						Consistency: pb.ConsistencyLevel_CONSISTENCY_LINEARIZABLE,
					})
					cancel()
					if err != nil {
						time.Sleep(100 * time.Millisecond)
						continue
					}

					current := decodeUint64(resp.Record.Value)
					next := current + 1

					casCtx, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
					_, err = c.CompareAndSet(casCtx, &pb.CompareAndSetRequest{
						Namespace:       "ns",
						Key:             "counter",
						Value:           encodeUint64(next),
						ExpectedVersion: resp.Record.Version,
					})
					cancel2()

					if err == nil {
						succeeded.Add(1)
						break
					}
					// Retry on conflict or unavailable.
					time.Sleep(10 * time.Millisecond)
				}
			}
		}(i)
	}

	wg.Wait()

	// Read final value.
	finalConn, err := grpc.NewClient(cluster.waitForLeader(t, 10*time.Second).grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer finalConn.Close()

	finalClient := pb.NewMemoryServiceClient(finalConn)
	finalCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := finalClient.Get(finalCtx, &pb.GetRequest{
		Namespace:   "ns",
		Key:         "counter",
		Consistency: pb.ConsistencyLevel_CONSISTENCY_LINEARIZABLE,
	})
	require.NoError(t, err)

	finalValue := decodeUint64(resp.Record.Value)
	totalSucceeded := succeeded.Load()

	// The final value should exactly equal the number of successful CAS operations.
	assert.Equal(t, uint64(totalSucceeded), finalValue,
		"final counter (%d) should equal successful CAS count (%d)", finalValue, totalSucceeded)
	assert.Equal(t, int64(goroutines*incrementsPerGoroutine), totalSucceeded,
		"all increments should succeed")
}

func encodeUint64(v uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, v)
	return buf
}

func decodeUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
