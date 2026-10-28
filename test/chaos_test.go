package test

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/shrec21/engram/internal/api"
	"github.com/shrec21/engram/internal/client"
	"github.com/shrec21/engram/internal/clock"
	"github.com/shrec21/engram/internal/decay"
	engramraft "github.com/shrec21/engram/internal/raft"
	"github.com/shrec21/engram/internal/store"
	pb "github.com/shrec21/engram/proto/engrampb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type chaosNode struct {
	id        string
	realRaft  string // real Raft bind address
	proxyRaft string // toxiproxy listen address (advertised to peers)
	grpcAddr  string
	proxyName string
	store     *store.Store
	raft      *engramraft.RaftNode
	grpcSrv   *grpc.Server
}

type chaosCluster struct {
	nodes []*chaosNode
	toxy  *toxiproxyServer
}

func newChaosCluster(t *testing.T, n int) *chaosCluster {
	t.Helper()

	toxy := startToxiproxy(t)
	if toxy == nil {
		return nil
	}

	clk := clock.RealClock{}

	type portInfo struct {
		id        string
		realRaft  string
		proxyRaft string
		grpcAddr  string
		grpcLis   net.Listener
		proxyName string
	}

	// Allocate ports for each node.
	infos := make([]portInfo, n)
	for i := 0; i < n; i++ {
		// Raft real bind port.
		realLis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		realAddr := realLis.Addr().String()
		realLis.Close()

		// Toxiproxy listen port (peers connect here).
		proxyLis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		proxyAddr := proxyLis.Addr().String()
		proxyLis.Close()

		// gRPC listen port.
		grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)

		proxyName := fmt.Sprintf("raft-%d", i)
		toxy.createProxy(t, proxyName, proxyAddr, realAddr)

		infos[i] = portInfo{
			id:        fmt.Sprintf("chaos-%d", i),
			realRaft:  realAddr,
			proxyRaft: proxyAddr,
			grpcAddr:  grpcLis.Addr().String(),
			grpcLis:   grpcLis,
			proxyName: proxyName,
		}
	}

	var nodes []*chaosNode
	for i := 0; i < n; i++ {
		var peers []engramraft.Peer
		for j := 0; j < n; j++ {
			if j == i {
				continue
			}
			peers = append(peers, engramraft.Peer{
				ID:       infos[j].id,
				RaftAddr: infos[j].proxyRaft,
				GRPCAddr: infos[j].grpcAddr,
			})
		}

		s, err := store.New(t.TempDir(), clk)
		require.NoError(t, err)

		raftNode, err := engramraft.NewRaftNode(engramraft.Config{
			ID:            infos[i].id,
			RaftAddr:      infos[i].realRaft,
			AdvertiseAddr: infos[i].proxyRaft,
			GRPCAddr:      infos[i].grpcAddr,
			DataDir:       t.TempDir(),
			Bootstrap:     i == 0,
			Peers:         peers,
		}, s)
		require.NoError(t, err)

		tracker := store.NewAccessTracker()
		grpcSrv := grpc.NewServer()
		pb.RegisterMemoryServiceServer(grpcSrv,
			api.NewServer(raftNode, s, raftNode, clk, tracker, nil, nil))
		pb.RegisterAdminServiceServer(grpcSrv,
			api.NewAdminServer(raftNode, raftNode, s))

		go grpcSrv.Serve(infos[i].grpcLis)

		sweeper := decay.NewSweeper(s, raftNode, clk, 60*time.Second)
		touchFlusher := decay.NewTouchFlusher(tracker, raftNode, 10*time.Second)
		monitor := engramraft.NewLeaderMonitor(raftNode.LeaderCh(), sweeper, touchFlusher)
		go monitor.Run()

		node := &chaosNode{
			id:        infos[i].id,
			realRaft:  infos[i].realRaft,
			proxyRaft: infos[i].proxyRaft,
			grpcAddr:  infos[i].grpcAddr,
			proxyName: infos[i].proxyName,
			store:     s,
			raft:      raftNode,
			grpcSrv:   grpcSrv,
		}
		nodes = append(nodes, node)

		t.Cleanup(func() {
			monitor.Stop()
			grpcSrv.Stop()
			raftNode.Shutdown()
			s.Close()
		})
	}

	return &chaosCluster{nodes: nodes, toxy: toxy}
}

func (c *chaosCluster) waitForLeader(t *testing.T, timeout time.Duration) *chaosNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range c.nodes {
			_, _, isLeader := n.raft.Leader()
			if isLeader {
				return n
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("no leader elected within timeout")
	return nil
}

func (c *chaosCluster) leaderIdx(t *testing.T) int {
	t.Helper()
	for i, n := range c.nodes {
		_, _, isLeader := n.raft.Leader()
		if isLeader {
			return i
		}
	}
	t.Fatal("no leader")
	return -1
}

func TestChaosLeaderIsolated(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}

	cluster := newChaosCluster(t, 3)
	if cluster == nil {
		return
	}

	leader := cluster.waitForLeader(t, 15*time.Second)

	c, err := client.New(leader.grpcAddr, "chaos-client")
	require.NoError(t, err)
	defer c.Close()

	ctx := context.Background()
	for i := 0; i < 10; i++ {
		_, err := c.Put(ctx, &pb.PutRequest{
			Namespace: "chaos",
			Key:       fmt.Sprintf("key-%d", i),
			Value:     []byte(fmt.Sprintf("value-%d", i)),
		})
		require.NoError(t, err)
	}

	// Isolate the leader.
	leaderIdx := cluster.leaderIdx(t)
	cluster.toxy.disableProxy(t, cluster.nodes[leaderIdx].proxyName)

	// Wait for a new leader.
	time.Sleep(3 * time.Second)

	var nonIsolated *chaosNode
	for i, n := range cluster.nodes {
		if i != leaderIdx {
			_, _, isLeader := n.raft.Leader()
			if isLeader {
				nonIsolated = n
				break
			}
		}
	}

	if nonIsolated != nil {
		c2, err := client.New(nonIsolated.grpcAddr, "chaos-client-2")
		require.NoError(t, err)
		defer c2.Close()

		_, err = c2.Put(ctx, &pb.PutRequest{
			Namespace: "chaos",
			Key:       "after-partition",
			Value:     []byte("new-leader-write"),
		})
		require.NoError(t, err)
	}

	// Heal.
	cluster.toxy.enableProxy(t, cluster.nodes[leaderIdx].proxyName)
	time.Sleep(2 * time.Second)
}

func TestChaosFollowerIsolated(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}

	cluster := newChaosCluster(t, 3)
	if cluster == nil {
		return
	}

	leader := cluster.waitForLeader(t, 15*time.Second)

	c, err := client.New(leader.grpcAddr, "chaos-client")
	require.NoError(t, err)
	defer c.Close()

	ctx := context.Background()

	// Isolate a follower.
	leaderIdx := cluster.leaderIdx(t)
	followerIdx := (leaderIdx + 1) % 3
	cluster.toxy.disableProxy(t, cluster.nodes[followerIdx].proxyName)

	// Writes should still succeed (2/3 majority).
	for i := 0; i < 20; i++ {
		_, err := c.Put(ctx, &pb.PutRequest{
			Namespace: "chaos",
			Key:       fmt.Sprintf("follower-isolated-%d", i),
			Value:     []byte(fmt.Sprintf("value-%d", i)),
		})
		require.NoError(t, err)
	}

	// Heal and let the follower catch up.
	cluster.toxy.enableProxy(t, cluster.nodes[followerIdx].proxyName)
	time.Sleep(2 * time.Second)

	// Verify follower has the data.
	conn, err := grpc.NewClient(cluster.nodes[followerIdx].grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	defer conn.Close()

	memClient := pb.NewMemoryServiceClient(conn)
	resp, err := memClient.Get(ctx, &pb.GetRequest{
		Namespace:   "chaos",
		Key:         "follower-isolated-0",
		Consistency: pb.ConsistencyLevel_CONSISTENCY_STALE,
	})
	require.NoError(t, err)
	require.NotNil(t, resp.Record)
}

func TestChaosHighLatency(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}

	cluster := newChaosCluster(t, 3)
	if cluster == nil {
		return
	}

	leader := cluster.waitForLeader(t, 15*time.Second)

	c, err := client.New(leader.grpcAddr, "chaos-client")
	require.NoError(t, err)
	defer c.Close()

	ctx := context.Background()

	// Add high latency to all proxies.
	for _, n := range cluster.nodes {
		cluster.toxy.addLatencyToxic(t, n.proxyName, 200, 50)
	}

	// Writes should still succeed, just slower.
	for i := 0; i < 5; i++ {
		writeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		_, err := c.Put(writeCtx, &pb.PutRequest{
			Namespace: "chaos",
			Key:       fmt.Sprintf("latency-%d", i),
			Value:     []byte(fmt.Sprintf("value-%d", i)),
		})
		cancel()
		require.NoError(t, err)
	}

	// Remove latency.
	for _, n := range cluster.nodes {
		cluster.toxy.removeToxic(t, n.proxyName, n.proxyName+"-latency")
	}
}

func TestChaosPartitionHealCycles(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}

	cluster := newChaosCluster(t, 3)
	if cluster == nil {
		return
	}

	cluster.waitForLeader(t, 15*time.Second)
	ctx := context.Background()

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for cycle := 0; cycle < 5; cycle++ {
		nodeIdx := rng.Intn(3)
		cluster.toxy.disableProxy(t, cluster.nodes[nodeIdx].proxyName)

		time.Sleep(2 * time.Second)

		var writeErr error
		for _, n := range cluster.nodes {
			_, _, isLeader := n.raft.Leader()
			if isLeader {
				gc, err := client.New(n.grpcAddr, fmt.Sprintf("chaos-cycle-%d", cycle))
				if err != nil {
					continue
				}
				_, writeErr = gc.Put(ctx, &pb.PutRequest{
					Namespace: "chaos",
					Key:       fmt.Sprintf("cycle-%d", cycle),
					Value:     []byte(fmt.Sprintf("cycle-%d", cycle)),
				})
				gc.Close()
				break
			}
		}
		_ = writeErr

		cluster.toxy.enableProxy(t, cluster.nodes[nodeIdx].proxyName)
		time.Sleep(2 * time.Second)
	}
}

func TestChaosAsymmetricPartition(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chaos test in short mode")
	}

	cluster := newChaosCluster(t, 3)
	if cluster == nil {
		return
	}

	leader := cluster.waitForLeader(t, 15*time.Second)

	c, err := client.New(leader.grpcAddr, "chaos-client")
	require.NoError(t, err)
	defer c.Close()

	ctx := context.Background()

	_, err = c.Put(ctx, &pb.PutRequest{
		Namespace: "chaos",
		Key:       "counter",
		Value:     []byte("0"),
	})
	require.NoError(t, err)

	// Isolate node 2.
	cluster.toxy.disableProxy(t, cluster.nodes[2].proxyName)

	var mu sync.Mutex
	var writes int
	var wg sync.WaitGroup

	for g := 0; g < 3; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			gc, err := client.New(leader.grpcAddr, fmt.Sprintf("chaos-goroutine-%d", id))
			if err != nil {
				return
			}
			defer gc.Close()

			for i := 0; i < 5; i++ {
				_, err := gc.Put(ctx, &pb.PutRequest{
					Namespace: "chaos",
					Key:       fmt.Sprintf("async-%d-%d", id, i),
					Value:     []byte(fmt.Sprintf("val-%d-%d", id, i)),
				})
				if err == nil {
					mu.Lock()
					writes++
					mu.Unlock()
				}
			}
		}(g)
	}
	wg.Wait()

	t.Logf("successful writes during asymmetric partition: %d/15", writes)

	// Heal.
	cluster.toxy.enableProxy(t, cluster.nodes[2].proxyName)
	time.Sleep(3 * time.Second)
}
