package test

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/shrec21/engram/internal/api"
	"github.com/shrec21/engram/internal/clock"
	"github.com/shrec21/engram/internal/decay"
	engramraft "github.com/shrec21/engram/internal/raft"
	"github.com/shrec21/engram/internal/store"
	pb "github.com/shrec21/engram/proto/engrampb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type testNode struct {
	id       string
	raftAddr string
	grpcAddr string
	store    *store.Store
	raft     *engramraft.RaftNode
	grpcSrv  *grpc.Server
	tracker  *store.AccessTracker
}

type testCluster struct {
	nodes []*testNode
}

func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()

	clk := clock.RealClock{}

	// Allocate ports.
	type nodeInfo struct {
		id       string
		raftLis  net.Listener
		grpcLis  net.Listener
		raftAddr string
		grpcAddr string
	}
	infos := make([]nodeInfo, n)
	for i := 0; i < n; i++ {
		raftLis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		infos[i] = nodeInfo{
			id:       fmt.Sprintf("node-%d", i),
			raftLis:  raftLis,
			grpcLis:  grpcLis,
			raftAddr: raftLis.Addr().String(),
			grpcAddr: grpcLis.Addr().String(),
		}
		// Close the raft listener — Raft transport will rebind.
		raftLis.Close()
	}

	// Build peer lists.
	var nodes []*testNode
	for i := 0; i < n; i++ {
		var peers []engramraft.Peer
		for j := 0; j < n; j++ {
			if j == i {
				continue
			}
			peers = append(peers, engramraft.Peer{
				ID:       infos[j].id,
				RaftAddr: infos[j].raftAddr,
				GRPCAddr: infos[j].grpcAddr,
			})
		}

		s, err := store.New(t.TempDir(), clk)
		require.NoError(t, err)

		raftNode, err := engramraft.NewRaftNode(engramraft.Config{
			ID:        infos[i].id,
			RaftAddr:  infos[i].raftAddr,
			GRPCAddr:  infos[i].grpcAddr,
			DataDir:   t.TempDir(),
			Bootstrap: i == 0, // only first node bootstraps
			Peers:     peers,
		}, s)
		require.NoError(t, err)

		tracker := store.NewAccessTracker()

		grpcSrv := grpc.NewServer()
		pb.RegisterMemoryServiceServer(grpcSrv,
			api.NewServer(raftNode, s, raftNode, clk, tracker))
		pb.RegisterAdminServiceServer(grpcSrv,
			api.NewAdminServer(raftNode))

		// Start gRPC.
		go grpcSrv.Serve(infos[i].grpcLis)

		// Start leader services.
		sweeper := decay.NewSweeper(s, raftNode, clk, 60*time.Second)
		touchFlusher := decay.NewTouchFlusher(tracker, raftNode, 10*time.Second)
		monitor := engramraft.NewLeaderMonitor(raftNode.LeaderCh(), sweeper, touchFlusher)
		go monitor.Run()

		nodes = append(nodes, &testNode{
			id:       infos[i].id,
			raftAddr: infos[i].raftAddr,
			grpcAddr: infos[i].grpcAddr,
			store:    s,
			raft:     raftNode,
			grpcSrv:  grpcSrv,
			tracker:  tracker,
		})

		t.Cleanup(func() {
			monitor.Stop()
			grpcSrv.Stop()
			raftNode.Shutdown()
			s.Close()
		})
	}

	return &testCluster{nodes: nodes}
}

// waitForLeader blocks until a leader is elected.
func (c *testCluster) waitForLeader(t *testing.T, timeout time.Duration) *testNode {
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

// leaderClient returns a gRPC client connected to the current leader.
func (c *testCluster) leaderClient(t *testing.T) (pb.MemoryServiceClient, *grpc.ClientConn) {
	t.Helper()
	leader := c.waitForLeader(t, 10*time.Second)
	conn, err := grpc.NewClient(leader.grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	return pb.NewMemoryServiceClient(conn), conn
}

// anyClient returns a gRPC client connected to any node.
func (c *testCluster) anyClient(t *testing.T, idx int) (pb.MemoryServiceClient, *grpc.ClientConn) {
	t.Helper()
	conn, err := grpc.NewClient(c.nodes[idx].grpcAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	return pb.NewMemoryServiceClient(conn), conn
}

// followerNode returns a non-leader node.
func (c *testCluster) followerNode(t *testing.T) *testNode {
	t.Helper()
	leader := c.waitForLeader(t, 10*time.Second)
	for _, n := range c.nodes {
		if n.id != leader.id {
			return n
		}
	}
	t.Fatal("no follower found")
	return nil
}

// killLeader shuts down the current leader's Raft and gRPC.
func (c *testCluster) killLeader(t *testing.T) *testNode {
	t.Helper()
	leader := c.waitForLeader(t, 10*time.Second)
	leader.grpcSrv.Stop()
	leader.raft.Shutdown()
	return leader
}

// localProposer for single-node usage in tests.
type localProposer struct {
	store *store.Store
}

func (p *localProposer) Propose(_ context.Context, cmd *pb.Command) (*pb.CommandResult, uint64, error) {
	result, err := p.store.Apply(cmd)
	return result, 0, err
}
