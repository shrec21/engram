package raft

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	hraft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/shrec21/engram/internal/store"
	pb "github.com/shrec21/engram/proto/engrampb"
	"google.golang.org/protobuf/proto"
)

// Peer represents a node in the Raft cluster with both Raft and gRPC addresses.
type Peer struct {
	ID       string
	RaftAddr string
	GRPCAddr string
}

// Config holds configuration for a RaftNode.
type Config struct {
	ID         string
	RaftAddr   string
	GRPCAddr   string
	DataDir    string
	Bootstrap  bool
	Peers      []Peer
	RaftConfig *hraft.Config
}

// RaftNode orchestrates the Raft cluster machinery.
type RaftNode struct {
	raft        *hraft.Raft
	fsm         *FSM
	indexWaiter *IndexWaiter
	transport   *hraft.NetworkTransport

	mu       sync.RWMutex
	grpcAddrs map[hraft.ServerID]string // Raft ID → gRPC address
}

// NewRaftNode creates and starts a Raft node.
func NewRaftNode(cfg Config, st *store.Store) (*RaftNode, error) {
	raftDir := filepath.Join(cfg.DataDir, "raft")
	if err := os.MkdirAll(raftDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir raft dir: %w", err)
	}

	snapDir := filepath.Join(raftDir, "snapshots")
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		return nil, fmt.Errorf("mkdir snap dir: %w", err)
	}

	// Raft configuration.
	rc := cfg.RaftConfig
	if rc == nil {
		rc = hraft.DefaultConfig()
	}
	rc.LocalID = hraft.ServerID(cfg.ID)

	// Log and stable store (BoltDB).
	boltStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	if err != nil {
		return nil, fmt.Errorf("bolt store: %w", err)
	}

	// Snapshot store.
	snaps, err := hraft.NewFileSnapshotStore(raftDir, 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("snapshot store: %w", err)
	}

	// Transport.
	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve addr: %w", err)
	}
	transport, err := hraft.NewTCPTransport(cfg.RaftAddr, addr, 3, 10*time.Second, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("transport: %w", err)
	}

	// Index waiter + dedup.
	iw := NewIndexWaiter()
	dedup := NewDedupTable(st.RawDB())
	fsm := NewFSM(st, dedup, iw, snapDir)

	r, err := hraft.NewRaft(rc, fsm, boltStore, boltStore, snaps, transport)
	if err != nil {
		transport.Close()
		return nil, fmt.Errorf("new raft: %w", err)
	}

	// Build gRPC address map.
	grpcAddrs := make(map[hraft.ServerID]string)
	grpcAddrs[hraft.ServerID(cfg.ID)] = cfg.GRPCAddr
	for _, p := range cfg.Peers {
		grpcAddrs[hraft.ServerID(p.ID)] = p.GRPCAddr
	}

	node := &RaftNode{
		raft:        r,
		fsm:         fsm,
		indexWaiter: iw,
		transport:   transport,
		grpcAddrs:   grpcAddrs,
	}

	// Bootstrap if requested and this is a fresh cluster.
	if cfg.Bootstrap {
		servers := []hraft.Server{
			{ID: hraft.ServerID(cfg.ID), Address: hraft.ServerAddress(cfg.RaftAddr)},
		}
		for _, p := range cfg.Peers {
			servers = append(servers, hraft.Server{
				ID:      hraft.ServerID(p.ID),
				Address: hraft.ServerAddress(p.RaftAddr),
			})
		}
		f := r.BootstrapCluster(hraft.Configuration{Servers: servers})
		if err := f.Error(); err != nil && err != hraft.ErrCantBootstrap {
			return nil, fmt.Errorf("bootstrap: %w", err)
		}
	}

	return node, nil
}

// Propose submits a command through Raft. Returns the result and the applied index.
// Must be called on the leader.
func (n *RaftNode) Propose(ctx context.Context, cmd *pb.Command) (*pb.CommandResult, uint64, error) {
	data, err := proto.Marshal(cmd)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal command: %w", err)
	}

	timeout := 10 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
	}

	future := n.raft.Apply(data, timeout)
	if err := future.Error(); err != nil {
		return nil, 0, err
	}

	resp := future.Response()
	ar, ok := resp.(*ApplyResult)
	if !ok {
		return nil, 0, fmt.Errorf("unexpected apply response type: %T", resp)
	}
	if ar.Err != nil {
		return nil, 0, ar.Err
	}

	return ar.Result, future.Index(), nil
}

// Leader returns the current leader's gRPC address, Raft ID, and whether this node is the leader.
func (n *RaftNode) Leader() (grpcAddr, id string, isLeader bool) {
	_, leaderID := n.raft.LeaderWithID()

	n.mu.RLock()
	gAddr := n.grpcAddrs[leaderID]
	n.mu.RUnlock()

	return gAddr, string(leaderID), n.raft.State() == hraft.Leader
}

// LinearizableBarrier issues a barrier read to confirm this node is still leader.
func (n *RaftNode) LinearizableBarrier(ctx context.Context) error {
	timeout := 10 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
	}
	f := n.raft.Barrier(timeout)
	return f.Error()
}

// WaitForIndex blocks until the applied index reaches the given value.
func (n *RaftNode) WaitForIndex(ctx context.Context, index uint64) error {
	return n.indexWaiter.WaitForIndex(ctx, index)
}

// AppliedIndex returns the last applied log index.
func (n *RaftNode) AppliedIndex() uint64 {
	return n.indexWaiter.AppliedIndex()
}

// LeaderCh returns the channel that signals leadership changes.
func (n *RaftNode) LeaderCh() <-chan bool {
	return n.raft.LeaderCh()
}

// AddVoter adds a voter to the cluster.
func (n *RaftNode) AddVoter(id, raftAddr, grpcAddr string) error {
	f := n.raft.AddVoter(hraft.ServerID(id), hraft.ServerAddress(raftAddr), 0, 10*time.Second)
	if err := f.Error(); err != nil {
		return err
	}

	n.mu.Lock()
	n.grpcAddrs[hraft.ServerID(id)] = grpcAddr
	n.mu.Unlock()
	return nil
}

// RemoveServer removes a server from the cluster.
func (n *RaftNode) RemoveServer(id string) error {
	f := n.raft.RemoveServer(hraft.ServerID(id), 0, 10*time.Second)
	return f.Error()
}

// GetClusterStatus returns the current cluster state.
func (n *RaftNode) GetClusterStatus() (*pb.GetClusterStatusResponse, error) {
	leaderAddr, leaderID := n.raft.LeaderWithID()

	n.mu.RLock()
	leaderGRPC := n.grpcAddrs[leaderID]
	n.mu.RUnlock()

	_ = leaderAddr // suppress unused warning; we use leaderGRPC

	future := n.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}

	var servers []*pb.ServerInfo
	for _, s := range future.Configuration().Servers {
		servers = append(servers, &pb.ServerInfo{
			Id:       string(s.ID),
			Address:  string(s.Address),
			Suffrage: s.Suffrage.String(),
		})
	}

	stats := n.raft.Stats()
	_ = stats

	return &pb.GetClusterStatusResponse{
		LeaderId:      string(leaderID),
		LeaderAddress: leaderGRPC,
		AppliedIndex:  n.AppliedIndex(),
		Servers:       servers,
	}, nil
}

// Shutdown gracefully shuts down the Raft node.
func (n *RaftNode) Shutdown() error {
	f := n.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}
	return n.transport.Close()
}

// State returns the current Raft state (Leader, Follower, Candidate).
func (n *RaftNode) State() hraft.RaftState {
	return n.raft.State()
}
