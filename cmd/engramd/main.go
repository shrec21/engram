package main

import (
	"context"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/shrec21/engram/internal/api"
	"github.com/shrec21/engram/internal/clock"
	"github.com/shrec21/engram/internal/decay"
	_ "github.com/shrec21/engram/internal/metrics" // register metrics
	engramraft "github.com/shrec21/engram/internal/raft"
	"github.com/shrec21/engram/internal/store"
	pb "github.com/shrec21/engram/proto/engrampb"
	"google.golang.org/grpc"
)

func main() {
	// gRPC flags.
	addr := flag.String("addr", ":50051", "gRPC listen address")
	dataDir := flag.String("data", "./engram-data", "data directory")
	sweepInterval := flag.Duration("sweep-interval", 60*time.Second, "TTL sweep interval")
	touchInterval := flag.Duration("touch-interval", 10*time.Second, "access metadata flush interval")

	// Metrics flags.
	metricsAddr := flag.String("metrics-addr", ":9090", "Prometheus metrics HTTP address (empty to disable)")

	// Raft flags. Empty raft-addr means single-node (no Raft).
	raftAddr := flag.String("raft-addr", "", "Raft transport address (e.g., :50052)")
	nodeID := flag.String("node-id", "", "Raft node ID")
	bootstrap := flag.Bool("bootstrap", false, "bootstrap a new cluster")
	peers := flag.String("peers", "", "initial peers: id1=raft:grpc,id2=raft:grpc,...")

	flag.Parse()

	clk := clock.RealClock{}

	s, err := store.New(*dataDir+"/pebble", clk)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer s.Close()

	tracker := store.NewAccessTracker()

	var (
		proposer    api.Proposer
		clusterInfo api.ClusterInfo
		raftNode    *engramraft.RaftNode
	)

	if *raftAddr != "" {
		// Raft mode.
		if *nodeID == "" {
			log.Fatal("--node-id is required when --raft-addr is set")
		}

		parsedPeers := parsePeers(*peers)

		raftNode, err = engramraft.NewRaftNode(engramraft.Config{
			ID:        *nodeID,
			RaftAddr:  *raftAddr,
			GRPCAddr:  *addr,
			DataDir:   *dataDir,
			Bootstrap: *bootstrap,
			Peers:     parsedPeers,
		}, s)
		if err != nil {
			log.Fatalf("start raft: %v", err)
		}
		defer raftNode.Shutdown()

		proposer = raftNode
		clusterInfo = raftNode

		// Leader-only services.
		sweeper := decay.NewSweeper(s, raftNode, clk, *sweepInterval)
		touchFlusher := decay.NewTouchFlusher(tracker, raftNode, *touchInterval)
		monitor := engramraft.NewLeaderMonitor(raftNode.LeaderCh(), sweeper, touchFlusher)
		go monitor.Run()
		defer monitor.Stop()
	} else {
		// Single-node mode (backward compatible).
		proposer = &localProposer{store: s}

		sweeper := decay.NewSweeper(s, proposer, clk, *sweepInterval)
		sweeper.Start()
		defer sweeper.Stop()

		touchFlusher := decay.NewTouchFlusher(tracker, proposer, *touchInterval)
		touchFlusher.Start()
		defer touchFlusher.Stop()
	}

	srv := grpc.NewServer()
	pb.RegisterMemoryServiceServer(srv, api.NewServer(proposer, s, clusterInfo, clk, tracker))
	if raftNode != nil {
		pb.RegisterAdminServiceServer(srv, api.NewAdminServer(raftNode))
	}

	// Start metrics HTTP server.
	if *metricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		go func() {
			log.Printf("metrics listening on %s", *metricsAddr)
			if err := http.ListenAndServe(*metricsAddr, mux); err != nil && err != http.ErrServerClosed {
				log.Printf("metrics server error: %v", err)
			}
		}()
	}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		log.Println("shutting down...")
		srv.GracefulStop()
	}()

	log.Printf("engram listening on %s (raft=%s)", *addr, *raftAddr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

// localProposer wraps store.Apply for single-node mode.
type localProposer struct {
	store *store.Store
}

func (p *localProposer) Propose(_ context.Context, cmd *pb.Command) (*pb.CommandResult, uint64, error) {
	result, err := p.store.Apply(cmd)
	return result, 0, err
}

// parsePeers parses "id1=raftAddr:grpcAddr,id2=raftAddr:grpcAddr"
func parsePeers(s string) []engramraft.Peer {
	if s == "" {
		return nil
	}
	var peers []engramraft.Peer
	for _, entry := range strings.Split(s, ",") {
		parts := strings.SplitN(entry, "=", 2)
		if len(parts) != 2 {
			log.Fatalf("invalid peer format: %q (expected id=raftAddr:grpcAddr)", entry)
		}
		addrs := strings.SplitN(parts[1], "/", 2)
		if len(addrs) != 2 {
			log.Fatalf("invalid peer addresses: %q (expected raftAddr/grpcAddr)", parts[1])
		}
		peers = append(peers, engramraft.Peer{
			ID:       parts[0],
			RaftAddr: addrs[0],
			GRPCAddr: addrs[1],
		})
	}
	return peers
}

// Verify localProposer satisfies Proposer at compile time.
var _ api.Proposer = (*localProposer)(nil)
