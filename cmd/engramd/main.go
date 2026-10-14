package main

import (
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/shrec21/engram/internal/api"
	"github.com/shrec21/engram/internal/clock"
	"github.com/shrec21/engram/internal/decay"
	"github.com/shrec21/engram/internal/store"
	pb "github.com/shrec21/engram/proto/engrampb"
	"google.golang.org/grpc"
)

func main() {
	addr := flag.String("addr", ":50051", "gRPC listen address")
	dataDir := flag.String("data", "./engram-data", "Pebble data directory")
	sweepInterval := flag.Duration("sweep-interval", 60*time.Second, "TTL sweep interval")
	flag.Parse()

	clk := clock.RealClock{}

	s, err := store.New(*dataDir, clk)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer s.Close()

	sweeper := decay.NewSweeper(s, clk, *sweepInterval)
	go sweeper.Run()
	defer sweeper.Stop()

	srv := grpc.NewServer()
	pb.RegisterMemoryServiceServer(srv, api.NewServer(s, clk))

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

	log.Printf("engram listening on %s", *addr)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
