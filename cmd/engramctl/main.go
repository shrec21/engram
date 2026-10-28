package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	mcpserver "github.com/mark3labs/mcp-go/server"
	"github.com/shrec21/engram/internal/bench"
	"github.com/shrec21/engram/internal/client"
	engrammcp "github.com/shrec21/engram/mcp"
	pb "github.com/shrec21/engram/proto/engrampb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	addr := os.Getenv("ENGRAM_ADDR")
	if addr == "" {
		addr = "localhost:50051"
	}

	switch os.Args[1] {
	case "mcp":
		cmdMCP(addr)
	case "cluster-status":
		cmdClusterStatus(addr)
	case "add-voter":
		cmdAddVoter(addr, os.Args[2:])
	case "remove-server":
		cmdRemoveServer(addr, os.Args[2:])
	case "set-decay-policy":
		cmdSetDecayPolicy(addr, os.Args[2:])
	case "get-decay-policy":
		cmdGetDecayPolicy(addr, os.Args[2:])
	case "bench":
		cmdBench(addr, os.Args[2:])
	default:
		// For standard memory commands, use the smart client.
		c, err := client.New(addr, "engramctl-"+fmt.Sprintf("%d", os.Getpid()))
		if err != nil {
			fatal(err)
		}
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		switch os.Args[1] {
		case "put":
			cmdPut(ctx, c, os.Args[2:])
		case "get":
			cmdGet(ctx, c, os.Args[2:])
		case "cas":
			cmdCAS(ctx, c, os.Args[2:])
		case "delete":
			cmdDelete(ctx, c, os.Args[2:])
		case "list":
			cmdList(ctx, c, os.Args[2:])
		case "search":
			cmdSearch(ctx, c, os.Args[2:])
		default:
			usage()
			os.Exit(1)
		}
	}
}

func cmdPut(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("put", flag.ExitOnError)
	createdBy := fs.String("created-by", "", "agent ID")
	source := fs.String("source", "", "provenance")
	confidence := fs.Float64("confidence", 1.0, "confidence score 0-1")
	ttl := fs.Duration("ttl", 0, "time-to-live (e.g. 5m, 1h)")
	fs.Parse(reorderArgs(args, fs))

	if fs.NArg() < 3 {
		fmt.Fprintf(os.Stderr, "usage: engramctl put <namespace> <key> <value> [--created-by X] [--source X] [--confidence N] [--ttl D]\n")
		os.Exit(1)
	}

	req := &pb.PutRequest{
		Namespace:  fs.Arg(0),
		Key:        fs.Arg(1),
		Value:      []byte(fs.Arg(2)),
		CreatedBy:  *createdBy,
		Source:     *source,
		Confidence: float32(*confidence),
	}

	if *ttl > 0 {
		req.ExpiresAt = timestamppb.New(time.Now().Add(*ttl))
	}

	resp, err := c.Put(ctx, req)
	if err != nil {
		fatal(err)
	}
	fmt.Printf("version: %d\n", resp.Version)
}

func cmdGet(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	consistency := fs.String("consistency", "session", "consistency level: session, linearizable, stale")
	fs.Parse(reorderArgs(args, fs))

	if fs.NArg() < 2 {
		fmt.Fprintf(os.Stderr, "usage: engramctl get <namespace> <key> [--consistency level]\n")
		os.Exit(1)
	}

	resp, err := c.Get(ctx, &pb.GetRequest{
		Namespace:   fs.Arg(0),
		Key:         fs.Arg(1),
		Consistency: parseConsistency(*consistency),
	})
	if err != nil {
		fatal(err)
	}

	printJSON(resp.Record)
}

func cmdCAS(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("cas", flag.ExitOnError)
	version := fs.Uint64("version", 0, "expected version (0 = create only)")
	createdBy := fs.String("created-by", "", "agent ID")
	source := fs.String("source", "", "provenance")
	confidence := fs.Float64("confidence", 1.0, "confidence score 0-1")
	ttl := fs.Duration("ttl", 0, "time-to-live")
	fs.Parse(reorderArgs(args, fs))

	if fs.NArg() < 3 {
		fmt.Fprintf(os.Stderr, "usage: engramctl cas <namespace> <key> <value> --version N [flags]\n")
		os.Exit(1)
	}

	req := &pb.CompareAndSetRequest{
		Namespace:       fs.Arg(0),
		Key:             fs.Arg(1),
		Value:           []byte(fs.Arg(2)),
		ExpectedVersion: *version,
		CreatedBy:       *createdBy,
		Source:          *source,
		Confidence:      float32(*confidence),
	}

	if *ttl > 0 {
		req.ExpiresAt = timestamppb.New(time.Now().Add(*ttl))
	}

	resp, err := c.CompareAndSet(ctx, req)
	if err != nil {
		fatal(err)
	}
	fmt.Printf("version: %d\n", resp.Version)
}

func cmdDelete(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("delete", flag.ExitOnError)
	version := fs.Uint64("version", 0, "expected version (0 = unconditional)")
	fs.Parse(reorderArgs(args, fs))

	if fs.NArg() < 2 {
		fmt.Fprintf(os.Stderr, "usage: engramctl delete <namespace> <key> [--version N]\n")
		os.Exit(1)
	}

	_, err := c.Delete(ctx, &pb.DeleteRequest{
		Namespace:       fs.Arg(0),
		Key:             fs.Arg(1),
		ExpectedVersion: *version,
	})
	if err != nil {
		fatal(err)
	}
	fmt.Println("deleted")
}

func cmdList(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	prefix := fs.String("prefix", "", "key prefix filter")
	limit := fs.Int("limit", 100, "max records per page")
	consistency := fs.String("consistency", "session", "consistency level: session, linearizable, stale")
	fs.Parse(reorderArgs(args, fs))

	if fs.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "usage: engramctl list <namespace> [--prefix X] [--limit N] [--consistency level]\n")
		os.Exit(1)
	}

	pageToken := ""
	for {
		resp, err := c.List(ctx, &pb.ListRequest{
			Namespace:   fs.Arg(0),
			KeyPrefix:   *prefix,
			Limit:       int32(*limit),
			PageToken:   pageToken,
			Consistency: parseConsistency(*consistency),
		})
		if err != nil {
			fatal(err)
		}

		for _, rec := range resp.Records {
			printJSON(rec)
		}

		if resp.NextPageToken == "" {
			break
		}
		pageToken = resp.NextPageToken
	}
}

func cmdSearch(ctx context.Context, c *client.Client, args []string) {
	fs := flag.NewFlagSet("search", flag.ExitOnError)
	k := fs.Int("k", 10, "number of results")
	prefix := fs.String("prefix", "", "key prefix filter")
	createdBy := fs.String("created-by", "", "filter by creator")
	minConf := fs.Float64("min-confidence", 0, "minimum confidence")
	consistency := fs.String("consistency", "session", "consistency level")
	fs.Parse(reorderArgs(args, fs))

	if fs.NArg() < 2 {
		fmt.Fprintf(os.Stderr, "usage: engramctl search <namespace> <query> [--k N] [--prefix X] [--created-by X]\n")
		os.Exit(1)
	}

	resp, err := c.Search(ctx, &pb.SearchRequest{
		Namespace:     fs.Arg(0),
		QueryText:     fs.Arg(1),
		K:             int32(*k),
		KeyPrefix:     *prefix,
		CreatedBy:     *createdBy,
		MinConfidence: float32(*minConf),
		Consistency:   parseConsistency(*consistency),
	})
	if err != nil {
		fatal(err)
	}

	for _, r := range resp.Results {
		printJSON(r)
	}
}

func cmdSetDecayPolicy(addr string, args []string) {
	fs := flag.NewFlagSet("set-decay-policy", flag.ExitOnError)
	halfLife := fs.Float64("half-life", 0, "half-life in seconds")
	accessWeight := fs.Float64("access-weight", 0, "access count weight")
	minScore := fs.Float64("min-score", 0, "minimum score for eviction")
	fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "usage: engramctl set-decay-policy <namespace> --half-life N --access-weight N --min-score N\n")
		os.Exit(1)
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(err)
	}
	defer conn.Close()

	adminClient := pb.NewAdminServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = adminClient.SetDecayPolicy(ctx, &pb.SetDecayPolicyRequest{
		Namespace: fs.Arg(0),
		Policy: &pb.DecayPolicy{
			HalfLifeSeconds: *halfLife,
			AccessWeight:    *accessWeight,
			MinScore:        *minScore,
		},
	})
	if err != nil {
		fatal(err)
	}
	fmt.Println("decay policy set")
}

func cmdGetDecayPolicy(addr string, args []string) {
	fs := flag.NewFlagSet("get-decay-policy", flag.ExitOnError)
	fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "usage: engramctl get-decay-policy <namespace>\n")
		os.Exit(1)
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(err)
	}
	defer conn.Close()

	adminClient := pb.NewAdminServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := adminClient.GetDecayPolicy(ctx, &pb.GetDecayPolicyRequest{
		Namespace: fs.Arg(0),
	})
	if err != nil {
		fatal(err)
	}
	printJSON(resp.Policy)
}

func cmdBench(addr string, args []string) {
	fs := flag.NewFlagSet("bench", flag.ExitOnError)
	namespace := fs.String("namespace", "bench", "benchmark namespace")
	numRecords := fs.Int("records", 1000, "records to pre-populate")
	duration := fs.Duration("duration", 10*time.Second, "benchmark duration")
	rate := fs.Int("rate", 0, "target ops/sec (0=unlimited)")
	workers := fs.Int("workers", 10, "concurrent workers")
	readPct := fs.Float64("read-pct", 0.5, "read fraction 0-1")
	valueSize := fs.Int("value-size", 256, "value size in bytes")
	fs.Parse(args)

	c, err := client.New(addr, "engramctl-bench")
	if err != nil {
		fatal(err)
	}
	defer c.Close()

	ctx := context.Background()
	cfg := bench.Config{
		Namespace:  *namespace,
		NumRecords: *numRecords,
		Duration:   *duration,
		RatePerSec: *rate,
		Workers:    *workers,
		ReadPct:    *readPct,
		ValueSize:  *valueSize,
	}

	fmt.Fprintf(os.Stderr, "running benchmark: %d records, %v duration, %d workers, %.0f%% reads\n",
		cfg.NumRecords, cfg.Duration, cfg.Workers, cfg.ReadPct*100)

	result, err := bench.Run(ctx, c, cfg)
	if err != nil {
		fatal(err)
	}

	data, err := result.ToJSON()
	if err != nil {
		fatal(err)
	}
	fmt.Println(string(data))
}

func cmdMCP(addr string) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(err)
	}
	defer conn.Close()

	memClient := pb.NewMemoryServiceClient(conn)
	srv := engrammcp.NewServer(memClient)
	stdio := mcpserver.NewStdioServer(srv)
	if err := stdio.Listen(context.Background(), os.Stdin, os.Stdout); err != nil {
		fatal(err)
	}
}

func cmdClusterStatus(addr string) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(err)
	}
	defer conn.Close()

	adminClient := pb.NewAdminServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := adminClient.GetClusterStatus(ctx, &pb.GetClusterStatusRequest{})
	if err != nil {
		fatal(err)
	}
	printJSON(resp)
}

func cmdAddVoter(addr string, args []string) {
	fs := flag.NewFlagSet("add-voter", flag.ExitOnError)
	fs.Parse(args)

	if fs.NArg() < 2 {
		fmt.Fprintf(os.Stderr, "usage: engramctl add-voter <server-id> <raft-address>\n")
		os.Exit(1)
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(err)
	}
	defer conn.Close()

	adminClient := pb.NewAdminServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = adminClient.AddVoter(ctx, &pb.AddVoterRequest{
		ServerId: fs.Arg(0),
		Address:  fs.Arg(1),
	})
	if err != nil {
		fatal(err)
	}
	fmt.Println("voter added")
}

func cmdRemoveServer(addr string, args []string) {
	fs := flag.NewFlagSet("remove-server", flag.ExitOnError)
	fs.Parse(args)

	if fs.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "usage: engramctl remove-server <server-id>\n")
		os.Exit(1)
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(err)
	}
	defer conn.Close()

	adminClient := pb.NewAdminServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err = adminClient.RemoveServer(ctx, &pb.RemoveServerRequest{
		ServerId: fs.Arg(0),
	})
	if err != nil {
		fatal(err)
	}
	fmt.Println("server removed")
}

// reorderArgs moves flags (--key val or --key=val) before positional args
// so Go's flag package parses them regardless of where the user placed them.
func reorderArgs(args []string, fs *flag.FlagSet) []string {
	// Build set of known flag names for this FlagSet.
	known := make(map[string]bool)
	fs.VisitAll(func(f *flag.Flag) {
		known[f.Name] = true
	})

	var flags, positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			positional = append(positional, args[i+1:]...)
			break
		}
		name := ""
		if strings.HasPrefix(a, "--") {
			name = strings.TrimPrefix(a, "--")
		} else if strings.HasPrefix(a, "-") && len(a) > 1 {
			name = strings.TrimPrefix(a, "-")
		}
		// Strip =value if present.
		if eqIdx := strings.IndexByte(name, '='); eqIdx >= 0 {
			name = name[:eqIdx]
		}
		if name != "" && known[name] {
			flags = append(flags, a)
			// If the flag doesn't contain '=' and isn't a bool flag, consume next arg as value.
			if !strings.Contains(a, "=") && i+1 < len(args) {
				flags = append(flags, args[i+1])
				i++
			}
		} else {
			positional = append(positional, a)
		}
	}
	return append(flags, positional...)
}

func parseConsistency(s string) pb.ConsistencyLevel {
	switch strings.ToLower(s) {
	case "linearizable":
		return pb.ConsistencyLevel_CONSISTENCY_LINEARIZABLE
	case "stale":
		return pb.ConsistencyLevel_CONSISTENCY_STALE
	default:
		return pb.ConsistencyLevel_CONSISTENCY_SESSION
	}
}

func printJSON(v interface{}) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		fatal(err)
	}
	fmt.Println(string(data))
}

func usage() {
	fmt.Fprintf(os.Stderr, `engram CLI

Usage:
  engramctl put    <namespace> <key> <value> [--created-by X] [--source X] [--confidence N] [--ttl D]
  engramctl get    <namespace> <key> [--consistency level]
  engramctl cas    <namespace> <key> <value> --version N [--created-by X] [--source X] [--confidence N] [--ttl D]
  engramctl delete <namespace> <key> [--version N]
  engramctl list   <namespace> [--prefix X] [--limit N] [--consistency level]
  engramctl search <namespace> <query> [--k N] [--prefix X] [--created-by X] [--min-confidence N]
  engramctl bench  [--records N] [--duration D] [--workers N] [--rate N] [--read-pct F]
  engramctl mcp    (start MCP stdio server)

Admin:
  engramctl cluster-status
  engramctl add-voter <server-id> <raft-address>
  engramctl remove-server <server-id>
  engramctl set-decay-policy <namespace> --half-life N --access-weight N --min-score N
  engramctl get-decay-policy <namespace>

Environment:
  ENGRAM_ADDR  gRPC server address (default: localhost:50051)

Consistency levels: session (default), linearizable, stale
`)
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}
