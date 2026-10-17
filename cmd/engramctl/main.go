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

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(err)
	}
	defer conn.Close()

	client := pb.NewMemoryServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	switch os.Args[1] {
	case "put":
		cmdPut(ctx, client, os.Args[2:])
	case "get":
		cmdGet(ctx, client, os.Args[2:])
	case "cas":
		cmdCAS(ctx, client, os.Args[2:])
	case "delete":
		cmdDelete(ctx, client, os.Args[2:])
	case "list":
		cmdList(ctx, client, os.Args[2:])
	case "mcp":
		cmdMCP(client)
	default:
		usage()
		os.Exit(1)
	}
}

func cmdPut(ctx context.Context, client pb.MemoryServiceClient, args []string) {
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

	resp, err := client.Put(ctx, req)
	if err != nil {
		fatal(err)
	}
	fmt.Printf("version: %d\n", resp.Version)
}

func cmdGet(ctx context.Context, client pb.MemoryServiceClient, args []string) {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	fs.Parse(reorderArgs(args, fs))

	if fs.NArg() < 2 {
		fmt.Fprintf(os.Stderr, "usage: engramctl get <namespace> <key>\n")
		os.Exit(1)
	}

	resp, err := client.Get(ctx, &pb.GetRequest{
		Namespace: fs.Arg(0),
		Key:       fs.Arg(1),
	})
	if err != nil {
		fatal(err)
	}

	printJSON(resp.Record)
}

func cmdCAS(ctx context.Context, client pb.MemoryServiceClient, args []string) {
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

	resp, err := client.CompareAndSet(ctx, req)
	if err != nil {
		fatal(err)
	}
	fmt.Printf("version: %d\n", resp.Version)
}

func cmdDelete(ctx context.Context, client pb.MemoryServiceClient, args []string) {
	fs := flag.NewFlagSet("delete", flag.ExitOnError)
	version := fs.Uint64("version", 0, "expected version (0 = unconditional)")
	fs.Parse(reorderArgs(args, fs))

	if fs.NArg() < 2 {
		fmt.Fprintf(os.Stderr, "usage: engramctl delete <namespace> <key> [--version N]\n")
		os.Exit(1)
	}

	_, err := client.Delete(ctx, &pb.DeleteRequest{
		Namespace:       fs.Arg(0),
		Key:             fs.Arg(1),
		ExpectedVersion: *version,
	})
	if err != nil {
		fatal(err)
	}
	fmt.Println("deleted")
}

func cmdList(ctx context.Context, client pb.MemoryServiceClient, args []string) {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	prefix := fs.String("prefix", "", "key prefix filter")
	limit := fs.Int("limit", 100, "max records per page")
	fs.Parse(reorderArgs(args, fs))

	if fs.NArg() < 1 {
		fmt.Fprintf(os.Stderr, "usage: engramctl list <namespace> [--prefix X] [--limit N]\n")
		os.Exit(1)
	}

	pageToken := ""
	for {
		resp, err := client.List(ctx, &pb.ListRequest{
			Namespace: fs.Arg(0),
			KeyPrefix: *prefix,
			Limit:     int32(*limit),
			PageToken: pageToken,
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

func cmdMCP(client pb.MemoryServiceClient) {
	srv := engrammcp.NewServer(client)
	stdio := mcpserver.NewStdioServer(srv)
	if err := stdio.Listen(context.Background(), os.Stdin, os.Stdout); err != nil {
		fatal(err)
	}
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
  engramctl get    <namespace> <key>
  engramctl cas    <namespace> <key> <value> --version N [--created-by X] [--source X] [--confidence N] [--ttl D]
  engramctl delete <namespace> <key> [--version N]
  engramctl list   <namespace> [--prefix X] [--limit N]
  engramctl mcp    (start MCP stdio server)

Environment:
  ENGRAM_ADDR  gRPC server address (default: localhost:50051)
`)
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}
