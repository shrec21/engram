package mcp

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/mark3labs/mcp-go/mcp"
	mcpserver "github.com/mark3labs/mcp-go/server"
	pb "github.com/shrec21/engram/proto/engrampb"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

// NewServer creates an MCP server backed by a gRPC MemoryService client.
func NewServer(client pb.MemoryServiceClient) *mcpserver.MCPServer {
	s := mcpserver.NewMCPServer("engram", "0.1.0",
		mcpserver.WithToolCapabilities(true),
	)

	h := &handler{client: client}

	s.AddTool(memoryGetTool(), h.handleGet)
	s.AddTool(memoryPutTool(), h.handlePut)
	s.AddTool(memoryCasTool(), h.handleCAS)
	s.AddTool(memoryDeleteTool(), h.handleDelete)
	s.AddTool(memoryListTool(), h.handleList)
	s.AddTool(memorySearchTool(), h.handleSearch)

	return s
}

type handler struct {
	client pb.MemoryServiceClient
}

// --- Tool definitions ---

func memoryGetTool() mcp.Tool {
	return mcp.NewTool("memory_get",
		mcp.WithDescription("Retrieve a memory record by namespace and key"),
		mcp.WithString("namespace", mcp.Description("Namespace"), mcp.Required()),
		mcp.WithString("key", mcp.Description("Record key"), mcp.Required()),
	)
}

func memoryPutTool() mcp.Tool {
	return mcp.NewTool("memory_put",
		mcp.WithDescription("Store a memory record (unconditional write)"),
		mcp.WithString("namespace", mcp.Description("Namespace"), mcp.Required()),
		mcp.WithString("key", mcp.Description("Record key"), mcp.Required()),
		mcp.WithString("value", mcp.Description("Record value"), mcp.Required()),
		mcp.WithString("created_by", mcp.Description("Agent ID")),
		mcp.WithString("source", mcp.Description("Provenance")),
		mcp.WithNumber("confidence", mcp.Description("Confidence score 0-1")),
		mcp.WithNumber("ttl_seconds", mcp.Description("Time-to-live in seconds")),
	)
}

func memoryCasTool() mcp.Tool {
	return mcp.NewTool("memory_cas",
		mcp.WithDescription("Compare-and-set: write only if current version matches expected_version. Use 0 to create only if absent."),
		mcp.WithString("namespace", mcp.Description("Namespace"), mcp.Required()),
		mcp.WithString("key", mcp.Description("Record key"), mcp.Required()),
		mcp.WithString("value", mcp.Description("New value"), mcp.Required()),
		mcp.WithNumber("expected_version", mcp.Description("Expected current version (0 = create only)"), mcp.Required()),
		mcp.WithString("created_by", mcp.Description("Agent ID")),
		mcp.WithString("source", mcp.Description("Provenance")),
		mcp.WithNumber("confidence", mcp.Description("Confidence score 0-1")),
		mcp.WithNumber("ttl_seconds", mcp.Description("Time-to-live in seconds")),
	)
}

func memoryDeleteTool() mcp.Tool {
	return mcp.NewTool("memory_delete",
		mcp.WithDescription("Delete a memory record"),
		mcp.WithString("namespace", mcp.Description("Namespace"), mcp.Required()),
		mcp.WithString("key", mcp.Description("Record key"), mcp.Required()),
		mcp.WithNumber("expected_version", mcp.Description("Expected version (0 = unconditional)")),
	)
}

func memoryListTool() mcp.Tool {
	return mcp.NewTool("memory_list",
		mcp.WithDescription("List memory records in a namespace"),
		mcp.WithString("namespace", mcp.Description("Namespace"), mcp.Required()),
		mcp.WithString("key_prefix", mcp.Description("Filter by key prefix")),
		mcp.WithNumber("limit", mcp.Description("Max records to return")),
	)
}

func memorySearchTool() mcp.Tool {
	return mcp.NewTool("memory_search",
		mcp.WithDescription("Search memory records by semantic similarity. Provide query_text for natural language search."),
		mcp.WithString("namespace", mcp.Description("Namespace"), mcp.Required()),
		mcp.WithString("query_text", mcp.Description("Natural language query")),
		mcp.WithNumber("k", mcp.Description("Number of results to return (default 10)")),
		mcp.WithString("key_prefix", mcp.Description("Filter by key prefix")),
		mcp.WithString("created_by", mcp.Description("Filter by creator")),
		mcp.WithNumber("min_confidence", mcp.Description("Minimum confidence threshold")),
	)
}

// --- Handlers ---

func (h *handler) handleGet(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := req.GetArguments()
	resp, err := h.client.Get(ctx, &pb.GetRequest{
		Namespace: argString(args, "namespace"),
		Key:       argString(args, "key"),
	})
	if err != nil {
		return mcp.NewToolResultError(status.Convert(err).Message()), nil
	}
	return jsonResult(resp.Record)
}

func (h *handler) handlePut(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := req.GetArguments()
	putReq := &pb.PutRequest{
		Namespace:  argString(args, "namespace"),
		Key:        argString(args, "key"),
		Value:      []byte(argString(args, "value")),
		CreatedBy:  argString(args, "created_by"),
		Source:     argString(args, "source"),
		Confidence: float32(argFloat(args, "confidence")),
	}

	if ttl := argFloat(args, "ttl_seconds"); ttl > 0 {
		putReq.ExpiresAt = timestamppb.New(time.Now().Add(time.Duration(ttl) * time.Second))
	}

	resp, err := h.client.Put(ctx, putReq)
	if err != nil {
		return mcp.NewToolResultError(status.Convert(err).Message()), nil
	}
	return jsonResult(map[string]uint64{"version": resp.Version})
}

func (h *handler) handleCAS(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := req.GetArguments()
	casReq := &pb.CompareAndSetRequest{
		Namespace:       argString(args, "namespace"),
		Key:             argString(args, "key"),
		Value:           []byte(argString(args, "value")),
		ExpectedVersion: uint64(argFloat(args, "expected_version")),
		CreatedBy:       argString(args, "created_by"),
		Source:          argString(args, "source"),
		Confidence:      float32(argFloat(args, "confidence")),
	}

	if ttl := argFloat(args, "ttl_seconds"); ttl > 0 {
		casReq.ExpiresAt = timestamppb.New(time.Now().Add(time.Duration(ttl) * time.Second))
	}

	resp, err := h.client.CompareAndSet(ctx, casReq)
	if err != nil {
		return mcp.NewToolResultError(status.Convert(err).Message()), nil
	}
	return jsonResult(map[string]uint64{"version": resp.Version})
}

func (h *handler) handleDelete(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := req.GetArguments()
	_, err := h.client.Delete(ctx, &pb.DeleteRequest{
		Namespace:       argString(args, "namespace"),
		Key:             argString(args, "key"),
		ExpectedVersion: uint64(argFloat(args, "expected_version")),
	})
	if err != nil {
		return mcp.NewToolResultError(status.Convert(err).Message()), nil
	}
	return jsonResult(map[string]string{"status": "deleted"})
}

func (h *handler) handleSearch(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := req.GetArguments()
	k := int32(argFloat(args, "k"))
	if k <= 0 {
		k = 10
	}

	searchReq := &pb.SearchRequest{
		Namespace:     argString(args, "namespace"),
		QueryText:     argString(args, "query_text"),
		K:             k,
		KeyPrefix:     argString(args, "key_prefix"),
		CreatedBy:     argString(args, "created_by"),
		MinConfidence: float32(argFloat(args, "min_confidence")),
	}

	resp, err := h.client.Search(ctx, searchReq)
	if err != nil {
		return mcp.NewToolResultError(status.Convert(err).Message()), nil
	}
	return jsonResult(resp.Results)
}

func (h *handler) handleList(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	args := req.GetArguments()
	limit := int32(argFloat(args, "limit"))
	if limit <= 0 {
		limit = 100
	}

	resp, err := h.client.List(ctx, &pb.ListRequest{
		Namespace: argString(args, "namespace"),
		KeyPrefix: argString(args, "key_prefix"),
		Limit:     limit,
	})
	if err != nil {
		return mcp.NewToolResultError(status.Convert(err).Message()), nil
	}
	return jsonResult(resp.Records)
}

// --- Helpers ---

func argString(args map[string]any, key string) string {
	v, _ := args[key].(string)
	return v
}

func argFloat(args map[string]any, key string) float64 {
	v, _ := args[key].(float64)
	return v
}

func jsonResult(v any) (*mcp.CallToolResult, error) {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal result: %w", err)
	}
	return &mcp.CallToolResult{
		Content: []mcp.Content{
			mcp.NewTextContent(string(data)),
		},
	}, nil
}
