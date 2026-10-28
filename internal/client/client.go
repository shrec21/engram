package client

import (
	"context"
	"sync"
	"sync/atomic"

	pb "github.com/shrec21/engram/proto/engrampb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const maxRedirects = 3

// Client is a smart Engram client that handles NOT_LEADER redirects,
// tracks session tokens (applied_index), and manages client_id + seq for dedup.
type Client struct {
	clientID string
	seq      atomic.Uint64

	mu           sync.RWMutex
	conn         *grpc.ClientConn
	memory       pb.MemoryServiceClient
	admin        pb.AdminServiceClient
	appliedIndex uint64
}

// New creates a new smart client connected to the given address.
func New(addr string, clientID string) (*Client, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &Client{
		clientID: clientID,
		conn:     conn,
		memory:   pb.NewMemoryServiceClient(conn),
		admin:    pb.NewAdminServiceClient(conn),
	}, nil
}

// Close closes the underlying connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) nextSeq() uint64 {
	return c.seq.Add(1)
}

func (c *Client) updateAppliedIndex(index uint64) {
	c.mu.Lock()
	if index > c.appliedIndex {
		c.appliedIndex = index
	}
	c.mu.Unlock()
}

func (c *Client) getAppliedIndex() uint64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.appliedIndex
}

// Put stores a record.
func (c *Client) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	req.ClientId = c.clientID
	req.Seq = c.nextSeq()

	for i := 0; i < maxRedirects; i++ {
		resp, err := c.memory.Put(ctx, req)
		if err == nil {
			c.updateAppliedIndex(resp.AppliedIndex)
			return resp, nil
		}
		if !c.handleRedirect(err) {
			return nil, err
		}
	}
	return nil, status.Error(codes.Unavailable, "too many redirects")
}

// Get retrieves a record.
func (c *Client) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	req.AppliedIndex = c.getAppliedIndex()

	for i := 0; i < maxRedirects; i++ {
		resp, err := c.memory.Get(ctx, req)
		if err == nil {
			c.updateAppliedIndex(resp.AppliedIndex)
			return resp, nil
		}
		if !c.handleRedirect(err) {
			return nil, err
		}
	}
	return nil, status.Error(codes.Unavailable, "too many redirects")
}

// CompareAndSet performs a CAS operation.
func (c *Client) CompareAndSet(ctx context.Context, req *pb.CompareAndSetRequest) (*pb.CompareAndSetResponse, error) {
	req.ClientId = c.clientID
	req.Seq = c.nextSeq()

	for i := 0; i < maxRedirects; i++ {
		resp, err := c.memory.CompareAndSet(ctx, req)
		if err == nil {
			c.updateAppliedIndex(resp.AppliedIndex)
			return resp, nil
		}
		if !c.handleRedirect(err) {
			return nil, err
		}
	}
	return nil, status.Error(codes.Unavailable, "too many redirects")
}

// Delete removes a record.
func (c *Client) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	req.ClientId = c.clientID
	req.Seq = c.nextSeq()

	for i := 0; i < maxRedirects; i++ {
		resp, err := c.memory.Delete(ctx, req)
		if err == nil {
			c.updateAppliedIndex(resp.AppliedIndex)
			return resp, nil
		}
		if !c.handleRedirect(err) {
			return nil, err
		}
	}
	return nil, status.Error(codes.Unavailable, "too many redirects")
}

// List returns records in a namespace.
func (c *Client) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	req.AppliedIndex = c.getAppliedIndex()

	for i := 0; i < maxRedirects; i++ {
		resp, err := c.memory.List(ctx, req)
		if err == nil {
			c.updateAppliedIndex(resp.AppliedIndex)
			return resp, nil
		}
		if !c.handleRedirect(err) {
			return nil, err
		}
	}
	return nil, status.Error(codes.Unavailable, "too many redirects")
}

// Search performs a semantic search.
func (c *Client) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchResponse, error) {
	req.AppliedIndex = c.getAppliedIndex()

	for i := 0; i < maxRedirects; i++ {
		resp, err := c.memory.Search(ctx, req)
		if err == nil {
			c.updateAppliedIndex(resp.AppliedIndex)
			return resp, nil
		}
		if !c.handleRedirect(err) {
			return nil, err
		}
	}
	return nil, status.Error(codes.Unavailable, "too many redirects")
}

// Admin returns the admin service client.
func (c *Client) Admin() pb.AdminServiceClient {
	return c.admin
}

// handleRedirect checks if the error is a NOT_LEADER redirect and reconnects.
// Returns true if a redirect was handled and the caller should retry.
func (c *Client) handleRedirect(err error) bool {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.Unavailable {
		return false
	}

	for _, detail := range st.Details() {
		if nld, ok := detail.(*pb.NotLeaderDetail); ok && nld.LeaderAddress != "" {
			// Reconnect to the leader.
			newConn, connErr := grpc.NewClient(nld.LeaderAddress,
				grpc.WithTransportCredentials(insecure.NewCredentials()))
			if connErr != nil {
				return false
			}
			c.mu.Lock()
			oldConn := c.conn
			c.conn = newConn
			c.memory = pb.NewMemoryServiceClient(newConn)
			c.admin = pb.NewAdminServiceClient(newConn)
			c.mu.Unlock()
			oldConn.Close()
			return true
		}
	}
	return false
}
