package api

import (
	"context"
	"strings"

	"github.com/shrec21/engram/internal/clock"
	"github.com/shrec21/engram/internal/store"
	pb "github.com/shrec21/engram/proto/engrampb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Proposer submits commands through Raft (leader only).
type Proposer interface {
	Propose(ctx context.Context, cmd *pb.Command) (*pb.CommandResult, uint64, error)
}

// ReadStore provides read access to the Pebble state.
type ReadStore interface {
	Get(namespace, key string) (*pb.MemoryRecord, error)
	List(namespace, keyPrefix string, limit int, pageToken string) ([]*pb.MemoryRecord, string, error)
}

// ClusterInfo provides Raft cluster state.
type ClusterInfo interface {
	Leader() (grpcAddr, id string, isLeader bool)
	LinearizableBarrier(ctx context.Context) error
	WaitForIndex(ctx context.Context, index uint64) error
	AppliedIndex() uint64
}

// Server implements pb.MemoryServiceServer with Raft-aware routing.
type Server struct {
	pb.UnimplementedMemoryServiceServer
	proposer Proposer
	store    ReadStore
	cluster  ClusterInfo
	clock    clock.Clock
	tracker  *store.AccessTracker
}

// NewServer creates a new gRPC server. When cluster is nil, single-node mode
// is used (all writes go directly through Proposer, reads are local).
func NewServer(proposer Proposer, st ReadStore, cluster ClusterInfo, clk clock.Clock, tracker *store.AccessTracker) *Server {
	return &Server{
		proposer: proposer,
		store:    st,
		cluster:  cluster,
		clock:    clk,
		tracker:  tracker,
	}
}

func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if err := validateNamespaceKey(req.Namespace, req.Key); err != nil {
		return nil, err
	}

	if err := s.requireLeader(); err != nil {
		return nil, err
	}

	cmd := &pb.Command{
		Timestamp: timestamppb.New(s.clock.Now()),
		ClientId:  req.ClientId,
		Seq:       req.Seq,
		Payload: &pb.Command_Put{
			Put: &pb.PutCommand{
				Namespace:  req.Namespace,
				Key:        req.Key,
				Value:      req.Value,
				CreatedBy:  req.CreatedBy,
				Source:     req.Source,
				Confidence: req.Confidence,
				ExpiresAt:  req.ExpiresAt,
			},
		},
	}

	result, index, err := s.proposer.Propose(ctx, cmd)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "propose: %v", err)
	}

	return &pb.PutResponse{
		Version:      result.GetMutation().Version,
		AppliedIndex: index,
	}, nil
}

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if err := validateNamespaceKey(req.Namespace, req.Key); err != nil {
		return nil, err
	}

	if err := s.waitForConsistency(ctx, req.Consistency, req.AppliedIndex); err != nil {
		return nil, err
	}

	rec, err := s.store.Get(req.Namespace, req.Key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get: %v", err)
	}
	if rec == nil {
		return nil, status.Errorf(codes.NotFound, "key %q not found in namespace %q", req.Key, req.Namespace)
	}

	// Track access for the TouchFlusher.
	if s.tracker != nil {
		s.tracker.Track(req.Namespace, req.Key, s.clock.Now())
	}

	appliedIndex := uint64(0)
	if s.cluster != nil {
		appliedIndex = s.cluster.AppliedIndex()
	}

	return &pb.GetResponse{
		Record:       rec,
		AppliedIndex: appliedIndex,
	}, nil
}

func (s *Server) CompareAndSet(ctx context.Context, req *pb.CompareAndSetRequest) (*pb.CompareAndSetResponse, error) {
	if err := validateNamespaceKey(req.Namespace, req.Key); err != nil {
		return nil, err
	}

	if err := s.requireLeader(); err != nil {
		return nil, err
	}

	cmd := &pb.Command{
		Timestamp: timestamppb.New(s.clock.Now()),
		ClientId:  req.ClientId,
		Seq:       req.Seq,
		Payload: &pb.Command_CompareAndSet{
			CompareAndSet: &pb.CompareAndSetCommand{
				Namespace:       req.Namespace,
				Key:             req.Key,
				Value:           req.Value,
				ExpectedVersion: req.ExpectedVersion,
				CreatedBy:       req.CreatedBy,
				Source:          req.Source,
				Confidence:      req.Confidence,
				ExpiresAt:       req.ExpiresAt,
			},
		},
	}

	result, index, err := s.proposer.Propose(ctx, cmd)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "propose: %v", err)
	}

	if conflict := result.GetCasConflict(); conflict != nil {
		return nil, status.Errorf(codes.FailedPrecondition,
			"version mismatch: expected %d, current %d",
			req.ExpectedVersion, conflict.CurrentVersion)
	}

	return &pb.CompareAndSetResponse{
		Version:      result.GetMutation().Version,
		AppliedIndex: index,
	}, nil
}

func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if err := validateNamespaceKey(req.Namespace, req.Key); err != nil {
		return nil, err
	}

	if err := s.requireLeader(); err != nil {
		return nil, err
	}

	cmd := &pb.Command{
		Timestamp: timestamppb.New(s.clock.Now()),
		ClientId:  req.ClientId,
		Seq:       req.Seq,
		Payload: &pb.Command_Delete{
			Delete: &pb.DeleteCommand{
				Namespace:       req.Namespace,
				Key:             req.Key,
				ExpectedVersion: req.ExpectedVersion,
			},
		},
	}

	result, index, err := s.proposer.Propose(ctx, cmd)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "propose: %v", err)
	}

	if conflict := result.GetCasConflict(); conflict != nil {
		return nil, status.Errorf(codes.FailedPrecondition,
			"version mismatch: expected %d, current %d",
			req.ExpectedVersion, conflict.CurrentVersion)
	}

	return &pb.DeleteResponse{AppliedIndex: index}, nil
}

func (s *Server) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	if req.Namespace == "" {
		return nil, status.Error(codes.InvalidArgument, "namespace is required")
	}

	if err := s.waitForConsistency(ctx, req.Consistency, req.AppliedIndex); err != nil {
		return nil, err
	}

	limit := int(req.Limit)
	if limit <= 0 {
		limit = 100
	}

	records, nextToken, err := s.store.List(req.Namespace, req.KeyPrefix, limit, req.PageToken)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list: %v", err)
	}

	appliedIndex := uint64(0)
	if s.cluster != nil {
		appliedIndex = s.cluster.AppliedIndex()
	}

	return &pb.ListResponse{
		Records:       records,
		NextPageToken: nextToken,
		AppliedIndex:  appliedIndex,
	}, nil
}

// requireLeader returns a NOT_LEADER error with redirect info if this node
// is not the leader. Returns nil if the node is the leader or no cluster info
// is available (single-node mode).
func (s *Server) requireLeader() error {
	if s.cluster == nil {
		return nil
	}
	grpcAddr, leaderID, isLeader := s.cluster.Leader()
	if isLeader {
		return nil
	}

	st := status.New(codes.Unavailable, "not the leader")
	detail := &pb.NotLeaderDetail{
		LeaderAddress: grpcAddr,
		LeaderId:      leaderID,
	}
	st, err := st.WithDetails(detail)
	if err != nil {
		return status.Error(codes.Unavailable, "not the leader")
	}
	return st.Err()
}

// waitForConsistency enforces the requested consistency level before a read.
func (s *Server) waitForConsistency(ctx context.Context, level pb.ConsistencyLevel, clientIndex uint64) error {
	if s.cluster == nil {
		return nil // single-node mode
	}

	switch level {
	case pb.ConsistencyLevel_CONSISTENCY_LINEARIZABLE:
		if err := s.cluster.LinearizableBarrier(ctx); err != nil {
			return status.Errorf(codes.Unavailable, "linearizable barrier: %v", err)
		}
	case pb.ConsistencyLevel_CONSISTENCY_SESSION:
		if clientIndex > 0 {
			if err := s.cluster.WaitForIndex(ctx, clientIndex); err != nil {
				return status.Errorf(codes.Unavailable, "wait for index: %v", err)
			}
		}
	case pb.ConsistencyLevel_CONSISTENCY_STALE:
		// No waiting — read local state immediately.
	}
	return nil
}

func validateNamespaceKey(namespace, key string) error {
	var missing []string
	if namespace == "" {
		missing = append(missing, "namespace")
	}
	if key == "" {
		missing = append(missing, "key")
	}
	if len(missing) > 0 {
		return status.Errorf(codes.InvalidArgument, "%s required", strings.Join(missing, " and "))
	}
	if strings.ContainsRune(namespace, 0) {
		return status.Error(codes.InvalidArgument, "namespace must not contain null bytes")
	}
	return nil
}

// Ensure Server implements the interface at compile time.
var _ pb.MemoryServiceServer = (*Server)(nil)
