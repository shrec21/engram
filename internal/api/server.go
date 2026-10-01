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

// Server implements pb.MemoryServiceServer.
type Server struct {
	pb.UnimplementedMemoryServiceServer
	store *store.Store
	clock clock.Clock
}

func NewServer(s *store.Store, clk clock.Clock) *Server {
	return &Server{store: s, clock: clk}
}

func (s *Server) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	if err := validateNamespaceKey(req.Namespace, req.Key); err != nil {
		return nil, err
	}

	cmd := &pb.Command{
		Timestamp: timestamppb.New(s.clock.Now()),
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

	result, err := s.store.Apply(cmd)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "apply: %v", err)
	}

	return &pb.PutResponse{Version: result.GetMutation().Version}, nil
}

func (s *Server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	if err := validateNamespaceKey(req.Namespace, req.Key); err != nil {
		return nil, err
	}

	rec, err := s.store.Get(req.Namespace, req.Key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get: %v", err)
	}
	if rec == nil {
		return nil, status.Errorf(codes.NotFound, "key %q not found in namespace %q", req.Key, req.Namespace)
	}

	return &pb.GetResponse{Record: rec}, nil
}

func (s *Server) CompareAndSet(ctx context.Context, req *pb.CompareAndSetRequest) (*pb.CompareAndSetResponse, error) {
	if err := validateNamespaceKey(req.Namespace, req.Key); err != nil {
		return nil, err
	}

	cmd := &pb.Command{
		Timestamp: timestamppb.New(s.clock.Now()),
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

	result, err := s.store.Apply(cmd)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "apply: %v", err)
	}

	if conflict := result.GetCasConflict(); conflict != nil {
		return nil, status.Errorf(codes.FailedPrecondition,
			"version mismatch: expected %d, current %d",
			req.ExpectedVersion, conflict.CurrentVersion)
	}

	return &pb.CompareAndSetResponse{Version: result.GetMutation().Version}, nil
}

func (s *Server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	if err := validateNamespaceKey(req.Namespace, req.Key); err != nil {
		return nil, err
	}

	cmd := &pb.Command{
		Timestamp: timestamppb.New(s.clock.Now()),
		Payload: &pb.Command_Delete{
			Delete: &pb.DeleteCommand{
				Namespace:       req.Namespace,
				Key:             req.Key,
				ExpectedVersion: req.ExpectedVersion,
			},
		},
	}

	result, err := s.store.Apply(cmd)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "apply: %v", err)
	}

	if conflict := result.GetCasConflict(); conflict != nil {
		return nil, status.Errorf(codes.FailedPrecondition,
			"version mismatch: expected %d, current %d",
			req.ExpectedVersion, conflict.CurrentVersion)
	}

	return &pb.DeleteResponse{}, nil
}

func (s *Server) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	if req.Namespace == "" {
		return nil, status.Error(codes.InvalidArgument, "namespace is required")
	}

	limit := int(req.Limit)
	if limit <= 0 {
		limit = 100
	}

	records, nextToken, err := s.store.List(req.Namespace, req.KeyPrefix, limit, req.PageToken)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list: %v", err)
	}

	return &pb.ListResponse{
		Records:       records,
		NextPageToken: nextToken,
	}, nil
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
