package api

import (
	"context"

	pb "github.com/shrec21/engram/proto/engrampb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ClusterAdmin provides admin operations on the Raft cluster.
type ClusterAdmin interface {
	AddVoter(id, raftAddr, grpcAddr string) error
	RemoveServer(id string) error
	GetClusterStatus() (*pb.GetClusterStatusResponse, error)
}

// AdminServer implements pb.AdminServiceServer.
type AdminServer struct {
	pb.UnimplementedAdminServiceServer
	admin ClusterAdmin
}

// NewAdminServer creates a new admin gRPC server.
func NewAdminServer(admin ClusterAdmin) *AdminServer {
	return &AdminServer{admin: admin}
}

func (s *AdminServer) AddVoter(ctx context.Context, req *pb.AddVoterRequest) (*pb.AddVoterResponse, error) {
	if req.ServerId == "" || req.Address == "" {
		return nil, status.Error(codes.InvalidArgument, "server_id and address required")
	}
	if err := s.admin.AddVoter(req.ServerId, req.Address, ""); err != nil {
		return nil, status.Errorf(codes.Internal, "add voter: %v", err)
	}
	return &pb.AddVoterResponse{}, nil
}

func (s *AdminServer) RemoveServer(ctx context.Context, req *pb.RemoveServerRequest) (*pb.RemoveServerResponse, error) {
	if req.ServerId == "" {
		return nil, status.Error(codes.InvalidArgument, "server_id required")
	}
	if err := s.admin.RemoveServer(req.ServerId); err != nil {
		return nil, status.Errorf(codes.Internal, "remove server: %v", err)
	}
	return &pb.RemoveServerResponse{}, nil
}

func (s *AdminServer) GetClusterStatus(ctx context.Context, req *pb.GetClusterStatusRequest) (*pb.GetClusterStatusResponse, error) {
	resp, err := s.admin.GetClusterStatus()
	if err != nil {
		return nil, status.Errorf(codes.Internal, "cluster status: %v", err)
	}
	return resp, nil
}

var _ pb.AdminServiceServer = (*AdminServer)(nil)
