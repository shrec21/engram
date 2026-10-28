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

// DecayPolicyStore provides read access to decay policies.
type DecayPolicyStore interface {
	GetDecayPolicy(namespace string) (*pb.DecayPolicy, error)
}

// AdminServer implements pb.AdminServiceServer.
type AdminServer struct {
	pb.UnimplementedAdminServiceServer
	admin      ClusterAdmin
	proposer   Proposer
	policyStore DecayPolicyStore
}

// NewAdminServer creates a new admin gRPC server.
// proposer and policyStore may be nil if decay policy management is not needed.
func NewAdminServer(admin ClusterAdmin, proposer Proposer, policyStore DecayPolicyStore) *AdminServer {
	return &AdminServer{admin: admin, proposer: proposer, policyStore: policyStore}
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

func (s *AdminServer) SetDecayPolicy(ctx context.Context, req *pb.SetDecayPolicyRequest) (*pb.SetDecayPolicyResponse, error) {
	if req.Namespace == "" {
		return nil, status.Error(codes.InvalidArgument, "namespace is required")
	}
	if req.Policy == nil {
		return nil, status.Error(codes.InvalidArgument, "policy is required")
	}
	if s.proposer == nil {
		return nil, status.Error(codes.Unimplemented, "proposer not configured")
	}

	cmd := &pb.Command{
		ClientId: req.ClientId,
		Seq:      req.Seq,
		Payload: &pb.Command_SetDecayPolicy{
			SetDecayPolicy: &pb.SetDecayPolicyCommand{
				Namespace: req.Namespace,
				Policy:    req.Policy,
			},
		},
	}

	_, index, err := s.proposer.Propose(ctx, cmd)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "propose: %v", err)
	}

	return &pb.SetDecayPolicyResponse{AppliedIndex: index}, nil
}

func (s *AdminServer) GetDecayPolicy(ctx context.Context, req *pb.GetDecayPolicyRequest) (*pb.GetDecayPolicyResponse, error) {
	if req.Namespace == "" {
		return nil, status.Error(codes.InvalidArgument, "namespace is required")
	}
	if s.policyStore == nil {
		return nil, status.Error(codes.Unimplemented, "policy store not configured")
	}

	policy, err := s.policyStore.GetDecayPolicy(req.Namespace)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "get policy: %v", err)
	}

	return &pb.GetDecayPolicyResponse{Policy: policy}, nil
}

var _ pb.AdminServiceServer = (*AdminServer)(nil)
