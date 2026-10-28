package api

import (
	"context"
	"math"
	"sort"
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
	GetDecayPolicy(namespace string) (*pb.DecayPolicy, error)
}

// SemanticSearcher provides vector similarity search.
type SemanticSearcher interface {
	Search(namespace string, query []float32, k int) []SearchHit
}

// SearchHit from HNSW index.
type SearchHit struct {
	Namespace  string
	Key        string
	Similarity float32
}

// Embedder produces vector embeddings from text.
type Embedder interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
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
	searcher SemanticSearcher // may be nil
	embedder Embedder        // may be nil
}

// NewServer creates a new gRPC server. When cluster is nil, single-node mode
// is used (all writes go directly through Proposer, reads are local).
// searcher and embedder may be nil if semantic search is not enabled.
func NewServer(proposer Proposer, st ReadStore, cluster ClusterInfo, clk clock.Clock, tracker *store.AccessTracker, searcher SemanticSearcher, embedder Embedder) *Server {
	return &Server{
		proposer: proposer,
		store:    st,
		cluster:  cluster,
		clock:    clk,
		tracker:  tracker,
		searcher: searcher,
		embedder: embedder,
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
				Pinned:     req.Pinned,
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
				Pinned:          req.Pinned,
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

func (s *Server) Search(ctx context.Context, req *pb.SearchRequest) (*pb.SearchResponse, error) {
	if req.Namespace == "" {
		return nil, status.Error(codes.InvalidArgument, "namespace is required")
	}
	if req.QueryText == "" && len(req.QueryVector) == 0 {
		return nil, status.Error(codes.InvalidArgument, "query_text or query_vector is required")
	}
	if s.searcher == nil {
		return nil, status.Error(codes.Unimplemented, "semantic search not enabled")
	}

	k := int(req.K)
	if k <= 0 {
		k = 10
	}

	if err := s.waitForConsistency(ctx, req.Consistency, req.AppliedIndex); err != nil {
		return nil, err
	}

	// Determine query vector.
	queryVec := req.QueryVector
	if len(queryVec) == 0 && req.QueryText != "" {
		if s.embedder == nil {
			return nil, status.Error(codes.FailedPrecondition, "embedder not configured for query_text")
		}
		vecs, err := s.embedder.Embed(ctx, []string{req.QueryText})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "embed query: %v", err)
		}
		queryVec = vecs[0]
	}

	// Search HNSW index.
	hits := s.searcher.Search(req.Namespace, queryVec, k*3)

	// Read records, apply filters, compute scores.
	alpha := float64(req.SimilarityWeight)
	if alpha <= 0 {
		alpha = 0.7
	}

	now := s.clock.Now()

	type scored struct {
		result *pb.SearchResult
		score  float64
	}
	var candidates []scored

	for _, hit := range hits {
		rec, err := s.store.Get(hit.Namespace, hit.Key)
		if err != nil || rec == nil {
			continue
		}

		// Apply filters.
		if req.KeyPrefix != "" && !strings.HasPrefix(rec.Key, req.KeyPrefix) {
			continue
		}
		if req.CreatedBy != "" && rec.CreatedBy != req.CreatedBy {
			continue
		}
		if req.MinConfidence > 0 && rec.Confidence < req.MinConfidence {
			continue
		}
		if req.UpdatedAfter != nil && rec.UpdatedAt != nil && rec.UpdatedAt.AsTime().Before(req.UpdatedAfter.AsTime()) {
			continue
		}

		// Compute decay score.
		decayScore := float64(rec.Confidence)
		policy, _ := s.store.GetDecayPolicy(rec.Namespace)
		if policy != nil && policy.HalfLifeSeconds > 0 {
			updatedAt := now
			if rec.UpdatedAt != nil {
				updatedAt = rec.UpdatedAt.AsTime()
			}
			age := now.Sub(updatedAt).Seconds()
			recency := math.Pow(2, -age/policy.HalfLifeSeconds)
			accessBoost := 1.0
			if policy.AccessWeight > 0 {
				accessBoost = 1.0 + policy.AccessWeight*math.Log1p(float64(rec.AccessCount))
			}
			decayScore = float64(rec.Confidence) * recency * accessBoost
		}

		embeddingCurrent := rec.EmbeddedVersion == rec.Version
		combined := alpha*float64(hit.Similarity) + (1-alpha)*decayScore

		candidates = append(candidates, scored{
			result: &pb.SearchResult{
				Record:           rec,
				Score:            float32(combined),
				Similarity:       hit.Similarity,
				DecayScore:       float32(decayScore),
				EmbeddingCurrent: embeddingCurrent,
			},
			score: combined,
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].score > candidates[j].score
	})

	if len(candidates) > k {
		candidates = candidates[:k]
	}

	results := make([]*pb.SearchResult, len(candidates))
	for i, c := range candidates {
		results[i] = c.result
	}

	appliedIndex := uint64(0)
	if s.cluster != nil {
		appliedIndex = s.cluster.AppliedIndex()
	}

	// Track access for returned records.
	if s.tracker != nil {
		for _, r := range results {
			s.tracker.Track(r.Record.Namespace, r.Record.Key, now)
		}
	}

	return &pb.SearchResponse{
		Results:      results,
		AppliedIndex: appliedIndex,
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
