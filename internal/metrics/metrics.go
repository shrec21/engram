package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Raft state metrics.
	RaftState = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "engram",
		Subsystem: "raft",
		Name:      "state",
		Help:      "Current Raft state (0=follower, 1=candidate, 2=leader)",
	})

	RaftTerm = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "engram",
		Subsystem: "raft",
		Name:      "term",
		Help:      "Current Raft term",
	})

	RaftAppliedIndex = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "engram",
		Subsystem: "raft",
		Name:      "applied_index",
		Help:      "Last applied Raft log index",
	})

	// Apply latency.
	ApplyDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "engram",
		Subsystem: "raft",
		Name:      "apply_duration_seconds",
		Help:      "Time to apply a command through Raft",
		Buckets:   prometheus.DefBuckets,
	}, []string{"type"}) // put, cas, delete, touch

	// gRPC request metrics.
	RequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "engram",
		Subsystem: "grpc",
		Name:      "request_duration_seconds",
		Help:      "gRPC request latency",
		Buckets:   prometheus.DefBuckets,
	}, []string{"method"})

	RequestTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "engram",
		Subsystem: "grpc",
		Name:      "requests_total",
		Help:      "Total gRPC requests by method and status",
	}, []string{"method", "code"})

	// Redirect and dedup metrics.
	RedirectTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "engram",
		Subsystem: "raft",
		Name:      "redirects_total",
		Help:      "Total NOT_LEADER redirects sent",
	})

	DedupHitsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "engram",
		Subsystem: "raft",
		Name:      "dedup_hits_total",
		Help:      "Total deduplicated (replayed) commands",
	})

	// Store metrics.
	StoreKeys = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "engram",
		Subsystem: "store",
		Name:      "keys",
		Help:      "Approximate number of keys in the store",
	})

	SweepDeletedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "engram",
		Subsystem: "decay",
		Name:      "sweep_deleted_total",
		Help:      "Total records deleted by TTL sweeper",
	})
)
