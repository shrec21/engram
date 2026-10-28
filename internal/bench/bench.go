package bench

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
	pb "github.com/shrec21/engram/proto/engrampb"
)

// MemoryClient is the subset of the gRPC client needed for benchmarks.
type MemoryClient interface {
	Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error)
	Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error)
}

// Config defines a benchmark run.
type Config struct {
	Namespace    string        `json:"namespace"`
	NumRecords   int           `json:"num_records"`   // records to pre-populate
	Duration     time.Duration `json:"duration"`       // how long to run
	RatePerSec   int           `json:"rate_per_sec"`   // target arrival rate (0=unlimited)
	Workers      int           `json:"workers"`        // concurrent workers
	ReadPct      float64       `json:"read_pct"`       // fraction of reads (0-1)
	ValueSize    int           `json:"value_size"`     // bytes per value
}

// Result holds benchmark output.
type Result struct {
	Config       Config         `json:"config"`
	Duration     time.Duration  `json:"actual_duration"`
	TotalOps     int64          `json:"total_ops"`
	PutOps       int64          `json:"put_ops"`
	GetOps       int64          `json:"get_ops"`
	Errors       int64          `json:"errors"`
	OpsPerSec    float64        `json:"ops_per_sec"`
	PutLatency   LatencyStats   `json:"put_latency_us"`
	GetLatency   LatencyStats   `json:"get_latency_us"`
}

// LatencyStats summarizes an HDR histogram.
type LatencyStats struct {
	Min    int64   `json:"min"`
	Max    int64   `json:"max"`
	Mean   float64 `json:"mean"`
	P50    int64   `json:"p50"`
	P90    int64   `json:"p90"`
	P95    int64   `json:"p95"`
	P99    int64   `json:"p99"`
	P999   int64   `json:"p999"`
}

func histStats(h *hdrhistogram.Histogram) LatencyStats {
	return LatencyStats{
		Min:  h.Min(),
		Max:  h.Max(),
		Mean: h.Mean(),
		P50:  h.ValueAtPercentile(50),
		P90:  h.ValueAtPercentile(90),
		P95:  h.ValueAtPercentile(95),
		P99:  h.ValueAtPercentile(99),
		P999: h.ValueAtPercentile(99.9),
	}
}

// Run executes the benchmark.
func Run(ctx context.Context, client MemoryClient, cfg Config) (*Result, error) {
	if cfg.Workers <= 0 {
		cfg.Workers = 10
	}
	if cfg.ValueSize <= 0 {
		cfg.ValueSize = 256
	}
	if cfg.ReadPct < 0 || cfg.ReadPct > 1 {
		cfg.ReadPct = 0.5
	}
	if cfg.Duration <= 0 {
		cfg.Duration = 10 * time.Second
	}

	// Pre-populate.
	if cfg.NumRecords > 0 {
		if err := prepopulate(ctx, client, cfg); err != nil {
			return nil, fmt.Errorf("prepopulate: %w", err)
		}
	}

	putHist := hdrhistogram.New(1, 60_000_000, 3) // 1us to 60s
	getHist := hdrhistogram.New(1, 60_000_000, 3)

	var totalOps, putOps, getOps, errors atomic.Int64

	// Token bucket for rate limiting.
	var ticker *time.Ticker
	var tokens chan struct{}
	if cfg.RatePerSec > 0 {
		interval := time.Second / time.Duration(cfg.RatePerSec)
		ticker = time.NewTicker(interval)
		tokens = make(chan struct{}, cfg.RatePerSec*2)
		go func() {
			for range ticker.C {
				select {
				case tokens <- struct{}{}:
				default:
				}
			}
		}()
	}

	deadline, cancel := context.WithTimeout(ctx, cfg.Duration)
	defer cancel()
	if ticker != nil {
		defer ticker.Stop()
	}

	start := time.Now()

	var wg sync.WaitGroup
	var putMu, getMu sync.Mutex

	for w := 0; w < cfg.Workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			val := make([]byte, cfg.ValueSize)
			rng.Read(val)

			for {
				select {
				case <-deadline.Done():
					return
				default:
				}

				// Rate limit.
				if tokens != nil {
					select {
					case <-tokens:
					case <-deadline.Done():
						return
					}
				}

				isRead := rng.Float64() < cfg.ReadPct
				key := fmt.Sprintf("bench-%d", rng.Intn(max(cfg.NumRecords, 1000)))

				opStart := time.Now()
				var opErr error

				if isRead {
					_, opErr = client.Get(deadline, &pb.GetRequest{
						Namespace: cfg.Namespace,
						Key:       key,
					})
					latUs := time.Since(opStart).Microseconds()
					getMu.Lock()
					getHist.RecordValue(latUs)
					getMu.Unlock()
					getOps.Add(1)
				} else {
					_, opErr = client.Put(deadline, &pb.PutRequest{
						Namespace: cfg.Namespace,
						Key:       key,
						Value:     val,
					})
					latUs := time.Since(opStart).Microseconds()
					putMu.Lock()
					putHist.RecordValue(latUs)
					putMu.Unlock()
					putOps.Add(1)
				}

				totalOps.Add(1)
				if opErr != nil {
					errors.Add(1)
				}
			}
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)

	result := &Result{
		Config:    cfg,
		Duration:  elapsed,
		TotalOps:  totalOps.Load(),
		PutOps:    putOps.Load(),
		GetOps:    getOps.Load(),
		Errors:    errors.Load(),
		OpsPerSec: float64(totalOps.Load()) / elapsed.Seconds(),
	}

	if putHist.TotalCount() > 0 {
		result.PutLatency = histStats(putHist)
	}
	if getHist.TotalCount() > 0 {
		result.GetLatency = histStats(getHist)
	}

	return result, nil
}

func prepopulate(ctx context.Context, client MemoryClient, cfg Config) error {
	val := make([]byte, cfg.ValueSize)
	rand.Read(val)

	for i := 0; i < cfg.NumRecords; i++ {
		_, err := client.Put(ctx, &pb.PutRequest{
			Namespace: cfg.Namespace,
			Key:       fmt.Sprintf("bench-%d", i),
			Value:     val,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// ToJSON serializes the result to JSON.
func (r *Result) ToJSON() ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}
