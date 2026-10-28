# Engram Benchmarks

## Methodology

Benchmarks use the open-loop engine in `internal/bench/bench.go` with a token
bucket arrival rate to avoid coordinated omission. Latencies are recorded in
HDR histograms for accurate tail measurements.

### Running

```bash
# Start a 3-node cluster (docker-compose or manual)
# then:
engramctl bench \
  --records 10000 \
  --duration 30s \
  --workers 20 \
  --rate 5000 \
  --read-pct 0.8 \
  --value-size 256

# Generate charts:
python3 tools/plot_bench.py results.json docs/
```

### Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--records` | 1000 | Records to pre-populate |
| `--duration` | 10s | Benchmark duration |
| `--workers` | 10 | Concurrent goroutines |
| `--rate` | 0 (unlimited) | Target ops/sec |
| `--read-pct` | 0.5 | Fraction of reads (0-1) |
| `--value-size` | 256 | Value size in bytes |
| `--namespace` | bench | Namespace for benchmark data |

## Hardware

Benchmarks should be run on your target hardware. Reference numbers below are
from local development and are not representative of production performance.

## Metrics

The benchmark outputs JSON with:
- Total operations, puts, gets, errors
- Operations per second
- Put/Get latency percentiles (p50, p90, p95, p99, p99.9) in microseconds

## Notes

- The open-loop design means arrival rate is independent of response time,
  preventing coordinated omission bias
- HDR histograms maintain accuracy across the full latency range
- Pre-population ensures the key space exists before the benchmark begins
- Smart client handles NOT_LEADER redirects transparently
