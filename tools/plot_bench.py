#!/usr/bin/env python3
"""Generate benchmark charts from engramctl bench JSON output.

Usage:
    engramctl bench --records 10000 --duration 30s > results.json
    python3 tools/plot_bench.py results.json
"""

import json
import sys
import os

try:
    import matplotlib.pyplot as plt
    import matplotlib
    matplotlib.use('Agg')
except ImportError:
    print("matplotlib required: pip install matplotlib", file=sys.stderr)
    sys.exit(1)


def plot_latency_bars(results, output_dir):
    """Bar chart of latency percentiles for puts and gets."""
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    for ax, op, key in [(axes[0], 'Put', 'put_latency_us'),
                         (axes[1], 'Get', 'get_latency_us')]:
        stats = results.get(key, {})
        if not stats:
            ax.text(0.5, 0.5, 'No data', ha='center', va='center')
            ax.set_title(f'{op} Latency')
            continue

        labels = ['p50', 'p90', 'p95', 'p99', 'p999']
        values = [stats.get(l, 0) for l in labels]

        bars = ax.bar(labels, values, color=['#2196F3', '#4CAF50', '#FF9800', '#F44336', '#9C27B0'])
        ax.set_ylabel('Latency (us)')
        ax.set_title(f'{op} Latency Percentiles')

        for bar, val in zip(bars, values):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                    f'{val:,}', ha='center', va='bottom', fontsize=9)

    fig.suptitle(f"Engram Benchmark: {results['total_ops']:,} ops, "
                 f"{results['ops_per_sec']:.0f} ops/sec, "
                 f"{results['errors']} errors",
                 fontsize=12)
    plt.tight_layout()
    path = os.path.join(output_dir, 'latency_percentiles.png')
    plt.savefig(path, dpi=150)
    print(f"Saved: {path}")
    plt.close()


def plot_summary(results, output_dir):
    """Summary text chart."""
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.axis('off')

    cfg = results.get('config', {})
    text = (
        f"Benchmark Summary\n"
        f"{'='*40}\n"
        f"Records:     {cfg.get('num_records', 'N/A'):>10,}\n"
        f"Duration:    {results.get('actual_duration', 'N/A'):>10}\n"
        f"Workers:     {cfg.get('workers', 'N/A'):>10}\n"
        f"Rate limit:  {cfg.get('rate_per_sec', 0):>10} ops/sec\n"
        f"Read pct:    {cfg.get('read_pct', 0)*100:>10.0f}%\n"
        f"{'='*40}\n"
        f"Total ops:   {results.get('total_ops', 0):>10,}\n"
        f"Ops/sec:     {results.get('ops_per_sec', 0):>10,.0f}\n"
        f"Put ops:     {results.get('put_ops', 0):>10,}\n"
        f"Get ops:     {results.get('get_ops', 0):>10,}\n"
        f"Errors:      {results.get('errors', 0):>10,}\n"
    )
    ax.text(0.1, 0.9, text, transform=ax.transAxes, fontsize=11,
            verticalalignment='top', fontfamily='monospace')

    path = os.path.join(output_dir, 'summary.png')
    plt.savefig(path, dpi=150, bbox_inches='tight')
    print(f"Saved: {path}")
    plt.close()


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <results.json> [output_dir]", file=sys.stderr)
        sys.exit(1)

    with open(sys.argv[1]) as f:
        results = json.load(f)

    output_dir = sys.argv[2] if len(sys.argv) > 2 else '.'
    os.makedirs(output_dir, exist_ok=True)

    plot_latency_bars(results, output_dir)
    plot_summary(results, output_dir)


if __name__ == '__main__':
    main()
