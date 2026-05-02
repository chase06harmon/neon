#!/usr/bin/env python3
"""Summarize a load-balancing benchmark run.

Reads:
  - one or more pgbench logfiles (`pgbench -l` per-transaction logs)
  - one sampled metrics JSONL (from sample_metrics.py)

Writes a CSV row and a human-readable text summary into the run directory.

Standard library only.
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import math
import os
import sys
from collections import defaultdict


def parse_pgbench_log(paths: list[str]) -> dict[str, float]:
    """Parse `pgbench -l` files. Each row: <client> <xact> <latency_us> <epoch> <us> ...

    Returns latency percentiles in milliseconds and total transaction count.
    """
    latencies_us: list[int] = []
    for path in paths:
        try:
            with open(path) as f:
                for line in f:
                    parts = line.split()
                    if len(parts) < 3:
                        continue
                    try:
                        latencies_us.append(int(parts[2]))
                    except ValueError:
                        continue
        except OSError as e:
            print(f"warn: cannot read {path}: {e}", file=sys.stderr)
    if not latencies_us:
        return {"tx_count": 0}
    latencies_us.sort()
    n = len(latencies_us)
    def pct(p: float) -> float:
        idx = max(0, min(n - 1, int(math.ceil(p / 100.0 * n)) - 1))
        return latencies_us[idx] / 1000.0
    return {
        "tx_count": float(n),
        "p50_ms": pct(50.0),
        "p95_ms": pct(95.0),
        "p99_ms": pct(99.0),
        "max_ms": latencies_us[-1] / 1000.0,
    }


def parse_pgbench_stdout(paths: list[str]) -> dict[str, float]:
    """Pull tps from pgbench stdout."""
    out = {"tps": 0.0}
    for path in paths:
        try:
            with open(path) as f:
                for line in f:
                    line = line.strip()
                    # `tps = NNNN.N (without initial connection time)`
                    if line.startswith("tps =") and "without" in line:
                        try:
                            out["tps"] += float(line.split()[2])
                        except (IndexError, ValueError):
                            pass
        except OSError:
            continue
    return out


def parse_metrics_jsonl(path: str) -> dict[str, float]:
    """Compute peak / mean of gauges, deltas of counters."""
    samples: list[dict[str, float | None]] = []
    try:
        with open(path) as f:
            for line in f:
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                samples.append(obj.get("samples", {}))
    except OSError as e:
        print(f"warn: cannot read metrics jsonl {path}: {e}", file=sys.stderr)
        return {}
    if not samples:
        return {}

    gauges = [
        "connections_idle",
        "connections_checked_out",
        "connections_connecting",
        "connections_overflow",
        "global_pressure",
    ]
    counters = [
        "checkout_immediate_hit",
        "checkout_miss_created",
        "checkout_queued_hit",
        "checkout_queued_created",
        "checkout_overflow",
        "checkout_timeout",
        "checkout_rejected",
        "checkout_failed",
        "overflow_taken",
        "overflow_refused",
    ]

    out: dict[str, float] = {}
    for k in gauges:
        vals = [s[k] for s in samples if isinstance(s.get(k), (int, float))]
        if vals:
            out[f"{k}_peak"] = float(max(vals))
            out[f"{k}_mean"] = float(sum(vals) / len(vals))

    # peak total physical conns = peak of (idle + checked_out + connecting + overflow)
    totals = []
    for s in samples:
        tot = 0.0
        ok = False
        for k in ("connections_idle", "connections_checked_out", "connections_connecting", "connections_overflow"):
            v = s.get(k)
            if isinstance(v, (int, float)):
                tot += float(v)
                ok = True
        if ok:
            totals.append(tot)
    if totals:
        out["physical_conns_peak"] = float(max(totals))
        out["physical_conns_mean"] = float(sum(totals) / len(totals))

    # Counter deltas: end - start over the whole run.
    for k in counters:
        first = next((s.get(k) for s in samples if isinstance(s.get(k), (int, float))), None)
        last = None
        for s in reversed(samples):
            v = s.get(k)
            if isinstance(v, (int, float)):
                last = v
                break
        if isinstance(first, (int, float)) and isinstance(last, (int, float)):
            out[f"{k}_delta"] = float(last) - float(first)

    return out


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--run-dir", required=True, help="directory containing logs and metrics")
    p.add_argument("--scenario", default="", help="benchmark scenario name (for the summary)")
    p.add_argument("--max-total-conns", type=int, default=0, help="configured global cap")
    p.add_argument("--overflow-limit", type=int, default=0, help="configured overflow budget")
    args = p.parse_args()

    pgbench_logs = sorted(glob.glob(os.path.join(args.run_dir, "pgbench-*.log")))
    pgbench_stdouts = sorted(glob.glob(os.path.join(args.run_dir, "pgbench-*.out")))
    metrics_jsonl = os.path.join(args.run_dir, "metrics.jsonl")

    pg_lat = parse_pgbench_log(pgbench_logs)
    pg_tps = parse_pgbench_stdout(pgbench_stdouts)
    metrics = parse_metrics_jsonl(metrics_jsonl) if os.path.exists(metrics_jsonl) else {}

    summary: dict[str, float | str] = {"scenario": args.scenario}
    summary.update({k: v for k, v in pg_lat.items()})
    summary.update({k: v for k, v in pg_tps.items()})
    summary.update({k: v for k, v in metrics.items()})

    # Cap-violation check: peak physical conns must be <= max_total + overflow.
    cap_violation = 0
    if args.max_total_conns > 0:
        ceiling = args.max_total_conns + args.overflow_limit
        peak = float(metrics.get("physical_conns_peak", 0.0))
        if peak > ceiling + 1e-6:
            cap_violation = 1
    summary["cap_violation"] = cap_violation

    # Write CSV
    csv_path = os.path.join(args.run_dir, "summary.csv")
    with open(csv_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(list(summary.keys()))
        w.writerow(list(summary.values()))

    # Write text summary
    txt_path = os.path.join(args.run_dir, "summary.txt")
    lines = [f"scenario: {args.scenario}"]
    if "tx_count" in pg_lat:
        lines.append(f"transactions: {int(pg_lat['tx_count'])}")
    if "tps" in pg_tps:
        lines.append(f"tps: {pg_tps['tps']:.1f}")
    if "p50_ms" in pg_lat:
        lines.append(
            f"latency ms: p50={pg_lat['p50_ms']:.1f} p95={pg_lat['p95_ms']:.1f} "
            f"p99={pg_lat['p99_ms']:.1f} max={pg_lat['max_ms']:.1f}"
        )
    if "physical_conns_peak" in metrics:
        lines.append(
            f"physical conns: peak={metrics['physical_conns_peak']:.0f} "
            f"mean={metrics['physical_conns_mean']:.1f}"
        )
    for k in ("checkout_timeout_delta", "checkout_rejected_delta", "checkout_failed_delta", "overflow_taken_delta"):
        if k in metrics:
            lines.append(f"{k}: {metrics[k]:.0f}")
    if args.max_total_conns > 0:
        lines.append(
            f"cap: max_total={args.max_total_conns} overflow={args.overflow_limit} "
            f"violation={cap_violation}"
        )

    with open(txt_path, "w") as f:
        f.write("\n".join(lines) + "\n")
    print("\n".join(lines))
    return 0


if __name__ == "__main__":
    sys.exit(main())
