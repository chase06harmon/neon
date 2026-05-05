#!/usr/bin/env python3
"""Summarize a single benchmark run into a small JSON blob.

Reads `pgbench-*.{out,err,<pid>}` and `metrics.jsonl` from a run dir.
Writes `summary.json` with the headline numbers used by `plot_compare.py`.
"""

from __future__ import annotations

import argparse
import glob
import json
import math
import os
import sys


def latencies_ms(run_dir: str) -> list[float]:
    out: list[float] = []
    # pgbench -l with --log-prefix=pgbench writes files named
    # `pgbench.<pid>[.<thread>]`. The dot-separated suffix means we
    # must glob `pgbench.*` rather than `pgbench-*`, and explicitly
    # exclude `.out`/`.err`/`.pid` etc.
    paths = [
        p for p in glob.glob(os.path.join(run_dir, "pgbench.*"))
        if not p.endswith((".out", ".err", ".log", ".cmd", ".pid"))
    ]
    for p in paths:
        try:
            with open(p) as f:
                for line in f:
                    parts = line.split()
                    if len(parts) >= 3:
                        try:
                            out.append(int(parts[2]) / 1000.0)
                        except ValueError:
                            pass
        except OSError:
            continue
    return out


def pct(vs: list[float], p: float) -> float:
    if not vs:
        return 0.0
    s = sorted(vs)
    idx = max(0, min(len(s) - 1, int(math.ceil(p / 100.0 * len(s))) - 1))
    return s[idx]


def pgbench_tps(run_dir: str) -> float:
    """Read `tps = N` from pgbench's stdout. pgbench prints two such
    lines for -C runs (`including reconnection times` and `excluding`)
    and one for normal runs (`without initial connection time`). We
    take the first one we encounter for each file — that's the headline
    number a human would quote."""
    total = 0.0
    for p in glob.glob(os.path.join(run_dir, "pgbench*.out")):
        try:
            with open(p) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("tps ="):
                        try:
                            total += float(line.split()[2])
                            break
                        except (IndexError, ValueError):
                            pass
        except OSError:
            continue
    return total


def pgbench_errors(run_dir: str) -> int:
    n = 0
    for p in glob.glob(os.path.join(run_dir, "pgbench-*.err")):
        try:
            with open(p) as f:
                for line in f:
                    if "aborted" in line:
                        n += 1
        except OSError:
            continue
    return n


def metrics_summary(run_dir: str) -> dict:
    path = os.path.join(run_dir, "metrics.jsonl")
    if not os.path.exists(path):
        return {}
    samples = []
    with open(path) as f:
        for line in f:
            try:
                samples.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    if not samples:
        return {}
    out: dict = {}

    # Per-compute peak/mean session counts.
    labels: set[str] = set()
    for s in samples:
        labels.update((s.get("compute_sessions") or {}).keys())
    sess_peak: dict[str, int] = {}
    sess_mean: dict[str, float] = {}
    for lab in labels:
        vals = [
            s["compute_sessions"][lab]
            for s in samples
            if isinstance((s.get("compute_sessions") or {}).get(lab), int)
        ]
        if vals:
            sess_peak[lab] = max(vals)
            sess_mean[lab] = sum(vals) / len(vals)
    out["sessions_peak"] = sess_peak
    out["sessions_mean"] = sess_mean
    out["sessions_total_peak"] = max(
        (sum(v for v in (s.get("compute_sessions") or {}).values()
             if isinstance(v, int))
         for s in samples),
        default=0,
    )

    # Proxy RSS peak (kB).
    rss_vals = [s["proxy_rss_kb"] for s in samples
                if isinstance(s.get("proxy_rss_kb"), int)]
    if rss_vals:
        out["proxy_rss_peak_kb"] = max(rss_vals)
        out["proxy_rss_mean_kb"] = int(sum(rss_vals) / len(rss_vals))

    # Container RSS — sum across containers per sample, take peak.
    container_totals = []
    for s in samples:
        cmap = s.get("container_rss") or {}
        valid = [v for v in cmap.values() if isinstance(v, int)]
        if valid:
            container_totals.append(sum(valid))
    if container_totals:
        out["compute_rss_peak_bytes"] = max(container_totals)
        out["compute_rss_mean_bytes"] = int(sum(container_totals) / len(container_totals))

    return out


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--run-dir", required=True)
    p.add_argument("--config", required=True, help="config label (vanilla/pool/pool_lb)")
    p.add_argument("--scenario", required=True, help="scenario label (steady/handshake)")
    args = p.parse_args()

    lats = latencies_ms(args.run_dir)
    tps = pgbench_tps(args.run_dir)
    errs = pgbench_errors(args.run_dir)
    metrics = metrics_summary(args.run_dir)

    summary = {
        "config": args.config,
        "scenario": args.scenario,
        "tx_count": len(lats),
        "tps": tps,
        "p50_ms": pct(lats, 50),
        "p95_ms": pct(lats, 95),
        "p99_ms": pct(lats, 99),
        "max_ms": max(lats) if lats else 0.0,
        "pgbench_errors": errs,
        **metrics,
    }
    out_path = os.path.join(args.run_dir, "summary.json")
    with open(out_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
