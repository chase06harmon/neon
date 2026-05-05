#!/usr/bin/env python3
"""Render comparison plots for the three-way LB benchmark.

Reads `summary.json` and `metrics.jsonl` from the latest run dir of each
(config, scenario) combination, renders comparison PNGs into the chosen
output directory.

Usage:
    plot_compare.py <runs_dir> <out_dir>
"""

from __future__ import annotations

import glob
import json
import os
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


CONFIGS = ["vanilla", "pool", "pool_lb"]
CONFIG_LABEL = {
    "vanilla": "vanilla Neon\n(no pool)",
    "pool": "Charles's pool\n(transaction)",
    "pool_lb": "pool + P2C LB\n(2 computes)",
}
CONFIG_COLOR = {
    "vanilla": "#A50F15",   # red — every-client-its-own-backend
    "pool": "#3182BD",      # blue — multiplexed onto few backends
    "pool_lb": "#31A354",   # green — multiplexed and distributed
}
SCENARIOS = ["steady", "handshake"]


# --- I/O ------------------------------------------------------------------

def latest(runs_dir: str, config: str, scenario: str) -> str | None:
    """Pick the most-recent run directory for (config, scenario)."""
    pat = os.path.join(runs_dir, f"{config}-{scenario}-*")
    matches = sorted(glob.glob(pat))
    return matches[-1] if matches else None


def load_summary(run_dir: str) -> dict:
    p = os.path.join(run_dir, "summary.json")
    if os.path.exists(p):
        with open(p) as f:
            return json.load(f)
    return {}


def load_metrics(run_dir: str) -> list[dict]:
    p = os.path.join(run_dir, "metrics.jsonl")
    if not os.path.exists(p):
        return []
    out = []
    with open(p) as f:
        for line in f:
            try:
                out.append(json.loads(line))
            except json.JSONDecodeError:
                pass
    return out


def latencies_ms(run_dir: str) -> list[float]:
    out: list[float] = []
    # See summarize.py for filename rationale.
    for p in glob.glob(os.path.join(run_dir, "pgbench.*")):
        if p.endswith((".out", ".err", ".log", ".cmd", ".pid")):
            continue
        try:
            with open(p) as f:
                for line in f:
                    parts = line.split()
                    if len(parts) >= 3:
                        try:
                            out.append(int(parts[2]) / 1000.0)
                        except ValueError:
                            continue
        except OSError:
            continue
    return out


# --- Plot helpers --------------------------------------------------------

def plot_latency_cdf(runs: dict[str, str], scenario: str, out_path: str) -> None:
    fig, ax = plt.subplots(figsize=(9, 4.5))
    drew = False
    for config in CONFIGS:
        run_dir = runs.get(config)
        if not run_dir:
            continue
        ms = latencies_ms(run_dir)
        if not ms:
            continue
        s = sorted(ms)
        n = len(s)
        ax.plot(s, [(i + 1) / n for i in range(n)],
                color=CONFIG_COLOR[config], linewidth=2.0,
                label=f"{CONFIG_LABEL[config]}    n={n}")
        drew = True
    if not drew:
        plt.close(fig)
        return
    ax.set_xlabel("transaction latency (ms)")
    ax.set_ylabel("CDF")
    ax.set_title(f"{scenario}: pgbench transaction latency CDF")
    ax.set_ylim(0, 1.02)
    ax.legend(loc="lower right", framealpha=0.9)
    ax.grid(alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


def plot_backend_conns(runs: dict[str, str], scenario: str, out_path: str) -> None:
    """Three-panel plot: backend session count over time per config.
    pool_lb's panel splits into per-compute lines so the LB distribution
    is visible."""
    fig, axes = plt.subplots(3, 1, figsize=(9, 9), sharex=True)
    for ax, config in zip(axes, CONFIGS):
        run_dir = runs.get(config)
        ax.set_title(CONFIG_LABEL[config].replace("\n", " "), fontsize=10)
        ax.set_ylabel("backends")
        ax.grid(alpha=0.3)
        if not run_dir:
            ax.text(0.5, 0.5, "no run", ha="center", va="center",
                    transform=ax.transAxes)
            continue
        rows = load_metrics(run_dir)
        if not rows:
            ax.text(0.5, 0.5, "no metrics", ha="center", va="center",
                    transform=ax.transAxes)
            continue
        t0 = rows[0]["ts"]
        ts = [r["ts"] - t0 for r in rows]
        for label, color in [("computeA", "#FF7F00"), ("computeB", "#984EA3")]:
            ys = [
                r.get("compute_sessions", {}).get(label) for r in rows
            ]
            # Replace None with 0 for plotting; we plot only series that
            # had at least one real reading.
            if any(isinstance(y, int) for y in ys):
                ys = [y if isinstance(y, int) else 0 for y in ys]
                ax.plot(ts, ys, color=color, linewidth=2.0, label=label)
        ax.legend(loc="upper right", fontsize=9)
        # Zero-line for visual cue.
        ax.axhline(0, color="black", linewidth=0.5, alpha=0.3)
    axes[-1].set_xlabel("time since bench start (s)")
    fig.suptitle(f"{scenario}: backend connections per compute")
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


def plot_summary_bars(runs: dict[str, dict[str, str]], out_path: str) -> None:
    """Multi-panel summary bars: TPS, p99 latency, peak backends, proxy
    RSS, compute RSS — for both scenarios stacked side by side."""
    fig, axes = plt.subplots(2, 3, figsize=(13, 7))

    metrics = [
        ("tps", "throughput (tps)", lambda s: s.get("tps", 0)),
        ("p99_ms", "p99 latency (ms)", lambda s: s.get("p99_ms", 0)),
        ("peak_backends", "peak backend sessions",
         lambda s: s.get("sessions_total_peak", 0)),
        ("proxy_rss", "proxy RSS (MB)",
         lambda s: (s.get("proxy_rss_peak_kb", 0) or 0) / 1024.0),
        ("compute_rss", "compute RSS (MB)",
         lambda s: (s.get("compute_rss_peak_bytes", 0) or 0) / (1024.0 ** 2)),
        ("errors", "pgbench errors",
         lambda s: s.get("pgbench_errors", 0)),
    ]

    for row, scenario in enumerate(SCENARIOS):
        for col, (key, title, getter) in enumerate(metrics[:3]):
            ax = axes[row, col]
            xs = list(range(len(CONFIGS)))
            vals = []
            for config in CONFIGS:
                rd = runs.get(scenario, {}).get(config)
                summary = load_summary(rd) if rd else {}
                vals.append(getter(summary))
            ax.bar(xs, vals, color=[CONFIG_COLOR[c] for c in CONFIGS])
            ax.set_xticks(xs)
            ax.set_xticklabels([CONFIG_LABEL[c] for c in CONFIGS], fontsize=8)
            ax.set_title(f"{scenario}: {title}", fontsize=10)
            ax.grid(axis="y", alpha=0.3)
            for x, v in zip(xs, vals):
                if isinstance(v, (int, float)) and v != 0:
                    ax.text(x, v, f"{v:.1f}" if v < 100 else f"{v:.0f}",
                            ha="center", va="bottom", fontsize=8)

    fig.suptitle("vanilla vs pool vs pool+LB — headline numbers",
                 fontsize=13, fontweight="bold")
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


def plot_resource_bars(runs: dict[str, dict[str, str]], out_path: str) -> None:
    """Resource panel: proxy RSS + compute RSS per config × scenario."""
    fig, axes = plt.subplots(1, 2, figsize=(13, 4.5))

    for ax, scenario in zip(axes, SCENARIOS):
        rss_proxy = []
        rss_compute = []
        for config in CONFIGS:
            rd = runs.get(scenario, {}).get(config)
            s = load_summary(rd) if rd else {}
            rss_proxy.append((s.get("proxy_rss_peak_kb", 0) or 0) / 1024.0)
            rss_compute.append((s.get("compute_rss_peak_bytes", 0) or 0)
                               / (1024.0 ** 2))

        x = list(range(len(CONFIGS)))
        width = 0.35
        ax.bar([i - width/2 for i in x], rss_proxy,
               width=width, color="#7191BA", label="proxy RSS (MB)")
        ax.bar([i + width/2 for i in x], rss_compute,
               width=width, color="#E6924D", label="compute RSS sum (MB)")
        ax.set_xticks(x)
        ax.set_xticklabels([CONFIG_LABEL[c] for c in CONFIGS], fontsize=8)
        ax.set_title(f"{scenario}: peak RSS", fontsize=10)
        ax.legend(loc="upper right", fontsize=9)
        ax.grid(axis="y", alpha=0.3)
        for i, (p, c) in enumerate(zip(rss_proxy, rss_compute)):
            if p > 0:
                ax.text(i - width/2, p, f"{p:.0f}", ha="center",
                        va="bottom", fontsize=8)
            if c > 0:
                ax.text(i + width/2, c, f"{c:.0f}", ha="center",
                        va="bottom", fontsize=8)

    fig.suptitle("Resource usage: proxy vs compute, by config",
                 fontsize=13, fontweight="bold")
    fig.tight_layout()
    fig.savefig(out_path, dpi=140)
    plt.close(fig)


# --- Main ----------------------------------------------------------------

def main() -> int:
    if len(sys.argv) < 3:
        print("usage: plot_compare.py <runs_dir> <out_dir>", file=sys.stderr)
        return 2
    runs_dir = sys.argv[1]
    out_dir = sys.argv[2]
    os.makedirs(out_dir, exist_ok=True)

    # Pick latest run per (config, scenario).
    chosen: dict[str, dict[str, str]] = {sc: {} for sc in SCENARIOS}
    for sc in SCENARIOS:
        for cfg in CONFIGS:
            rd = latest(runs_dir, cfg, sc)
            if rd:
                chosen[sc][cfg] = rd

    # Per-scenario plots.
    for sc in SCENARIOS:
        plot_latency_cdf(chosen[sc], sc,
                         os.path.join(out_dir, f"{sc}_latency_cdf.png"))
        plot_backend_conns(chosen[sc], sc,
                           os.path.join(out_dir, f"{sc}_backends.png"))

    # Summary across scenarios.
    plot_summary_bars(chosen, os.path.join(out_dir, "summary_bars.png"))
    plot_resource_bars(chosen, os.path.join(out_dir, "resource_bars.png"))

    print(f"Plots written to {out_dir}")
    for sc, by_cfg in chosen.items():
        for cfg, rd in by_cfg.items():
            print(f"  {sc}/{cfg}: {os.path.basename(rd)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
