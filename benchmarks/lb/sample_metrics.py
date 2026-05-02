#!/usr/bin/env python3
"""Sample proxy Prometheus metrics once per second to a JSONL file.

Standard library only — no third-party dependencies. Tolerates missing
metrics; an absent metric is recorded as `null` rather than crashing.

Usage:
    sample_metrics.py --url URL --out PATH [--interval SECS] [--duration SECS]

Each output line is a JSON object:
    {"ts": <unix_secs>, "samples": {<metric_key>: <value or null>, ...}}

The metric keys are stable across runs so summarize_lb.py can compute
peaks and means over a sweep.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.error
import urllib.request

# Subset of the new TCP-pool metrics that benchmarks care about. Each entry
# is (output_key, metric_name, label_filter). label_filter is a dict of
# {label_name: label_value} or None for unlabelled metrics. We sum across
# matching label sets, which keeps cardinality low.
METRICS_OF_INTEREST: list[tuple[str, str, dict[str, str] | None]] = [
    ("connections_idle", "proxy_tcp_pool_connections", {"state": "idle"}),
    ("connections_checked_out", "proxy_tcp_pool_connections", {"state": "checked_out"}),
    ("connections_connecting", "proxy_tcp_pool_connections", {"state": "connecting"}),
    ("connections_overflow", "proxy_tcp_pool_connections", {"state": "overflow"}),
    ("global_pressure", "proxy_tcp_pool_global_pressure", None),
    # Counters — the summarizer takes deltas.
    ("checkout_immediate_hit", "proxy_tcp_pool_checkout_total", {"outcome": "immediate_hit"}),
    ("checkout_miss_created", "proxy_tcp_pool_checkout_total", {"outcome": "miss_created"}),
    ("checkout_queued_hit", "proxy_tcp_pool_checkout_total", {"outcome": "queued_hit"}),
    ("checkout_queued_created", "proxy_tcp_pool_checkout_total", {"outcome": "queued_created"}),
    ("checkout_overflow", "proxy_tcp_pool_checkout_total", {"outcome": "overflow"}),
    ("checkout_timeout", "proxy_tcp_pool_checkout_total", {"outcome": "timeout"}),
    ("checkout_rejected", "proxy_tcp_pool_checkout_total", {"outcome": "rejected"}),
    ("checkout_failed", "proxy_tcp_pool_checkout_total", {"outcome": "failed"}),
    ("overflow_taken", "proxy_tcp_pool_overflow_connections_total", {"outcome": "taken"}),
    ("overflow_refused", "proxy_tcp_pool_overflow_connections_total", {"outcome": "refused"}),
]

# Prometheus text exposition format: each metric line looks like
#   metric_name{lab1="v1",lab2="v2"} 12.3
# The label list is optional. Comments start with `#`.
_LINE_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)"
    r"(\{(?P<labels>[^}]*)\})?"
    r"\s+(?P<value>[^\s]+)\s*$"
)
_LABEL_RE = re.compile(r'(?P<key>[a-zA-Z_][a-zA-Z0-9_]*)="(?P<val>[^"]*)"')


def parse_prom(text: str) -> dict[tuple[str, frozenset[tuple[str, str]]], float]:
    """Parse Prometheus text exposition into {(name, labels): value}.

    Labels are returned as a frozenset of (k, v) tuples for set-like
    matching. Missing labels => empty frozenset.
    """
    out: dict[tuple[str, frozenset[tuple[str, str]]], float] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        m = _LINE_RE.match(line)
        if not m:
            continue
        name = m.group("name")
        labels_raw = m.group("labels") or ""
        labels = frozenset(
            (lm.group("key"), lm.group("val")) for lm in _LABEL_RE.finditer(labels_raw)
        )
        try:
            val = float(m.group("value"))
        except ValueError:
            continue
        out[(name, labels)] = val
    return out


def extract(
    parsed: dict[tuple[str, frozenset[tuple[str, str]]], float],
    name: str,
    label_filter: dict[str, str] | None,
) -> float | None:
    """Sum values for `name` across label sets that contain `label_filter`."""
    total = 0.0
    found = False
    needed = frozenset(label_filter.items()) if label_filter else None
    for (n, labels), v in parsed.items():
        if n != name:
            continue
        if needed is None or needed.issubset(labels):
            total += v
            found = True
    return total if found else None


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--url", required=True, help="proxy /metrics URL")
    p.add_argument("--out", required=True, help="output JSONL path")
    p.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="seconds between scrapes (default 1.0)",
    )
    p.add_argument(
        "--duration",
        type=float,
        default=0.0,
        help="run for at most N seconds (default 0 = until SIGTERM)",
    )
    p.add_argument(
        "--timeout-ms",
        type=int,
        default=2000,
        help="HTTP scrape timeout in ms (default 2000)",
    )
    args = p.parse_args()

    end_at = time.monotonic() + args.duration if args.duration > 0 else None
    with open(args.out, "w", buffering=1) as f:
        while True:
            ts = time.time()
            samples: dict[str, float | None] = {}
            try:
                with urllib.request.urlopen(args.url, timeout=args.timeout_ms / 1000.0) as resp:
                    body = resp.read().decode("utf-8", errors="replace")
                parsed = parse_prom(body)
                for key, name, lf in METRICS_OF_INTEREST:
                    samples[key] = extract(parsed, name, lf)
            except (urllib.error.URLError, OSError, ValueError) as e:
                samples["__error__"] = str(e)  # type: ignore[assignment]
            f.write(json.dumps({"ts": ts, "samples": samples}) + "\n")
            if end_at is not None and time.monotonic() >= end_at:
                break
            time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    sys.exit(main())
