#!/usr/bin/env python3
"""Sample resource and load metrics during a benchmark run.

Per second, scrapes:
  * backend session count on each configured Postgres compute
    (via `psql -c "select count(*) from pg_stat_activity where ..."`)
  * proxy RSS (via `ps -o rss= -p PID`)
  * compute container RSS (via `docker stats --no-stream --format`)

All values written one per line as JSON. Standard library + psql/docker
on PATH; no third-party Python dependencies.

Usage:
    sample_metrics.py --out PATH --proxy-pid PID
                      --compute "label=host:port" [--compute "..."]
                      [--container NAME]... [--duration SECS]
                      [--interval SECS]

Each compute target is a `label=host:port` pair. label is the human
name shown in plots; host:port is what psql connects to.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time

PG_USER = os.environ.get("PGUSER", "proxytest")
PG_DB = os.environ.get("PGDATABASE", "proxytest_db")
PG_PASSWORD = os.environ.get("PGPASSWORD", "testpw")


def parse_compute(s: str) -> tuple[str, str, int]:
    label, _, hostport = s.partition("=")
    host, _, port_s = hostport.partition(":")
    return label, host, int(port_s) if port_s else 5432


def session_count(host: str, port: int, user: str = PG_USER) -> int | None:
    """Run `select count(*) from pg_stat_activity where usename = user
    and pid != pg_backend_pid()` and return the integer, or None on error."""
    if shutil.which("psql") is None:
        return None
    cmd = [
        "psql",
        "-h", host, "-p", str(port),
        "-U", user, "-d", PG_DB,
        "-A", "-t", "-c",
        f"select count(*) from pg_stat_activity "
        f"where usename = '{user}' and pid != pg_backend_pid()",
    ]
    env = os.environ.copy()
    env["PGPASSWORD"] = PG_PASSWORD
    try:
        out = subprocess.run(
            cmd, capture_output=True, text=True, timeout=2.0, env=env,
        )
        if out.returncode != 0:
            return None
        return int(out.stdout.strip() or "0")
    except (subprocess.SubprocessError, ValueError):
        return None


def proxy_rss_kb(pid: int) -> int | None:
    if pid <= 0:
        return None
    try:
        out = subprocess.run(
            ["ps", "-o", "rss=", "-p", str(pid)],
            capture_output=True, text=True, timeout=2.0,
        )
        if out.returncode != 0:
            return None
        s = out.stdout.strip()
        return int(s) if s else None
    except (subprocess.SubprocessError, ValueError):
        return None


def container_rss_bytes(name: str) -> int | None:
    """`docker stats --no-stream --format '{{.MemUsage}}'` returns
    e.g. `150.4MiB / 7.654GiB`. Parse the left half into bytes."""
    if shutil.which("docker") is None:
        return None
    try:
        out = subprocess.run(
            ["docker", "stats", "--no-stream", "--format",
             "{{.MemUsage}}", name],
            capture_output=True, text=True, timeout=3.0,
        )
        if out.returncode != 0:
            return None
        left = out.stdout.strip().split("/")[0].strip()
        return parse_human_bytes(left)
    except subprocess.SubprocessError:
        return None


def parse_human_bytes(s: str) -> int | None:
    """Parse strings like `150.4MiB`, `7.6GB`, `512KiB`."""
    s = s.strip()
    if not s:
        return None
    units = {
        "B": 1, "KB": 1000, "KiB": 1024,
        "MB": 1000 ** 2, "MiB": 1024 ** 2,
        "GB": 1000 ** 3, "GiB": 1024 ** 3,
    }
    for unit, mul in sorted(units.items(), key=lambda kv: -len(kv[0])):
        if s.endswith(unit):
            try:
                return int(float(s[:-len(unit)]) * mul)
            except ValueError:
                return None
    try:
        return int(s)
    except ValueError:
        return None


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", required=True, help="output JSONL path")
    p.add_argument("--proxy-pid", type=int, default=0, help="proxy process id (0 = skip)")
    p.add_argument("--compute", action="append", default=[],
                   help="compute target as `label=host:port` (repeatable)")
    p.add_argument("--container", action="append", default=[],
                   help="docker container name to track RSS (repeatable)")
    p.add_argument("--duration", type=float, default=0.0,
                   help="run for at most N seconds (default 0 = until SIGTERM)")
    p.add_argument("--interval", type=float, default=1.0)
    args = p.parse_args()

    targets = [parse_compute(s) for s in args.compute]
    end_at = time.monotonic() + args.duration if args.duration > 0 else None

    with open(args.out, "w", buffering=1) as f:
        while True:
            ts = time.time()
            entry: dict = {"ts": ts}

            entry["compute_sessions"] = {
                label: session_count(host, port) for (label, host, port) in targets
            }
            entry["proxy_rss_kb"] = proxy_rss_kb(args.proxy_pid)
            entry["container_rss"] = {
                name: container_rss_bytes(name) for name in args.container
            }

            f.write(json.dumps(entry) + "\n")
            if end_at is not None and time.monotonic() >= end_at:
                break
            time.sleep(args.interval)
    return 0


if __name__ == "__main__":
    sys.exit(main())
