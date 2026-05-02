# Load-balancing benchmarks

Phase 1 of the pool-aware load-balancing work. This directory ships the
benchmark harness for two scenarios — `global_cap` and `checkout_deadline`
— that exercise the new global semaphore + deadline-aware checkout in
`proxy/src/tcp_pool.rs`.

The other scenarios listed in the original brief (`noisy_neighbor`,
`transaction_multiplex`, `proxy_affinity`, `compute_failure`,
`pgbouncer_memory`, `overflow`, `session_hygiene`,
`cold_start_wake_storm`) are scaffolded so the runner shape is stable,
but report **not implemented** and exit cleanly. They depend on
endpoint-fairness, transaction-mode metrics, multi-compute control plane
work, and PgBouncer comparison harnesses that this phase does not ship.

## Prerequisites

- `pgbench`, `psql` (libpq client tools)
- `python3` (≥ 3.8, standard library only — no third-party deps)
- `curl`
- A running proxy built with `--features testing`, exposing
  `proxy_tcp_pool_*` metrics on its `--http` listener (default
  `http://127.0.0.1:7001/metrics`)
- A running compute (Postgres) reachable via the proxy

## Configuring the proxy for these benchmarks

The pool flags relevant to Phase 1 (see `proxy/src/binary/proxy.rs`):

```
--tcp-pool-enabled true
--tcp-pool-mode session
--tcp-pool-max-total-conns 16
--tcp-pool-max-conns-per-key 8
--tcp-pool-overflow-limit 0
--tcp-pool-checkout-timeout 250ms
--tcp-pool-idle-timeout 5m
```

For the `global_cap` benchmark, set `--tcp-pool-max-total-conns` to a
small number (e.g. `16`) and aim the workload at far more endpoints
than that. For `checkout_deadline`, set `--tcp-pool-checkout-timeout`
to ≤250ms and run a workload that holds connections for ~1s.

To compare the new pool against the old (effectively-unbounded)
behavior, drop `--tcp-pool-enabled true` (legacy direct-connect path,
no global cap) — the runner exposes `MAX_TOTAL_CONNS=...` so you can
sweep cap sizes without rebuilding.

## Running

```
BENCH=global_cap ./benchmarks/lb/run_lb_bench.sh
BENCH=checkout_deadline ./benchmarks/lb/run_lb_bench.sh
```

Common knobs (with defaults):

```
DURATION=30                # seconds per scenario
TENANTS=20                 # endpoint count for fan-out scenarios
CLIENTS_PER_TENANT=4
NOISY_CLIENTS=100          # clients on a single endpoint (deadline scenario)
MAX_TOTAL_CONNS=16
MAX_CONNS_PER_KEY=8
POOL_WAIT_TIMEOUT_MS=250
OVERFLOW_LIMIT=0
PROXY_PORT=4432
PROXY_METRICS_URL=http://127.0.0.1:7001/metrics
PGHOST=127.0.0.1
PGUSER=proxytest
PGDATABASE=proxytest_db
PGPASSWORD=testpw
```

Each run writes to `benchmarks/lb/runs/<bench>-<timestamp>/`:

- `pgbench-*.log` — per-transaction latency log (`pgbench -l`)
- `pgbench-*.out`, `pgbench-*.err` — pgbench stdout/stderr
- `metrics.jsonl` — proxy metrics scraped once per second
- `summary.csv`, `summary.txt` — output of `summarize_lb.py`
- `header.txt` — env knobs as configured

Use `DRY_RUN=1` to validate plumbing without invoking pgbench:

```
DRY_RUN=1 BENCH=global_cap ./benchmarks/lb/run_lb_bench.sh
```

## What each scenario proves

### `global_cap`

Many endpoints × few clients each, all running `sleep_50ms.sql`. Total
demand exceeds `--tcp-pool-max-total-conns`. The summary's
`physical_conns_peak` (sum of `proxy_tcp_pool_connections{state}` over
`idle/checked_out/connecting/overflow`) must be ≤
`max_total + overflow`. `cap_violation` reads `0` on success. Compare
runs with the cap set vs `--tcp-pool-enabled false` (no cap) to see the
old behavior of unbounded backend conns.

### `checkout_deadline`

One endpoint with many clients running `sleep_1s.sql` against a small
`MAX_TOTAL_CONNS` and a short `POOL_WAIT_TIMEOUT_MS`. With the new pool,
the system stays bounded and `checkout_timeout_delta > 0` is visible in
the summary. With the old pool it would have queued indefinitely (the
365-day timeout in the prior code).

## Limitations

- Phase 1 implements global cap, deadline, release-reason tracking,
  controlled overflow, and low-cardinality metrics. It does **not**
  implement endpoint fairness, transaction-mode hold/idle metrics,
  compute health / circuit breaking, or multi-proxy affinity. Scenarios
  that depend on those exit with a `not implemented` notice.
- `compute_failure` and `pgbouncer_memory` need multi-compute mock /
  PgBouncer harness work that is out of scope here.
- Metric scraping is HTTP/text — large metric outputs may slow the
  sampler. The 1-second cadence is sufficient for the bench durations
  used in `global_cap` / `checkout_deadline`.
