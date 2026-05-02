#!/usr/bin/env bash
# Load-balancing benchmark runner. Phase 1 supports two scenarios:
#   BENCH=global_cap         backend connections never exceed max_total_conns
#   BENCH=checkout_deadline  saturated pool returns explicit timeouts
# Other scenarios are scaffolded but report "not implemented".
#
# Required tools: pgbench, psql, python3, curl
# Required env (or sensible defaults below):
#   PGHOST, PGUSER, PGDATABASE, PGPASSWORD, PROXY_PORT,
#   PROXY_METRICS_URL, MAX_TOTAL_CONNS, MAX_CONNS_PER_KEY,
#   POOL_WAIT_TIMEOUT_MS, OVERFLOW_LIMIT, DURATION
#
# DRY_RUN=1 validates argument plumbing without running pgbench.

set -euo pipefail

BENCH="${BENCH:-}"
if [[ -z "$BENCH" ]]; then
    echo "usage: BENCH=<scenario> $0" >&2
    echo "scenarios: global_cap, checkout_deadline (others not implemented in Phase 1)" >&2
    exit 2
fi

DRY_RUN="${DRY_RUN:-0}"

# --- env knobs ---------------------------------------------------------------
DURATION="${DURATION:-30}"
REPS="${REPS:-1}"
TENANTS="${TENANTS:-20}"
CLIENTS_PER_TENANT="${CLIENTS_PER_TENANT:-4}"
NOISY_CLIENTS="${NOISY_CLIENTS:-100}"
VICTIM_TENANTS="${VICTIM_TENANTS:-10}"
MAX_TOTAL_CONNS="${MAX_TOTAL_CONNS:-16}"
MAX_CONNS_PER_KEY="${MAX_CONNS_PER_KEY:-8}"
MAX_CONNS_PER_ENDPOINT="${MAX_CONNS_PER_ENDPOINT:-0}"  # phase 3
POOL_WAIT_TIMEOUT_MS="${POOL_WAIT_TIMEOUT_MS:-250}"
OVERFLOW_LIMIT="${OVERFLOW_LIMIT:-0}"
PROXY_PORT="${PROXY_PORT:-4432}"
PROXY_METRICS_URL="${PROXY_METRICS_URL:-http://127.0.0.1:7001/metrics}"
PGHOST="${PGHOST:-127.0.0.1}"
PGUSER="${PGUSER:-proxytest}"
PGDATABASE="${PGDATABASE:-proxytest_db}"
PGPASSWORD="${PGPASSWORD:-testpw}"

BENCH_DIR="$(cd "$(dirname "$0")" && pwd)"
RUN_TS="$(date +%Y%m%d-%H%M%S)"
RUN_DIR="$BENCH_DIR/runs/${BENCH}-${RUN_TS}"

# --- tool checks -------------------------------------------------------------
require_cmd() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "error: required tool not on PATH: $1" >&2
        exit 1
    fi
}

require_cmd python3
if [[ "$DRY_RUN" != "1" ]]; then
    require_cmd pgbench
    require_cmd psql
    require_cmd curl
fi

mkdir -p "$RUN_DIR"
echo "BENCH=$BENCH RUN_DIR=$RUN_DIR DRY_RUN=$DRY_RUN" | tee "$RUN_DIR/header.txt"
echo "MAX_TOTAL_CONNS=$MAX_TOTAL_CONNS MAX_CONNS_PER_KEY=$MAX_CONNS_PER_KEY OVERFLOW_LIMIT=$OVERFLOW_LIMIT POOL_WAIT_TIMEOUT_MS=$POOL_WAIT_TIMEOUT_MS" | tee -a "$RUN_DIR/header.txt"

# --- helpers -----------------------------------------------------------------
endpoint_url() {
    # Encode endpoint id into the libpq options field, mirroring run_bench.sh.
    local ep="$1"
    echo "postgresql://${PGUSER}:${PGPASSWORD}@${PGHOST}:${PROXY_PORT}/${PGDATABASE}?sslmode=disable&options=endpoint%3D${ep}"
}

start_metrics_sampler() {
    local out="$RUN_DIR/metrics.jsonl"
    if [[ "$DRY_RUN" == "1" ]]; then
        echo "DRY_RUN: would sample $PROXY_METRICS_URL -> $out"
        return
    fi
    python3 "$BENCH_DIR/sample_metrics.py" \
        --url "$PROXY_METRICS_URL" \
        --out "$out" \
        --interval 1.0 \
        --duration "$DURATION" \
        > "$RUN_DIR/sampler.log" 2>&1 &
    echo $! > "$RUN_DIR/sampler.pid"
}

stop_metrics_sampler() {
    if [[ -f "$RUN_DIR/sampler.pid" ]]; then
        local pid
        pid="$(cat "$RUN_DIR/sampler.pid")"
        kill "$pid" 2>/dev/null || true
        wait "$pid" 2>/dev/null || true
    fi
}

run_pgbench() {
    local tag="$1"
    local concurrency="$2"
    local url="$3"
    local script="$4"
    local logf="$RUN_DIR/pgbench-${tag}.log"
    local outf="$RUN_DIR/pgbench-${tag}.out"
    local errf="$RUN_DIR/pgbench-${tag}.err"

    if [[ "$DRY_RUN" == "1" ]]; then
        echo "DRY_RUN: pgbench tag=$tag c=$concurrency script=$script url=${url//$PGPASSWORD/***}" | tee -a "$RUN_DIR/dryrun.txt"
        return
    fi
    pgbench \
        -n \
        -f "$script" \
        -c "$concurrency" \
        -j "$concurrency" \
        -T "$DURATION" \
        -l --log-prefix="$RUN_DIR/pgbench-${tag}" \
        "$url" \
        >"$outf" 2>"$errf" &
}

wait_pgbench() {
    if [[ "$DRY_RUN" == "1" ]]; then return; fi
    wait
}

summarize() {
    if ! python3 "$BENCH_DIR/summarize_lb.py" \
        --run-dir "$RUN_DIR" \
        --scenario "$BENCH" \
        --max-total-conns "$MAX_TOTAL_CONNS" \
        --overflow-limit "$OVERFLOW_LIMIT"; then
        echo "summarize_lb.py failed (non-fatal)" >&2
    fi
}

# --- scenarios ---------------------------------------------------------------
scenario_global_cap() {
    # TENANTS endpoints, each with CLIENTS_PER_TENANT clients running
    # sleep_50ms.sql. The total demand is TENANTS*CLIENTS_PER_TENANT,
    # which should exceed MAX_TOTAL_CONNS — the metric peak must still
    # respect the cap.
    local script="$BENCH_DIR/workloads/sleep_50ms.sql"
    start_metrics_sampler
    for ((t=0; t<TENANTS; t++)); do
        local url
        url="$(endpoint_url "ep-test-$t")"
        run_pgbench "ep$t" "$CLIENTS_PER_TENANT" "$url" "$script"
    done
    wait_pgbench
    stop_metrics_sampler
    summarize
}

scenario_checkout_deadline() {
    # One endpoint, many clients, sleep_1s.sql, small MAX_TOTAL_CONNS,
    # short POOL_WAIT_TIMEOUT_MS. The summary should show non-zero
    # checkout_timeout_delta.
    local script="$BENCH_DIR/workloads/sleep_1s.sql"
    local url
    url="$(endpoint_url "ep-test-deadline")"
    start_metrics_sampler
    run_pgbench "single" "$NOISY_CLIENTS" "$url" "$script"
    wait_pgbench
    stop_metrics_sampler
    summarize
}

scenario_not_implemented() {
    echo "scenario '$BENCH' is not implemented in Phase 1." | tee "$RUN_DIR/skipped.txt"
    echo "Phase 1 ships: global_cap, checkout_deadline." | tee -a "$RUN_DIR/skipped.txt"
    echo "Phase 2+ will add: noisy_neighbor, transaction_multiplex, proxy_affinity," | tee -a "$RUN_DIR/skipped.txt"
    echo "  compute_failure, pgbouncer_memory, overflow, session_hygiene, cold_start_wake_storm." | tee -a "$RUN_DIR/skipped.txt"
}

case "$BENCH" in
    global_cap)         scenario_global_cap ;;
    checkout_deadline)  scenario_checkout_deadline ;;
    noisy_neighbor|transaction_multiplex|proxy_affinity|compute_failure|pgbouncer_memory|overflow|session_hygiene|cold_start_wake_storm)
                        scenario_not_implemented ;;
    *)                  echo "unknown BENCH=$BENCH" >&2; exit 2 ;;
esac

echo "done. results in $RUN_DIR"
