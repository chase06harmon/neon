#!/usr/bin/env bash
# Run the three-way comparison benchmark:
#   A: vanilla   — proxy with --tcp-pool-enabled=false (no transaction pooling)
#   B: pool      — proxy with bb8 transaction pool, single compute target
#   C: pool_lb   — proxy with bb8 transaction pool + P2C LB across 2 computes
#
# Two scenarios per config:
#   steady     — c=20 sleep_50ms.sql, persistent connections (-l latency log)
#   handshake  — c=20 short_select.sql with -C (reconnect per tx)
#
# Output: benchmarks/lb_v2/runs/<config>-<scenario>-<ts>/{summary.json,
# pgbench-*, metrics.jsonl, header.txt, proxy.log}
#
# Required env (with defaults):
#   PROXY_BIN        path to proxy binary (default: target/debug/proxy)
#   PG_USER, PG_DB, PG_PASSWORD
#   COMPUTE_A_PORT (5433), COMPUTE_B_PORT (5434)
#   COMPUTE_A_CONTAINER (proxytest-compute), COMPUTE_B_CONTAINER (proxytest-compute-b)
#   STEADY_DURATION (30), HANDSHAKE_DURATION (15)
#   STEADY_CLIENTS (20), HANDSHAKE_CLIENTS (20)
#   POOL_MAX_PER_KEY (8), POOL_MAX_TOTAL (16)
#
# DRY_RUN=1 prints the plan without spawning anything.

set -uo pipefail

BENCH_DIR="$(cd "$(dirname "$0")" && pwd)"
RUNS_DIR="$BENCH_DIR/runs"
mkdir -p "$RUNS_DIR"

PROXY_BIN="${PROXY_BIN:-/Users/mel/neon/target/debug/proxy}"
PG_USER="${PG_USER:-proxytest}"
PG_DB="${PG_DB:-proxytest_db}"
PG_PASSWORD="${PG_PASSWORD:-testpw}"
COMPUTE_A_PORT="${COMPUTE_A_PORT:-5433}"
COMPUTE_B_PORT="${COMPUTE_B_PORT:-5434}"
COMPUTE_A_CONTAINER="${COMPUTE_A_CONTAINER:-proxytest-compute}"
COMPUTE_B_CONTAINER="${COMPUTE_B_CONTAINER:-proxytest-compute-b}"
STEADY_DURATION="${STEADY_DURATION:-30}"
HANDSHAKE_DURATION="${HANDSHAKE_DURATION:-15}"
STEADY_CLIENTS="${STEADY_CLIENTS:-20}"
HANDSHAKE_CLIENTS="${HANDSHAKE_CLIENTS:-20}"
POOL_MAX_PER_KEY="${POOL_MAX_PER_KEY:-8}"
POOL_MAX_TOTAL="${POOL_MAX_TOTAL:-16}"
DRY_RUN="${DRY_RUN:-0}"

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || { echo "missing: $1" >&2; exit 1; }
}
[[ "$DRY_RUN" == "1" ]] || {
    require_cmd pgbench
    require_cmd psql
    require_cmd python3
    require_cmd docker
    [[ -x "$PROXY_BIN" ]] || { echo "proxy binary not executable: $PROXY_BIN" >&2; exit 1; }
}

start_proxy() {
    # Args: $1=config (vanilla/pool/pool_lb)  $2=run_dir
    local config="$1" run_dir="$2"
    pkill -9 -f "target/debug/proxy" 2>/dev/null || true
    while lsof -iTCP:4432 -sTCP:LISTEN >/dev/null 2>&1; do sleep 0.2; done

    local map_arg=""
    local pool_arg
    local lb_arg=""

    case "$config" in
        vanilla)
            pool_arg="--tcp-pool-enabled=false"
            map_arg="--compute-endpoint=postgresql://localhost:${COMPUTE_A_PORT}/${PG_DB}?sslmode=disable"
            ;;
        pool)
            pool_arg="--tcp-pool-enabled=true --tcp-pool-mode=transaction --tcp-pool-max-conns-per-key=${POOL_MAX_PER_KEY} --tcp-pool-max-total-conns=${POOL_MAX_TOTAL} --tcp-pool-fallback-direct-connect=false"
            map_arg="--compute-endpoint=postgresql://localhost:${COMPUTE_A_PORT}/${PG_DB}?sslmode=disable"
            ;;
        pool_lb)
            pool_arg="--tcp-pool-enabled=true --tcp-pool-mode=transaction --tcp-pool-max-conns-per-key=${POOL_MAX_PER_KEY} --tcp-pool-max-total-conns=${POOL_MAX_TOTAL} --tcp-pool-fallback-direct-connect=false"
            map_arg="--compute-endpoint-map=ep-test-multi=postgresql://localhost:${COMPUTE_A_PORT}/${PG_DB}?sslmode=disable|postgresql://localhost:${COMPUTE_B_PORT}/${PG_DB}?sslmode=disable"
            lb_arg="--compute-lb-policy=p2c"
            ;;
        *)
            echo "unknown config: $config" >&2
            return 1
            ;;
    esac

    if [[ "$DRY_RUN" == "1" ]]; then
        echo "DRY_RUN: would start proxy $config" \
             "$pool_arg $map_arg $lb_arg"
        echo "0" > "$run_dir/proxy.pid"
        return 0
    fi

    RUST_LOG="proxy=warn" PGPASSWORD="$PG_PASSWORD" "$PROXY_BIN" \
        --auth-backend=postgres \
        --auth-endpoint="postgresql://${PG_USER}@localhost:5432/${PG_DB}" \
        $map_arg \
        --proxy=127.0.0.1:4432 --mgmt=127.0.0.1:7000 --http=127.0.0.1:7001 --wss=127.0.0.1:7002 \
        $pool_arg \
        $lb_arg \
        --endpoint-rps-limit "100000@1s" --endpoint-rps-limit "100000@60s" --endpoint-rps-limit "100000@600s" \
        --wake-compute-limit "100000@1s" --wake-compute-limit "100000@60s" --wake-compute-limit "100000@600s" \
        > "$run_dir/proxy.log" 2>&1 &
    local pid=$!
    echo "$pid" > "$run_dir/proxy.pid"
    until lsof -iTCP:4432 -sTCP:LISTEN >/dev/null 2>&1; do sleep 0.2; done
    echo "started proxy $config pid=$pid"
}

stop_proxy() {
    local run_dir="$1"
    if [[ -f "$run_dir/proxy.pid" ]]; then
        local pid; pid=$(cat "$run_dir/proxy.pid")
        kill "$pid" 2>/dev/null || true
        sleep 0.5
        kill -9 "$pid" 2>/dev/null || true
    fi
    pkill -9 -f "target/debug/proxy" 2>/dev/null || true
    while lsof -iTCP:4432 -sTCP:LISTEN >/dev/null 2>&1; do sleep 0.2; done
}

start_sampler() {
    local run_dir="$1" duration="$2" pid="$3"
    if [[ "$DRY_RUN" == "1" ]]; then
        echo "DRY_RUN: would start sampler in $run_dir for ${duration}s"
        return
    fi
    PGUSER="$PG_USER" PGDATABASE="$PG_DB" PGPASSWORD="$PG_PASSWORD" \
        python3 "$BENCH_DIR/sample_metrics.py" \
        --out "$run_dir/metrics.jsonl" \
        --proxy-pid "$pid" \
        --compute "computeA=127.0.0.1:${COMPUTE_A_PORT}" \
        --compute "computeB=127.0.0.1:${COMPUTE_B_PORT}" \
        --container "${COMPUTE_A_CONTAINER}" \
        --container "${COMPUTE_B_CONTAINER}" \
        --duration "$duration" \
        > "$run_dir/sampler.log" 2>&1 &
    echo $! > "$run_dir/sampler.pid"
}

run_pgbench_steady() {
    # Synchronous: blocks the caller until pgbench exits.
    local run_dir="$1"
    local script="$BENCH_DIR/workloads/sleep_50ms.sql"
    local url="postgresql://${PG_USER}:${PG_PASSWORD}@127.0.0.1:4432/${PG_DB}?sslmode=disable&options=endpoint%3Dep-test-multi"
    if [[ "$DRY_RUN" == "1" ]]; then
        echo "DRY_RUN: pgbench steady c=$STEADY_CLIENTS T=$STEADY_DURATION"
        return
    fi
    pgbench -n -f "$script" \
        -c "$STEADY_CLIENTS" -j "$STEADY_CLIENTS" \
        -T "$STEADY_DURATION" \
        -l --log-prefix="$run_dir/pgbench" \
        "$url" \
        >"$run_dir/pgbench.out" 2>"$run_dir/pgbench.err"
}

run_pgbench_handshake() {
    local run_dir="$1"
    local script="$BENCH_DIR/workloads/short_select.sql"
    local url="postgresql://${PG_USER}:${PG_PASSWORD}@127.0.0.1:4432/${PG_DB}?sslmode=disable&options=endpoint%3Dep-test-multi"
    if [[ "$DRY_RUN" == "1" ]]; then
        echo "DRY_RUN: pgbench handshake -C c=$HANDSHAKE_CLIENTS T=$HANDSHAKE_DURATION"
        return
    fi
    pgbench -n -C -f "$script" \
        -c "$HANDSHAKE_CLIENTS" -j "$HANDSHAKE_CLIENTS" \
        -T "$HANDSHAKE_DURATION" \
        -l --log-prefix="$run_dir/pgbench" \
        "$url" \
        >"$run_dir/pgbench.out" 2>"$run_dir/pgbench.err"
}

wait_sampler() {
    # Wait specifically for the sampler (whose pid we wrote earlier).
    # `wait` on its own would also block on the proxy `&`-child, which
    # we don't want — the proxy is killed explicitly via stop_proxy.
    local run_dir="$1"
    if [[ -f "$run_dir/sampler.pid" ]]; then
        local pid; pid=$(cat "$run_dir/sampler.pid")
        # `wait <pid>` only works for child processes of *this* shell;
        # the sampler is one such child, so this is safe.
        wait "$pid" 2>/dev/null || true
    fi
}

run_one() {
    # Args: $1=config $2=scenario
    local config="$1" scenario="$2"
    local ts; ts=$(date +%Y%m%d-%H%M%S)
    local run_dir="$RUNS_DIR/${config}-${scenario}-${ts}"
    mkdir -p "$run_dir"
    {
        echo "config=$config scenario=$scenario ts=$ts"
        echo "STEADY_DURATION=$STEADY_DURATION HANDSHAKE_DURATION=$HANDSHAKE_DURATION"
        echo "STEADY_CLIENTS=$STEADY_CLIENTS HANDSHAKE_CLIENTS=$HANDSHAKE_CLIENTS"
        echo "POOL_MAX_PER_KEY=$POOL_MAX_PER_KEY POOL_MAX_TOTAL=$POOL_MAX_TOTAL"
    } > "$run_dir/header.txt"

    start_proxy "$config" "$run_dir"
    local pid; pid=$(cat "$run_dir/proxy.pid")

    case "$scenario" in
        steady)    duration="$STEADY_DURATION" ;;
        handshake) duration="$HANDSHAKE_DURATION" ;;
        *) echo "unknown scenario: $scenario" >&2; return 1 ;;
    esac

    start_sampler "$run_dir" "$duration" "$pid"
    sleep 1

    case "$scenario" in
        steady)    run_pgbench_steady "$run_dir" ;;
        handshake) run_pgbench_handshake "$run_dir" ;;
    esac

    if [[ "$DRY_RUN" != "1" ]]; then
        wait_sampler "$run_dir"
    fi
    stop_proxy "$run_dir"

    if [[ "$DRY_RUN" != "1" ]]; then
        python3 "$BENCH_DIR/summarize.py" \
            --run-dir "$run_dir" \
            --config "$config" --scenario "$scenario" \
            > "$run_dir/summary.txt" 2>&1 || true
    fi
    echo "→ $run_dir"
}

# ---------------------------------------------------------------------------
# Main: cycle through (config × scenario).
# ---------------------------------------------------------------------------
for config in vanilla pool pool_lb; do
    for scenario in steady handshake; do
        run_one "$config" "$scenario"
    done
done

echo "ALL DONE"
