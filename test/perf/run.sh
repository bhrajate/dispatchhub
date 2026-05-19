#!/usr/bin/env bash
# DispatchHub baseline perf orchestrator (open-model RPS, wrk2 + ghz).
#
# Pipeline:
#   1. Capture server + client environment metadata.
#   2. Start apiserver with test/perf/configs/apiserver.yaml (rate_limit already off,
#      pointed at dh-perf-* containers on 16380/22379/33307).
#   3. Seed one task ID via the apiserver HTTP port for read scripts.
#   4. Loop endpoints × RPS:
#        driver script (wrk2 or ghz) → metrics snapshot → verdict.
#   5. Always: kill apiserver.
#
# Env knobs:
#   MATRIX=smoke|trim|full   default trim
#   APISERVER_BIN            default ./bin/apiserver
#   APISERVER_CONFIG         default test/perf/configs/apiserver.yaml
#   SKIP_RESTART=1           reuse a running apiserver, don't start one
#   DURATION                 default 60s
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

DATE_TAG="$(date +%Y-%m-%d)"
RESULT_DIR="$ROOT/test/perf/results/$DATE_TAG"
mkdir -p "$RESULT_DIR"
ln -sfn "$DATE_TAG" "$ROOT/test/perf/results/latest"

LOG() { printf '\e[36m[%s]\e[0m %s\n' "$(date +%H:%M:%S)" "$*"; }

MATRIX="${MATRIX:-trim}"
DURATION="${DURATION:-60s}"
APISERVER_BIN="${APISERVER_BIN:-./bin/apiserver}"
APISERVER_CONFIG="${APISERVER_CONFIG:-test/perf/configs/apiserver.yaml}"
PROTO_ROOT="$ROOT/api/proto"

# Direct loopback endpoints — drivers talk straight to apiserver.
DIRECT_HTTP="http://127.0.0.1:8080"
DIRECT_GRPC="127.0.0.1:9090"

declare -A ENDPOINT_DRIVER=(
    [http_submit_task]=drivers/wrk2/http_submit_task.sh
    [http_get_task]=drivers/wrk2/http_get_task.sh
    [http_queue_stats]=drivers/wrk2/http_queue_stats.sh
    [grpc_submit_task]=drivers/ghz/grpc_submit_task.sh
)

ENDPOINT_ORDER=(http_submit_task grpc_submit_task http_get_task http_queue_stats)

case "$MATRIX" in
    smoke) RPS_LIST=(100) ; ENDPOINT_ORDER=(http_submit_task) ;;
    trim)  RPS_LIST=(100 300 600 1000) ;;
    full)  RPS_LIST=(100 300 600 1000 2000 5000) ;;
    *)     echo "MATRIX must be smoke|trim|full" >&2; exit 2 ;;
esac

# --- 1. capture env -----------------------------------------------------------
LOG "capturing env metadata"
bash test/perf/env/capture-server.sh "$RESULT_DIR/env-server.txt"
bash test/perf/env/capture-client.sh "$RESULT_DIR/env-client.txt"

# --- 2. start apiserver -------------------------------------------------------
APISERVER_PID=""

cleanup() {
    if [[ -z "${SKIP_RESTART:-}" && -n "${APISERVER_PID:-}" ]] \
       && kill -0 "$APISERVER_PID" 2>/dev/null; then
        LOG "stopping apiserver pid=$APISERVER_PID"
        kill "$APISERVER_PID" || true
        wait "$APISERVER_PID" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

if [[ -z "${SKIP_RESTART:-}" ]]; then
    if [[ ! -x "$APISERVER_BIN" ]]; then
        LOG "building apiserver"
        make build >/dev/null
    fi
    LOG "starting apiserver ($APISERVER_CONFIG)"
    "$APISERVER_BIN" --config="$APISERVER_CONFIG" \
        > "$RESULT_DIR/apiserver.log" 2>&1 &
    APISERVER_PID=$!
    for i in $(seq 1 30); do
        if curl -fsS "$DIRECT_HTTP/healthz" >/dev/null 2>&1; then
            LOG "apiserver ready (pid=$APISERVER_PID)"
            break
        fi
        sleep 1
        if ! kill -0 "$APISERVER_PID" 2>/dev/null; then
            LOG "apiserver exited during boot — see $RESULT_DIR/apiserver.log"
            exit 1
        fi
    done
fi

# Confirm rate-limiter actually disabled (defensive — config says enabled:false).
if [[ -f "$RESULT_DIR/apiserver.log" ]] \
   && ! grep -q "rate limiter disabled by config" "$RESULT_DIR/apiserver.log"; then
    LOG "WARNING: apiserver did not log 'rate limiter disabled by config'"
fi

# --- 3. seed read-path task ---------------------------------------------------
LOG "seeding read-path task"
BASE_URL="$DIRECT_HTTP" bash test/perf/env/seed-task.sh "$RESULT_DIR"
# shellcheck disable=SC1091
source "$RESULT_DIR/seed.env"
export SEED_TASK_ID SEED_QUEUE SEED_TYPE

# --- 4. matrix ----------------------------------------------------------------
RESULT_INDEX="$RESULT_DIR/index.csv"
echo "endpoint,rps_target,rps_actual,p50_ms,p95_ms,p99_ms,errors,verdict" > "$RESULT_INDEX"

snapshot_metrics() {
    local out="$1"
    # apiserver mounts /metrics on the HTTP server (:8080), not on a separate port.
    curl -fsS http://127.0.0.1:8080/metrics > "$out" 2>/dev/null || true
}

# Read process_cpu_seconds_total (Prometheus counter, in seconds) from /metrics.
# Echoes "<seconds> <wallclock_ns>" so the caller can diff two samples and
# compute mean CPU% over the cell window without depending on `pidstat`.
sample_cpu_marker() {
    local cpu wall
    cpu=$(curl -fsS http://127.0.0.1:8080/metrics 2>/dev/null \
            | awk '$1 == "process_cpu_seconds_total" { print $2; exit }')
    wall=$(date +%s%N)
    if [[ -n "$cpu" ]]; then
        printf '%s %s\n' "$cpu" "$wall"
    fi
}

eval_verdict() {
    local summary="$1" endpoint="$2" target_rps="$3"
    python3 - "$summary" "$endpoint" "$target_rps" <<'PY'
import json, sys
summary, endpoint, target = sys.argv[1], sys.argv[2], float(sys.argv[3])
try:
    with open(summary) as f:
        d = json.load(f)
except Exception:
    print("0,0,0,0,1.0,FAIL_NO_SUMMARY", end="")
    sys.exit(0)

if endpoint.startswith("grpc"):
    # ghz JSON: rps, count, average (ns), latencyDistribution = [{percentage, latency(ns)}, ...]
    actual = float(d.get("rps", 0) or 0)
    count = int(d.get("count", 0) or 0)
    err_count = sum((d.get("errorDistribution") or {}).values()) if isinstance(d.get("errorDistribution"), dict) else 0
    # statusCodeDistribution: map status → count, OK == success.
    sc = d.get("statusCodeDistribution") or {}
    if sc:
        ok = int(sc.get("OK", 0) or 0)
        total_sc = sum(int(v) for v in sc.values())
        err_count = max(err_count, total_sc - ok)
    err_rate = (err_count / count) if count else 1.0
    p = {item.get("percentage"): item.get("latency", 0) for item in (d.get("latencyDistribution") or [])}
    def pct(target_pct, fallback_keys=()):
        # ghz emits common percentiles (50, 75, 90, 95, 99); pick exact, else nearest available.
        if target_pct in p:
            return p[target_pct] / 1e6
        # fallback: scan numeric keys
        candidates = [(abs(k - target_pct), p[k]) for k in p if isinstance(k, (int, float))]
        return (min(candidates)[1] / 1e6) if candidates else 0.0
    p50, p95, p99 = pct(50), pct(95), pct(99)
else:
    # wrk2 JSON (emitted by drivers/wrk2/lua/{done,submit}.lua):
    #   throughput (req/s), requests, non2xx (status>=400 + socket errs),
    #   p50_ms / p95_ms / p99_ms (already in ms; converted from HdrHistogram μs).
    actual = float(d.get("throughput", 0) or 0)
    requests = int(d.get("requests", 0) or 0)
    non2xx = int(d.get("non2xx", 0) or 0)
    err_rate = (non2xx / requests) if requests > 0 else 1.0
    p50 = float(d.get("p50_ms", 0) or 0)
    p95 = float(d.get("p95_ms", 0) or 0)
    p99 = float(d.get("p99_ms", 0) or 0)

verdict = "PASS" if (err_rate <= 0.01 and actual >= target * 0.95) else "FAIL"
print(f"{actual:.1f},{p50:.1f},{p95:.1f},{p99:.1f},{err_rate:.4f},{verdict}", end="")
PY
}

for endpoint in "${ENDPOINT_ORDER[@]}"; do
    driver="${ENDPOINT_DRIVER[$endpoint]}"
    for rps in "${RPS_LIST[@]}"; do
        cell="$endpoint-$rps"
        cell_dir="$RESULT_DIR/$cell"
        mkdir -p "$cell_dir"

        conc=$(( rps / 50 ))
        (( conc < 50 )) && conc=50

        LOG "→ $cell (rps=$rps conc=$conc dur=$DURATION)"

        # Mean CPU% over the cell window via process_cpu_seconds_total diff.
        # Captured before/after the driver run so it reflects the actual load
        # window without needing `pidstat` (which is not always installed).
        sample_cpu_marker > "$cell_dir/cpu-start.txt" || true

        # GOMAXPROCS caps the Go ghz driver's OS thread count; GODEBUG=netdns=go
        # disables the cgo resolver so ghz doesn't spawn pthreads for DNS under
        # saturation. wrk2 ignores both and uses its own thread pool (-t).
        RPS="$rps" \
        DURATION="$DURATION" \
        CONCURRENCY="$conc" \
        BASE_URL="$DIRECT_HTTP" \
        GRPC_ADDR="$DIRECT_GRPC" \
        OUT_DIR="$cell_dir" \
        PROTO_ROOT="$PROTO_ROOT" \
        SEED_TASK_ID="$SEED_TASK_ID" \
        SEED_QUEUE="$SEED_QUEUE" \
        GOMAXPROCS="${LOAD_GOMAXPROCS:-4}" \
        GODEBUG="${LOAD_GODEBUG:-netdns=go}" \
            bash "test/perf/$driver" \
                > "$cell_dir/stdout.log" 2>&1 \
            || true

        sample_cpu_marker > "$cell_dir/cpu-end.txt" || true

        snapshot_metrics "$cell_dir/server-metrics.prom"

        if [[ -f "$cell_dir/summary.json" ]]; then
            line=$(eval_verdict "$cell_dir/summary.json" "$endpoint" "$rps")
            verdict=$(printf '%s' "$line" | awk -F, '{print $NF}')
            echo "$endpoint,$rps,$line" >> "$RESULT_INDEX"
            echo "verdict=$verdict line=$line" > "$cell_dir/verdict.txt"
        else
            echo "$endpoint,$rps,0,0,0,0,1.0,FAIL_NO_SUMMARY" >> "$RESULT_INDEX"
            echo "verdict=FAIL_NO_SUMMARY" > "$cell_dir/verdict.txt"
        fi

        sleep 3
    done
done

LOG "matrix complete; index → $RESULT_INDEX"
column -t -s, "$RESULT_INDEX" 2>/dev/null || cat "$RESULT_INDEX"
LOG "raw artefacts under $RESULT_DIR"
