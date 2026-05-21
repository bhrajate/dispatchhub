#!/usr/bin/env bash
# DispatchHub 基线性能编排脚本（open-model RPS，wrk2 + ghz）。
#
# 流程：
#   1. 采集 server 和 client 环境元数据。
#   2. 使用 test/perf/configs/apiserver.yaml 启动 apiserver
#      （rate_limit 已关闭，并指向 16380/22379/33307 上的 dh-perf-* 容器）。
#   3. 通过 apiserver HTTP 端口种入一个 task ID，供读路径脚本使用。
#   4. 遍历 endpoint × RPS：
#        driver script（wrk2 或 ghz）→ metrics 快照 → verdict。
#   5. 始终清理 apiserver。
#
# 环境变量：
#   MATRIX=smoke|trim|full   默认 trim
#   APISERVER_BIN            默认 ./bin/apiserver
#   APISERVER_CONFIG         默认 test/perf/configs/apiserver.yaml
#   SKIP_RESTART=1           复用正在运行的 apiserver，不重新启动
#   DURATION                 默认 60s
#   CONNECTIONS              默认 1 (ghz --connections; wrk2 uses CONCURRENCY as -c)
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

# 直接 loopback endpoint，driver 直接访问 apiserver。
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
    custom)
        if [[ -z "${CUSTOM_RPS:-}" ]]; then
            echo "MATRIX=custom requires CUSTOM_RPS env var (space-separated)" >&2; exit 2
        fi
        read -ra RPS_LIST <<< "$CUSTOM_RPS"
        ;;
    *)     echo "MATRIX must be smoke|trim|full|custom" >&2; exit 2 ;;
esac

# --- 1. 采集环境 --------------------------------------------------------------
LOG "capturing env metadata"
bash test/perf/env/capture-server.sh "$RESULT_DIR/env-server.txt"
bash test/perf/env/capture-client.sh "$RESULT_DIR/env-client.txt"

# --- 2. 启动 apiserver --------------------------------------------------------
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

# 确认 rate-limiter 实际已禁用（防御性检查，配置中 enabled:false）。
if [[ -f "$RESULT_DIR/apiserver.log" ]] \
   && ! grep -q "rate limiter disabled by config" "$RESULT_DIR/apiserver.log"; then
    LOG "WARNING: apiserver did not log 'rate limiter disabled by config'"
fi

# --- 3. 种入读路径任务 ---------------------------------------------------------
LOG "seeding read-path task"
BASE_URL="$DIRECT_HTTP" bash test/perf/env/seed-task.sh "$RESULT_DIR"
# shellcheck disable=SC1091
source "$RESULT_DIR/seed.env"
export SEED_TASK_ID SEED_QUEUE SEED_TYPE

# --- 4. matrix ---------------------------------------------------------------
RESULT_INDEX="$RESULT_DIR/index.csv"
echo "endpoint,rps_target,rps_actual,p50_ms,p95_ms,p99_ms,errors,verdict" > "$RESULT_INDEX"

snapshot_metrics() {
    local out="$1"
    # apiserver 将 /metrics 挂在 HTTP server（:8080）上，而不是单独端口。
    curl -fsS http://127.0.0.1:8080/metrics > "$out" 2>/dev/null || true
}

# 从 /metrics 读取 process_cpu_seconds_total（Prometheus counter，单位秒）。
# 输出 "<seconds> <wallclock_ns>"，调用方可对两次采样做差，
# 在不依赖 `pidstat` 的情况下计算 cell 窗口内的平均 CPU%。
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
    # ghz JSON：rps、count、average（ns）、latencyDistribution = [{percentage, latency(ns)}, ...]
    actual = float(d.get("rps", 0) or 0)
    count = int(d.get("count", 0) or 0)
    err_count = sum((d.get("errorDistribution") or {}).values()) if isinstance(d.get("errorDistribution"), dict) else 0
    # statusCodeDistribution：status → count 的 map，OK 表示成功。
    sc = d.get("statusCodeDistribution") or {}
    if sc:
        ok = int(sc.get("OK", 0) or 0)
        total_sc = sum(int(v) for v in sc.values())
        err_count = max(err_count, total_sc - ok)
    err_rate = (err_count / count) if count else 1.0
    p = {item.get("percentage"): item.get("latency", 0) for item in (d.get("latencyDistribution") or [])}
    def pct(target_pct, fallback_keys=()):
        # ghz 输出常见百分位（50、75、90、95、99）；优先精确匹配，否则取最近可用值。
        if target_pct in p:
            return p[target_pct] / 1e6
        # fallback：扫描数值 key
        candidates = [(abs(k - target_pct), p[k]) for k in p if isinstance(k, (int, float))]
        return (min(candidates)[1] / 1e6) if candidates else 0.0
    p50, p95, p99 = pct(50), pct(95), pct(99)
else:
    # wrk2 JSON（由 drivers/wrk2/lua/{done,submit}.lua 输出）：
    #   throughput（req/s）、requests、non2xx（status>=400 + socket error）、
    #   p50_ms / p95_ms / p99_ms（已为 ms，由 HdrHistogram μs 转换而来）。
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

        # 通过 process_cpu_seconds_total 差分计算 cell 窗口内的平均 CPU%。
        # 在 driver 运行前后采集，使其反映真实负载窗口，
        # 且不需要 `pidstat`（它不一定已安装）。
        sample_cpu_marker > "$cell_dir/cpu-start.txt" || true

        # GOMAXPROCS 限制 Go ghz driver 的 OS thread 数；
        # GODEBUG=netdns=go 禁用 cgo resolver，避免 ghz 在饱和时为 DNS 派生 pthread。
        # wrk2 会忽略这两项，并使用自己的线程池（-t）。
        RPS="$rps" \
        DURATION="$DURATION" \
        CONCURRENCY="$conc" \
        CONNECTIONS="${CONNECTIONS:-1}" \
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
