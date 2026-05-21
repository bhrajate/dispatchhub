#!/usr/bin/env bash
# ghz driver：以固定 RPS 调用 dispatch.v1.DispatchService/SubmitTask，持续 DURATION。
# 必需环境变量：RPS、DURATION、CONCURRENCY、GRPC_ADDR、OUT_DIR、PROTO_ROOT。
# 可选环境变量：GHZ_CONNECTIONS（默认 = CONCURRENCY）。
#
# 关于 --connections：ghz 文档中的默认值为 1，
# 即无论 --concurrency 如何设置，所有并发 gRPC 调用都会共享单条
# TCP / HTTP/2 连接（已核对 ghz 文档：默认连接数为 1，
# 并发会均匀分布到所有连接上）。
# 单条 HTTP/2 连接只有一个 read-loop、一个 write-loop 和一个 outbound
# flow-control window，单连接帧处理速率会成为观测到的 gRPC 吞吐上限，
# 但这不是协议本身的真实饱和点。
#
# 相比之下，wrk2 的设计是保持 -c 条连接打开，每个 thread 处理
# N = connections/threads（根据 wrk README 和上游设计推断，
# 未在本地运行时通过 `ss` 检查验证）。两个工具的客户端连接模型不对称。
#
# 因此默认让 --connections 等于 --concurrency，使每个 ghz worker
# 拥有自己的连接，让两个工具大致处于同等基准。
# 参见 docs/performance/reports/r3-ultra9-185h-2026-05-19.md §3.2。
set -euo pipefail

: "${RPS:?}" "${DURATION:?}" "${CONCURRENCY:?}" "${GRPC_ADDR:?}" \
  "${OUT_DIR:?}" "${PROTO_ROOT:?}"

CONNECTIONS="${GHZ_CONNECTIONS:-1}"

DRIVER_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_FILE="$DRIVER_DIR/req-submit.json"

mkdir -p "$OUT_DIR"

ghz \
    --proto "$PROTO_ROOT/dispatch.proto" \
    --import-paths "$PROTO_ROOT" \
    --call dispatch.v1.DispatchService/SubmitTask \
    --rps="$RPS" \
    --concurrency="$CONCURRENCY" \
    --connections="$CONNECTIONS" \
    --duration="$DURATION" \
    --data-file="$DATA_FILE" \
    --insecure \
    --format=json \
    --output="$OUT_DIR/summary.json" \
    "$GRPC_ADDR"

# 向 stdout 输出简短摘要，供编排脚本日志使用。
python3 - "$OUT_DIR/summary.json" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    d = json.load(f)
print(f"ghz: count={d.get('count')} rps={d.get('rps'):.1f} avg={d.get('average')} errors={len(d.get('errorDistribution') or {})}")
PY
