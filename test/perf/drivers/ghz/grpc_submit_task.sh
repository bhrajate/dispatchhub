#!/usr/bin/env bash
# ghz driver: dispatch.v1.DispatchService/SubmitTask at fixed RPS for DURATION.
# Required env: RPS, DURATION, CONCURRENCY, GRPC_ADDR, OUT_DIR, PROTO_ROOT.
set -euo pipefail

: "${RPS:?}" "${DURATION:?}" "${CONCURRENCY:?}" "${GRPC_ADDR:?}" \
  "${OUT_DIR:?}" "${PROTO_ROOT:?}"

DRIVER_DIR="$(cd "$(dirname "$0")" && pwd)"
DATA_FILE="$DRIVER_DIR/req-submit.json"

mkdir -p "$OUT_DIR"

ghz \
    --proto "$PROTO_ROOT/dispatch.proto" \
    --import-paths "$PROTO_ROOT" \
    --call dispatch.v1.DispatchService/SubmitTask \
    --rps="$RPS" \
    --concurrency="$CONCURRENCY" \
    --duration="$DURATION" \
    --data-file="$DATA_FILE" \
    --insecure \
    --format=json \
    --output="$OUT_DIR/summary.json" \
    "$GRPC_ADDR"

# Mirror a short summary to stdout for the orchestrator log.
python3 - "$OUT_DIR/summary.json" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    d = json.load(f)
print(f"ghz: count={d.get('count')} rps={d.get('rps'):.1f} avg={d.get('average')} errors={len(d.get('errorDistribution') or {})}")
PY
