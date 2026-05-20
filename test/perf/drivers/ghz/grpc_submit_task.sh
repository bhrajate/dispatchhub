#!/usr/bin/env bash
# ghz driver: dispatch.v1.DispatchService/SubmitTask at fixed RPS for DURATION.
# Required env: RPS, DURATION, CONCURRENCY, GRPC_ADDR, OUT_DIR, PROTO_ROOT.
# Optional env: GHZ_CONNECTIONS (default = CONCURRENCY).
#
# About --connections: ghz documents default = 1, i.e. all --concurrency
# concurrent gRPC calls share a SINGLE TCP / HTTP/2 connection regardless of
# --concurrency value (verified: ghz docs, "Number of connections to use.
# Concurrency is distributed evenly among all the connections (default 1)").
# A single HTTP/2 connection has one read-loop, one write-loop, and one
# outbound flow-control window — that single-connection frame-processing
# rate becomes the apparent gRPC throughput ceiling, NOT the protocol's
# real saturation point.
#
# wrk2 by contrast is designed to keep -c connections open with each thread
# handling N = connections/threads (per wrk README — connection model
# inferred from upstream design, not locally verified by inspecting `ss`
# during a run). The two tools' client connection models are asymmetric.
#
# We therefore default --connections to match --concurrency so each ghz
# worker owns its own connection, putting both tools on roughly equivalent
# footing. See docs/performance/reports/r3-ultra9-185h-2026-05-19.md §3.2.
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

# Mirror a short summary to stdout for the orchestrator log.
python3 - "$OUT_DIR/summary.json" <<'PY'
import json, sys
with open(sys.argv[1]) as f:
    d = json.load(f)
print(f"ghz: count={d.get('count')} rps={d.get('rps'):.1f} avg={d.get('average')} errors={len(d.get('errorDistribution') or {})}")
PY
