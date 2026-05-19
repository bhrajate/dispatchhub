#!/usr/bin/env bash
# wrk2 driver: GET /api/v1/tasks/{SEED_TASK_ID} at fixed RPS for DURATION.
# Required env: RPS, DURATION, BASE_URL, OUT_DIR, CONCURRENCY, SEED_TASK_ID.
set -euo pipefail

: "${RPS:?}" "${DURATION:?}" "${BASE_URL:?}" "${OUT_DIR:?}" \
  "${CONCURRENCY:?}" "${SEED_TASK_ID:?}"

DRIVER_DIR="$(cd "$(dirname "$0")" && pwd)"
LUA="$DRIVER_DIR/lua/done.lua"

mkdir -p "$OUT_DIR"

THREADS="${WRK_THREADS:-4}"

OUT_DIR="$OUT_DIR" \
    wrk2 \
        -t"$THREADS" \
        -c"$CONCURRENCY" \
        -d"$DURATION" \
        -R"$RPS" \
        -L \
        -s "$LUA" \
        "$BASE_URL/api/v1/tasks/$SEED_TASK_ID"
