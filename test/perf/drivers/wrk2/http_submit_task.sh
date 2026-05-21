#!/usr/bin/env bash
# wrk2 driver：以固定 RPS 请求 POST /api/v1/tasks，持续 DURATION。
# 必需环境变量：RPS、DURATION、BASE_URL、OUT_DIR、CONCURRENCY、SEED_QUEUE。
set -euo pipefail

: "${RPS:?}" "${DURATION:?}" "${BASE_URL:?}" "${OUT_DIR:?}" \
  "${CONCURRENCY:?}" "${SEED_QUEUE:?}"

DRIVER_DIR="$(cd "$(dirname "$0")" && pwd)"
LUA="$DRIVER_DIR/lua/submit.lua"

mkdir -p "$OUT_DIR"

THREADS="${WRK_THREADS:-4}"

OUT_DIR="$OUT_DIR" SEED_QUEUE="$SEED_QUEUE" \
    wrk2 \
        -t"$THREADS" \
        -c"$CONCURRENCY" \
        -d"$DURATION" \
        -R"$RPS" \
        -L \
        -s "$LUA" \
        "$BASE_URL/api/v1/tasks"
