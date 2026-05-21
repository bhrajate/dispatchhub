#!/usr/bin/env bash
# 采集 client 侧硬件、OS 和压测工具版本，用于性能报告头部。
# 单机 WSL2 场景下它与 server 是同一主机，但保留拆分以便移植。
# 用法：capture-client.sh [output_path]
set -euo pipefail

OUT="${1:-test/perf/results/latest/env-client.txt}"
mkdir -p "$(dirname "$OUT")"

run() {
    local label="$1"; shift
    echo "----- $label -----"
    "$@" 2>&1 || true
}

{
    echo "===== captured: $(date -Iseconds) ====="
    run "uname -a"          uname -a
    run "/etc/os-release"   cat /etc/os-release
    run "lscpu"             lscpu
    run "free -h"           free -h
    run "nproc"             nproc
    run "wrk2 -v"           bash -c 'command -v wrk2 >/dev/null && wrk2 -v 2>&1 | head -n1 || echo "wrk2 not installed"'
    run "ghz --version"     bash -c 'command -v ghz >/dev/null && ghz --version || echo "ghz not installed"'
    run "tc -V"             bash -c 'command -v tc >/dev/null && tc -V || echo "tc not installed"'
    run "ulimit -a"         bash -c 'ulimit -a'
} > "$OUT"

echo "wrote $OUT"
