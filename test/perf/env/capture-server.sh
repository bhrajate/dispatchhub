#!/usr/bin/env bash
# Capture server-side hardware / OS / dependency versions for the perf report header.
# Usage: capture-server.sh [output_path]
set -euo pipefail

OUT="${1:-test/perf/results/latest/env-server.txt}"
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

    run "go version"        bash -c 'command -v go >/dev/null && go version || echo "go not installed"'
    run "docker --version"  bash -c 'command -v docker >/dev/null && docker --version || echo "docker not installed"'

    echo "----- redis -----"
    if command -v redis-cli >/dev/null; then
        redis-cli --version || true
        redis-cli INFO server 2>/dev/null \
            | grep -E '^(redis_version|os|arch_bits|process_id|tcp_port|uptime_in_seconds)' || true
    else
        echo "redis-cli not installed"
    fi

    echo "----- mysql -----"
    if command -v mysql >/dev/null; then
        mysql --version || true
        mysql -uroot -e "SELECT @@version, @@version_compile_os, @@version_compile_machine\G" 2>/dev/null || true
    else
        echo "mysql client not installed"
    fi

    echo "----- etcd -----"
    if command -v etcdctl >/dev/null; then
        etcdctl version || true
    else
        curl -fsS http://localhost:2379/version 2>/dev/null && echo || echo "etcd not reachable"
    fi

    echo "----- apiserver build -----"
    if [[ -x ./bin/apiserver ]]; then
        ./bin/apiserver --version 2>&1 || true
    else
        echo "bin/apiserver not built"
    fi

    run "ulimit -a"         bash -c 'ulimit -a'
    run "sysctl tcp"        sysctl net.ipv4.tcp_tw_reuse net.core.somaxconn net.ipv4.tcp_fin_timeout
} > "$OUT"

echo "wrote $OUT"
