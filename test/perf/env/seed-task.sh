#!/usr/bin/env bash
# 提交一个任务，并将其 ID 写入 results/<date>/seed.env，
# 使读路径脚本可以访问一个确定存在的任务。
# 用法：seed-task.sh <output_dir>
set -euo pipefail

OUT_DIR="${1:?output_dir required}"
BASE_URL="${BASE_URL:-http://127.0.0.1:8080}"
QUEUE="${SEED_QUEUE:-default}"
TASK_TYPE="${SEED_TYPE:-example.echo}"

mkdir -p "$OUT_DIR"

resp=$(curl -fsS -X POST "$BASE_URL/api/v1/tasks" \
    -H 'Content-Type: application/json' \
    -d "{
        \"name\": \"perf-seed\",
        \"namespace\": \"perf\",
        \"type\": \"$TASK_TYPE\",
        \"queue_name\": \"$QUEUE\",
        \"priority\": 5,
        \"payload\": {\"seed\": true},
        \"timeout\": \"30s\"
    }")

# 不依赖 jq 提取 task_id。
id=$(printf '%s' "$resp" | grep -oE '"task_id":"[^"]+"' | head -1 | cut -d'"' -f4)
if [[ -z "$id" ]]; then
    echo "failed to parse task_id from response: $resp" >&2
    exit 1
fi

{
    echo "SEED_TASK_ID=$id"
    echo "SEED_QUEUE=$QUEUE"
    echo "SEED_TYPE=$TASK_TYPE"
} > "$OUT_DIR/seed.env"

echo "seeded task_id=$id queue=$QUEUE type=$TASK_TYPE → $OUT_DIR/seed.env"
