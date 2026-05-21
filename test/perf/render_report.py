#!/usr/bin/env python3
"""
根据 index.csv 和每个 cell 的 server-metrics.prom 快照渲染 Markdown 表格。

用法：
    python3 test/perf/render_report.py [results_dir]

输出到 stdout：
- 每个 endpoint 的 actual-RPS / p99 表（行 = 目标 RPS）
- 资源使用表（每个有非零 metrics 的 cell 一行）
- 概要表（每个 endpoint 最高通过 RPS 阶梯）

index.csv schema（open-model，RPS 驱动）：
    endpoint,rps_target,rps_actual,p50_ms,p95_ms,p99_ms,errors,verdict
"""
import csv
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
RESULT_DIR = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT / "test/perf/results/latest"

ENDPOINT_LABEL = {
    "http_submit_task":   "POST /api/v1/tasks",
    "grpc_submit_task":   "gRPC SubmitTask",
    "http_get_task":      "GET /api/v1/tasks/{id}",
    "http_queue_stats":   "GET /api/v1/queues/{name}/stats",
}
ENDPOINT_ORDER = ["http_submit_task", "grpc_submit_task", "http_get_task", "http_queue_stats"]
RPS_ORDER = [100, 300, 500, 600, 1000, 2000, 5000, 10000]


def load_index(path):
    rows = []
    with open(path) as f:
        reader = csv.DictReader(f)
        for r in reader:
            r["rps_target"] = int(r["rps_target"])
            r["rps_actual"] = float(r["rps_actual"])
            r["p50_ms"]     = float(r["p50_ms"])
            r["p95_ms"]     = float(r["p95_ms"])
            r["p99_ms"]     = float(r["p99_ms"])
            r["errors"]     = float(r["errors"])
            rows.append(r)
    return rows


def parse_metrics(prom_path):
    """从 Prometheus 文本 dump 中提取几个核心序列。"""
    if not prom_path.exists():
        return {}
    out = {}
    text = prom_path.read_text(errors="ignore")
    patterns = {
        "go_goroutines":                  r'^go_goroutines\s+([\d.eE+-]+)',
        "process_resident_memory_bytes":  r'^process_resident_memory_bytes\s+([\d.eE+-]+)',
        "process_cpu_seconds_total":      r'^process_cpu_seconds_total\s+([\d.eE+-]+)',
    }
    for k, pat in patterns.items():
        m = re.search(pat, text, re.MULTILINE)
        if m:
            try:
                out[k] = float(m.group(1))
            except ValueError:
                pass
    return out


def cpu_pct_from_markers(start_path, end_path):
    """通过 process_cpu_seconds_total 差分计算 cell 窗口内的平均 CPU%。

    每个 marker 文件包含一行 "<cpu_seconds> <wallclock_ns>"，
    由 run.sh::sample_cpu_marker 在 driver 前后采集。
    返回平均 CPU%（100 * Δcpu / Δwall_seconds）；
    100 表示单核满载，多核并行时可以超过 100。
    """
    def _read(p):
        if not p.exists():
            return None
        try:
            parts = p.read_text().split()
            if len(parts) < 2:
                return None
            return float(parts[0]), int(parts[1])
        except (ValueError, OSError):
            return None

    start = _read(start_path)
    end = _read(end_path)
    if start is None or end is None:
        return None
    cpu0, wall0 = start
    cpu1, wall1 = end
    dwall = (wall1 - wall0) / 1e9
    if dwall <= 0:
        return None
    return 100.0 * (cpu1 - cpu0) / dwall


def fmt_num(x, kind):
    if x is None:
        return "—"
    if kind == "rps":
        if x >= 1000: return f"{x:,.0f}"
        if x >= 100:  return f"{x:.0f}"
        return f"{x:.1f}"
    if kind == "ms":
        if x >= 100: return f"{x:.0f}"
        return f"{x:.1f}"
    if kind == "pct":
        return f"{x:.1f}%"
    return str(x)


def rps_columns(rows):
    """按标准顺序返回稳定且仅包含已出现值的 RPS 列集合。"""
    present = {r["rps_target"] for r in rows}
    cols = [r for r in RPS_ORDER if r in present]
    extras = sorted(present - set(cols))
    return cols + extras


def render_endpoint_tables(rows, endpoint, cols):
    by_rps = {r["rps_target"]: r for r in rows if r["endpoint"] == endpoint}

    def header():
        rps_h = " | ".join(fmt_num(c, "rps") for c in cols)
        sep   = " | ".join(["---"] * len(cols))
        return [f"| 指标 \\ 目标 RPS | {rps_h} |", f"| --- | {sep} |"]

    def row(label, metric, kind):
        cells = []
        for c in cols:
            r = by_rps.get(c)
            cells.append("—" if r is None else fmt_num(r[metric], kind))
        return f"| {label} | " + " | ".join(cells) + " |"

    def attain_row():
        cells = []
        for c in cols:
            r = by_rps.get(c)
            if r is None or c == 0:
                cells.append("—")
            else:
                cells.append(fmt_num(100 * r["rps_actual"] / c, "pct"))
        return "| 达标率 | " + " | ".join(cells) + " |"

    def verdict_row():
        cells = []
        for c in cols:
            r = by_rps.get(c)
            cells.append("—" if r is None else r["verdict"])
        return "| verdict | " + " | ".join(cells) + " |"

    out = header()
    out.append(row("实际 RPS", "rps_actual", "rps"))
    out.append(attain_row())
    out.append(row("p50 (ms)", "p50_ms", "ms"))
    out.append(row("p95 (ms)", "p95_ms", "ms"))
    out.append(row("p99 (ms)", "p99_ms", "ms"))
    out.append(verdict_row())
    return "\n".join(out)


def highest_pass(rows, endpoint):
    cands = [r for r in rows if r["endpoint"] == endpoint and r["verdict"] == "PASS"]
    if not cands:
        return None
    return max(cands, key=lambda r: r["rps_target"])


def main():
    idx = RESULT_DIR / "index.csv"
    if not idx.exists():
        print(f"(no index.csv at {idx})")
        return
    rows = load_index(idx)
    cols = rps_columns(rows)

    # 概要：每个 endpoint 的最高 PASS 阶梯。
    print("## 一、概要 (Summary)\n")
    print("场景：loopback；驱动模型：open-model 固定 RPS 阶梯；verdict = `errors ≤ 1% 且 实际 RPS ≥ 目标 × 95%`。\n")
    print("> **测量工具差异提醒**：HTTP 用 wrk2 + HdrHistogram，对 coordinated")
    print("> omission 做了修正；gRPC 用 ghz，**未做修正**。同一服务下 ghz 的 p99")
    print("> 通常显著低于 wrk2，差异多为工具差而非服务性能差，跨协议比较 p99 时")
    print("> 请以达标率 / verdict 为主，p99 仅在同一协议内做横向比较。\n")
    print("| 接口 | 最高通过阶梯 (RPS) | 该阶梯实际 RPS | 达标率 | p99 (ms) |")
    print("| --- | --- | --- | --- | --- |")
    for ep in ENDPOINT_ORDER:
        best = highest_pass(rows, ep)
        label = ENDPOINT_LABEL[ep]
        if ep.startswith("grpc"):
            label += " ⁽¹⁾"
        if best is None:
            print(f"| `{label}` | — | — | — | — |")
            continue
        attain = 100 * best["rps_actual"] / best["rps_target"] if best["rps_target"] else 0
        print(f"| `{label}` | {fmt_num(best['rps_target'],'rps')} | "
              f"{fmt_num(best['rps_actual'],'rps')} | {fmt_num(attain,'pct')} | "
              f"{fmt_num(best['p99_ms'],'ms')} |")
    print()
    print("⁽¹⁾ 见上方 ghz 不修正 coordinated omission 的提醒。\n")

    # 按 endpoint 输出表格
    titles = {
        "http_submit_task":   "### 4.1 `POST /api/v1/tasks` (HTTP 写入)",
        "grpc_submit_task":   "### 4.2 gRPC `SubmitTask`",
        "http_get_task":      "### 4.3 `GET /api/v1/tasks/{id}` (MySQL 单点读)",
        "http_queue_stats":   "### 4.4 `GET /api/v1/queues/{name}/stats` (Redis 聚合读)",
    }
    for ep in ENDPOINT_ORDER:
        print(titles[ep])
        print()
        print(render_endpoint_tables(rows, ep, cols))
        print()

    # 资源使用表
    print("## 五、服务端资源占用\n")
    print("CPU% = cell 起止两次 `process_cpu_seconds_total` 的差分除以墙钟秒数")
    print("（100 = 单核满载，>100 表示多核并行）；RSS / goroutines = cell 结束")
    print("时 `/metrics` 的瞬时值。\n")
    print("| 接口 | 目标 RPS | apiserver CPU% | RSS MB | goroutines |")
    print("| --- | --- | --- | --- | --- |")
    for r in rows:
        cell = RESULT_DIR / f"{r['endpoint']}-{r['rps_target']}"
        m = parse_metrics(cell / "server-metrics.prom")
        cpu = cpu_pct_from_markers(cell / "cpu-start.txt", cell / "cpu-end.txt")
        rss = m.get("process_resident_memory_bytes")
        gor = m.get("go_goroutines")
        cpu_s = f"{cpu:.0f}" if cpu is not None else "—"
        rss_s = f"{rss / (1024*1024):.0f}" if rss else "—"
        gor_s = f"{gor:.0f}" if gor else "—"
        print(f"| `{ENDPOINT_LABEL[r['endpoint']]}` | {fmt_num(r['rps_target'],'rps')} | "
              f"{cpu_s} | {rss_s} | {gor_s} |")


if __name__ == "__main__":
    main()
