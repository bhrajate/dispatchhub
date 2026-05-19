# DispatchHub 压测套件

API Server 的基线性能测试脚手架。HTTP 路径用 **wrk2** 驱动，gRPC 路径用 **ghz**
驱动，覆盖 `POST /api/v1/tasks`、`SubmitTask`、`GET /api/v1/tasks/{id}` 与
`GET /api/v1/queues/{name}/stats` 四个接口，全部走 loopback、采用
**固定 RPS open-model** 工作负载。

## 为什么用 open-model

之前的 k6 套件是 closed-model VUs：每个虚拟用户必须等到响应才发下一笔请求，
所以达成的 RPS 隐式被服务端时延封顶。本套件改为对每个阶梯**强制注入固定目标
RPS**——服务端要么跟得上（错误率低、actual ≈ target），要么饱和（错误率上升 /
actual 落后于 target），饱和点直接体现在报告里。

wrk2 在此基础上额外通过 HdrHistogram 修正 **coordinated omission**：p99 反映
的是"用户期望的请求速率"下的真实尾延迟，而不是"服务端能跟上的速率"下的尾延迟，
比 vegeta 的 `latencies.99th` 单样本近似更可信。

## 前置依赖

- `wrk2`（HTTP 压力发生器，giltene/wrk2 分支；二进制安装为 `wrk2`，与发行版自带的
  经典 `wrk` 4.2 区分开——后者**不支持** `-R`（恒定速率）参数）。从源码构建：

  ```bash
  sudo apt-get install -y build-essential libssl-dev zlib1g-dev
  git clone https://github.com/giltene/wrk2.git /tmp/wrk2
  make -C /tmp/wrk2
  sudo install /tmp/wrk2/wrk /usr/local/bin/wrk2
  ```

  用 `wrk2 -v` 验证（giltene 分支会显示 `wrk 4.0.0`），并确认 `wrk2 --help`
  能列出 `-R, --rate` 选项。

- `ghz`（gRPC 压力发生器）：`go install github.com/bojand/ghz/cmd/ghz@latest`
- `python3`（`run.sh` 用它解析 wrk2 / ghz 的 JSON，无额外依赖）
- 与 `test/perf/configs/apiserver.yaml` 中地址匹配的 Redis / MySQL / etcd
  实例。开发环境的 `dh-perf-*` 容器分别监听 16380 / 33307 / 22379。
- 一个订阅 `default` 队列、注册了 `example.echo` 任务类型的在线 worker，
  否则 `RouteValidator` 会拒绝写入。

一键校验/安装：

```bash
make bench-deps
```

## 一键运行

```bash
make build               # 产出 bin/apiserver
bash test/perf/run.sh    # 默认 MATRIX=trim
```

如果想后台跑、把 orchestrator 输出和每 cell 产物一起留档：

```bash
DATE=$(date +%F)
mkdir -p "test/perf/results/$DATE"
nohup bash test/perf/run.sh > "test/perf/results/$DATE/orchestrator.log" 2>&1 &
```

`run.sh` 会按下列顺序执行：

1. 把服务端 + 客户端环境元信息写入 `test/perf/results/<date>/env-*.txt`。
2. 启动 `bin/apiserver --config=test/perf/configs/apiserver.yaml`（该 config
   已经把限流器关掉）。apiserver 进程归 orchestrator 所有，退出时一并被 kill。
3. 提交一条种子任务（队列 `default`、类型 `example.echo`）供读路径使用。
4. 对每个 (endpoint × 目标 RPS) cell：调用对应 driver
   （`drivers/wrk2/*.sh` 或 `drivers/ghz/*.sh`），抓 `/metrics` + `pidstat`
   快照，写 `verdict.txt`。
5. 退出时（无论成功还是被中断）：kill apiserver。

## 矩阵旋钮

| 环境变量 | 取值 | 作用 |
|---|---|---|
| `MATRIX` | `smoke` / `trim` / `full` | smoke = 仅 `http_submit_task` × 1 个 RPS 阶梯；trim = 4 endpoint × 4 阶梯；full = 在 trim 基础上加 2k / 5k 阶梯探饱和 |
| `DURATION` | `60s`（默认） | 每 cell 测量窗口 |
| `APISERVER_BIN` | `./bin/apiserver` | apiserver 二进制 |
| `APISERVER_CONFIG` | `test/perf/configs/apiserver.yaml` | 必须保证限流器是关的 |
| `SKIP_RESTART=1` | — | 复用一个手动启动的 apiserver |
| `WRK_THREADS` | `4` | wrk2 worker 线程数（每 cell） |

> **关于 DURATION**：wrk2 启动后有 ~10s 的 calibration（线程速率校准）阶段，期间不
> 记录直方图样本。`DURATION` 必须留足 calibration 之后的 measurement 窗口，**建议
> ≥ 30s**；默认 60s 可放心使用。如果误把 DURATION 调到 ≤ 10s，`p50/p95/p99`
> 会全部为 0，但请求计数仍正常——这是 wrk2 的预期行为，不是统计 bug。

trim 阶梯：`100 / 300 / 600 / 1000`（围绕 WSL2 开发机写路径 ~600 RPS 的饱和点）。
`full` 在此基础上追加 `2000 / 5000`。ghz 的 `--concurrency` 自动按
`max(50, target_rps / 50)` 取值，wrk2 的 `-c` 复用同一个值。

`GOMAXPROCS=4` 与 `GODEBUG=netdns=go`（可用 `LOAD_GOMAXPROCS` /
`LOAD_GODEBUG` 覆盖）只对 ghz 这种 Go driver 起作用——它们用来给 ghz 限制 OS
线程数、关闭 cgo DNS resolver，避免饱和时 `pthread_create` 撞 `RLIMIT_NPROC`。
wrk2 不读这些环境变量，它的线程池由 `-t`（即 `WRK_THREADS`，默认 4）决定。

### Verdict 规则

一个 cell 判定为 `PASS` 当且仅当：

- `error_rate ≤ 1%`（wrk2：HTTP 非 2xx + socket / timeout 错误；
  ghz：非 `OK` 状态码），**并且**
- `actual_rps ≥ target_rps × 0.95`。

任何一条不满足就翻为 `FAIL`。这两条同时考察"硬错误"和"apiserver 返回 200 但跟
不上目标速率"两种情况。

> **跨协议比较 p99 的注意事项**：wrk2 的 HdrHistogram 修正了 coordinated
> omission，ghz **没有**。同一服务在饱和段下，ghz 的 p99 通常会显著低于
> wrk2 —— 这是测量工具差异，而非 gRPC 比 HTTP 快。HTTP / gRPC 之间只比较
> 达标率与 verdict 即可，p99 仅在同一协议内做横向比较。

## Driver 单独调用（调试用）

```bash
# wrk2 —— POST /api/v1/tasks，1k RPS 跑 30s
RPS=1000 DURATION=30s CONCURRENCY=50 \
BASE_URL=http://127.0.0.1:8080 \
SEED_QUEUE=default \
OUT_DIR=/tmp/perf-cell \
bash test/perf/drivers/wrk2/http_submit_task.sh

# ghz —— SubmitTask，2k RPS、50 并发流，跑 30s
RPS=2000 DURATION=30s CONCURRENCY=50 \
GRPC_ADDR=127.0.0.1:9090 \
PROTO_ROOT="$PWD/api/proto" \
OUT_DIR=/tmp/perf-cell \
bash test/perf/drivers/ghz/grpc_submit_task.sh
```

## 产物布局

```
test/perf/results/<YYYY-MM-DD>/
├── env-server.txt         服务端硬件 + 依赖版本
├── env-client.txt         客户端硬件 + driver 版本
├── apiserver.log          orchestrator 启动的 apiserver 的 stdout/stderr
├── seed.env               SEED_TASK_ID + SEED_QUEUE + SEED_TYPE
├── index.csv              每个 cell 一行：endpoint,rps_target,rps_actual,p50,p95,p99,err,verdict
└── <endpoint>-<target_rps>/
    ├── stdout.log         driver stdout（wrk2 的 HdrHistogram 文本 / ghz 单行汇总）
    ├── summary.json       wrk2 done.lua 输出 / ghz 原生 JSON
    ├── server-metrics.prom cell 结束时的 Prometheus 快照 + pidstat
    └── verdict.txt        PASS / FAIL 与原因
```

HTTP（wrk2）cell 的 `summary.json` 故意写成扁平 schema，让
`run.sh eval_verdict` 解析尽量简单：

```json
{"throughput": <rps>, "requests": <n>, "non2xx": <n>,
 "p50_ms": <f>, "p95_ms": <f>, "p99_ms": <f>}
```

gRPC（ghz）cell 仍然保留 ghz 原生 JSON 结构（在 `eval_verdict` 里单独分支解析）。

## 渲染报告

跑完后：

```bash
python3 test/perf/render_report.py > /tmp/tables.md
```

该脚本读 `index.csv` 与每 cell 的 `server-metrics.prom`，生成概要表、按
endpoint 的明细表、资源占用表（Markdown 格式）。把它们拼到
`docs/performance/reports/baseline-YYYY-MM-DD.md`，再附上 `env-server.txt`、
`env-client.txt` 内容即可。
