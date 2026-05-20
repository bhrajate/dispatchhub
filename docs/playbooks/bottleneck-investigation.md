# Playbook：定位与修复服务端性能瓶颈

> 这是一份**可复用流程**，把 [../performance/optimizations/r1-postmortem-2026-05-19-write-path.md](../performance/optimizations/r1-postmortem-2026-05-19-write-path.md) 那次"vegeta→wrk2→profile→GORM 默认事务"过程里抽象出来的步骤、工具、判断准则沉淀下来。下次再遇到"某接口饱和 / 某请求慢 / 某 CPU 跑满"，按这个流程走就行，不要从零靠经验拍。
>
> 适用场景：
> - HTTP / gRPC 服务的吞吐 / 延迟问题
> - 单实例 / 单进程层面的瓶颈定位（不覆盖分布式系统横向扩缩问题）
> - 已知瓶颈在 API Server 进程内（或想先排除这一层）
>
> 不适用：
> - 业务正确性问题（用 trace + 日志，不是这份）
> - 数据库本身瓶颈（用 `EXPLAIN` / slow query log，不是这份）

## 0. 一句话原则

> **看代码猜瓶颈基本不靠谱，profile 才靠谱**。模式匹配（"我见过这种慢"）给出的假设质量低，因为同一段代码常常同时撞了 3-5 个常见模式，全靠 profile 排序。**先量化、再下结论**。

每一步都要先想"我能拿什么数据回答这个问题"，而不是"这看起来像不像那个 bug"。

## 1. 流程总览

```
┌─────────────────────────────────────────────────────────┐
│ Phase 1: 量化现状（不要跳过）                           │
│   open-model 压测套件跑出 baseline，覆盖待测接口×多 RPS │
│   产出：每接口最高 PASS RPS、p99、CPU%、RSS、goroutines │
└─────────────────────────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────────┐
│ Phase 2: 找饱和点                                        │
│   找 actual_rps 与 target_rps 开始脱钩 / errors 飙升的  │
│   阶梯。这就是你后续 profile 要打的 RPS 值              │
└─────────────────────────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────────┐
│ Phase 3: 看 CPU% 判断瓶颈在不在本进程                   │
│   单核满载 → 本进程 → 进 Phase 4 (profile)              │
│   多核未满 → 不是 CPU → 进 Phase 5 (锁/IO/外部)         │
│   CPU 饱和后回落 → 锁/scheduler stall → trace 而非 prof │
└─────────────────────────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────────┐
│ Phase 4: pprof 量化热点                                  │
│   抓 30s CPU profile @ 饱和 RPS，go tool pprof -top     │
│   再 -list <function> 看每行 cum 时间                   │
└─────────────────────────────────────────────────────────┘
                       ↓
┌─────────────────────────────────────────────────────────┐
│ Phase 5: 下假设、做最小改动、A/B 重测                    │
│   每次只改一处，重跑同一矩阵，对比 actual / p99 / CPU%   │
└─────────────────────────────────────────────────────────┘
```

## 2. Phase 1：量化现状

**目标**：拿到一份可对比的 baseline。**不要在没有 baseline 的情况下做"优化"**——不然修完了不知道是真有效还是迷信。

### 工具

本仓库已有 `test/perf/` 套件：

```bash
make build
make bench-deps             # 校验 wrk2 + ghz 已装
MATRIX=trim bash test/perf/run.sh        # 4 endpoint × 4 RPS × 60s ≈ 17 min
python3 test/perf/render_report.py
```

产出：

```
test/perf/results/<DATE>/
├── index.csv           endpoint, rps_target, rps_actual, p50/95/99, errors, verdict
├── env-server.txt      硬件 + 依赖版本（基线对照必须留档）
├── env-client.txt
└── <endpoint>-<rps>/   summary.json, stdout.log, server-metrics.prom,
                        cpu-start.txt, cpu-end.txt
```

### 必须有的元数据

- 硬件 / OS / 依赖版本（CPU 型号、核数、kernel、Go、MySQL/Redis/etcd 版本）
- 提交哈希 / build 时间
- ulimit、关键 sysctl（`net.core.somaxconn`、`net.ipv4.tcp_tw_reuse`、`net.ipv4.tcp_fin_timeout`）
- 服务端 RSS / goroutines / **CPU%**（见 §6 关于 CPU% 采样）

`test/perf/env/capture-server.sh` 与 `capture-client.sh` 自动收集前三类。

### Open-model 是底线

**永远用 open-model**（恒定 RPS 注入），不要用 closed-model（恒定并发数）。
原因：closed-model 的 RPS 受服务端延迟反向调制，"看起来稳"但其实是被服务端拖住，看不出饱和点。本仓库 wrk2 driver 用 `-R <rps>` 强制注入，ghz 用 `--rps`。

### 量化口径要稳定

对比前后必须用**同一份 verdict**。本仓库 verdict：

> `error_rate ≤ 1% && actual_rps ≥ target_rps × 0.95`

不要为了让数字好看改阈值。

## 3. Phase 2：找饱和点 + 区分失败模式

`index.csv` 顺着 RPS 阶梯往下扫，找：

- **actual_rps 开始脱钩** 的阶梯（actual / target < 95%）
- 或 **errors > 1%** 的阶梯
- 或 **p99 跳一个数量级**的阶梯（10ms→1s）

找到后**一定要先区分失败模式**——不同模式用不同工具，下面 Phase 走向不同：

| 失败模式 | 现象 | 用什么工具 |
|---|---|---|
| **算力瓶颈** | actual 不达标 + CPU 单核满载 | Phase 4：CPU profile |
| **错误率高** | actual 达标但 errors > 1% | **Phase 3.x：先看日志和 OS 层**（见下） |
| **尾延迟抖** | actual 达标 + errors 低 + p99 跳水 | mutex/block profile + trace |
| **混合型** | 几种现象同时出现 | 上面工具组合，但**先修错误率，错误率会污染 profile** |

⚠️ **不要在错误率高 / 未饱和阶梯抓 profile**：
- 错误率高时，profile 会被失败请求的 retry / error path 占满，看不出真瓶颈。
- 未饱和阶梯 CPU 没跑满，热点不会显出来，你看到的是空载下的相对噪声。

### Phase 3.x：errors 高时先做的事（不进 Phase 4）

profile 看的是"在 CPU 上消耗的时间"。连接池 churn、TCP 异常、外部依赖超时**不消耗 CPU**，profile 看不见。这种情况：

```bash
# 1. 日志分类（10 秒）
grep '"level":"error"' apiserver.log \
  | grep -oE '"msg":"[^"]*"' | sort | uniq -c | sort -rn | head -10

# 2. 时序看是不是冷启动 / 周期性
grep '<dominant error msg>' apiserver.log \
  | grep -oE '"ts":"[^"]+"' | head -5
grep ... | tail -5

# 3. OS 层看 socket / connection 状态
ss -tan | grep ':<port>' | awk '{print $1}' | sort | uniq -c
# 看 ESTAB / TIME-WAIT / CLOSE-WAIT 各多少

# 4. 如果是 TIME-WAIT 多 → 连接 churn，看 client 池子配置
# 5. 如果是 CLOSE-WAIT 多 → 服务端没正确关连接
# 6. 如果是 5xx 类型为 backend 超时 → 看下游依赖状态
```

只有当 errors 已经压到 < 1%，再进 Phase 4 抓 profile 看下一个瓶颈。
**先解错误率、再谈算力优化**——这是 [r2 postmortem](../performance/optimizations/r2-postmortem-pool-tuning-2026-05-19.md)的核心教训。

## 4. Phase 3：CPU% 判断瓶颈在哪一层

`index.csv` 配合 `cpu-start.txt`/`cpu-end.txt`（diff 出 cell 期间的 CPU%）。

| 现象 | 含义 | 下一步 |
|---|---|---|
| CPU ≈ 100% 单核 | 本进程单 goroutine 路径吃满一核 | Phase 4 抓 cpu profile |
| CPU 100-400%（多核但未满） | 本进程占多核但还有余量 | Phase 4 + 看 trace 找锁 |
| CPU < 50% 但延迟高 / 错误多 | 不是 CPU 瓶颈：锁、IO、上游、连接池 | Phase 5，先看 IO / 外部依赖 |
| **CPU 饱和后反而回落** | 单核饱和后 Go scheduler 在锁/futex 上空转 | Phase 4 抓 profile + trace |
| CPU 多核却均不满 | 锁让一核多 goroutine 串行（Amdahl） | trace（`go tool trace`） |

**对照实验**：拿读路径和写路径的 CPU% 比，**模式不一致就说明有"全局串行化"**。
本次案例里：读路径 1000 RPS = CPU 164%（多核并行），写路径 300 RPS = CPU 105%（单核满载）—— 同一进程不同接口，CPU 模式不同，本身就是"写路径有串行点"的强证据。

## 5. Phase 4：CPU profile 量化热点

### 装 pprof

本仓库用 build tag 隔离：`internal/apiserver/interfaces/http/pprof.go`（带 `pprof` tag）
+ `pprof_off.go`（默认）。生产 build 不带 handlers，benchmark build 带。

```bash
CGO_ENABLED=0 go build -tags pprof -o bin/apiserver ./cmd/apiserver
./bin/apiserver --config=test/perf/configs/apiserver.yaml &
curl http://127.0.0.1:8080/debug/pprof/  # 验证
```

如果你的服务**还没**挂 pprof，照这两个文件抄。这是一次性投入，下一次性能调查直接用。

### 抓 profile

```bash
# 30s 窗口，与饱和 RPS 同步
curl http://127.0.0.1:8080/debug/pprof/profile?seconds=30 -o cpu.prof &
PROF_PID=$!
sleep 1                                   # 让 profile 先跑起来
RPS=600 DURATION=30s ... bash test/perf/drivers/wrk2/http_submit_task.sh
wait $PROF_PID
```

⚠️ 在**有压力但还没崩**的负载下抓最有用：
- **未饱和**：CPU 没跑满，热点不显，看到的是空载噪声。
- **完全饱和**：profile 被排队 / retry / GC 等级联效应占满，看不出真瓶颈。
- **甜区**：找到 verdict PASS 的最高阶梯，往下一档（首 FAIL 阶梯）的 80% 处抓。
  例如 r1 → r2 案例里，trim 跑 600 RPS 时 actual 只能到 287（饱和级联），但单独跑 30s @ 600 时 actual=599（甜区），后者的 profile 真实反映瓶颈构成。

### 读 profile 的固定动作

```bash
# Step 1: 看 cumulative top
go tool pprof -top -cum -nodecount=30 cpu.prof | head -40

# Step 2: 找你的入口函数（handler / use-case 函数）
go tool pprof -list 'YourFuncName' cpu.prof

# Step 3: 顺着 cum 大头一路下钻
go tool pprof -list 'NextHotFunc' cpu.prof
```

`-cum`（累计）比 `flat`（自身）有用得多——上层 wrapper 的 flat 通常是 0，但 cum 告诉你"经过它的请求总共花了多少时间"。

### 怎么读出"非显式代码"的开销

你看不到的代码通常最贵：

| 模式 | 在 profile 里的显形 | 可能的根因 |
|---|---|---|
| ORM 默认事务 wrap | `gorm/callbacks.BeginTransaction` / `CommitOrRollback` | `SkipDefaultTransaction` 没开 |
| ORM 反射构造 SQL | `gorm.(*Statement).Build` 或 `reflect.*` | 该用原生 SQL 的批量场景没用 |
| JSON encode/decode | `encoding/json.*` 在 top 5 | response 太大、payload 没 stream |
| 同步日志写 stdout | `os.(*File).Write` + `runtime.write` | log sink 没 buffer / 用了 sugared |
| Prometheus label 锁 | `prometheus.(*MetricVec).getOrCreateMetricWithLabelValues` | label 基数太低，全局锁争用 |
| GC | `runtime.gcBgMarkWorker` 占比高 | 分配过多、对象逃逸 |
| futex / scheduler | `runtime.futex` + `runtime.findRunnable` 高 | **歧义**：可能是锁争用、可能是 M parking、可能是 channel 阻塞——必须用 mutex/block profile 区分（见下） |

### CPU profile 看到的与看不到的（关键认知）

Go CPU profile 基于 `setitimer(ITIMER_PROF)` + SIGPROF，每秒 ~100 次采样，
**只采样正在被某个 M 调度运行的 goroutine**。这意味着：

| 状态 | CPU profile 能看到吗 | 例子 |
|---|---|---|
| goroutine 在 CPU 上跑 | ✅ | 计算、序列化、busy loop |
| goroutine 在 syscall 中（M 不释放） | ✅ | 同步读写文件、`read(2)`/`write(2)` |
| goroutine 在 netpoll wait（M 释放给 P） | ❌ | 等数据库响应、等 Redis、等 HTTP response |
| goroutine 被 `gopark` 挂起 | ❌ | 等 channel、等 mutex、等 connection pool |
| GC 工作 | ✅（`gcBgMarkWorker`）| GC 标记阶段 |

**陷阱**："cpu profile 顶部没看到 X" **不能**反证"X 没在等"。等 channel 的
goroutine 对 SIGPROF 完全不可见，1000 个 goroutine 等池子和 0 个等池子，
在 cpu profile 里看不出区别。

### profile 总分布判断：先看是 CPU 满还是不满

读 profile 第一件事是**看 CPU% 与 Go runtime 占比**，回答"瓶颈在哪类资源上"：

| CPU% | Go runtime 主要占比 | 最可能的瓶颈类型 | 怎么进一步定位 |
|---|---|---|---|
| 满载（≈ 100% × N 核） | 业务函数 cum 高 | **真 CPU bound** | 优化业务函数（cum 顶部） |
| 满载 | `gcBgMarkWorker` / `mallocgc` 高 | **GC bound**，分配压力大 | 减分配（sync.Pool、字段裁剪、避免反射） |
| 满载 | `findRunnable` / `schedule` 高 + 业务 flat 小 | goroutine 调度开销 | 看是不是有大量短命 goroutine |
| 远未满 + 业务 RPS 上不去 | `syscall.Syscall6` cum 高 | 真在做 syscall（IO 或 cgo） | 看 syscall 调用链，定位是哪类 IO |
| 远未满 + 业务 RPS 上不去 | `runtime.futex` 高 + 业务 cum 不高 | **歧义场景**：锁争用 / M parking / channel 阻塞 | **必须用 block / mutex profile 才能区分**，cpu profile 不够 |
| 远未满 + 业务 RPS 上不去 | runtime 占比也不高 | 大量 goroutine 在 sleep | block profile / `goroutine?debug=2` 看堆栈 |

⚠️ **远未满 + RPS 上不去**这一类，cpu profile 给的信息都是间接的。要直接答案，
启用 block profile 然后看：

```bash
# 启动时设置 (默认 rate=0, 不采样, 必须显式开启)
runtime.SetBlockProfileRate(1)        # 1 = 记录所有阻塞事件
runtime.SetMutexProfileFraction(1)    # 1 = 记录所有 mutex 争用

# 采集
curl http://127.0.0.1:8080/debug/pprof/block -o block.prof
curl http://127.0.0.1:8080/debug/pprof/mutex -o mutex.prof

go tool pprof -top -cum block.prof
go tool pprof -list 'sql.(*DB).conn' block.prof    # 看是不是在等连接池
go tool pprof -list 'YourMutex' mutex.prof
```

block profile 会精确告诉你"哪个 channel/mutex 上累计阻塞了多少时间"——
**这才是连接池争用、channel 阻塞、mutex 争用的金标**。

### 不要靠 cpu profile 反证"X 不是瓶颈"

需要直接证据时，这些是正确工具：

| 想证明 | 用什么 |
|---|---|
| 连接池够不够 | `db.Stats().WaitCount` / `WaitDuration`（最直接、零开销）；或 block profile + `list sql.(*DB).conn` |
| 哪个 mutex 在争用 | mutex profile（`SetMutexProfileFraction(1)` 后采集） |
| 哪个 channel 在阻塞 | block profile |
| goroutine 卡在哪 | `curl /debug/pprof/goroutine?debug=2`（全栈 dump，1 次性，免开销）|
| GC 影响多大 | `GODEBUG=gctrace=1` 看每次 GC 时间 + cpu profile 的 `gcBgMarkWorker` |

**反例（来自 r2 案例）**：本仓库 [r2 profile analysis](../performance/optimizations/r2-profile-analysis-2026-05-19.md)
初版用"cpu profile 顶部没看到 `sql.(*DB).conn`"作为"池子不是瓶颈"的论据——
**这是错的**（看不到 ≠ 不存在）。结论侥幸是对的，但靠的是别的证据
（errors=0% + 容量粗算）。已勘误。下次类似场景，**直接用 `db.Stats()` 或
block profile**。

## 6. Phase 3.5：CPU% 怎么采样（坑）

我们试过两种方法：

### ❌ pidstat（原方案，不推荐）

`pidstat -p <pid> 1 60` 起 sampler。**坑**：
- `sysstat` 包很多 minimal 镜像默认不装，silently fall through
- pidstat 与被测进程同进程组，benchmark 收尾 kill 进程时一起被 SIGTERM
- 如果 sysstat 装了但 pidstat 时序错了，你会拿到一个空文件而不是报错

**检查方法**：每次跑完压测确认 `cell_dir/pidstat.txt` 有内容**且**包含被测进程 PID 行。

### ✅ /metrics 差分（当前方案）

```bash
# cell 开始
cpu0=$(curl -s :8080/metrics | awk '$1=="process_cpu_seconds_total"{print $2}')
wall0=$(date +%s%N)

# 运行压测...

# cell 结束
cpu1=...
wall1=...
echo "scale=1; ($cpu1 - $cpu0) * 100 / (($wall1 - $wall0)/1e9)" | bc
```

**优点**：
- 零依赖，只要服务暴露了 `/metrics`
- 跨平台（pidstat 在 macOS 没有）
- 多核可超 100%，语义清晰

**缺点**：
- 平均值，看不到峰值。如果要峰值需要多次采样自己平均。
- `process_cpu_seconds_total` 只是 self-reported，与外部观测可能差 1-2%。

实现见 `test/perf/run.sh::sample_cpu_marker`。

## 7. Phase 5：下假设 → 改 → A/B 验证

### 一次只改一处

每次改动单独跑同一份 trim 矩阵对比。理由：

- 改 3 处一起跑，看到 RPS +50% 你不知道哪一刀贡献多大
- 多个改动可能互相掩盖：A 让锁不再是瓶颈，B（缓存）就显得没用——其实 B 单独上也有用
- 出问题时 bisect 难度爆炸

### 改动要尽量"最小"

| ✅ 好的改动 | ❌ 不好的改动 |
|---|---|
| 一行 GORM 配置 | 重写整个持久化层 |
| 一处 sync.Pool | 引入新依赖框架 |
| 删一行同步 log | 把所有 log 改异步 |
| 一处 `errgroup` 并发 | 把整条调用链改成 actor 模式 |

**最小化的目的不是"少写代码"，是"让 A/B 数据可解释"**。

### 对比口径

每次必须报：

| 维度 | 含义 |
|---|---|
| **同一阶梯 actual_rps** | 看吞吐变化 |
| **同一阶梯 p99** | 看尾延迟变化 |
| **同一阶梯 CPU%** | 看资源消耗变化 |
| **errors%** | 看正确性是否退化 |
| **最高 PASS 阶梯** | 整体 verdict 是否前进了 |

只看一个维度容易自欺。本次案例里写路径 RPS +51% / p99 −73% / CPU −43% 互相印证，置信度高；如果只 RPS +51% 而 errors 同步 +50%，那是把错误吞掉而不是真优化。

### 验证安全性

**改全局配置前必须 grep 所有调用点**，确认改动不破坏任何已有依赖。本次案例：

```bash
grep -rn 'db\.Transaction\|\.Begin()\|\.Commit()\|\.Rollback()' \
    internal/ pkg/ cmd/ | grep -v _test.go
```

任何显式事务用法都得在改 `SkipDefaultTransaction` 时手动 `db.Transaction(...)` 包回去。

## 8. 错误假设登记表（避免再犯）

本次过程里推翻的假设，下次遇到要先排除：

| 假设 | 为什么诱人 | 怎么验证 |
|---|---|---|
| "vegeta vs wrk2 RPS 差异是 idempotency 缓存命中" | README 提到了"避免缓存"，听起来合理 | grep `unique` / `idempot`；看 schema 是否有 unique 约束；看 task service 是否有去重逻辑 |
| "瓶颈是 zap 同步写 stdout" | 是 Go 服务真实存在的瓶颈、模式熟 | profile 看 `os.(*File).Write` cum |
| "pidstat 在 cell 收尾被杀掉" | "进程组 SIGTERM" 听起来合理 | `command -v pidstat` 先检查包是否装了！ |
| "RouteValidator 锁住 etcd 调用" | `defer mu.RUnlock()` 看起来覆盖整段函数 | 真正读 `refresh()` 函数体而不是 wrapper |

## 9. 常用命令速查

### 起一次完整 bench

```bash
make build && make bench-deps
DATE=$(date +%F); mkdir -p test/perf/results/$DATE
nohup bash test/perf/run.sh > test/perf/results/$DATE/orchestrator.log 2>&1 &
# 17 分钟后
python3 test/perf/render_report.py
```

### 起一次带 pprof 的 bench

```bash
CGO_ENABLED=0 go build -tags pprof -o bin/apiserver ./cmd/apiserver
./bin/apiserver --config=test/perf/configs/apiserver.yaml > /tmp/api.log 2>&1 &

# 抓 30s profile + 同步驱动 600 RPS 写
curl http://127.0.0.1:8080/debug/pprof/profile?seconds=30 -o /tmp/cpu.prof &
sleep 1
RPS=600 DURATION=30s CONCURRENCY=50 BASE_URL=http://127.0.0.1:8080 \
  SEED_QUEUE=default OUT_DIR=/tmp/cell \
  bash test/perf/drivers/wrk2/http_submit_task.sh

go tool pprof -top -cum -nodecount=30 /tmp/cpu.prof | head -40
go tool pprof -list 'SubmitTask' /tmp/cpu.prof
```

### 单独驱动一个 cell（调试用）

```bash
RPS=300 DURATION=30s CONCURRENCY=50 \
BASE_URL=http://127.0.0.1:8080 SEED_QUEUE=default \
OUT_DIR=/tmp/cell bash test/perf/drivers/wrk2/http_submit_task.sh
```

## 10. 何时停下来

每一刀做完问自己：

- 最高 PASS 阶梯前进了吗？
- 还有没有"明显"剩下的瓶颈（profile top 还有大头吗）？
- 当前性能距离业务需求还差多少？业务需要 1k RPS 而你已经做到 1.5k → 停。

**避免无限优化**：性能调优 ROI 是递减的，第一刀通常 +50%，第三刀可能 +5%。
看 ROI 决定何时收手，不要为了优化而优化。

## 相关文档

- [../performance/optimizations/r1-postmortem-2026-05-19-write-path.md](../performance/optimizations/r1-postmortem-2026-05-19-write-path.md) —— 本流程被验证的真实案例
- [../performance/reports/r0-i5-13500h-2026-05-19.md](../performance/reports/r0-i5-13500h-2026-05-19.md) —— r0 优化前 baseline 报告模板
- [../performance/reports/r1-i5-13500h-2026-05-19.md](../performance/reports/r1-i5-13500h-2026-05-19.md) —— r1 A/B 对比报告模板
- [../../test/perf/README.md](../../test/perf/README.md) —— 压测套件使用文档
