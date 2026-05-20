# Postmortem：2026-05-19 写路径吞吐瓶颈

> **TL;DR**：API Server 写路径在 ~300 RPS 撞顶。CPU profile 显示瓶颈是 GORM 给单条 `Create()` 默认包的 `BEGIN`+`COMMIT`，占 SubmitTask 总耗时 **39%**。
> 一行 `SkipDefaultTransaction: true` 让 600 RPS 阶梯实际 RPS **+51%**、 100 RPS 阶梯 p99 **−73%**。
>
> **更值得复盘的不是修复**，而是过程里推翻的两个错误假设：
> 1. "vegeta vs wrk2 RPS 差异是 idempotency 缓存命中差" —— 代码里根本没有缓存。
> 2. "瓶颈是 zap 同步写 stdout 的 INFO 日志" —— profile 显示日志只占 9%。
>
> 看代码猜瓶颈不靠谱，profile 才靠谱。
>
> 配套文档：
> - [../../playbooks/bottleneck-investigation.md](../../playbooks/bottleneck-investigation.md) —— 抽象出来的可复用流程
> - [../reports/r0-i5-13500h-2026-05-19.md](../reports/r0-i5-13500h-2026-05-19.md) —— r0 优化前 baseline
> - [../reports/r1-i5-13500h-2026-05-19.md](../reports/r1-i5-13500h-2026-05-19.md) —— r1 优化 A 后

## 时间线

| 时间 | 动作 |
|---|---|
| 14:30 | vegeta 套件迁移到 wrk2，写路径采用 wrk2 Lua 给每请求生成唯一 `name` |
| 14:45 | 第一次跑 `MATRIX=trim` baseline：写路径 100 RPS PASS / 300 起 FAIL |
| 15:00 | 发现资源表 CPU% 全是 `—`（pidstat 未装）。改用 `process_cpu_seconds_total` 差分采样 |
| 15:30 | 第二次跑 trim：拿到 CPU 数据，写路径 300 RPS 起 CPU 100%（单核满载） |
| 16:00 | 看代码"猜"瓶颈：怀疑 zap log 同步写 stdout |
| 16:30 | 加 pprof（build tag）、起服务、跑 `pprof?seconds=30` |
| 16:34 | profile 推翻假设：日志 9%，**GORM 默认事务 39%** |
| 16:42 | 加 `SkipDefaultTransaction: true`、重 build |
| 16:45 | 第三次跑 trim：600 RPS actual 287 → 434（+51%）、100 RPS p99 258 → 69ms |

## 现象

baseline 报告关键数据：

```
http_submit_task: 100 RPS PASS / 300 RPS FAIL (errors 15%)
                  600 RPS FAIL (actual 287, p99 32s)
                 1000 RPS FAIL (actual 282, p99 43s)

CPU%:  100 RPS=30%   300 RPS=105%   600 RPS=100%   1000 RPS=91%
       └─ 单核满载点恰好落在饱和阶梯 ────────┘
```

读路径 `http_get_task@1000` CPU 仅 164%（多核并行），形成对照——**写路径有"全局串行化"模式让多个核都在等同一把资源**。

## 调查过程

### 第一轮：看代码猜（错的）

通过 grep + 阅读源码列出写路径上的同步操作：

```
SubmitTask
  ├─ uuid.New()
  ├─ beforeSubmit (limiter, perf 配置关闭，nil 短路)
  ├─ routeValidator.Validate    ← sync.RWMutex + 每 10s 一次 etcd refresh
  ├─ taskStore.Create           ← MySQL INSERT
  ├─ broker.Enqueue             ← Redis ZADD/XADD
  └─ afterSubmit
       ├─ metrics.Inc()         ← Prometheus counter
       └─ log.Infof(...)        ← zap sugared, JSON, stdout sink
```

**初步假设排序**（从最像到最不像）：
1. **zap log 同步写 stdout**（最像）：每写一行 INFO 日志，sugared logger + JSON encode + 无 buffer 的 `os.Stdout`。
2. RouteValidator thundering herd：cache 失效时 N 个并发请求同时调 etcd（无 singleflight）。但 etcd 调用其实没在锁内，只影响 p99 抖动。
3. Prometheus counter 锁：少量 label 元组下争用同一个 child。
4. MySQL+Redis 串行 IO：每请求拿满一个 goroutine 时间片。

**这套分析的问题**：每个点都"看上去对"，但量化全靠脑补。读代码无法回答"哪一项贡献了真正吃掉单核的那 60% CPU"。

### 第二轮：profile（对的）

加 `net/http/pprof`（用 build tag `pprof` 隔离不进默认二进制），重 build 跑：

```bash
curl http://127.0.0.1:8080/debug/pprof/profile?seconds=30 -o cpu.prof &
RPS=600 ... bash test/perf/drivers/wrk2/http_submit_task.sh
go tool pprof -top -cum cpu.prof
```

`go tool pprof -list 'SubmitTask'` 给出每行 cum 时间：

```
SubmitTask                       14.93s (cum)
├─ taskStore.Create              10.81s   73%   ← 真正大头
│  └─ gorm.(*DB).Create          10.68s   33% of Total
│     ├─ callbacks.BeginTransaction      4.33s
│     ├─ 实际 INSERT 执行          ~5.0s
│     └─ callbacks.CommitOrRollback      1.48s
├─ broker.Enqueue                 2.49s   17%
├─ afterSubmit                    1.37s    9%   ← "头号嫌疑"实际很小
└─ routeValidator.Validate        0.07s  0.5%   ← 几乎不耗 CPU
```

`gorm BeginTransaction` 在 `gorm.io/gorm/callbacks/transaction.go:9` 调 `db.Begin()`：GORM 默认给每个 `Create/Update/Delete` 起一个事务，多走 `BEGIN` 与 `COMMIT` 两个 RTT。**29% + 10% = 39%** 直接花在事务 wrap 上。

实际 INSERT（5.0s）+ Redis Enqueue（2.49s）+ 其他（4.7s ≈ go runtime + IO 系统调用）合起来正好等于剩余的 12.2s。

### 关键发现：错误假设的来源

| 错误假设 | 来源 | 真相 |
|---|---|---|
| "wrk2 vs vegeta 落差是 idempotency 缓存命中差" | 看到 README 提到"避免 idempotency 命中"，假设代码里有去重逻辑 | grep `Name.*unique` / `idempot` 全无；`task.Name` 只是 `gorm:"index"`。**代码里根本没有 cache** |
| "瓶颈是 zap 同步写 stdout" | 这是 Go 服务里**真实存在过**的瓶颈，对模式很熟 | profile 显示 zap+metrics 一起才 9%；BEGIN+COMMIT 占 39% |
| "RouteValidator 锁住 etcd 调用" | sub-agent 看 `defer mu.RUnlock()` 误判 lock scope 包含 etcd 调用 | 实际 `refresh()` line 70 在锁外调 etcd，line 91 才短暂上写锁。p99 抖动有，但不是 RPS 瓶颈 |

**教训**：经验和模式匹配在这种场景里给出的假设质量很低，因为：
- 性能问题的"高频模式"很多（log、锁、序列化、连接池），它们都"看上去对"
- 但同一个代码可能同时有 5 个高频模式中招，没有量化数据没法排序
- ORM 默认事务这种"非显式代码"在 grep 里看不到，但 profile 一查就在 top

### 验证

`internal/`+`pkg/`+`cmd/` 全量 grep 显式事务（确认全局开 `SkipDefaultTransaction` 安全）：

```bash
grep -rn 'db\.Transaction\|\.Begin()\|\.Commit()\|\.Rollback()' \
    internal/ pkg/ cmd/ | grep -v _test.go
# (no output)
```

修复：

```diff
 func NewMySQLDB(cfg config.MySQLConfig) (*gorm.DB, error) {
-    db, err := gorm.Open(mysql.Open(cfg.DSN), &gorm.Config{})
+    db, err := gorm.Open(mysql.Open(cfg.DSN), &gorm.Config{
+        SkipDefaultTransaction: true,
+    })
```

重跑 `MATRIX=trim` 对比：

| 接口 | 阶梯 | 指标 | baseline | 优化 A | 变化 |
|---|---|---|---|---|---|
| `POST /api/v1/tasks` | 100 | p99 / CPU% | 258ms / 30% | **69ms** / **17%** | p99 −73%，CPU −43% |
| `POST /api/v1/tasks` | 600 | actual RPS | 287 | **434** | **+51%** |
| `POST /api/v1/tasks` | 1000 | actual RPS | 282 | **380** | **+35%** |
| gRPC `SubmitTask` | 100 | p99 | 220ms | **101ms** | −54% |
| gRPC `SubmitTask` | 300 | errors | 14% | 7% | −50% |

**预期 −39% CPU per request 与实测 −43% CPU @ 100 RPS 吻合**。改动落地一次性生效。

## 还没解决的事

1. **300 RPS 阶梯仍 FAIL**：errors 从 15% 降到 10%，仍超 1% 阈值。新瓶颈来自哪？需要下一轮 profile。预测：Redis Enqueue 占比会从 17% 升到接近 50%（因为 MySQL 那部分缩短了），并发执行 MySQL+Redis（`errgroup`）应能再 +15%。
2. **资源表 CPU% 在饱和段反而下降**（300→105%、600→123%、1000→106% 后又回落）：典型的"饱和后 Go scheduler 在锁/IO 上浪费时间"，但还没量化。需要 trace（`go tool trace`）而非 cpu profile 才能看清。
3. **wrk2 calibration 期 100 RPS 阶梯 p99 偏高**（69ms 仍比稳态高）：这是 wrk2 校准窗口对 60s 测试的"前 10s"影响，把 DURATION 加到 120s 能验证。

## 工程性产物

- `internal/apiserver/interfaces/http/pprof.go` + `pprof_off.go`：build tag `pprof` 控制是否暴露 `/debug/pprof/`（默认不进生产二进制）。**这是这次留下的最大基础设施**，下一次性能问题不需要再加。
- `test/perf/run.sh::sample_cpu_marker`：用 `process_cpu_seconds_total` 差分而不是 `pidstat` 采样 CPU%，零依赖。原 `pidstat` 路径其实从来没工作过（包没装），baseline 报告里"被 SIGTERM 杀掉"的解释也是猜错的。

## 复用建议

去 [../../playbooks/bottleneck-investigation.md](../../playbooks/bottleneck-investigation.md) 看抽象出来的可复用流程。本文是"故事"，playbook 是"流程"。
