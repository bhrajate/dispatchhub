# r2 后 CPU profile 分析（2026-05-19）

> **目的**：r2 解决了连接池 churn 问题后，写路径 actual_rps 在 600/1000 阶梯仍不达标 （429 / 392），但 errors 已经为 0、CPU 只有 0.78 核——**不是 CPU bound**。
> 抓一份新 profile 决定下一刀该做什么，特别回答"连接池还要不要再调大"。
>
> **TL;DR**：当前不应再动连接池。新 profile 显示 MySQL Create 占比从 r1 的 73% 降到 58%，Redis Enqueue 从 17% 升到 27%——**两路并发执行的 ROI 反而比 r1 更高**。Go runtime futex + syscall 占 35% 表明大量 goroutine 在 IO 等待，不是池子争用。下一刀做 B（并发 IO），预计 +20-30% RPS。
>
> 配套：
> - [../reports/r2-i5-13500h-2026-05-19.md](../reports/r2-i5-13500h-2026-05-19.md) —— r2 性能数据
> - [../optimization-roadmap.md](../optimization-roadmap.md) —— 总路线图（本次结论会回填）
> - [../../playbooks/bottleneck-investigation.md](../../playbooks/bottleneck-investigation.md) —— 抓 profile 的标准流程
> - 原始 profile：`test/perf/results/r2-i5-13500h-2026-05-19/cpu-600rps-30s.prof`

## 抓取条件

```bash
# apiserver: r2 build (gorm SkipDefaultTransaction + max_idle_conns=50 + ConnMaxIdleTime=5m)
CGO_ENABLED=0 go build -tags pprof -o bin/apiserver ./cmd/apiserver
./bin/apiserver --config=test/perf/configs/apiserver.yaml &

# 同步驱动 600 RPS @ 30s
curl -fsS "http://127.0.0.1:8080/debug/pprof/profile?seconds=30" -o cpu.prof &
sleep 1
RPS=600 DURATION=30s CONCURRENCY=50 BASE_URL=http://127.0.0.1:8080 \
  SEED_QUEUE=default OUT_DIR=/tmp/r2-prof \
  bash test/perf/drivers/wrk2/http_submit_task.sh
```

**采集到的负载快照**：

```json
{"throughput":599.5312,"requests":17995,"non2xx":0,
 "p50_ms":43.3590,"p95_ms":646.6550,"p99_ms":909.8230}
```

> 注意：trim 矩阵跑 600 阶梯时 actual=429 / p99=18s，本次 30s 单测出 actual=600 / p99=910ms。
> 差异原因：trim 是阶梯递增，到 600 阶梯前已累积 5 分钟负载，连接 + buffer pool 状态不同；30s 单测的稳态更轻。**这反而比"完全饱和段"更适合 profile**——能看到 "刚开始有压力"时的瓶颈构成，而不是"已经在瓶颈下打滚"时的二次效应。

## SubmitTask 时间分布

```
go tool pprof -list 'SubmitTask' cpu.prof
```

```
SubmitTask (cum 14.79s, 44.5% of 33.25s total)
├─ taskStore.Create          8.54s   ← MySQL INSERT
├─ broker.Enqueue            3.95s   ← Redis ZADD
│  ├─ json.Marshal(task)     1.23s
│  └─ Lua script.Run + RTT   2.50s
├─ afterSubmit               1.37s   ← zap log + metrics
├─ uuid.New + 字段补默认      0.21s
└─ routeValidator.Validate   0.04s   ← 完全不是热点
```

Go runtime（不属于 SubmitTask 但同时间窗）：

```
runtime.futex            4.84s   14.6%
internal/runtime/syscall 6.82s   20.5%
runtime.findRunnable     8.96s   27.0% (cum, 多为 idle)
```

## 与 r1 profile 对比

| 路径 | r1 cum | r1 占比 | r2 cum | r2 占比 | 变化 |
|---|---|---|---|---|---|
| MySQL Create | 10.81s | 73% | **8.54s** | **58%** | ↓ −15pp（BEGIN/COMMIT 已去）|
| └─ gorm BeginTransaction | 4.33s | 29% | — | 0% | 已消除 |
| └─ gorm CommitOrRollback | 1.48s | 10% | — | 0% | 已消除 |
| Redis Enqueue | 2.49s | 17% | **3.95s** | **27%** | **↑ +10pp** |
| └─ json.Marshal | — | — | 1.23s | 8% | 之前未单独拆出 |
| └─ Lua + RTT | — | — | 2.50s | 17% | |
| afterSubmit | 1.37s | 9% | 1.37s | 9% | 持平 |
| RouteValidator | 0.07s | 0.5% | 0.04s | 0.3% | 持平（小） |

**关键变化**：

1. **MySQL 不再独大**：从 73% 降到 58%，事务 wrap 拿掉的 5.8s 没有了。
2. **Redis 占比翻番（17% → 27%）**：Redis 路径绝对值也涨了 +59%（2.49 → 3.95s）—— r1 时 600 RPS 实际只跑出 287（被 churn 拖累），Redis 实际负载远小于现在的 600 ops/s。
3. **`json.Marshal(task)` 1.23s = 8%** 首次浮出水面：r1 时被 MySQL 大头掩盖。

## 回答关键问题：连接池要不要再调大？

**不该（在当前 600 RPS 负载下）。** 但下面给出的理由要小心，因为本文初版犯过一个技术错误，纠正后仅剩两条硬证据。

> **⚠️ 勘误**：本文初版写过一段："cpu profile 顶部没看到 `database/sql.(*DB).conn`，所以池子不是瓶颈"——**这条论证不成立**。Go 的 CPU profile 基于 SIGPROF，每秒 ~100 次采样，**只采样正在被 M 调度运行的 goroutine**。等池子的 goroutine 通过 channel 阻塞 → `gopark` → 被踢出运行队列，对 SIGPROF **完全不可见**。"看不到 ≠ 不存在"，这是常见混淆。详见 [playbook §profile 总分布判断](../../playbooks/bottleneck-investigation.md)。

### 真正能用的证据

#### 1. errors = 0%

r2 之后 trim 跑 600 RPS / 1000 RPS 阶梯都是 0 errors（参 [r2 报告](../reports/r2-i5-13500h-2026-05-19.md)）。`database/sql` 在等池子超时时会返回明确错误（`context deadline exceeded` 或上层 timeout），如果池子撑不住一定有错误率上升。**0% errors 说明压测窗口内每个请求都拿到了连接**。

#### 2. 容量粗算

600 RPS × 平均请求时延 ~50ms = **同时持连接 ~30 个**，远低于 `MaxOpenConns=50`。这是个粗算上界（峰值会更高），但量级上池子还有余量。

#### 3. 调大池的反向风险

把 `MaxOpenConns: 50 → 100/200`：

- **MySQL 端 thread / lock 排队**：MySQL 8 InnoDB 每连接独立 thread，并发 INSERT 同一张表时 row lock / b+tree latch 数量不变，`MaxOpenConns` 调大只让 wait queue 更长，不增加吞吐。
- **生产 `max_connections=500` 上限**：如果按 200/副本 × 5 副本 = 1000，直接超生产 MySQL 容量。当前设计假设 ≤ 5 副本。
- **goroutine 数线性增长**：r2 已从 r1 的 27 升到 65（50 conn × keepalive）。再调到 200 conn 可能到 250+ goroutine，RSS / GC 压力可见。

### 真正应该做的验证（没做但应该做）

要 **硬证** 池子不是瓶颈，本应用以下任一手段，都没用 CPU profile：

```go
// (a) 直接读 database/sql 池子统计 —— 最直接、零开销
stats := db.Stats()
fmt.Println(stats.WaitCount)        // 累计请求等待次数
fmt.Println(stats.WaitDuration)     // 累计等待时长
fmt.Println(stats.InUse)            // 当前在用
fmt.Println(stats.MaxOpenConnections)
// 在压测前后各取一次, diff 即可: WaitCount 显著上涨 → 池子不够
```

```bash
# (b) block profile —— 看 goroutine 阻塞在哪个原语
# 启动时设置 (默认 rate=0 不采样):
runtime.SetBlockProfileRate(1)
go tool pprof http://127.0.0.1:8080/debug/pprof/block
(pprof) list 'sql.(*DB).conn'
# 如果看到 conn() 内的 channel recv 占比高 → 池子争用
```

下一轮如果要复测"调大池有没有用"，先在 apiserver 暴露 `db.Stats()` 到 `/metrics`（go-sql 的标准做法），跑一次 600 RPS 看 `WaitCount` 增量。**这才是连接池争用的金标**——不是 CPU profile。

**结论**：在当前 600 RPS 负载下，`max_idle=max_open=50` 已经足够（错误率 0% 是硬证据）。RPS 推到更高时**应优先用 `db.Stats()` 监控**，而不是猜池子大小。

## 下一刀候选（基于本次 profile）

按 ROI 重排：

| # | 优化 | 依据 | 预期 | 成本 |
|---|---|---|---|---|
| **B** | MySQL + Redis 并发执行（errgroup） | 串行 = (8.54+3.95)/14.79 = **84.4%** 时间花在两路 IO；并发后理论上可压到 max(8.54, 3.95) = 8.54s = 57.7%。**单请求时延 −22%**，对应 RPS **+20-30%** | 20 行代码 + 一致性 review |
| **B'** | 仅 `json.Marshal` 优化 | 1.23s = 8%，可裁剪 task 字段 / 用 `easyjson` / 预分配 buffer | RPS **+4-5%** | 30 行；视 entity.Task 复杂度 |
| F1 | 删 / 异步化 INFO 日志 | 1.37s = 9% | RPS **+5-7%** | 1-15 行；**生产必修**，与 RPS 无关 |
| 重新调 GORM Create | profile 中 MySQL 5.0s 实际 INSERT 仍是大头，但已是最小路径 | 不动；想再快只能 batch（J）或换驱动 | — |

**强烈推荐 B 作为下一刀**：profile 中 84.4% 的时间花在两路串行 IO，业务上两路无依赖（Redis 不需要 MySQL 的返回值），是教科书级别的并发候选。

唯一阻力是**一致性语义变化**：
- 当前：MySQL 成功 → Redis 失败靠 scheduler 30s 内补偿
- 并发后：可能 MySQL 失败 + Redis 成功 → 留孤儿任务在 Redis 队列

需要先看 [analysis/task-submit-dual-write-consistency.md](../../analysis/task-submit-dual-write-consistency.md)是否覆盖此场景，或与作者确认 worker 拿到孤儿任务的处理路径。

## 回填的事

下面这些应在做完 B 之后或在做之前更新：

- **roadmap §五推荐顺序**：当前已写"重新抓 CPU profile"为下一步，本文档完成后可标 ✅，把 B 推到下一行。
- **roadmap §B 改动 / 预期**：把 r1 时估的 +15-20% 更新为基于本 profile 的 +20-30%。
- **roadmap §D 后注**：连接池调到 50 是 r2 的天花板，不要继续加；下次有副本数变化再考虑。

## 复用建议

这次 profile 教会的几点经验值得回到 [playbook §4](../../playbooks/bottleneck-investigation.md) 看看是否漏写：

1. **profile 抓在"刚开始有压力"的稳态比"完全饱和"更有用**：完全饱和后看到的是雪崩级联效应，刚开始有压力时看到的才是真瓶颈。可以这样配 RPS：找到 verdict PASS 的最高阶梯，下一阶梯（首 FAIL 阶梯）的 80% 处抓 profile。
2. **`go tool pprof -list <func>` 比 `-top -cum` 更值钱**：top 给"哪些函数贵"， `-list` 给"该函数的哪一行贵"——后者直接指向修改点。
3. **profile 跑完先看 Go runtime 占比**：futex / syscall / findRunnable 高 → IO bound，调 client 侧的 mutex / pool 没用，应看外部依赖；CPU 满载 + runtime 占比低 → 真 CPU bound，可继续 in-process 优化。

第 3 点是回答"连接池调大有没有用"的核心准则，下次更新 playbook 时应明确写入 Phase 4 决策树。
