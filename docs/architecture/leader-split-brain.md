# Leader 选举与脑裂防护

本文记录 Scheduler Leader 选举的脑裂分析、双主危害评估，以及当前选用的防护方案（CAS on `next_run_at`）的设计权衡。配合阅读：[`election.go`](../../internal/scheduler/infrastructure/election/election.go)、[`scheduler.go`](../../internal/scheduler/domain/service/scheduler.go)。

---

## 1. 单活的实现路径

Scheduler 的"单活"由三层闸门叠加而成，缺一不可：

| 层 | 实现 | 保证 |
|---|---|---|
| ① etcd 选举 | `concurrency.NewElection.Campaign` | 任意时刻最多一个 Campaign 返回 |
| ② 业务循环只用 leaderCtx 启动 | `cmd/scheduler/main.go:114`：`schedulerApp.Run(leaderCtx)` | 非 Leader 实例的 7 个后台 goroutine 从未启动 |
| ③ 失主时立即 cancel(leaderCtx) | `election.go:108-114` 监听 `session.Done()` | 网络分区时旧 Leader 在毫秒级停下业务 |

非 Leader 实例的进程是活的（election 客户端、ops server 还在跑），但**它从未调用过 `schedulerApp.Run`**，所以 cron / promote / compensate / cleanup 这些循环根本没被启动。

## 2. 双主窗口为什么不能消除

Lease + 网络分区下，"失主事实"和"失主感知"之间存在**物理意义上的延迟**：

```
t=0    Leader A 与 etcd 失联
t=15s  etcd 端 Lease 过期，B 当选成为新 Leader（事实层面 A 已失主）
t=15s  A 还不知道，因为它和 etcd 失联
t≈15s  A 的 KeepAlive 失败累积到一定阈值 → session.Done() 触发
t≈15s  cancel(leaderCtx) → 7 个 goroutine 在 ms 级退出
```

**双主窗口 ≈ Lease TTL（默认 15s）**。这段时间 A 仍以 Leader 身份在跑业务循环。

要彻底消除这个窗口，理论上需要 STONITH（带外通道杀掉旧 Leader）或 Quorum write（每次写都要 majority 确认），代价远超收益。**工业界的事实标准是接受窗口 + 业务层做防双写防护**。

## 3. 双主期间各操作的危害评估

把 `SchedulerAppService` 的 7 个后台循环全部过一遍：

| 循环 | 双主时是否安全 | 原因 |
|---|---|---|
| `watchWorkers` | ✅ 安全 | 仅更新本地内存视图 |
| `healthCheckLoop` | ✅ 安全 | 仅更新本地内存 |
| `metricsLoop` | ✅ 安全 | 只读 |
| `promoteDelayedLoop` | ✅ 安全 | Lua 脚本里 `ZADD ready` + `ZREM delayed` 在 Redis 单线程串行执行；同 score+member 的 ZADD 幂等 |
| `compensateLoop` | ✅ 安全 | `EnqueueIfNotInflight` 用 `HEXISTS inflight` 防重；同 score+member 的 ZADD 幂等 |
| `cleanupLoop` | ✅ 安全 | DELETE 走 MySQL 行锁串行；最多互相吃删除范围，总量正确 |
| **`cronLoop`** | ❌ **不安全** | 每次触发都生成**新 UUID** + 新 task 行 + 新入队，无幂等性 |

**所以脑裂的真实风险点只有一个：cron 重复触发**。

不修复时的具体故障形态：
- `next_run_at = 12:00:00` 的 cron job
- 双主窗口期 A 和 B 都扫到这条 due job
- 各自生成 `uuid_1`、`uuid_2`，各自插入 MySQL，各自入队
- **业务上同一个 cron 周期被触发两次**，对幂等性差的下游就是数据错乱

## 4. 方案选型：CAS vs Fence Token

| 维度 | CAS on `next_run_at` (本项目选择) | Fence Token |
|---|---|---|
| 解决范围 | 仅 cron 重复触发 | 任意双主期间的双写 |
| Schema 变更 | 0 | tasks/cron_jobs 加 `leader_epoch` 列 |
| 代码改动 | ~30 行（1 个新方法 + 1 处调用顺序调整） | ~200 行（election 暴露 epoch + 每个写路径带 epoch + 每个 SQL 加 WHERE） |
| 适用前提 | 已知 leader-only 写操作只有 cron 一处 | 多处分散的 leader-only 写操作 |
| 迁移成本 | 旧数据零迁移 | 需要给历史行回填 epoch |
| 性能开销 | 0（本来就要 UPDATE next_run_at） | 每次写多一个 WHERE 条件 |

### 为什么本项目选 CAS

§3 表里只有 cron 这一行 leader-only 且非幂等的写。Fence token 是通用解，但**通用性的代价是覆盖了大量本来就不需要保护的路径**。当唯一的风险点能用 1 处 CAS 治掉时，CAS 是更精确的工具。

### 什么场景应该升级到 fence token

满足任一条件就要重新评估：

1. 新增了 leader-only 的非幂等写操作（≥ 3 处分散在不同模块）
2. 业务对"双触发"零容忍（金融、库存）
3. 跨服务调用 RPC（CAS 防不住外部系统的副作用）
4. 需要审计"哪个 epoch 的 leader 做的这次写"

## 5. CAS 实现细节

### 5.1 接口

`internal/shared/domain/repository/cronjob_repository.go`：

```go
type CronJobWriter interface {
    CreateCronJob(ctx context.Context, job *entity.CronJob) error
    UpdateCronJob(ctx context.Context, job *entity.CronJob) error
    DeleteCronJob(ctx context.Context, id string) error
    // ClaimCronJob 以 next_run_at 作为 CAS 条件原子地推进调度时间。
    ClaimCronJob(ctx context.Context, jobID string,
        expectedNextRunAt, newLastRunAt, newNextRunAt time.Time) (bool, error)
}
```

### 5.2 SQL

`internal/shared/infrastructure/persistence/mysql/cronjob_repository.go`：

```sql
UPDATE cron_jobs
SET last_run_at = ?, next_run_at = ?
WHERE id = ? AND next_run_at = ?
```

返回 `RowsAffected == 1` 即抢占成功，`RowsAffected == 0` 即被其他实例抢走。

**为什么 next_run_at 适合做 CAS 条件**：

- 它是触发逻辑里**必然会推进**的字段（不变更就会被下一轮反复扫到）
- 推进语义和"我已经触发过这次了"完全等价
- 索引 `idx_enabled_next` 已经覆盖它，CAS UPDATE 走主键 + 索引列，性能开销忽略不计

### 5.3 调用顺序的关键

`scheduler.go` 改造前后对比：

```diff
  if 并发策略=Forbid && HasRunningTasks {
-     job.NextRunAt = &nextTime
-     UpdateCronJob(job)              // 无防护：双主时两边都会跳过 + 更新
+     ClaimCronJob(...)               // 用 CAS 保证只有一边推进时间
      continue
  }

+ // 关键：CAS 必须在 Create+Enqueue 之前
+ claimed, err := ClaimCronJob(...)
+ if !claimed { continue }            // 抢占失败直接跳过

  Create(task)
  Enqueue(task)

- // 改造前在这里 UpdateCronJob，已被前置的 Claim 取代
- UpdateCronJob(job)
```

**调用顺序不能反**：CAS 放在 `Create+Enqueue` 之后等于"先重复入队再防御"，没有意义。

### 5.4 CAS 失败后的语义

CAS 成功的实例继续 Create+Enqueue。CAS 失败的实例：

- **不**重新读取 job 后再尝试（否则陷入 lifelock）
- **不**报错（不是异常，是预期行为）
- 直接 `continue` 跳过本次触发，等下一轮 `cronLoop` ticker 重新扫描

这意味着：双主期间一个周期最多只触发一次（成功的那个），漏触发的概率 = 0（成功的那个会写 Create+Enqueue）。

### 5.5 边界情况

| 场景 | 行为 |
|---|---|
| Claim 成功，Create 失败 | 返回 lastErr；本次触发漏掉，但 next_run_at 已推进 → **下一周期才触发**。比"双触发"安全 |
| Claim 成功，Enqueue 失败 | task 已写 MySQL，Redis 没入队 → 由 `compensateLoop` 在 30s 内补偿入队 |
| Claim 时网络抖动 | 错误返回，下一轮重试。不会有"半成功"状态（CAS UPDATE 是单语句原子） |
| 同实例反复 Claim 同一 job | next_run_at 已被自己推进，第二次 WHERE 不匹配，自然失败跳过 |

## 6. 还没解决的问题

CAS 只覆盖了 cron 触发这一条路径。如果未来出现以下需求，需要重新设计：

- [x] **worker 失联后的 running 任务回收**：已实现 `reclaimInflightLoop`（每 5s）+ lease zset（dequeue 时写入 deadline，默认 30s 可见性超时），由 `reclaimInflightScript` 把过期任务 HDEL inflight + ZADD ready。注意这是基于"任务级 lease 超时"而非"worker 级心跳过期"的方案，更通用，对 worker 慢/卡死同样兜底。详见 [queue-design.md](./queue-design.md) §"可见性超时回收"
- [ ] **dead_letters 自动归档**：当前无清理逻辑
- [ ] **Lease TTL 太长**：默认 15s 让双主窗口偏大；调小代价是抖动期间频繁切换。值得做基线测试找平衡点
- [ ] 如果未来 leader-only 写操作扩展到 ≥ 3 处，应当升级到 fence token

## 7. 相关代码与文档

- 接口定义：`internal/shared/domain/repository/cronjob_repository.go`
- MySQL 实现：`internal/shared/infrastructure/persistence/mysql/cronjob_repository.go`
- 调用方改造：`internal/scheduler/domain/service/scheduler.go:TriggerDueCronJobs`
- Leader 选举源码：`internal/scheduler/infrastructure/election/election.go`
- 选举竞态修复历史：[`fixes/2026-04-16-election-race-fix.md`](../fixes/2026-04-16-election-race-fix.md)
- defer 顺序修复：[`fixes/2026-04-17-election-defer-deadlock-fix.md`](../fixes/2026-04-17-election-defer-deadlock-fix.md)
