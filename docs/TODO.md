# DispatchHub TODO 清单

> 最后更新：2026-04-20

## 一、已完成（2026-04-14 ~ 2026-04-17）

### [optimization-analysis](fixes/2026-04-14-optimization-analysis.md) 中列出的问题

- [x] **P0 1.1** 乐观锁与悲观锁混用 — 已移除 `clause.Locking`，纯乐观锁
- [x] **P0 1.2** Labels/Duration 缺少 GORM Valuer/Scanner — 已在 `entity/task.go` 实现（Duration 采用 int64 纳秒存储）
- [x] **P0 1.3** WorkerRegistry.leases map 并发不安全 — 已加 `sync.RWMutex`
- [x] **P0 1.4** CancelTask 未从 Redis 队列移除 — 已调用 `broker.Remove` + `broker.PublishCancel`（详见 [2026-04-16-task-cancellation.md](fixes/2026-04-16-task-cancellation.md)）
- [x] **P1 2.1** Heartbeat 每次读 etcd — Heartbeat 改为直接接收完整 `WorkerInfo`
- [x] **P1 2.2** Worker fetchLoop 空轮询 — 已实现指数退避（100ms → 2s）
- [x] **P1 2.3** readyz 未检查依赖 — 已支持注入 healthCheck 回调
- [x] **P1 2.4** SubmitTask 双写错误策略 — Redis 入队失败不返回 error，补偿循环兜底
- [x] **P2 3.1** 基础设施初始化重复 — 已提取到 `persistence/factory.go`
- [x] **P2 3.2** Scheduler 队列列表静态 — 已改为从 workers 动态计算（详见 [2026-04-16-scheduler-queues-consistency.md](fixes/2026-04-16-scheduler-queues-consistency.md)）
- [x] **P2 3.3** Config env tag 未生效 — 已实现 `applyEnvOverrides`
- [x] **P2 3.4** CronJob 缺少并发控制 — 已支持 `ConcurrencyPolicy` + `HasRunningTasks`
- [x] **P3 4.1** HTTP 错误泄露内部信息 — 错误详情走日志，客户端只看通用消息
- [x] **P3 4.2** 缺少 Task TTL 清理 — 已实现 `cleanupLoop` + `DeleteTerminalOlderThan`
- [x] **P3 4.4** Worker version 硬编码 — 已使用 `version.Version` 注入
- [x] **P3 4.5** PromoteDelayed batch size 硬编码 — 已支持参数化

### Leader 选举相关（2026-04-16 ~ 2026-04-17）

- [x] Leader Election 竞态条件（session/election 无锁读写） — 详见 [2026-04-16-election-race-fix.md](fixes/2026-04-16-election-race-fix.md)
- [x] Leader Election defer 顺序死锁 — 详见 [2026-04-17-election-defer-deadlock-fix.md](fixes/2026-04-17-election-defer-deadlock-fix.md)

### 路由增强（2026-04-17）

- [x] Queue-Type 路由校验 — 详见 [2026-04-17-queue-type-route-validation.md](fixes/2026-04-17-queue-type-route-validation.md)

---

## 二、未完成（低优先级）

### 已知遗留问题（来自 [optimization-analysis](fixes/2026-04-14-optimization-analysis.md)）

- [ ] **P3 4.3** Rate Limiter `Wait` 使用 10ms 忙等待
  - 文件：`pkg/ratelimit/ratelimit.go:42-53`
  - 替代方案：根据当前 token 计算精确等待时间，或直接使用 `golang.org/x/time/rate`
- [ ] **P3 4.6** `TouchUpdatedAt` 错误被静默忽略
  - 文件：`internal/scheduler/domain/service/scheduler.go:147`
  - 失败时应记录 Warn 日志，防止下轮补偿循环重复入队

### 功能增强

- [ ] **单元测试**：项目暂无测试文件，接口设计已支持 mock，应补齐核心链路测试
- [ ] **认证鉴权**：API 层无 auth，假设运行在可信 K8s 集群内；生产需要补充 mTLS 或 API Key
- [ ] **OpenTelemetry 链路追踪**：跨 API Server / Scheduler / Worker 的端到端追踪
- [ ] **CronJob Update API**：当前只有 Create/Get/List/Delete，缺少 Update（启用/禁用/修改表达式）
- [ ] **Stats 维度细化**
  - 现状：`entity.QueueStats` 只有 `Pending/Active/Scheduled/Retrying/Completed/Failed` 六个标量；`stats_key` 哈希里只 `HINCRBY enqueued/completed/failed`，没有时序、没有分位数
  - 想加的维度：
    1. **吞吐**：enqueue/complete/fail 速率（rolling window 或 Prometheus counter）
    2. **延迟**：任务从 enqueue 到 dequeue 的等待时间分位数（P50/P95/P99）、handler 执行时长分位数
    3. **失败分类**：按 task.Type 或 error 模式拆分 failed 计数，定位"是哪类任务在挂"
    4. **per-handler / per-queue 分组**：现在所有维度都是 per-queue，看不到同队列内 handler 维度
  - 实现方向：接入 Prometheus（推荐），`/metrics` 由 apiserver/worker/scheduler 各自暴露，避免在 Redis hash 里堆滑窗
  - 触发：当 `GetQueueStats` 不再够用、用户开始问"哪个 handler 拖慢了 default 队列"时
- [ ] **死信队列（DLQ）**
  - 现状：`Nack` 在 `!task.CanRetry()` 分支只 `HINCRBY failed +1` 然后丢弃 task JSON；终态任务靠 MySQL `tasks` 表保留，但没有专门的"待人工处理"入口
  - DLQ 价值：
    1. 重试用尽的任务进 DLQ 而不是直接终结，运维可批量重投或检查 payload
    2. 隔离"毒任务"——某条任务每次都让 worker panic，DLQ 后正常任务不再受影响
    3. 配合 Stats：DLQ size 是健康度的一线指标
  - 设计要点：
    - 新增 `dispatchhub:queue:%s:dlq` sorted set（按 enqueue 时间排序）或单独的 MySQL `dead_tasks` 表
    - `Nack` 在 `!CanRetry` 分支改为投 DLQ，保留完整 task JSON + 最后一次 error
    - API 提供 `ListDLQ / RequeueFromDLQ / PurgeDLQ`
    - 决定 DLQ 容量与 TTL（无限堆积会成为新的故障点）
  - 触发：业务侧出现"重试用尽后想人工干预"的真实诉求，或线上观测到毒任务连环失败
- [ ] **CronJob 历史轨迹**
  - 现状：`CronJob.LastRunAt/NextRunAt` 只存最近一次触发时间；想看"过去 7 天这个 cron 触发了多少次、有几次失败、漂移了多久"，只能去 `tasks` 表按 `name` 模糊匹配，没有可靠关联
  - 想要的能力：
    1. 每次触发记录一条 `cron_run` 历史（cron_id、scheduled_at、actual_triggered_at、produced_task_id、最终 task state）
    2. 触发漂移监控：`actual - scheduled` 持续 > 阈值 → 报警（说明 scheduler 调度循环慢了）
    3. `Forbid` 策略 skip 计数也应有记录，便于回答"为什么这个 cron 看起来漏了一次"
  - 实现方向：
    - 新增 `cron_runs` 表：`(id, cron_id FK, scheduled_at, triggered_at, status enum{triggered,skipped,failed}, task_id, error)`
    - `SchedulerService.triggerCron` 在 CAS 成功后写一条记录；skip / 入队失败也各写一条
    - API 提供 `ListCronRuns(cron_id, from, to)`，UI 可以画时间线
    - TTL 清理（与 task cleanup 联动）
  - 触发：用户问"这个 cron 昨晚有没有跑"或"为什么 5 分钟的 cron 实际间隔成了 10 分钟"时
- [ ] **Worker 周期性续约 lease**（按需）
  - 当前：`Dequeue` 时一次性把 lease 拉到 `task.Timeout + LeaseBuffer`，长任务 worker 真死时回收延迟 ≈ Timeout
  - 续约后：基础 lease 短（如 30s），worker 每 N 秒 `ZADD XX lease deadline taskID` 续约，死亡 ≤ N+lease 内被回收
  - 触发条件之一再做：
    1. 业务引入小时级长任务（视频转码 / 离线推理 / 大批量生成）
    2. 监控显示 reclaim 延迟尾部接近 `Timeout` 而非真实 worker 死亡时刻
  - 设计要点：续约 goroutine 生命周期跟随 handler、续约失败的降级策略、shutdown 顺序、续约频率与 Redis QPS 的权衡
