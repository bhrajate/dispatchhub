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
