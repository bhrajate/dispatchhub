# 架构设计

## 整体架构

DispatchHub 是一个分布式任务调度系统，由三个可独立部署的服务组成：

- **API Server**：系统唯一的外部入口，提供 HTTP REST + gRPC 接口
- **Scheduler**：后台调度控制面，通过 etcd Leader 选举保证单 Leader 运行
- **Worker**：任务执行数据面，Pull 模型从 Redis 拉取任务

```
                          ┌──────────────────────┐
                          │    Client / CLI       │
                          └──────────┬───────────┘
                                     │ HTTP :8080 / gRPC :9090
                          ┌──────────▼───────────┐
                          │    API Server (N)     │  无状态, 水平扩展
                          │  Redis + MySQL only   │
                          └──────┬─────────┬─────┘
                                 │         │
                  ┌──────────────┘         └──────────────┐
                  ▼                                       ▼
           ┌────────────┐                          ┌────────────┐
           │   Redis    │                          │   MySQL    │
           │  (任务队列) │                          │ (持久化)    │
           └──────┬─────┘                          └──────┬─────┘
                  │                                       │
      ┌───────── │ ───────────────────────────────────────│─────────┐
      │          │                                        │         │
      │          ▼                                        ▼         │
      │  ┌──────────────────────────────────────────────────────┐  │
      │  │  Scheduler (Leader)      etcd + Redis + MySQL        │  │
      │  │  ┌─────────────────────────────────────────────────┐ │  │
      │  │  │ 7 loops: watchWorkers | promoteDelayed(1s)      │ │  │
      │  │  │ healthCheck(10s) | metrics(5s) | compensate(30s)│ │  │
      │  │  │ cron(1s) | cleanup(1h)                          │ │  │
      │  │  └─────────────────────────────────────────────────┘ │  │
      │  └──────────────────────────────────────────────────────┘  │
      │                                                            │
      │  ┌──────────────────────────────────────────────────────┐  │
      │  │  Scheduler (Standby)      Campaign 阻塞等待          │  │
      │  └──────────────────────────────────────────────────────┘  │
      │                                                            │
      │         ┌──────────┐                                       │
      │         │   etcd   │  Leader 选举 + Worker 注册            │
      │         └────┬─────┘                                       │
      │              │                                             │
      │  ┌───────────┴──────────────────────────────────────────┐  │
      │  │  Worker Pool          etcd + Redis + MySQL           │  │
      │  │  ┌────┐ ┌────┐ ┌────┐                                │  │
      │  │  │ W1 │ │ W2 │ │ W3 │ ...   Pull 模型, HPA 弹性伸缩 │  │
      │  │  └────┘ └────┘ └────┘                                │  │
      │  └──────────────────────────────────────────────────────┘  │
      └────────────────────────────────────────────────────────────┘
```

## 组件职责总览

| 组件 | 二进制 | 连接的基础设施 | 对外 API | 核心职责 |
|------|--------|---------------|----------|----------|
| API Server | `cmd/apiserver` | Redis + MySQL | HTTP REST `:8080` + gRPC `:9090` + healthz/readyz/metrics | 任务和 CronJob 的完整 CRUD |
| Scheduler | `cmd/scheduler` | etcd + Redis + MySQL | 仅 `/healthz` `/readyz` `/metrics` (运维端点) | Leader 选举, 7 个后台循环 |
| Worker | `cmd/worker` | etcd + Redis + MySQL | 仅 `/healthz` `/readyz` `/metrics` (运维端点) | 拉取任务、执行、心跳上报 |

---

## API Server

> 源码: `cmd/apiserver/main.go`, `internal/apiserver/`

API Server 是**系统唯一的外部入口**。纯无状态网关，不连接 etcd，不参与 Leader 选举，不运行调度循环，不执行任务。可部署任意数量副本，前置 LoadBalancer/Ingress 即可水平扩展。

### 连接的基础设施

通过 `internal/shared/infrastructure/persistence/factory.go` 创建连接：

- **Redis**: `persistence.NewRedisClient(cfg.Redis)` -- 用于入队/延迟入队/队列统计
- **MySQL**: `persistence.NewMySQLDB(cfg.MySQL)` -- 用于任务和 CronJob 持久化

不连接 etcd。

### 暴露的接口

**HTTP REST (`:8080`)**:

| 方法 | 路径 | 功能 |
|------|------|------|
| `POST` | `/api/v1/tasks` | 提交任务 |
| `GET` | `/api/v1/tasks/{id}` | 获取单个任务 |
| `GET` | `/api/v1/tasks` | 列表查询任务 |
| `POST` | `/api/v1/tasks/{id}/cancel` | 取消任务 |
| `GET` | `/api/v1/queues/{name}/stats` | 队列统计 |
| `POST` | `/api/v1/cronjobs` | 创建 CronJob |
| `GET` | `/api/v1/cronjobs/{id}` | 获取 CronJob |
| `GET` | `/api/v1/cronjobs` | 列表查询 CronJob |
| `DELETE` | `/api/v1/cronjobs/{id}` | 删除 CronJob |
| `GET` | `/healthz` | 存活探针 |
| `GET` | `/readyz` | 就绪探针 (检查 Redis + MySQL) |
| `GET` | `/metrics` | Prometheus 指标 |

**gRPC (`:9090`)**:

SubmitTask, GetTask, ListTasks, CancelTask, GetQueueStats, CreateCronJob, GetCronJob, ListCronJobs, DeleteCronJob

### 核心设计

- **TaskServiceImpl** (`internal/apiserver/domain/service/task_service_impl.go`): 实现 `TaskService` 接口，包含 Task CRUD 和 CronJob CRUD
- **BeforeSubmit hook**: 在 `cmd/apiserver/main.go` 中注入，用于令牌桶限流 (`pkg/ratelimit`)
- **AfterSubmit hook**: 在 `cmd/apiserver/main.go` 中注入，用于 Prometheus 指标递增和日志记录
- **readyz 健康检查**: 通过 `HealthChecker` 回调检查 Redis Ping + MySQL Ping，3s 超时
- **HTTP 错误处理**: 内部错误记录详细日志 (`log.Errorf`)，对客户端返回通用错误消息 ("failed to submit task")

---

## Scheduler

> 源码: `cmd/scheduler/main.go`, `internal/scheduler/`

Scheduler 是有状态的控制面组件。通过 etcd Leader 选举保证**同一时刻只有一个 Leader 运行调度循环**，其余副本 Standby 等待接管。**不对外暴露任务管理 API**。

### 连接的基础设施

- **etcd**: Leader 选举 + Worker 拓扑 Watch
- **Redis**: 延迟任务晋升 + 队列统计 + 补偿入队
- **MySQL**: 孤儿任务扫描 + CronJob 触发 + 终态任务清理

### 7 个后台循环

Scheduler Leader 启动后并发运行以下循环 (`internal/scheduler/application/scheduler_app_service.go`):

| 循环 | 触发方式 | 周期 | 功能 | 调用的 domain 方法 |
|------|---------|------|------|--------------------|
| `watchWorkers` | 事件驱动 | - | Watch etcd Worker 注册变更，调用 `HandleWorkerEvent` 更新内存 Worker 列表 | `HandleWorkerEvent()` |
| `promoteDelayedLoop` | Ticker | 1s | 遍历所有已知队列，对每个队列调用 `PromoteDelayed(ctx, q, 100)`，将到期延迟任务移入就绪队列 | `Broker().PromoteDelayed()` |
| `healthCheckLoop` | Ticker | 10s | 检测 LastHeartbeat 超过 30s 的 Worker，从内存 map 中删除 | `DetectStaleWorkers(30s)` |
| `metricsLoop` | Ticker | 5s | 遍历所有已知队列，查询 Stats，发布 pending/active/scheduled 到 Prometheus | `Broker().Stats()` |
| `compensateLoop` | Ticker | 30s | 扫描 MySQL 中 Pending 且 updated_at > 30s 的任务，用 `EnqueueIfNotInflight` 安全重新入队，`TouchUpdatedAt` 刷新时间戳 | `CompensateOrphanedTasks()` |
| `cronLoop` | Ticker | 1s | 查询 next_run_at <= now 的 CronJob，创建 Task 并入队，推进 NextRunAt；若 ConcurrencyPolicy=Forbid 且有 Running 任务则跳过 | `TriggerDueCronJobs()` |
| `cleanupLoop` | Ticker | 1h | 删除 7 天前的终态任务 (Completed/Failed/Cancelled/Timeout)，每批最多 1000 条 | `CleanupTerminalTasks()` |

### SchedulerService domain 方法

> 源码: `internal/scheduler/domain/service/scheduler.go`

| 方法 | 功能 |
|------|------|
| `TriggerDueCronJobs(ctx, limit)` | 查询到期 CronJob，检查 ConcurrencyPolicy (Forbid 时调用 `HasRunningTasks`)，创建 Task 并入队，推进 NextRunAt |
| `CompensateOrphanedTasks(ctx, olderThan, limit)` | 查询 Pending 任务，对每个调用 `EnqueueIfNotInflight` 原子检查 inflight + 入队，成功后 `TouchUpdatedAt` 防止下次重复扫描 |
| `CleanupTerminalTasks(ctx, olderThan, limit)` | 委托 `TaskCompensator.DeleteTerminalOlderThan` 批量删除 |
| `SyncWorkers(ctx)` | 从 etcd `ListWorkers`，重建内存 Worker map，从 Worker 注册信息动态发现队列列表 |
| `HandleWorkerEvent(event)` | 处理 Joined/Left/Updated 事件，更新内存 Worker map |
| `DetectStaleWorkers(threshold)` | 单锁批量检查 LastHeartbeat，删除超时 Worker，返回被删除的 Worker ID 列表 |
| `Queues()` / `Broker()` / `Registry()` | 访问器方法 |

### Leader 选举

> 源码: `internal/scheduler/infrastructure/election/election.go`

```
Scheduler-1                Scheduler-2              Scheduler-3
    |                          |                        |
    +-- Campaign() ────────────+────────────────────────+
    |   (竞选 Leader)           |                        |
    v                          |                        |
 当选 Leader                    |                        |
    |                          |                        |
    +-- Run() 启动 7 个循环     +-- Campaign (阻塞等待)   +-- Campaign (阻塞等待)
    |   watchWorkers            |                        |
    |   promoteDelayed          |                        |
    |   healthCheck             |                        |
    |   metrics                 |                        |
    |   compensate              |                        |
    |   cron                    |                        |
    |   cleanup                 |                        |
    |                          |                        |
    X 崩溃 / Session 过期        |                        |
                               v                        |
                            当选 Leader                  |
                               |                        |
                               +-- Run() 接管调度         +-- Campaign (继续等待)
```

- Session TTL: 15s (默认)，崩溃后 Standby 在 TTL 过期后接管
- 失败后自动重试竞选，间隔 3s
- 失去 Leadership 后主动 Resign (5s 超时)

---

## Worker

> 源码: `cmd/worker/main.go`, `internal/worker/`

Worker 是完全无状态的数据面组件，采用 **Pull 模型**从 Redis 队列拉取任务执行。**不对外暴露任务管理 API**。

### 连接的基础设施

- **etcd**: 注册/注销/心跳
- **Redis**: Dequeue + Ack/Nack
- **MySQL**: 任务状态读写 (通过 `TaskStore` = `TaskReader` + `TaskWriter`)

### Fetch Loop 与指数退避

> 源码: `internal/worker/application/service/worker_app_service.go`

```
fetchLoop:
  ┌─────────────────────┐
  │  sem <- struct{}     │  <-- 协程池满时阻塞 (背压)
  │  (获取信号量)         │
  └─────────┬───────────┘
            v
  ┌─────────────────────┐
  │  broker.Dequeue()    │  <-- 从 Redis ZPOPMIN 原子出队
  └─────────┬───────────┘
            |
    ┌───────┴───────┐
    |               |
    v               v
  有任务          空队列
    |               |
  backoff = 100ms   backoff *= 2 (上限 2s)
    |               |
  go processTask()  time.After(backoff) 后重试
    |
  defer <-sem + wg.Done()
```

退避范围: 100ms (minBackoff) -- 2s (maxBackoff)，拿到任务后立即重置为 100ms。

### processTask 终态检查

执行任务前，Worker 从 MySQL 查询最新状态：

```go
if latest, err := w.taskStore.Get(ctx, task.ID); err == nil && latest != nil && latest.IsTerminal() {
    // 任务已被取消/完成, 跳过执行, 直接 Ack
}
```

这确保了已取消的任务不会被无效执行。

### 心跳上报

每 `HeartbeatInterval` (默认 5s) 直接发送完整 `*WorkerInfo` 到 etcd，不需要先 GET 再 PUT：

```go
w.info.ActiveTasks = int(atomic.LoadInt64(&w.active))
w.info.CPUUsage = cpuPct
w.info.MemUsage = memPct
w.info.LastHeartbeat = time.Now()
w.registry.Heartbeat(ctx, w.info)
```

- `WorkerInfo` 没有独立的 Heartbeat struct，直接用 `*WorkerInfo` 整体
- `LastHeartbeat` 在注册时初始化为 `time.Now()`，避免刚注册就被判定为 stale
- Worker version 使用 `version.Version` (通过 ldflags 注入)

### 优雅停机

1. 收到 SIGINT/SIGTERM，context 被取消
2. `fetchLoop` 退出 (不再拉取新任务)
3. 等待 `sync.WaitGroup` (in-flight 任务完成)
4. 超时保护：最长等待 `ShutdownTimeout` (默认 30s)
5. 调用 `registry.Deregister()` 从 etcd 注销

---

## 组件间协作

三个组件之间**零直接通信**，全部通过基础设施 (Redis / etcd / MySQL) 解耦：

```
外部客户端 ──HTTP/gRPC──> API Server ──TaskServiceImpl──> MySQL + Redis
                                                              |
                                                              |
Scheduler(Leader) ──reconciliation loops──────────────────────|
  - promoteDelayed     --> Redis (PromoteDelayed)             |
  - compensateLoop     --> MySQL (FindStaleByState)           |
                       --> Redis (EnqueueIfNotInflight)        |
  - cronLoop           --> MySQL (FindDueCronJobs, Create)    |
                       --> Redis (Enqueue)                    |
  - healthCheckLoop    --> 内存 Worker map                    |
  - watchWorkers       --> etcd (Watch)                       |
  - metricsLoop        --> Redis (Stats)                      |
  - cleanupLoop        --> MySQL (DeleteTerminalOlderThan)    |
                                                              |
Worker(N) ──Dequeue──────────────── Redis <───────────────────+
          ──Heartbeat/Register───── etcd
          ──Get/Update──────────── MySQL
```

**关键点**: API Server 不与 Scheduler/Worker 直接通信。Scheduler 不与 Worker 直接通信。所有协作通过共享存储完成。

---

## 基础设施读写矩阵

| 基础设施 | API Server | Scheduler | Worker |
|---------|------------|-----------|--------|
| **Redis** | 写: Enqueue, EnqueueDelayed | 读: Stats, 写: PromoteDelayed, EnqueueIfNotInflight | 读: Dequeue, 写: Ack, Nack |
| **etcd** | -- | 读: WatchWorkers, ListWorkers, 写: Leader 选举 (Campaign/Resign) | 写: Register, Deregister, Heartbeat |
| **MySQL** | 读: Get, List, 写: Create, Update | 读: FindStaleByState, FindDueCronJobs, HasRunningTasks, 写: Create (cron task), UpdateCronJob, TouchUpdatedAt, DeleteTerminalOlderThan | 读: Get (终态检查), 写: Update (Running/Completed/Failed/Retrying) |

---

## 三高设计方案

### 高并发 (High Concurrency)

| 机制 | 实现 | 文件位置 |
|------|------|----------|
| 优先级队列 | Redis Sorted Set，score = 负优先级，ZPOPMIN 获取最高优先级任务 | `internal/shared/infrastructure/persistence/redis/queue_broker.go` |
| 原子出队 | Lua 脚本: ZPOPMIN + HSET inflight 原子操作，避免竞态 | `internal/shared/infrastructure/persistence/redis/queue_broker.go` |
| 原子入队容量限制 | Lua 脚本: ZCARD 检查 + ZADD + HINCRBY stats 原子操作 | `internal/shared/infrastructure/persistence/redis/queue_broker.go` |
| 协程池 | 基于 channel semaphore 的有界协程池，超出时自动阻塞 (背压) | `internal/worker/application/service/worker_app_service.go` |
| 令牌桶限流 | 每个队列独立的令牌桶限流器 (BeforeSubmit hook) | `pkg/ratelimit/ratelimit.go` |
| Pipeline | Redis Pipeline 批量执行 Ack/Nack/Stats，减少网络往返 | `internal/shared/infrastructure/persistence/redis/queue_broker.go` |
| 连接池 | Redis PoolSize=100，MySQL MaxOpenConns=50 | `internal/shared/infrastructure/config/config.go` |

### 高可用 (High Availability)

| 机制 | 实现 | 文件位置 |
|------|------|----------|
| Leader 选举 | 基于 etcd `concurrency.Election`，Campaign/Resign/Observe | `internal/scheduler/infrastructure/election/election.go` |
| 多副本热备 | Scheduler 多副本部署，仅 Leader 运行循环，Standby 阻塞等待 | `cmd/scheduler/main.go` |
| 心跳检测 | Worker 每 5s 发送完整 WorkerInfo，Scheduler 30s 未收到则标记 offline | `internal/worker/application/service/worker_app_service.go` |
| 临时注册 | etcd Lease 机制 (TTL=15s)，Worker 崩溃后自动注销 | `internal/shared/infrastructure/persistence/etcd/worker_registry.go` |
| 乐观锁 | MySQL Version 字段，`WHERE version = old_version` 防止并发冲突 | `internal/shared/infrastructure/persistence/mysql/task_repository.go` |
| 优雅停机 | 信号捕获 + WaitGroup 等待 in-flight 任务完成 (30s 超时) | `internal/worker/application/service/worker_app_service.go` |
| 补偿机制 | MySQL 写成功但 Redis 入队失败时，补偿循环 30s 内重新入队 | `internal/scheduler/domain/service/scheduler.go` |

### 高性能 (High Performance)

| 机制 | 实现 | 文件位置 |
|------|------|----------|
| gRPC 长连接 | Keepalive 心跳 (10s)，连接复用，Protobuf 高效序列化，MaxRecvMsgSize=16MB | `internal/apiserver/interfaces/grpc/server.go` |
| 延迟晋升批量处理 | 每次最多晋升 100 个，Lua 脚本原子执行 ZRANGEBYSCORE + ZADD + ZREM | `internal/shared/infrastructure/persistence/redis/queue_broker.go` |
| 指数退避 | Worker 空队列时 100ms->2s 指数退避，避免空轮询浪费资源 | `internal/worker/application/service/worker_app_service.go` |
| 无锁计数 | Worker 活跃任务数使用 `atomic.Int64`，避免锁竞争 | `internal/worker/application/service/worker_app_service.go` |
| 动态队列发现 | SyncWorkers 从 Worker 注册信息提取队列列表，仅扫描有 Worker 的队列 | `internal/scheduler/domain/service/scheduler.go` |

---

## 数据流

### 任务提交到完成

```
1. Client ──POST /api/v1/tasks──> API Server
                                       |
2.                              TaskServiceImpl.SubmitTask()
                                       |
3.                        BeforeSubmit hook (限流检查)
                                       |
4.                            ┌────────┴────────┐
                              |                 |
                              v                 v
                        MySQL.Create()    Redis.Enqueue()
                        (持久化, 先执行)    (入队, 失败不返错)
                              |                 |
5.                  AfterSubmit hook       Worker.fetchLoop()
                    (日志 + 指标)                |
                                                v
                                          Redis.Dequeue()   <-- Lua 原子出队+移入inflight
                                                |
6.                                    MySQL.Get() 终态检查
                                                |
7.                                    taskStore.Update(Running)
                                                |
8.                                    中间件链(Recovery/Logging/Timeout)
                                                |
                                          Handler.Handle()
                                                |
                                      ┌─────────┴─────────┐
                                      |                   |
9.                               成功: Ack()          失败: Nack()
                                      |                   |
                                      v                   v
                                MySQL.Update       CanRetry?
                                (Completed)        Yes: Retrying + 重入队列
                                                   No:  Failed + Ack
```

### CronJob 触发流程

```
1. Client ──POST /api/v1/cronjobs──> API Server
       |
       v
   cronutil.NextRunTime() 计算 NextRunAt
       |
       v
   MySQL.CreateCronJob() 持久化

2. Scheduler.cronLoop() (每 1s 执行)
       |
       v
   MySQL.FindDueCronJobs(next_run_at <= now)
       |
       v
   ┌─────────────────────────────────────────┐
   │ 遍历每个到期 CronJob:                     │
   │                                          │
   │  ConcurrencyPolicy == Forbid?            │
   │    Yes -> HasRunningTasks(type, ns)?      │
   │              Yes -> 跳过, 但仍推进 NextRunAt │
   │              No  -> 继续                   │
   │    No  -> 继续                            │
   │                                          │
   │  job.ToTask() -> MySQL.Create(task)       │
   │  Redis.Enqueue(task)                      │
   │  cronutil.NextRunTime() -> 更新 NextRunAt  │
   │  MySQL.UpdateCronJob()                    │
   └─────────────────────────────────────────┘

3. Worker 正常从 Redis 拉取并执行
```

### 延迟任务流程

```
1. SubmitTask(delay=30s)
       |
       v
   Redis ZADD delayed_queue (score = now + 30s 的毫秒时间戳)
       |
       |  ... 30 秒后 ...
       |
2. Scheduler.promoteDelayedLoop() (每 1s 执行)
       |
       v
   Lua Script: ZRANGEBYSCORE delayed <= now LIMIT 100
               -> ZADD ready (score = -priority)
               -> ZREM delayed
       |
3.     v
   Worker.fetchLoop() 正常出队处理
```

### 补偿循环流程

```
Scheduler.compensateLoop() (每 30s 执行)
       |
       v
   MySQL.FindStaleByState(Pending, olderThan=30s, limit=100)
       |
       v
   ┌────────────────────────────────────────┐
   │ 遍历每个孤儿任务:                        │
   │                                        │
   │  Redis.EnqueueIfNotInflight(queue, task)│
   │  (Lua 原子: HEXISTS inflight? 跳过 : ZADD)│
   │                                        │
   │  enqueued?                             │
   │    Yes -> MySQL.TouchUpdatedAt(id)     │
   │           (仅刷新 updated_at, 不加 version)│
   │    No  -> 跳过 (任务正在 inflight 中)    │
   └────────────────────────────────────────┘
```

---

## 设计决策

### 为什么选择 Redis Sorted Set 作为队列?

1. **优先级支持**: ZSET 天然按 score 排序，用负优先级作为 score 实现优先级队列
2. **原子操作**: Lua 脚本保证出队 + 移入 inflight 的原子性
3. **延迟队列**: 利用 score 存储执行时间戳 (毫秒)，ZRANGEBYSCORE 实现定时触发
4. **幂等入队**: ZADD 对相同 member 是幂等的，补偿循环安全
5. **Redis Cluster**: 使用 `redis.UniversalClient`，支持 Standalone 和 Cluster 模式

### 为什么选择 etcd 而非 ZooKeeper?

1. **云原生生态**: etcd 是 Kubernetes 核心组件，与云原生技术栈一致
2. **Watch 机制**: 基于 gRPC 的 Watch + WithPrevKV 比 ZK Watcher 更直观
3. **Lease 机制**: 天然支持节点临时注册 (TTL=15s)，Worker 崩溃后自动清理
4. **强一致性**: Raft 协议保证数据一致，适合 Leader 选举场景

### 为什么 API Server 不连接 etcd?

API Server 仅需要读写任务数据 (MySQL) 和入队 (Redis)，不需要感知 Worker 拓扑或参与选举。减少依赖项提升 API Server 的可用性 -- 即使 etcd 不可用，任务提交和查询仍然正常工作。

### 为什么 Worker 使用拉模型 (Pull) 而非推模型 (Push)?

1. **天然背压**: channel semaphore 满时 fetchLoop 阻塞，自动停止从 Redis 出队
2. **简化调度**: Scheduler 无需维护 Worker 负载状态来做推送决策
3. **容错性**: Worker 崩溃后任务留在 Redis，其他 Worker 可接管
4. **弹性伸缩**: 新 Worker 上线后立即开始拉取，无需 Scheduler 感知

### 为什么 SubmitTask 中 Redis 入队失败不返回错误?

```go
// 入队失败时不返回 error, 避免客户端重试导致 MySQL 中创建重复任务
_ = s.broker.Enqueue(ctx, task.QueueName, task)
```

任务已持久化到 MySQL，Scheduler 的补偿循环会在 30s 内检测到并重新入队。对客户端而言任务提交成功。

### 为什么 TouchUpdatedAt 不递增 version?

补偿循环重新入队后，需要刷新 `updated_at` 防止下次扫描重复处理，但 Worker 从 Redis JSON 中拿到的是原始 version。如果 TouchUpdatedAt 也递增了 version，Worker 的乐观锁更新就会失败。

---

## 可靠性保证

| 场景 | 处理方式 | 恢复时间 |
|------|----------|---------|
| Worker 崩溃 | etcd Lease 到期 (15s) 后自动注销；inflight 任务可由补偿循环重新入队 | < 45s |
| Scheduler Leader 崩溃 | Standby 在 Session TTL (15s) 过期后接管，重启 7 个循环 | < 15s |
| Redis 不可用 | 任务已持久化到 MySQL，Redis 恢复后补偿循环自动重建队列 | 取决于 Redis 恢复时间 |
| MySQL 不可用 | 已在 Redis 中的任务仍可执行，但状态更新暂缓 | 取决于 MySQL 恢复时间 |
| 任务执行超时 | Timeout 中间件 `context.WithTimeout` 强制取消，进入重试流程 | 立即 |
| Handler Panic | Recovery 中间件捕获，加上 `safeHandle` 双重保护，转为 Failed 触发重试 | 立即 |
| Redis 入队失败 | 任务留在 MySQL，补偿循环 30s 内重新入队 | < 30s |
| 任务被取消后仍在队列中 | Worker processTask 先查 MySQL 终态，发现已取消则跳过并 Ack | 立即 |
| 网络分区 | etcd Lease 到期后 Worker 自动注销，Scheduler 通过 Watch 感知 | < 15s |
| 乐观锁冲突 | `WHERE version = old_version` 失败时返回错误，避免脏写 | 立即 |
