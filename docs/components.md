# 核心组件

## DDD 架构概览

DispatchHub 采用**服务优先 + DDD 四层**组织代码。每个服务在 `internal/` 下有独立目录，通过 `shared/` 共享领域模型和基础设施：

```
internal/
  shared/                                    # 跨服务共享
    domain/
      entity/                                # 实体 & 值对象
        task.go                              #   Task (8 states), TaskFilter, TaskResult
        cronjob.go                           #   CronJob (with ConcurrencyPolicy)
        worker.go                            #   WorkerInfo (no Heartbeat struct)
        queue.go                             #   QueueStats, DefaultQueueName
      repository/                            # 仓储接口 (纯接口, 零实现)
        task_repository.go                   #   TaskReader, TaskWriter, TaskStore, TaskCompensator
        queue_broker.go                      #   QueueBroker (with EnqueueIfNotInflight)
        cronjob_repository.go                #   CronJobReader, CronJobWriter, CronJobStore
        worker_registry.go                   #   WorkerRegistry, WorkerEvent
    infrastructure/
      config/config.go                       # YAML 配置 + applyEnvOverrides
      version/version.go                     # ldflags 注入版本信息
      persistence/
        factory.go                           # NewRedisClient, NewMySQLDB, NewEtcdClient
        redis/queue_broker.go                # QueueBroker 实现 (Lua scripts)
        etcd/worker_registry.go              # WorkerRegistry 实现 (Lease + Watch)
        mysql/
          task_repository.go                 # TaskStore + TaskCompensator 实现
          cronjob_repository.go              # CronJobStore 实现

  apiserver/                                 # API Server 服务
    domain/service/
      task_service.go                        # TaskService 接口
      task_service_impl.go                   # TaskServiceImpl (hooks)
    interfaces/
      http/server.go                         # HTTP REST 路由
      grpc/server.go                         # gRPC DispatchService 实现

  scheduler/                                 # Scheduler 服务
    domain/service/scheduler.go              # SchedulerService (调度领域逻辑)
    application/scheduler_app_service.go     # 7 个 reconciliation loops 编排
    infrastructure/election/election.go      # etcd Leader 选举

  worker/                                    # Worker 服务
    application/service/worker_app_service.go # 执行引擎 (fetch/process/heartbeat)
    interfaces/middleware/middleware.go       # Recovery / Logging / Timeout

pkg/                                         # 通用工具包
  log/logger.go                              # Uber Zap 结构化日志
  metrics/metrics.go                         # Prometheus 预定义指标
  ratelimit/ratelimit.go                     # 令牌桶限流器
  cronutil/cronutil.go                       # cron 表达式解析
  signals/signals.go                         # SIGINT/SIGTERM 信号处理

cmd/
  apiserver/main.go                          # API Server 入口
  scheduler/main.go                          # Scheduler 入口
  worker/main.go                             # Worker 入口
```

## 依赖规则

```
cmd/apiserver  --> internal/apiserver + internal/shared + pkg/*
cmd/scheduler  --> internal/scheduler + internal/shared + pkg/*
cmd/worker     --> internal/worker    + internal/shared + pkg/*
```

- **三个服务之间零交叉引用**: apiserver 不引用 scheduler/worker 的任何代码，反之亦然
- **domain 层零基础设施依赖**: 日志/指标通过 hook 在 application 层或 cmd 层注入
- **依赖方向**: interfaces -> application -> domain -> entity（仅向内依赖）
- **shared/ 被所有服务共享**: 实体、仓储接口、基础设施实现

---

## API Server（接入网关）

> 源码: `cmd/apiserver/main.go`, `internal/apiserver/`

### TaskService 接口

> 源码: `internal/apiserver/domain/service/task_service.go`

```go
type TaskService interface {
    // Task operations
    SubmitTask(ctx context.Context, task *entity.Task) error
    GetTask(ctx context.Context, taskID string) (*entity.Task, error)
    ListTasks(ctx context.Context, filter entity.TaskFilter) ([]*entity.Task, int64, error)
    CancelTask(ctx context.Context, taskID string) error
    QueueStats(ctx context.Context, queue string) (*entity.QueueStats, error)
    // CronJob operations
    CreateCronJob(ctx context.Context, job *entity.CronJob) error
    GetCronJob(ctx context.Context, id string) (*entity.CronJob, error)
    ListCronJobs(ctx context.Context, namespace string, limit, offset int) ([]*entity.CronJob, int64, error)
    DeleteCronJob(ctx context.Context, id string) error
}
```

该接口定义在 API Server 的 domain 层，由 `TaskServiceImpl` 实现。HTTP 和 gRPC 接口层均依赖此接口。

### TaskServiceImpl 与 Hooks

> 源码: `internal/apiserver/domain/service/task_service_impl.go`

`TaskServiceImpl` 持有三个仓储依赖：

| 字段 | 类型 | 用途 |
|------|------|------|
| `broker` | `repository.QueueBroker` | Redis 入队/队列统计 |
| `taskStore` | `repository.TaskStore` | MySQL 任务 CRUD |
| `cronStore` | `repository.CronJobStore` | MySQL CronJob CRUD |

**Hook 机制**:

| Hook | 签名 | 注入位置 | 用途 |
|------|------|---------|------|
| `BeforeSubmitHook` | `func(task *entity.Task) error` | `cmd/apiserver/main.go` | 令牌桶限流，返回 error 则拒绝提交 |
| `AfterSubmitHook` | `func(task *entity.Task)` | `cmd/apiserver/main.go` | Prometheus 指标递增 + 日志记录 |

**SubmitTask 完整流程**:

```
SubmitTask(ctx, task)
  |
  v
1. 设置默认值:
   ID = uuid.New()
   QueueName = "default"
   Priority = 5
   MaxRetries = 3
   Timeout = 5m
   State = Pending
   CreatedAt/UpdatedAt = now
  |
  v
2. BeforeSubmit hook (限流检查, 返错则拒绝)
  |
  v
3. taskStore.Create(ctx, task)     --> MySQL INSERT
  |
  v
4. 判断是否延迟:
   有 ScheduleAt 或 Delay? --> broker.EnqueueDelayed()
   否则                    --> broker.Enqueue()
   (入队失败忽略 error, 补偿循环兜底)
  |
  v
5. AfterSubmit hook (指标 + 日志)
```

### gRPC Server

> 源码: `internal/apiserver/interfaces/grpc/server.go`

通过 proto codegen 生成 `dispatchpb.DispatchServiceServer` 接口，Server 实现该接口并嵌入 `UnimplementedDispatchServiceServer`。

**拦截器链** (Unary):

| 顺序 | 拦截器 | 功能 |
|------|--------|------|
| 1 | `grpc_prometheus.UnaryServerInterceptor` | Prometheus 请求指标 |
| 2 | `grpc_recovery.UnaryServerInterceptor` | Panic 恢复，返回 `codes.Internal` |
| 3 | `loggingUnaryInterceptor()` | 请求日志 (方法名、状态码、耗时) |

**Keepalive 配置**:

| 参数 | 值 | 含义 |
|------|-----|------|
| MaxConnectionIdle | 5m | 空闲连接最大存活时间 |
| MaxConnectionAge | 30m | 连接最大存活时间 |
| MaxConnectionAgeGrace | 10s | 关闭前宽限期 |
| Time | 10s | Keepalive ping 间隔 |
| Timeout | 3s | Ping 响应超时 |
| MaxRecvMsgSize | 16MB | 最大接收消息大小 |

**附加服务**: Health Check (`grpc_health_v1`), Reflection (开发调试), Prometheus 指标导出。

**gRPC 实现的 RPC**:

Task: SubmitTask, GetTask, ListTasks, CancelTask, GetQueueStats

CronJob: CreateCronJob, GetCronJob, ListCronJobs, DeleteCronJob

CreateCronJob 中使用 `cronutil.NextRunTime` 计算初始 NextRunAt -- 这是 infrastructure concern，不放在 domain 层。

### HTTP REST

> 源码: `internal/apiserver/interfaces/http/server.go`

**超时配置**:

| 参数 | 值 |
|------|-----|
| ReadTimeout | 10s |
| WriteTimeout | 30s |
| IdleTimeout | 60s |

**路由** (Go 1.22+ `http.ServeMux` 模式匹配):

Task 路由: `POST /api/v1/tasks`, `GET /api/v1/tasks/{id}`, `GET /api/v1/tasks`, `POST /api/v1/tasks/{id}/cancel`, `GET /api/v1/queues/{name}/stats`

CronJob 路由: `POST /api/v1/cronjobs`, `GET /api/v1/cronjobs/{id}`, `GET /api/v1/cronjobs`, `DELETE /api/v1/cronjobs/{id}`

运维路由: `GET /healthz`, `GET /readyz`, `GET /metrics`

### readyz 健康检查

通过 `HealthChecker` 回调函数注入（`cmd/apiserver/main.go` 中定义），检查 Redis Ping + MySQL Ping，3s 超时：

```go
httpServer := apiserverhttp.NewServer(taskSvc, cfg.Server.HTTPAddr, func(ctx context.Context) error {
    if err := redisClient.Ping(ctx).Err(); err != nil {
        return fmt.Errorf("redis: %w", err)
    }
    checkDB, _ := db.DB()
    if err := checkDB.PingContext(ctx); err != nil {
        return fmt.Errorf("mysql: %w", err)
    }
    return nil
})
```

失败时返回 HTTP 503 + `{"status": "not ready", "error": "..."}`。

### HTTP 错误处理

API Server 对客户端隐藏内部错误细节：

```go
if err := s.taskSvc.SubmitTask(r.Context(), task); err != nil {
    log.Errorf("submit task: %v", err)                          // 详细错误写日志
    writeError(w, http.StatusInternalServerError, "failed to submit task") // 通用消息给客户端
    return
}
```

---

## Scheduler（调度器）

> 源码: `cmd/scheduler/main.go`, `internal/scheduler/`

### DDD 分层

| 层 | 文件 | 职责 |
|----|------|------|
| domain | `internal/scheduler/domain/service/scheduler.go` | 纯调度业务逻辑: 补偿、CronJob 触发、Worker 拓扑管理，零基础设施依赖 |
| application | `internal/scheduler/application/scheduler_app_service.go` | 7 个 reconciliation loops 编排，日志/指标注入 |
| infrastructure | `internal/scheduler/infrastructure/election/election.go` | etcd Leader 选举 (Campaign/Resign/Observe) |

### 7 个 Reconciliation Loops

| 循环 | 触发方式 | 周期 | 功能 |
|------|---------|------|------|
| `watchWorkers` | 事件驱动 | - | Watch etcd Worker 变更事件 (Joined/Left/Updated)，更新内存 Worker 列表 |
| `promoteDelayedLoop` | Ticker | 1s | 遍历 `Queues()` 列表，对每个队列调用 `PromoteDelayed(ctx, q, 100)` |
| `healthCheckLoop` | Ticker | 10s | `DetectStaleWorkers(30s)`，日志记录被摘除的 Worker |
| `metricsLoop` | Ticker | 5s | 遍历 `Queues()` 列表，查询 Stats，发布 pending/active/scheduled 到 `metrics.QueueDepth` |
| `compensateLoop` | Ticker | 30s | `CompensateOrphanedTasks(ctx, 30s, 100)` |
| `cronLoop` | Ticker | 1s | `TriggerDueCronJobs(ctx, 100)` (batch size 可通过配置覆盖) |
| `cleanupLoop` | Ticker | 1h | `CleanupTerminalTasks(ctx, 7*24h, 1000)` |

### Domain Service 方法详解

> 源码: `internal/scheduler/domain/service/scheduler.go`

**TriggerDueCronJobs(ctx, limit)**:

1. `cronMaint.FindDueCronJobs(ctx, limit)` -- 查询 MySQL 中 enabled=true 且 next_run_at <= now 的 CronJob
2. 对每个 job，先用 `cronutil.NextRunTime` 计算下次运行时间（无效表达式则跳过）
3. 若 `ConcurrencyPolicy == Forbid`，调用 `taskMaint.HasRunningTasks(ctx, job.Type, job.Namespace)` 检查是否有 Running 任务
   - 有 Running 任务: 跳过触发，但仍推进 NextRunAt
   - 无 Running 任务: 继续触发
4. `job.ToTask()` 创建 Task，设置 ID/State/CreatedAt/UpdatedAt
5. `taskMaint.Create(ctx, task)` 持久化到 MySQL
6. `broker.Enqueue(ctx, task.QueueName, task)` 入队到 Redis
7. 更新 `job.LastRunAt` + `job.NextRunAt`，`cronMaint.UpdateCronJob(ctx, job)`

**CompensateOrphanedTasks(ctx, olderThan, limit)**:

1. `taskMaint.FindStaleByState(ctx, Pending, olderThan, limit)` -- 查询 MySQL
2. 对每个任务调用 `broker.EnqueueIfNotInflight(ctx, queue, task)`:
   - Lua 脚本原子检查 inflight hash → 存在则跳过，不存在则 ZADD ready
3. 成功入队后调用 `taskMaint.TouchUpdatedAt(ctx, id)`:
   - 仅更新 `updated_at`，**不递增 version** (避免 Worker 乐观锁冲突)

**SyncWorkers(ctx)**:

1. `registry.ListWorkers(ctx)` 获取所有 Worker
2. 过滤 `State == Online` 的 Worker 重建内存 map
3. 从每个 Worker 的 `Queues` 字段动态收集队列列表
4. 无 Worker 注册时回退为 `["default"]`

**DetectStaleWorkers(threshold)**:

单锁 (`sync.Mutex`) 批量操作: 遍历 workers map，检查 `LastHeartbeat.Before(threshold)`，一次性删除所有超时 Worker，返回被删除的 ID 列表。

### CronJob 并发策略 (ConcurrencyPolicy)

> 源码: `internal/shared/domain/entity/cronjob.go`

| 策略 | 值 | 行为 |
|------|-----|------|
| `Allow` | `"Allow"` | 默认策略，允许并发执行，每次到期都触发新任务 |
| `Forbid` | `"Forbid"` | 如果该 Type + Namespace 下存在 Running 状态的任务，跳过本次触发 |

Forbid 检查通过 `HasRunningTasks(ctx, taskType, namespace)` 实现，查询 MySQL `WHERE type = ? AND namespace = ? AND state = Running`。

---

## Worker（工作节点）

> 源码: `cmd/worker/main.go`, `internal/worker/`

### Handler 注册

> 源码: `internal/worker/application/service/worker_app_service.go`

**接口方式**:

```go
type Handler interface {
    Handle(ctx context.Context, task *entity.Task) *entity.TaskResult
}
```

**函数方式**:

```go
w.RegisterFunc("email.send", func(ctx context.Context, task *entity.Task) *entity.TaskResult {
    return &entity.TaskResult{Output: "sent"}
})
```

`HandlerFunc` 适配器将普通函数转为 `Handler` 接口。Handler 按 `task.Type` 查找，未找到则直接 `handleFailure`。

### 中间件链

> 源码: `internal/worker/interfaces/middleware/middleware.go`

```go
type Middleware func(Handler) Handler

w.Use(
    middleware.Recovery(),    // 最外层: Panic 恢复
    middleware.Logging(),     // 中间层: 日志记录
    middleware.Timeout(5m),   // 最内层: 超时控制
)
```

应用顺序: 逆序包装 (洋葱模型)，`for i := len(w.mw) - 1; i >= 0; i--`，即先注册的在外层。

| 中间件 | 功能 | 实现细节 |
|--------|------|---------|
| `Recovery` | 捕获 Handler Panic，转为 `TaskResult.Error` | `defer recover()`，返回 `entity.ErrPanic(r)` |
| `Logging` | 记录开始/结束时间、耗时、结果 | `log.With("task_id", ..., "type", ..., "queue", ..., "retry", ...)` |
| `Timeout` | 基于 `context.WithTimeout` 的执行超时 | 优先使用 `task.Timeout.Duration`，回退到 `defaultTimeout`；buffered channel (cap=1) 防止 goroutine 泄漏 |

此外 `processTask` 中还有 `safeHandle` 作为双重 Panic 保护。

### Fetch Loop 与指数退避

```
fetchLoop 伪代码:

const minBackoff = 100ms
const maxBackoff = 2s
backoff = minBackoff

for {
    select ctx.Done: return       // 停机信号

    sem <- struct{}               // 获取信号量 (池满时阻塞 = 背压)

    task, err = broker.Dequeue(ctx, queues)

    if err:
        <-sem                     // 释放信号量
        sleep(1s)                 // 错误时固定等待
        continue

    if task == nil:               // 空队列
        <-sem                     // 释放信号量
        sleep(backoff)            // 指数退避等待
        backoff = min(backoff*2, maxBackoff)
        continue

    backoff = minBackoff          // 拿到任务, 重置退避

    wg.Add(1)
    go processTask(task)          // defer: <-sem, wg.Done()
}
```

### processTask 详细流程

```
processTask(ctx, task):
  1. atomic.AddInt64(&active, 1)           // 递增活跃计数
     metrics.ActiveTasks.Inc()

  2. taskStore.Get(ctx, task.ID)           // MySQL 终态检查
     if latest.IsTerminal():
       broker.Ack() + return              // 跳过已取消/已完成的任务

  3. 查找 handler (按 task.Type)
     if !found: handleFailure() + return

  4. 逆序包装中间件链

  5. task.State = Running
     task.WorkerID = w.cfg.ID
     task.StartedAt = now
     taskStore.Update(ctx, task)           // MySQL UPDATE

  6. result = safeHandle(ctx, handler, task) // Panic 安全执行

  7. metrics.TaskDuration.Observe(duration)

  8. if result.Error != nil:
       handleFailure(ctx, task, err)
         task.RetryCount++
         CanRetry?
           Yes: State=Retrying, Update, Nack (重入队列)
           No:  State=Failed, FinishedAt=now, Update, Ack
     else:
       handleSuccess(ctx, task, result)
         State=Completed, Result=output, FinishedAt=now
         Update, Ack

  9. atomic.AddInt64(&active, -1)           // 递减活跃计数
     metrics.ActiveTasks.Dec()
```

### 心跳上报

每 `HeartbeatInterval` (默认 5s) 发送完整 `*WorkerInfo` 到 etcd:

- **直接 PUT**: 不需要先 GET 再 PUT，整个 WorkerInfo JSON 覆盖写入 etcd key
- **WorkerInfo** 包含: ID, Hostname, Queues, Concurrency, State, ActiveTasks, CPUUsage, MemUsage, LastHeartbeat, Version
- **没有独立的 Heartbeat struct**: `WorkerRegistry.Heartbeat(ctx, *WorkerInfo)` 接口直接接收 `*WorkerInfo`
- **LastHeartbeat 初始化**: 注册时设为 `time.Now()`，避免刚注册就被 Scheduler 判定为 stale
- **version 字段**: 使用 `version.Version` (通过 ldflags 在编译时注入)
- **系统指标**: 使用 `gopsutil` 采集 CPU 和内存使用率

### 优雅停机

```
1. SIGINT/SIGTERM --> signals.SetupSignalContext() 取消 context
2. fetchLoop 检测 ctx.Done(), 退出循环 (不再拉取新任务)
3. 启动 goroutine 等待 wg.Wait() (所有 in-flight 任务完成)
4. select:
     case <-done:    // 所有任务完成
     case <-timeout: // ShutdownTimeout (默认 30s) 到期, 强制退出
5. registry.Deregister(background_ctx, workerID) // 从 etcd 注销
```

`signals.SetupSignalContext()` 支持二次信号强制退出: 第一次 SIGTERM 触发优雅停机, 第二次 SIGTERM 调用 `os.Exit(1)`。

---

## 存储层

### Redis（快速队列）

> 源码: `internal/shared/infrastructure/persistence/redis/queue_broker.go`

实现 `repository.QueueBroker` 接口，使用 Redis Sorted Set + Hash 实现优先级队列。

**Key 结构**:

| Key 模式 | 类型 | 用途 |
|----------|------|------|
| `dispatchhub:queue:{name}:ready` | Sorted Set | 就绪队列，score = -priority |
| `dispatchhub:queue:{name}:delayed` | Sorted Set | 延迟队列，score = 执行时间戳 (毫秒) |
| `dispatchhub:queue:{name}:inflight` | Hash | 正在处理的任务，field = taskID, value = task JSON |
| `dispatchhub:queue:{name}:stats` | Hash | 队列统计计数器 (enqueued, completed, failed) |

**Lua 脚本**:

| 脚本 | 功能 | 原子性保证 |
|------|------|-----------|
| `enqueueWithCapScript` | 容量检查 + ZADD + HINCRBY stats | 入队时不超过最大容量 |
| `dequeueScript` | 遍历多个队列 ZPOPMIN + HSET inflight | 出队 + 移入 inflight 不可分割 |
| `promoteScript` | ZRANGEBYSCORE + ZADD ready + ZREM delayed | 批量晋升延迟任务 (每次最多 batchSize 个) |
| `enqueueIfNotInflightScript` | HEXISTS inflight → 存在则跳过，否则 ZADD ready | 补偿入队时避免重复处理 |

**Ack/Nack 使用 Pipeline** (非 Lua):
- Ack: HDel inflight + HIncrBy stats completed
- Nack: HDel inflight + (有 RetryBackoff? ZADD delayed : ZADD ready)

**Redis 客户端**: `redis.UniversalClient`，支持 Standalone 和 Cluster 模式，通过 `config.RedisConfig.ClusterMode` 切换。

### etcd（协调层）

> 源码: `internal/shared/infrastructure/persistence/etcd/worker_registry.go`

实现 `repository.WorkerRegistry` 接口，提供 Worker 服务注册与发现。

**Key 结构**: `/dispatchhub/workers/{workerID}` -> JSON(WorkerInfo)

| 操作 | etcd 机制 | 说明 |
|------|-----------|------|
| Register | Grant Lease (TTL=15s) + Put with Lease + KeepAlive | 注册并启动自动续约 |
| Deregister | Revoke Lease + Delete key | 主动注销 |
| Heartbeat | Put with existing Lease | 覆盖写入最新 WorkerInfo JSON |
| GetWorker | Get key | 单个 Worker 查询 |
| ListWorkers | Get with prefix | 前缀扫描所有 Worker |
| WatchWorkers | Watch with prefix + WithPrevKV | 事件驱动，区分 Joined (IsCreate) / Updated / Left (Delete) |

**Lease 机制**: Worker 注册时创建 15s TTL 的 Lease，KeepAlive 持续续约。Worker 崩溃后 KeepAlive 停止，15s 后 etcd 自动删除 key。

**Watch 事件处理**: EventTypePut 时，通过 `ev.IsCreate()` 区分 Joined 和 Updated；EventTypeDelete 时，通过 `ev.PrevKv` 获取被删除 Worker 的信息。Watch channel 缓冲区大小为 64，满时丢弃事件并记录警告。

### MySQL（持久层）

> 源码: `internal/shared/infrastructure/persistence/mysql/task_repository.go`, `cronjob_repository.go`

**TaskRepository** 实现以下接口: `TaskReader`, `TaskWriter`, `TaskCompensator`

| 操作 | 实现 | 说明 |
|------|------|------|
| Create | GORM `Create` | 自动生成 CreatedAt/UpdatedAt |
| Get | `WHERE id = ?` | 未找到返回 `(nil, nil)` |
| Update | `WHERE id = ? AND version = ?`，version++ | 乐观锁，RowsAffected=0 则冲突 |
| List | 多条件过滤 + `ORDER BY priority DESC, created_at ASC` | 支持 Namespace/Group/Type/State/QueueName/WorkerID 过滤，默认 limit=100 |
| FindStaleByState | `WHERE state = ? AND updated_at < ?` | 补偿循环使用 |
| TouchUpdatedAt | `UPDATE updated_at = now WHERE id = ?` | 仅更新时间，不加 version |
| HasRunningTasks | `COUNT WHERE type = ? AND namespace = ? AND state = Running` | CronJob Forbid 策略检查 |
| DeleteTerminalOlderThan | `DELETE WHERE state IN (...) AND finished_at < ?` | 清理终态任务 |

**CronJobRepository** 实现 `CronJobReader` + `CronJobWriter`:

| 操作 | 实现 |
|------|------|
| CreateCronJob | GORM `Create` |
| GetCronJob | `WHERE id = ?` |
| UpdateCronJob | GORM `Save` (全字段更新) |
| DeleteCronJob | `WHERE id = ?` |
| ListCronJobs | 按 namespace 过滤，`ORDER BY created_at DESC`，默认 limit=100 |
| FindDueCronJobs | `WHERE enabled = true AND next_run_at IS NOT NULL AND next_run_at <= now` |

**AutoMigrate**: 两个 Repository 在构造时自动执行 `db.AutoMigrate`，根据 struct tag 创建表和索引。

---

## 共享实体

### Task（8 个状态）

> 源码: `internal/shared/domain/entity/task.go`

```
Pending --> Running --> Completed
  |           |
  |           +--> Retrying --> Running (重试)
  |           |
  |           +--> Failed (重试耗尽)
  |           |
  |           +--> Timeout
  |
  +--> Cancelled (用户取消)
  |
  +--> Scheduled (预留状态)
```

终态: Completed, Failed, Cancelled, Timeout (`IsTerminal() == true`)

**Labels**: `map[string]string`，实现 `driver.Valuer` / `sql.Scanner`，在 MySQL 中存储为 JSON TEXT。

**Duration**: 包装 `time.Duration`，实现 JSON 序列化 (字符串格式如 "5m")，实现 `driver.Valuer` / `sql.Scanner` (MySQL 中存储为纳秒 int64)。

### CronJob（定时任务）

> 源码: `internal/shared/domain/entity/cronjob.go`

关键字段:

- `CronExpr`: 5 字段 cron 表达式 + 描述符 (如 `@every 5m`)
- `ConcurrencyPolicy`: `Allow` (默认) 或 `Forbid`
- `NextRunAt` / `LastRunAt`: Scheduler cronLoop 读写
- `ToTask()`: 从 CronJob spec 创建新 Task 实例

### WorkerInfo（Worker 信息）

> 源码: `internal/shared/domain/entity/worker.go`

没有独立的 Heartbeat struct。WorkerInfo 直接包含所有心跳数据:

- ID, Hostname, IP, Port
- State: Online / Draining / Offline
- Queues, Concurrency
- ActiveTasks, CompletedTotal, FailedTotal
- CPUUsage, MemUsage
- StartedAt, LastHeartbeat
- Version

### QueueStats（队列统计）

> 源码: `internal/shared/domain/entity/queue.go`

字段: Name, Pending, Active, Scheduled, Retrying, Completed, Failed

`DefaultQueueName = "default"`

---

## 共享仓储接口

> 源码: `internal/shared/domain/repository/`

| 接口 | 组合 | 关键方法 |
|------|------|---------|
| `TaskReader` | - | Get, List |
| `TaskWriter` | - | Create, Update |
| `TaskStore` | TaskReader + TaskWriter | (API Server 和 Worker 使用) |
| `TaskCompensator` | - | FindStaleByState, TouchUpdatedAt, HasRunningTasks, DeleteTerminalOlderThan |
| `QueueBroker` | - | Enqueue, EnqueueDelayed, Dequeue, Ack, Nack, PromoteDelayed(batchSize), Len, Stats, EnqueueIfNotInflight |
| `CronJobReader` | - | GetCronJob, ListCronJobs, FindDueCronJobs |
| `CronJobWriter` | - | CreateCronJob, UpdateCronJob, DeleteCronJob |
| `CronJobStore` | CronJobReader + CronJobWriter | (API Server 和 Scheduler 使用) |
| `WorkerRegistry` | - | Register, Deregister, Heartbeat(*WorkerInfo), GetWorker, ListWorkers, WatchWorkers |

---

## 共享基础设施

### persistence/factory.go

> 源码: `internal/shared/infrastructure/persistence/factory.go`

三个工厂函数，由各 `cmd/*/main.go` 按需调用:

| 函数 | 返回类型 | 使用方 |
|------|---------|--------|
| `NewRedisClient(cfg.Redis)` | `redis.UniversalClient` | API Server, Scheduler, Worker |
| `NewMySQLDB(cfg.MySQL)` | `*gorm.DB` | API Server, Scheduler, Worker |
| `NewEtcdClient(cfg.Etcd)` | `*clientv3.Client` | Scheduler, Worker (API Server 不使用) |

### config

> 源码: `internal/shared/infrastructure/config/config.go`

YAML 配置加载 + 环境变量覆盖:

1. `DefaultConfig()` 提供所有默认值
2. `LoadFromFile(path)` 解析 YAML 并合并到默认配置
3. `applyEnvOverrides(cfg)` 从环境变量覆盖敏感字段

支持的环境变量:

| 环境变量 | 覆盖字段 |
|---------|---------|
| `DISPATCH_GRPC_ADDR` | `Server.GRPCAddr` |
| `DISPATCH_HTTP_ADDR` | `Server.HTTPAddr` |
| `DISPATCH_REDIS_ADDR` | `Redis.Addr` |
| `DISPATCH_REDIS_PASSWORD` | `Redis.Password` |
| `DISPATCH_MYSQL_DSN` | `MySQL.DSN` |
| `DISPATCH_ETCD_ENDPOINTS` | `Etcd.Endpoints` (逗号分隔) |
| `DISPATCH_LOG_LEVEL` | `Log.Level` |

### version

> 源码: `internal/shared/infrastructure/version/version.go`

通过 ldflags 注入: `Version`, `GitCommit`, `BuildDate`。Worker 心跳使用 `version.Version` 标识版本。

---

## 工具包 (pkg/)

### log（结构化日志）

> 源码: `pkg/log/logger.go`

基于 Uber Zap，全局单例 `SugaredLogger`:

- **格式**: JSON (默认) 或 Console
- **级别**: Debug / Info / Warn / Error / Fatal
- **输出**: stdout (默认) / stderr / 文件路径
- **API**: `log.Info()`, `log.Infof()`, `log.With("key", "value")`, `log.Sync()`

### metrics（Prometheus 指标）

> 源码: `pkg/metrics/metrics.go`

使用 `promauto` 自动注册，预定义指标:

| 指标 | 类型 | Labels | 说明 |
|------|------|--------|------|
| `dispatchhub_scheduler_tasks_submitted_total` | Counter | queue, type, priority | 提交的任务总数 |
| `dispatchhub_scheduler_leader_elections_total` | Counter | - | Leader 选举次数 |
| `dispatchhub_worker_tasks_processed_total` | Counter | queue, type, status | 处理的任务总数 |
| `dispatchhub_worker_task_duration_seconds` | Histogram | queue, type | 任务执行耗时 |
| `dispatchhub_worker_active_count` | Gauge | - | 当前活跃 Worker 数 |
| `dispatchhub_worker_active_tasks` | Gauge | queue | 当前正在处理的任务数 |
| `dispatchhub_worker_heartbeats_total` | Counter | worker | 心跳发送总数 |
| `dispatchhub_queue_depth` | Gauge | queue, state | 队列深度 (pending/active/scheduled) |

### ratelimit（限流器）

> 源码: `pkg/ratelimit/ratelimit.go`

令牌桶算法:

- `Limiter`: 单队列限流器，支持 `Allow()` (非阻塞) 和 `Wait(ctx)` (阻塞)
- `MultiQueueLimiter`: 管理多个队列的限流器实例，按队列名懒创建
- API Server 的 BeforeSubmit hook 中使用: `limiter.Allow(task.QueueName)`

### cronutil（cron 表达式）

> 源码: `pkg/cronutil/cronutil.go`

基于 `robfig/cron/v3`，支持 5 字段标准 cron + 描述符 (如 `@every 5m`, `@hourly`):

- `NextRunTime(expr, after)`: 解析 cron 表达式并返回 `after` 之后的下一次执行时间
- 在 HTTP/gRPC handler 中计算 CronJob 初始 NextRunAt
- 在 Scheduler `TriggerDueCronJobs` 中推进 NextRunAt

### signals（信号处理）

> 源码: `pkg/signals/signals.go`

`SetupSignalContext()` 返回一个在 SIGINT/SIGTERM 时取消的 context:

- 第一次信号: 取消 context，触发优雅停机
- 第二次信号: `os.Exit(1)` 强制退出
- 所有三个服务 (`cmd/*/main.go`) 都使用此函数
