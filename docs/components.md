# 核心组件

## 概览

DispatchHub 由三个可独立部署的二进制组件和三层基础设施组成，采用 **DDD（领域驱动设计）** 分层架构：

| 组件 | 二进制 | 职责 | 对外 API |
|------|--------|------|----------|
| API Server | `cmd/apiserver` | 无状态 API 网关，**系统唯一外部入口** | HTTP REST + gRPC + healthz/readyz/metrics |
| Scheduler | `cmd/scheduler` | 任务调度控制面 | 仅 healthz/readyz/metrics（运维端点） |
| Worker | `cmd/worker` | 任务执行数据面 | 仅 healthz/readyz/metrics（运维端点） |

## DDD 分层架构

项目按**服务优先 + DDD 四层**组织，每个服务在 `internal/` 下有独立目录，通过 `shared/` 共享领域模型和基础设施：

```
internal/
  shared/                              # 跨服务共享
    domain/
      entity/                          # 实体 & 值对象 (Task, Worker, Queue)
      repository/                      # 仓储接口 (TaskRepository, QueueBroker, WorkerRegistry)
      service/                         # 服务接口 (TaskService) + 基础实现 (TaskServiceImpl)
    infrastructure/
      config/                          # 配置管理
      version/                         # 版本信息
      persistence/{mysql,redis,etcd}/  # 仓储实现

  apiserver/                           # API Server 服务
    interfaces/{http,grpc}/            # HTTP REST + gRPC 接口

  scheduler/                           # Scheduler 服务
    domain/service/                    # 调度领域服务 (SchedulerService)
    application/                       # 应用编排 (reconciliation loops)
    infrastructure/election/           # etcd Leader 选举

  worker/                              # Worker 服务
    application/service/               # Worker 执行引擎 (WorkerAppService)
    interfaces/middleware/             # 中间件 (Recovery/Logging/Timeout)
```

**依赖规则**：
- `cmd/apiserver` → `internal/apiserver` + `internal/shared`
- `cmd/scheduler` → `internal/scheduler` + `internal/shared`
- `cmd/worker` → `internal/worker` + `internal/shared`
- **三个服务之间零交叉引用**
- **domain 层零基础设施依赖**（日志/指标通过 hook 在 application 层注入）

---

## API Server（接入网关）

> 源码：`cmd/apiserver/main.go`、`internal/apiserver/interfaces/grpc/server.go`、`internal/apiserver/interfaces/http/server.go`

### 职责

1. **系统唯一的外部入口**，对外提供 HTTP REST 和 gRPC 两种 API
2. 暴露 Prometheus 指标端点和健康检查
3. 通过 `shared/domain/service/TaskServiceImpl` 处理任务管理请求

### 特点

- **无状态**：不参与 Leader 选举，不运行调度循环
- **可水平扩展**：部署任意数量副本，前面放 LoadBalancer/Ingress
- **与 Scheduler 解耦**：依赖 `shared/domain/service` 中的 `TaskService` 接口和 `TaskServiceImpl` 实现，不引用 Scheduler 任何代码

### SubmitTask 流程

> 源码：`internal/shared/domain/service/task_service_impl.go`

```go
func (s *TaskServiceImpl) SubmitTask(ctx context.Context, task *entity.Task) error
```

1. **设置默认值**：ID（UUID）、QueueName（"default"）、Priority（5）、MaxRetries（3）、Timeout（5m）
2. **设置状态**：State = Pending，CreatedAt/UpdatedAt = now
3. **持久化**：`taskRepo.Create(ctx, task)` → MySQL INSERT
4. **入队**：根据是否有 Delay/ScheduleAt 选择 `Enqueue` 或 `EnqueueDelayed`
5. **回调**：通过 `TaskSubmittedHook` 在 application 层执行日志记录和指标递增（domain 层本身不依赖 log/metrics）

### gRPC Server

拦截器链：

| 拦截器 | 功能 |
|--------|------|
| `grpc_prometheus.UnaryServerInterceptor` | Prometheus 请求指标 |
| `grpc_recovery.UnaryServerInterceptor` | Panic 恢复 |
| `loggingUnaryInterceptor` | 请求日志（方法、状态码、耗时） |

Keepalive 配置：

| 参数 | 值 | 含义 |
|------|-----|------|
| MaxConnectionIdle | 5m | 空闲连接最大存活时间 |
| MaxConnectionAge | 30m | 连接最大存活时间 |
| Time | 10s | Keepalive ping 间隔 |
| Timeout | 3s | Ping 响应超时 |
| MaxRecvMsgSize | 16MB | 最大接收消息大小 |

附加服务：Health Check、Reflection（开发调试）、Prometheus 指标导出。

### HTTP Server

超时配置：

| 参数 | 值 |
|------|-----|
| ReadTimeout | 10s |
| WriteTimeout | 30s |
| IdleTimeout | 60s |

---

## Scheduler（调度器）

> 源码：`internal/scheduler/domain/service/scheduler.go`、`internal/scheduler/application/scheduler_app_service.go`、`internal/scheduler/infrastructure/election/election.go`

### 职责

1. 周期性晋升延迟队列中到期的任务到就绪队列
2. 补偿扫描 MySQL 中的孤儿 Pending 任务（MySQL 写成功但 Redis 入队失败），重新入队
3. 监控 Worker 拓扑变化（注册/注销）
4. 健康检查，摘除超过 30s 无心跳的 Worker
5. 定期发布队列深度等 Prometheus 指标
6. 仅暴露运维端点（`/healthz`、`/readyz`、`/metrics`），**不对外暴露任务管理 API**

### DDD 分层

| 层 | 文件 | 职责 |
|----|------|------|
| domain | `scheduler.go` | 纯调度业务逻辑：Worker 拓扑管理、孤儿任务补偿，零基础设施依赖 |
| application | `scheduler_app_service.go` | reconciliation loops 编排，日志/指标注入（通过 hook） |
| infrastructure | `election/election.go` | etcd Leader 选举实现 |

### Leader 选举

Scheduler 通过 etcd 实现 Leader 选举，**同一时刻只有一个 Leader 运行调度循环**。

```go
// internal/scheduler/infrastructure/election/election.go
type Config struct {
    Client           *clientv3.Client
    ElectionPrefix   string                    // "/dispatchhub/scheduler/leader"
    ID               string                    // 候选人唯一标识
    TTL              int                       // Lease TTL, 默认 15s
    OnStartedLeading func(ctx context.Context) // 成为 Leader 时的回调
    OnStoppedLeading func()                    // 失去 Leader 时的回调
    OnNewLeader      func(identity string)     // 新 Leader 通知
}
```

选举流程：

1. 创建 etcd Session（带 TTL 的 Lease）
2. 调用 `election.Campaign()` 参与竞选，**阻塞直到当选**
3. 当选后执行 `OnStartedLeading` 回调，启动调度循环
4. Session 到期或被取消时执行 `OnStoppedLeading`，主动 Resign
5. 失败后自动重试竞选（3s 间隔）

### 内部循环

Scheduler Leader 启动后并发运行以下循环：

| 循环 | 周期 | 功能 |
|------|------|------|
| `promoteDelayedLoop` | 1s | 扫描延迟队列，将到期任务移入就绪队列 |
| `healthCheckLoop` | 10s | 检测 Worker 心跳，摘除超时节点 |
| `metricsLoop` | 5s | 采集队列深度指标发布到 Prometheus |
| `watchWorkers` | 事件驱动 | Watch etcd Worker 变更事件，更新 Worker 列表 |
| `compensateLoop` | 30s | 扫描 MySQL 孤儿 Pending 任务，安全重新入队 |

### SchedulerService 方法

```
SchedulerService
  ├── CompensateOrphanedTasks()  # 扫描+重新入队孤儿任务（原子检查 inflight）
  ├── SyncWorkers()              # 从 etcd 刷新 Worker 列表
  ├── HandleWorkerEvent()        # 处理 Worker 加入/离开/更新
  ├── DetectStaleWorkers()       # 检测心跳超时的 Worker
  └── Queues()/Broker()/Registry()
```

---

## Worker（工作节点）

> 源码：`internal/worker/application/service/worker_app_service.go`、`internal/worker/interfaces/middleware/middleware.go`

### 职责

1. 启动时向 etcd 注册，声明支持的队列和并发度
2. 从 Redis 队列拉取任务，在协程池中执行
3. 定期发送心跳（CPU、内存、活跃任务数）
4. 处理结果上报：成功 → Ack；失败 → Nack（重试或标记失败）
5. 收到停机信号后进入 Draining 模式，等待 in-flight 任务完成
6. 仅暴露运维端点（`/healthz`、`/readyz`、`/metrics`），**不对外暴露任务管理 API**

### Handler 注册

```go
// 接口方式
type Handler interface {
    Handle(ctx context.Context, task *entity.Task) *entity.TaskResult
}

// 函数方式
w.RegisterFunc("email.send", func(ctx context.Context, task *entity.Task) *entity.TaskResult {
    // 业务逻辑
    return &entity.TaskResult{Output: "sent"}
})
```

### 中间件链

```go
type Middleware func(Handler) Handler

// 应用顺序（洋葱模型，先注册的在外层）
w.Use(
    middleware.Recovery(),   // 最外层: Panic 恢复
    middleware.Logging(),    // 中间层: 日志记录
    middleware.Timeout(5m),  // 最内层: 超时控制
)
```

| 中间件 | 功能 | 源码 |
|--------|------|------|
| `Recovery` | 捕获 Handler Panic，转为 TaskResult.Error | `internal/worker/interfaces/middleware/middleware.go` |
| `Logging` | 记录任务开始/结束时间、耗时、结果 | `internal/worker/interfaces/middleware/middleware.go` |
| `Timeout` | 基于 context.WithTimeout 的执行超时控制 | `internal/worker/interfaces/middleware/middleware.go` |

### 背压机制

```go
// 基于 channel semaphore 的协程池
sem := make(chan struct{}, concurrency) // 容量 = 最大并发数

// fetchLoop 中
case w.sem <- struct{}{}: // 获取令牌, 池满时阻塞 → 停止从 Redis 出队
    // ...
    go func() {
        defer func() { <-w.sem }() // 完成后释放令牌
        w.processTask(ctx, task)
    }()
```

这种设计使得当所有协程都在忙时，Worker 自动停止从 Redis 拉取任务，形成天然的背压保护。

### 任务执行流程

```
processTask(task):
  1. atomic.Add(&active, 1)           # 递增活跃计数
  2. 查找 Handler (按 task.Type)
  3. 应用中间件链 (逆序包装)
  4. 更新任务状态 → Running
  5. 创建超时 context
  6. safeHandle(ctx, handler, task)    # Panic 安全执行
  7. 记录耗时指标
  8. 成功 → handleSuccess()           # State=Completed, Ack
     失败 → handleFailure()           # CanRetry? Nack : State=Failed
  9. atomic.Add(&active, -1)          # 递减活跃计数
```

### 心跳上报

每 `HeartbeatInterval`（默认 5s）向 etcd 发送心跳：

```go
type Heartbeat struct {
    WorkerID    string
    State       WorkerState // Online / Draining
    ActiveTasks int         // 当前活跃任务数
    CPUUsage    float64     // 系统 CPU 使用率
    MemUsage    float64     // 系统内存使用率
    Timestamp   time.Time
}
```

心跳更新 etcd 中 Worker 的信息（保持 Lease 有效），同时被 Scheduler 用于健康检查。

### 优雅停机

1. 收到 SIGINT/SIGTERM，context 被取消
2. fetchLoop 退出（不再拉取新任务）
3. 等待 WaitGroup（in-flight 任务完成）
4. 超时保护：最长等待 `ShutdownTimeout`（默认 30s）
5. 调用 `registry.Deregister()` 从 etcd 注销

---

## 存储层

### Redis（快速队列）

> 源码：`internal/shared/infrastructure/persistence/redis/queue_broker.go`

实现 `repository.QueueBroker` 接口，使用 Sorted Set 作为优先级队列。

详见 [队列设计](queue-design.md)。

### etcd（协调层）

> 源码：`internal/shared/infrastructure/persistence/etcd/worker_registry.go`

实现 `repository.WorkerRegistry` 接口，提供 Worker 服务注册与发现。

| 功能 | etcd 机制 |
|------|-----------|
| Worker 注册 | PUT key with Lease |
| Worker 注销 | Revoke Lease / DELETE key |
| 自动注销 | Lease TTL 到期（15s），etcd 自动删除 key |
| 心跳维持 | KeepAlive 持续续约 Lease |
| 拓扑变更通知 | Watch prefix with PrevKV |
| Leader 选举 | concurrency.Election (Campaign/Resign/Observe) |

Key 结构：`/dispatchhub/workers/{workerID}` → JSON(WorkerInfo)

### MySQL（持久层）

> 源码：`internal/shared/infrastructure/persistence/mysql/task_repository.go`

实现 `repository.TaskRepository` 接口。

| 功能 | 实现 |
|------|------|
| 创建任务 | GORM Create，自动生成 CreatedAt/UpdatedAt |
| 更新任务 | 乐观锁：WHERE version = old_version，Version 自增 |
| 查询任务 | 支持 Namespace/Group/Type/State/Queue/Worker 过滤 |
| 排序 | priority DESC, created_at ASC |
| 自动建表 | AutoMigrate 根据 struct tag 创建表和索引 |

---

## 辅助包

### log（结构化日志）

> 源码：`pkg/log/logger.go`

基于 Uber Zap，支持 JSON/Console 格式，Debug/Info/Warn/Error 级别，输出到 stdout/stderr/文件。

### metrics（Prometheus 指标）

> 源码：`pkg/metrics/metrics.go`

预定义指标覆盖 Scheduler（提交数、调度延迟）、Worker（处理数、执行耗时、活跃数）、Queue（队列深度）。

详见 [配置参考](configuration.md) 中的指标列表。

### ratelimit（限流器）

> 源码：`pkg/ratelimit/ratelimit.go`

令牌桶算法，支持按队列独立限速，`MultiQueueLimiter` 管理多个限流器实例。

### signals（信号处理）

> 源码：`pkg/signals/signals.go`

封装 SIGINT/SIGTERM 处理：首次信号触发优雅停机，二次信号强制退出。
