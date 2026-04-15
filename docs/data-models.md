# 数据模型

## Task（任务）

> 源码：`internal/shared/domain/entity/task.go`

Task 是系统的核心领域对象，代表一个待执行的工作单元。

### 字段定义

#### 标识字段

| 字段 | 类型 | GORM Tag | 说明 |
|------|------|----------|------|
| `ID` | string | `primaryKey;size:64` | 任务唯一标识，UUID 自动生成 |
| `Name` | string | `index;size:255` | 任务可读名称 |
| `Namespace` | string | `index;size:128` | 命名空间，用于多租户隔离 |
| `Group` | string | `index;size:128` | 逻辑分组，用于亲和性调度 |

#### 载荷字段

| 字段 | 类型 | GORM Tag | 说明 |
|------|------|----------|------|
| `Type` | string | `index;size:128` | Handler 类型标识，如 `"email.send"`、`"report.generate"` |
| `Payload` | json.RawMessage | `type:text` | 任意 JSON 载荷，由 Handler 解析 |
| `Labels` | Labels (map[string]string) | `type:text` | K8s 风格标签，实现了 `driver.Valuer` / `sql.Scanner` 接口，GORM 自动 JSON 序列化 |

#### 调度字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `Priority` | TaskPriority (int) | 优先级，1-10，越大越优先 |
| `Delay` | Duration | 入队后延迟执行的时长 |
| `ScheduleAt` | *time.Time | 绝对执行时间点 |
| `Timeout` | Duration | 单次执行超时时长，默认 5 分钟 |

> Go entity 中不包含 `cron_expr` 和 `deadline` 字段。schema.sql DDL 中虽预留了这两列，但 GORM struct 未映射。

#### 重试字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `MaxRetries` | int | 最大重试次数，默认 3 |
| `RetryCount` | int | 已重试次数 |
| `RetryBackoff` | Duration | 重试退避基础时长。为 0 时立即重试（回到 ready 队列），大于 0 时延迟重试（回到 delayed 队列） |

#### 状态字段

| 字段 | 类型 | GORM Tag | 说明 |
|------|------|----------|------|
| `State` | TaskState (int) | `index` | 当前生命周期状态 |
| `Result` | string | `type:text` | 成功时的输出结果 |
| `Error` | string | `type:text` | 失败时的错误信息 |
| `WorkerID` | string | `index;size:128` | 当前或最后执行的 Worker |
| `QueueName` | string | `index;size:128` | 所属队列名称，默认 `"default"` |

#### 元数据字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `CreatedAt` | time.Time | 创建时间（GORM autoCreateTime） |
| `UpdatedAt` | time.Time | 更新时间（GORM autoUpdateTime） |
| `StartedAt` | *time.Time | 开始执行时间 |
| `FinishedAt` | *time.Time | 完成时间 |
| `Version` | int64 | 乐观锁版本号，每次 `Update` 调用 +1 |

### 状态机

Task 具有 8 种生命周期状态：

```
                                 ┌──────────────────┐
                                 │                  │
                                 ▼                  │
 ┌─────────┐    ┌───────────┐    ┌─────────┐    ┌──┴──────┐
 │ Pending  │───▶│ Scheduled │───▶│ Running │───▶│Retrying │
 │  (0)     │    │   (1)     │    │  (2)    │    │  (3)    │
 └────┬─────┘    └───────────┘    └────┬────┘    └─────────┘
      │                                │
      │                          ┌─────┴──────┬────────────┐
      │                          │            │            │
      │                     ┌────▼────┐  ┌────▼────┐  ┌────▼────┐
      │                     │Completed│  │ Failed  │  │ Timeout │
      │                     │   (4)   │  │  (5)    │  │  (7)    │
      │                     └─────────┘  └─────────┘  └─────────┘
      │
      │         ┌───────────┐
      └────────▶│ Cancelled │
                │   (6)     │
                └───────────┘
```

#### 状态说明

| 状态 | 值 | 说明 | 终态 |
|------|-----|------|------|
| `Pending` | 0 | 已提交，等待调度 | 否 |
| `Scheduled` | 1 | 已分配给 Worker（预留） | 否 |
| `Running` | 2 | 正在执行中 | 否 |
| `Retrying` | 3 | 执行失败，等待重试 | 否 |
| `Completed` | 4 | 执行成功完成 | **是** |
| `Failed` | 5 | 用尽所有重试，最终失败 | **是** |
| `Cancelled` | 6 | 被用户主动取消 | **是** |
| `Timeout` | 7 | 执行超时 | **是** |

#### 状态转换规则

| 来源状态 | 目标状态 | 触发条件 |
|----------|----------|----------|
| Pending | Running | Worker Dequeue 后调用 `taskStore.Update`，设置 `State=Running, WorkerID, StartedAt` |
| Pending | Cancelled | 用户调用 `CancelTask`，`TaskServiceImpl` 设置终态 |
| Running | Completed | Handler 返回 `result.Error == nil` |
| Running | Retrying | Handler 返回错误且 `task.CanRetry() == true`（`RetryCount < MaxRetries`） |
| Running | Failed | Handler 返回错误且 `task.CanRetry() == false` |
| Running | Timeout | context 超时，中间件捕获 |
| Retrying | Running | 重试间隔到期（delayed 队列晋升后），Worker 重新拉取执行 |

#### 判断方法

```go
// 是否处于终态（不可再变更）
func (t *Task) IsTerminal() bool {
    // Completed / Failed / Cancelled / Timeout
}

// 是否可以重试
func (t *Task) CanRetry() bool {
    return t.RetryCount < t.MaxRetries && !t.IsTerminal()
}
```

Worker 在 processTask 中会先通过 `taskStore.Get` 检查最新状态，如果任务已处于终态则直接 Ack 跳过执行。

### TaskPriority（优先级）

| 常量 | 值 | 说明 |
|------|-----|------|
| `PriorityLow` | 1 | 低优先级，空闲时处理 |
| `PriorityDefault` | 5 | 默认优先级 |
| `PriorityHigh` | 8 | 高优先级，优先处理 |
| `PriorityCritical` | 10 | 最高优先级，立即处理 |

在 Redis 中使用负值作为 score（-10 < -8 < -5 < -1），`ZPOPMIN` 取出 score 最小的即优先级最高的。

---

## CronJob（定时任务）

> 源码：`internal/shared/domain/entity/cronjob.go`

CronJob 定义周期性执行的任务模板。每次触发时调用 `ToTask()` 生成一个独立的 Task 实例入队。

### 字段定义

| 字段 | 类型 | 说明 |
|------|------|------|
| `ID` | string | 定时任务唯一标识，UUID |
| `Name` | string | 任务名称 |
| `Namespace` | string | 命名空间 |
| `Type` | string | Handler 类型 |
| `Payload` | json.RawMessage | 载荷模板，每次触发时复制到新 Task |
| `Labels` | Labels | 标签，同样复制到新 Task |
| `CronExpr` | string | Cron 表达式（5/6 字段格式） |
| `QueueName` | string | 目标队列，默认 `"default"` |
| `Priority` | TaskPriority | 优先级 |
| `Timeout` | Duration | 执行超时 |
| `MaxRetries` | int | 最大重试次数 |
| `RetryBackoff` | Duration | 重试退避时长 |
| `ConcurrencyPolicy` | ConcurrencyPolicy | 并发策略：`Allow` 或 `Forbid` |
| `Enabled` | bool | 是否启用 |
| `LastRunAt` | *time.Time | 上次执行时间 |
| `NextRunAt` | *time.Time | 下次执行时间 |
| `CreatedAt` | time.Time | 创建时间 |
| `UpdatedAt` | time.Time | 更新时间 |

### ConcurrencyPolicy（并发策略）

| 策略 | 行为 |
|------|------|
| `Allow` | 默认。允许并发执行，即使上次触发的任务仍在 Running 也投递新任务 |
| `Forbid` | 跳过本次触发，如果同 type + namespace 下存在 Running 态任务（通过 `TaskCompensator.HasRunningTasks` 查询） |

### ToTask() 方法

```go
func (c *CronJob) ToTask() *Task {
    return &Task{
        Name:         c.Name,
        Namespace:    c.Namespace,
        Type:         c.Type,
        Payload:      c.Payload,
        Labels:       c.Labels,
        QueueName:    c.QueueName,
        Priority:     c.Priority,
        Timeout:      c.Timeout,
        MaxRetries:   c.MaxRetries,
        RetryBackoff: c.RetryBackoff,
    }
}
```

生成的 Task 不包含 ID（由 `TaskServiceImpl.SubmitTask` 填充 UUID）和时间字段（由提交流程自动设置）。

---

## TaskFilter（查询过滤器）

用于 `ListTasks` 接口的查询条件。

| 字段 | 类型 | 说明 |
|------|------|------|
| `Namespace` | string | 按命名空间过滤 |
| `Group` | string | 按分组过滤 |
| `Type` | string | 按 Handler 类型过滤 |
| `State` | *TaskState | 按状态过滤（指针类型，nil 表示不过滤） |
| `Labels` | map[string]string | 按标签选择器过滤 |
| `QueueName` | string | 按队列名过滤 |
| `WorkerID` | string | 按 Worker 过滤 |
| `Limit` | int | 分页大小（默认 100） |
| `Offset` | int | 分页偏移 |

结果排序：`priority DESC, created_at ASC`（高优先级 + 先进先出）。

---

## TaskResult（执行结果）

Handler 处理完成后返回的结果。

```go
type TaskResult struct {
    Output string // 成功时的输出内容
    Error  error  // 失败时的错误（nil 表示成功）
}
```

Worker 根据 `Error` 是否为 nil 决定走 `handleSuccess` 还是 `handleFailure` 路径。

---

## Labels（标签）

K8s 风格的键值对标签，附加在 Task 和 CronJob 上用于分类和过滤。

```go
type Labels map[string]string
```

实现了 `driver.Valuer` 和 `sql.Scanner` 接口，在 MySQL 中以 JSON TEXT 存储：

```go
// Value: marshal 为 JSON string 写入 MySQL
func (l Labels) Value() (driver.Value, error)

// Scan: 从 MySQL 读取后 unmarshal 回 map
func (l *Labels) Scan(value any) error
```

示例值：

```json
{
    "env": "production",
    "team": "platform",
    "priority-class": "batch"
}
```

---

## Duration（时长包装器）

包装 `time.Duration`，支持双向序列化：

- **JSON**：字符串格式（`"5m"`、`"30s"`、`"1h2m3s"`），通过 `MarshalJSON` / `UnmarshalJSON` 实现
- **MySQL**：BIGINT 纳秒（`300000000000`），通过 `driver.Valuer` / `sql.Scanner` 实现

```go
// JSON 中使用字符串格式
{"timeout": "5m", "delay": "30s", "retry_backoff": "1s"}

// MySQL 中存储为 BIGINT 纳秒
// 5m → 300000000000
// 30s → 30000000000
```

支持所有 Go 标准 duration 格式：`"300ms"`、`"1.5s"`、`"2m30s"`、`"1h"`。

---

## Worker 相关模型

### WorkerInfo（Worker 信息）

> 源码：`internal/shared/domain/entity/worker.go`

描述一个注册在集群中的 Worker 节点。存储在 etcd 中，JSON 序列化。

| 字段 | 类型 | JSON Key | 说明 |
|------|------|----------|------|
| `ID` | string | `id` | Worker 唯一标识，格式 `{hostname}-{uuid8}` |
| `Hostname` | string | `hostname` | 主机名 |
| `IP` | string | `ip` | IP 地址 |
| `Port` | int | `port` | 服务端口 |
| `State` | WorkerState | `state` | 当前状态 |
| `Labels` | map[string]string | `labels` | 节点标签，如 `"gpu": "true"` |
| `Queues` | []string | `queues` | 订阅的队列列表 |
| `Concurrency` | int | `concurrency` | 最大并发任务数 |
| `ActiveTasks` | int | `active_tasks` | 当前活跃任务数（心跳时更新） |
| `CompletedTotal` | int64 | `completed_total` | 累计完成任务数 |
| `FailedTotal` | int64 | `failed_total` | 累计失败任务数 |
| `CPUUsage` | float64 | `cpu_usage` | CPU 使用率 (%) |
| `MemUsage` | float64 | `mem_usage` | 内存使用率 (%) |
| `StartedAt` | time.Time | `started_at` | 启动时间 |
| `LastHeartbeat` | time.Time | `last_heartbeat` | 最后心跳时间 |
| `Version` | string | `version` | 程序版本号 |

> `WorkerRegistry.Heartbeat` 方法的参数是 `*entity.WorkerInfo`，不使用独立的心跳结构。Worker 每次心跳更新 `ActiveTasks`、`CPUUsage`、`MemUsage`、`LastHeartbeat` 等字段后，将完整 WorkerInfo 覆写到 etcd。

### WorkerState（Worker 状态）

```
  ┌────────┐     drain      ┌──────────┐
  │ Online │───────────────▶│ Draining │
  │  (0)   │                │   (1)    │
  └────┬───┘                └────┬─────┘
       │                         │
       │  crash/timeout          │  tasks done
       │                         │
       ▼                         ▼
  ┌─────────┐              ┌──────────┐
  │ Offline │◀─────────────│ Offline  │
  │  (2)    │              │   (2)    │
  └─────────┘              └──────────┘
```

| 状态 | 值 | 说明 |
|------|-----|------|
| `Online` | 0 | 正常工作，接受新任务 |
| `Draining` | 1 | 停止接受新任务，等待 in-flight 任务完成 |
| `Offline` | 2 | 离线 |

### WorkerEvent（Worker 事件）

> 源码：`internal/shared/domain/repository/worker_registry.go`

```go
type WorkerEvent struct {
    Type     WorkerEventType  // Joined(0) / Left(1) / Updated(2)
    WorkerID string
    Worker   *WorkerInfo      // Left 事件时通过 PrevKv 获取，可能为 nil
}
```

由 `WorkerRegistry.WatchWorkers` 返回的 channel 推送。channel 缓冲区大小为 64，满时丢弃事件（记录 warning 日志）。

---

## Queue 相关模型

### QueueStats（队列统计）

> 源码：`internal/shared/domain/entity/queue.go`

| 字段 | 类型 | 说明 | 数据来源 |
|------|------|------|----------|
| `Name` | string | 队列名称 | - |
| `Pending` | int64 | 就绪等待处理的任务数 | `ZCARD :ready` |
| `Active` | int64 | 正在处理中 (inflight) 的任务数 | `HLEN :inflight` |
| `Scheduled` | int64 | 延迟等待中的任务数 | `ZCARD :delayed` |
| `Retrying` | int64 | 等待重试的任务数 | - |
| `Completed` | int64 | 累计完成数 | `HGET :stats completed` |
| `Failed` | int64 | 累计失败数 | `HGET :stats failed` |

`Stats` 方法通过 Redis Pipeline 一次性获取所有指标，减少网络往返。

---

## 存储接口

> 源码：`internal/shared/domain/repository/`

遵循 DDD Repository 模式和接口隔离原则，拆分为细粒度接口。MySQL TaskRepository 同时实现 `TaskReader`、`TaskWriter`、`TaskCompensator` 三个接口，通过编译期类型断言验证。

### TaskReader / TaskWriter / TaskStore

```go
type TaskReader interface {
    Get(ctx context.Context, id string) (*Task, error)
    List(ctx context.Context, filter TaskFilter) ([]*Task, int64, error)
}

type TaskWriter interface {
    Create(ctx context.Context, task *Task) error
    Update(ctx context.Context, task *Task) error   // 乐观锁: WHERE version=?
}

type TaskStore interface {
    TaskReader
    TaskWriter
}
```

### TaskCompensator

```go
type TaskCompensator interface {
    FindStaleByState(ctx context.Context, state TaskState, olderThan time.Duration, limit int) ([]*Task, error)
    TouchUpdatedAt(ctx context.Context, id string) error          // 不递增 version
    HasRunningTasks(ctx context.Context, taskType, namespace string) (bool, error)
    DeleteTerminalOlderThan(ctx context.Context, olderThan time.Duration, limit int) (int64, error)
}
```

### CronJobReader / CronJobWriter / CronJobStore

```go
type CronJobReader interface {
    GetCronJob(ctx context.Context, id string) (*CronJob, error)
    ListCronJobs(ctx context.Context, namespace string, limit, offset int) ([]*CronJob, int64, error)
    FindDueCronJobs(ctx context.Context, limit int) ([]*CronJob, error)
}

type CronJobWriter interface {
    CreateCronJob(ctx context.Context, job *CronJob) error
    UpdateCronJob(ctx context.Context, job *CronJob) error
    DeleteCronJob(ctx context.Context, id string) error
}

type CronJobStore interface {
    CronJobReader
    CronJobWriter
}
```

### QueueBroker

```go
type QueueBroker interface {
    Enqueue(ctx context.Context, queue string, task *Task) error
    EnqueueDelayed(ctx context.Context, queue string, task *Task) error
    Dequeue(ctx context.Context, queues []string) (*Task, error)
    Ack(ctx context.Context, queue string, taskID string) error
    Nack(ctx context.Context, queue string, task *Task) error
    PromoteDelayed(ctx context.Context, queue string, batchSize int) (int64, error)
    Len(ctx context.Context, queue string) (int64, error)
    Stats(ctx context.Context, queue string) (*QueueStats, error)
    EnqueueIfNotInflight(ctx context.Context, queue string, task *Task) (bool, error)
}
```

### WorkerRegistry

```go
type WorkerRegistry interface {
    Register(ctx context.Context, worker *WorkerInfo) error
    Deregister(ctx context.Context, workerID string) error
    Heartbeat(ctx context.Context, worker *WorkerInfo) error   // 参数为完整 WorkerInfo
    GetWorker(ctx context.Context, workerID string) (*WorkerInfo, error)
    ListWorkers(ctx context.Context) ([]*WorkerInfo, error)
    WatchWorkers(ctx context.Context) (<-chan WorkerEvent, error)
}
```
