# 数据模型

## Task（任务）

> 源码：`pkg/types/task.go`

Task 是系统的核心领域对象，代表一个待执行的工作单元。

### 字段定义

#### 标识字段

| 字段 | 类型 | 数据库约束 | 说明 |
|------|------|-----------|------|
| `ID` | string | PK, size:64 | 任务唯一标识，自动生成 UUID |
| `Name` | string | INDEX, size:255 | 任务可读名称 |
| `Namespace` | string | INDEX, size:128 | 命名空间，用于多租户隔离 |
| `Group` | string | INDEX, size:128 | 逻辑分组，用于亲和性调度 |

#### 载荷字段

| 字段 | 类型 | 数据库约束 | 说明 |
|------|------|-----------|------|
| `Type` | string | INDEX, size:128 | Handler 类型标识，如 `"email.send"`、`"report.generate"` |
| `Payload` | json.RawMessage | TEXT | 任意 JSON 载荷，由 Handler 解析 |
| `Labels` | Labels (map[string]string) | TEXT | K8s 风格标签，支持选择器匹配 |

#### 调度字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `Priority` | TaskPriority (int) | 优先级，1-10，越大越优先 |
| `Delay` | Duration | 入队后延迟执行的时长 |
| `ScheduleAt` | *time.Time | 绝对执行时间点 |
| `CronExpr` | string | Cron 表达式（周期性任务） |
| `Timeout` | Duration | 单次执行超时时长 |
| `Deadline` | *time.Time | 绝对截止时间 |

#### 重试字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `MaxRetries` | int | 最大重试次数，默认 3 |
| `RetryCount` | int | 已重试次数 |
| `RetryBackoff` | Duration | 重试退避基础时长 |

#### 状态字段

| 字段 | 类型 | 数据库约束 | 说明 |
|------|------|-----------|------|
| `State` | TaskState (int) | INDEX | 当前生命周期状态 |
| `Result` | string | TEXT | 成功时的输出结果 |
| `Error` | string | TEXT | 失败时的错误信息 |
| `WorkerID` | string | INDEX, size:128 | 当前或最后执行的 Worker |
| `QueueName` | string | INDEX, size:128 | 所属队列名称 |

#### 元数据字段

| 字段 | 类型 | 说明 |
|------|------|------|
| `CreatedAt` | time.Time | 创建时间（自动） |
| `UpdatedAt` | time.Time | 更新时间（自动） |
| `StartedAt` | *time.Time | 开始执行时间 |
| `FinishedAt` | *time.Time | 完成时间 |
| `Version` | int64 | 乐观锁版本号，每次更新 +1 |

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
| Pending | Running | Worker 取出并开始执行 |
| Pending | Cancelled | 用户调用 CancelTask |
| Running | Completed | Handler 返回无错误 |
| Running | Retrying | Handler 返回错误且 CanRetry() == true |
| Running | Failed | Handler 返回错误且 CanRetry() == false |
| Running | Timeout | 执行超过 Timeout 时长 |
| Retrying | Running | 重试间隔到期，Worker 重新执行 |

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

### TaskPriority（优先级）

| 常量 | 值 | 说明 |
|------|-----|------|
| `PriorityLow` | 1 | 低优先级，空闲时处理 |
| `PriorityDefault` | 5 | 默认优先级 |
| `PriorityHigh` | 8 | 高优先级，优先处理 |
| `PriorityCritical` | 10 | 最高优先级，立即处理 |

在 Redis 中使用负值作为 score（-10 < -8 < -5 < -1），ZPOPMIN 取出 score 最小的即优先级最高的。

---

## TaskEvent（任务事件）

> 源码：`pkg/types/task.go`

记录任务生命周期中的每次状态变更，用于审计追踪。

| 字段 | 类型 | 说明 |
|------|------|------|
| `ID` | string | 事件唯一 ID |
| `TaskID` | string | 关联的任务 ID |
| `Type` | string | 事件类型：created / scheduled / started / completed / failed / retried / cancelled |
| `OldState` | TaskState | 变更前状态 |
| `NewState` | TaskState | 变更后状态 |
| `WorkerID` | string | 触发事件的 Worker |
| `Message` | string | 事件附加信息 |
| `Timestamp` | time.Time | 事件发生时间 |

---

## TaskFilter（查询过滤器）

用于 ListTasks 接口的查询条件。

| 字段 | 说明 |
|------|------|
| `Namespace` | 按命名空间过滤 |
| `Group` | 按分组过滤 |
| `Type` | 按 Handler 类型过滤 |
| `State` | 按状态过滤 |
| `Labels` | 按标签选择器过滤 |
| `QueueName` | 按队列名过滤 |
| `WorkerID` | 按 Worker 过滤 |
| `Limit` | 分页大小（默认 100） |
| `Offset` | 分页偏移 |

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

---

## Labels（标签）

K8s 风格的键值对标签，附加在 Task 上用于分类和过滤。

```go
type Labels map[string]string

// 选择器匹配：selector 中的所有 k-v 都必须存在于 labels 中
func (l Labels) Matches(selector map[string]string) bool
```

示例：

```json
{
    "env": "production",
    "team": "platform",
    "priority-class": "batch"
}
```

---

## Duration（时长包装器）

包装 `time.Duration`，支持 JSON 字符串序列化。

```go
// JSON 中使用字符串格式
{"timeout": "5m", "delay": "30s", "retry_backoff": "1s"}

// 支持 Go 标准 duration 格式
"300ms", "1.5s", "2m30s", "1h"
```

---

## Worker 相关模型

### WorkerInfo（Worker 信息）

> 源码：`pkg/types/worker.go`

描述一个注册在集群中的 Worker 节点。

| 字段 | 类型 | 说明 |
|------|------|------|
| `ID` | string | Worker 唯一标识，格式 `hostname-uuid8` |
| `Hostname` | string | 主机名 |
| `IP` | string | IP 地址 |
| `Port` | int | 服务端口 |
| `State` | WorkerState | 当前状态 |
| `Labels` | map[string]string | 节点标签，如 `"gpu": "true"` |
| `Queues` | []string | 订阅的队列列表 |
| `Concurrency` | int | 最大并发任务数 |
| `ActiveTasks` | int | 当前活跃任务数 |
| `CompletedTotal` | int64 | 累计完成任务数 |
| `FailedTotal` | int64 | 累计失败任务数 |
| `CPUUsage` | float64 | CPU 使用率 (%) |
| `MemUsage` | float64 | 内存使用率 (%) |
| `StartedAt` | time.Time | 启动时间 |
| `LastHeartbeat` | time.Time | 最后心跳时间 |
| `Version` | string | 程序版本号 |

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
| `Draining` | 1 | 停止接受新任务，完成 in-flight 任务 |
| `Offline` | 2 | 离线 |

判断方法：

```go
// 是否可以接受新任务
func (w *WorkerInfo) IsAvailable() bool {
    return w.State == WorkerStateOnline && w.ActiveTasks < w.Concurrency
}

// 当前负载比 (0.0 ~ 1.0)
func (w *WorkerInfo) Load() float64 {
    return float64(w.ActiveTasks) / float64(w.Concurrency)
}
```

### Heartbeat（心跳）

Worker 周期性发送给 Scheduler 的状态报告。

| 字段 | 类型 | 说明 |
|------|------|------|
| `WorkerID` | string | Worker 标识 |
| `State` | WorkerState | 当前状态 |
| `ActiveTasks` | int | 活跃任务数 |
| `CPUUsage` | float64 | CPU 使用率 |
| `MemUsage` | float64 | 内存使用率 |
| `Timestamp` | time.Time | 心跳时间戳 |

---

## Queue 相关模型

### QueueConfig（队列配置）

> 源码：`pkg/types/queue.go`

| 字段 | 类型 | 说明 |
|------|------|------|
| `Name` | string | 队列名称 |
| `Priority` | int | 队列优先级（多队列竞争时） |
| `MaxSize` | int64 | 最大容量，0 = 不限 |
| `RateLimit` | int | 每秒最大入队数，0 = 不限 |
| `Concurrency` | int | 每 Worker 从此队列的最大并发数 |
| `Paused` | bool | 是否暂停 |

### QueueStats（队列统计）

| 字段 | 类型 | 说明 |
|------|------|------|
| `Name` | string | 队列名称 |
| `Pending` | int64 | 就绪等待处理的任务数 |
| `Active` | int64 | 正在处理中（inflight）的任务数 |
| `Scheduled` | int64 | 延迟等待中的任务数 |
| `Retrying` | int64 | 等待重试的任务数 |
| `Completed` | int64 | 累计完成数 |
| `Failed` | int64 | 累计失败数 |

---

## 存储接口

### TaskStore

> 源码：`pkg/store/store.go`

```go
type TaskStore interface {
    Create(ctx, task)                         error
    Get(ctx, id)                              (*Task, error)
    Update(ctx, task)                         error          // 乐观锁
    Delete(ctx, id)                           error
    List(ctx, filter)                         ([]*Task, int64, error)
    BatchUpdateState(ctx, ids, from, to)      (int64, error) // 批量状态转换
}
```

### QueueBroker

```go
type QueueBroker interface {
    Enqueue(ctx, queue, task)          error           // 入队
    EnqueueDelayed(ctx, queue, task)   error           // 延迟入队
    Dequeue(ctx, queues)               (*Task, error)  // 原子出队
    Ack(ctx, queue, taskID)            error           // 确认完成
    Nack(ctx, queue, task)             error           // 退回重试
    PromoteDelayed(ctx, queue)         (int64, error)  // 晋升延迟任务
    Len(ctx, queue)                    (int64, error)  // 队列长度
    Stats(ctx, queue)                  (*QueueStats, error)
}
```

### Registry

```go
type Registry interface {
    Register(ctx, worker)              error
    Deregister(ctx, workerID)          error
    Heartbeat(ctx, heartbeat)          error
    GetWorker(ctx, workerID)           (*WorkerInfo, error)
    ListWorkers(ctx)                   ([]*WorkerInfo, error)
    WatchWorkers(ctx)                  (<-chan WorkerEvent, error)
}
```

### WorkerEvent

```go
type WorkerEvent struct {
    Type     WorkerEventType  // Joined(0) / Left(1) / Updated(2)
    WorkerID string
    Worker   *WorkerInfo      // Left 事件时可能为 nil
}
```
