# 任务取消功能增强：支持终止正在执行的任务

> 日期：2026-04-16

## 一、背景

原有的 `CancelTask` 实现仅更新 MySQL 中的任务状态为 `Cancelled`，存在两个问题：

1. **队列污染** — 已取消的任务仍留在 Redis 队列（ready/delayed/inflight）中，Worker 会继续取出并尝试执行，浪费计算资源。
2. **无法终止正在执行的任务** — 如果任务已被 Worker 取出并正在执行，Cancel 操作不会通知 Worker 停止，任务会一直执行到完成或超时。

## 二、修改方案

采用 **队列移除 + Redis Pub/Sub 取消信号** 的方案，覆盖任务在各阶段的取消需求：

| 任务阶段 | 取消机制 | 效果 |
|---------|---------|------|
| 在 ready/delayed 队列中 | `Remove` 从队列移除 | 立即生效，Worker 不会取到该任务 |
| 在 inflight 中（已出队未执行） | `Remove` 从 inflight 移除 + MySQL 状态检查 | Worker 执行前发现已取消，跳过 |
| 正在执行中 | Redis Pub/Sub 通知 Worker cancel context | Handler 通过 `ctx.Done()` 感知取消，优雅退出 |

### 2.1 取消流程

```
Client
  │
  ▼
POST /api/v1/tasks/{id}/cancel
  │
  ▼
TaskServiceImpl.CancelTask()
  ├── 1. 更新 MySQL 状态为 Cancelled（核心保证）
  ├── 2. broker.Remove() 从 Redis 队列移除（best-effort）
  └── 3. broker.PublishCancel() 发布取消信号（best-effort）
                                    │
                    ┌───────────────┘
                    ▼
            Redis Pub/Sub Channel
            "dispatchhub:task:cancel"
                    │
          ┌─────────┼─────────┐
          ▼         ▼         ▼
       Worker1   Worker2   Worker3
          │
          ▼
    cancelListenLoop 收到 taskID
          │
          ▼
    在 cancels map 中查找 → 找到 → cancel(taskCtx)
          │
          ▼
    Handler 的 ctx.Done() 触发 → 优雅退出
          │
          ▼
    processTask 检测到 context.Canceled + MySQL Cancelled 状态
          │
          ▼
    Ack 任务 + 记录 cancelled 指标
```

### 2.2 设计决策

- **MySQL 状态更新是核心保证**：即使 Redis 操作失败，MySQL 中已标记为 Cancelled，Worker 执行前的 MySQL 状态检查会跳过已取消的任务。
- **Redis 操作是 best-effort**：Remove 和 PublishCancel 失败仅记录日志，不返回错误，不影响 Cancel API 的正确性。
- **使用单一 Pub/Sub 频道**：所有取消信号发布到 `dispatchhub:task:cancel`，Worker 在本地 map 中 O(1) 过滤，避免为每个任务创建独立订阅。
- **双重状态检查关闭竞态窗口**：Worker 在 `trackCancel` 前后各检查一次 MySQL 状态，防止取消信号在注册前到达的竞态。

## 三、修改文件

### 3.1 QueueBroker 接口

**文件：** `internal/shared/domain/repository/queue_broker.go`

接口新增三个方法：

```go
// Remove removes a task from all queue stages (ready, delayed, inflight).
// Used when a task is cancelled to prevent workers from picking it up.
Remove(ctx context.Context, queue string, taskID string) error
// PublishCancel publishes a cancel signal for the given task ID.
PublishCancel(ctx context.Context, taskID string) error
// SubscribeCancel subscribes to task cancel signals.
// Returns a channel of cancelled task IDs and a cleanup function.
SubscribeCancel(ctx context.Context) (<-chan string, func(), error)
```

### 3.2 Redis QueueBroker 实现

**文件：** `internal/shared/infrastructure/persistence/redis/queue_broker.go`

#### Remove — Lua 脚本原子移除

从三个数据结构中移除任务：

- **inflight（Hash）**：`HDEL` by taskID，O(1)
- **ready（Sorted Set）**：`ZSCAN` + `MATCH` 模式 `*"id":"<taskID>"*` 找到对应 member 后 `ZREM`
- **delayed（Sorted Set）**：同 ready

```lua
-- KEYS[1] = ready key, KEYS[2] = delayed key, KEYS[3] = inflight key
-- ARGV[1] = task ID
local removed = 0
-- Remove from inflight hash (O(1))
if redis.call('HDEL', KEYS[3], ARGV[1]) == 1 then
    removed = removed + 1
end
-- Remove from ready sorted set (scan for matching task ID in JSON member)
local cursor = "0"
repeat
    local result = redis.call('ZSCAN', KEYS[1], cursor, 'MATCH', '*"id":"' .. ARGV[1] .. '"*', 'COUNT', 100)
    cursor = result[1]
    local members = result[2]
    for i = 1, #members, 2 do
        redis.call('ZREM', KEYS[1], members[i])
        removed = removed + 1
    end
until cursor == "0"
-- Remove from delayed sorted set (same approach)
cursor = "0"
repeat
    local result = redis.call('ZSCAN', KEYS[2], cursor, 'MATCH', '*"id":"' .. ARGV[1] .. '"*', 'COUNT', 100)
    cursor = result[1]
    local members = result[2]
    for i = 1, #members, 2 do
        redis.call('ZREM', KEYS[2], members[i])
        removed = removed + 1
    end
until cursor == "0"
return removed
```

> **注意**：Sorted Set 的 member 是完整 JSON，无法按 taskID 直接 ZREM，需要 ZSCAN 匹配。对于典型队列规模（数千级别），性能可接受。且取消操作本身不频繁。

#### PublishCancel / SubscribeCancel — Redis Pub/Sub

```go
const cancelChannel = keyPrefix + "task:cancel"  // "dispatchhub:task:cancel"

func (q *QueueBroker) PublishCancel(ctx context.Context, taskID string) error {
    return q.client.Publish(ctx, cancelChannel, taskID).Err()
}

func (q *QueueBroker) SubscribeCancel(ctx context.Context) (<-chan string, func(), error) {
    pubsub := q.client.Subscribe(ctx, cancelChannel)
    if _, err := pubsub.Receive(ctx); err != nil {
        _ = pubsub.Close()
        return nil, nil, fmt.Errorf("subscribe cancel channel: %w", err)
    }
    ch := make(chan string, 64)
    go func() {
        defer close(ch)
        for msg := range pubsub.Channel() {
            select {
            case ch <- msg.Payload:
            case <-ctx.Done():
                return
            }
        }
    }()
    cleanup := func() { _ = pubsub.Close() }
    return ch, cleanup, nil
}
```

### 3.3 Worker 取消上下文追踪

**文件：** `internal/worker/application/service/worker_app_service.go`

#### 结构体新增字段

```go
type WorkerAppService struct {
    // ... 原有字段 ...
    cancelsMu sync.RWMutex
    cancels   map[string]context.CancelFunc  // taskID -> cancel function
}
```

#### 辅助方法

```go
func (w *WorkerAppService) trackCancel(taskID string, cancel context.CancelFunc)
func (w *WorkerAppService) untrackCancel(taskID string)
func (w *WorkerAppService) cancelRunningTask(taskID string) bool
```

#### cancelListenLoop — 监听取消信号

在 `Run()` 中启动，从 Pub/Sub channel 接收 taskID，在本地 `cancels` map 中查找并调用 cancel：

```go
func (w *WorkerAppService) cancelListenLoop(ctx context.Context, ch <-chan string) {
    for {
        select {
        case <-ctx.Done():
            return
        case taskID, ok := <-ch:
            if !ok {
                return
            }
            if w.cancelRunningTask(taskID) {
                log.Infof("received cancel signal for task %s, context cancelled", taskID)
            }
        }
    }
}
```

#### processTask 修改

1. 创建 `context.WithCancel(ctx)` 并注册到 `cancels` map
2. 注册后二次检查 MySQL 状态（关闭竞态窗口）
3. 将 `taskCtx` 传给 `safeHandle`（而非原始 `ctx`），使取消信号能传播到 Timeout 中间件和 Handler
4. Handler 返回后检测 `context.Canceled` + MySQL 状态为 `Cancelled`，按取消处理（Ack + 记录 metrics）

```go
// 创建可取消的 context
taskCtx, taskCancel := context.WithCancel(ctx)
defer taskCancel()
defer w.untrackCancel(task.ID)
w.trackCancel(task.ID, taskCancel)

// 二次检查关闭竞态窗口
if latest, err := w.taskStore.Get(ctx, task.ID); err == nil && latest != nil && latest.IsTerminal() {
    _ = w.broker.Ack(ctx, task.QueueName, task.ID)
    return
}

// ... 中间件链包装 handler ...

// 使用 taskCtx 执行 handler
result := w.safeHandle(taskCtx, handler, task)

// 检测显式取消
if result.Error != nil && taskCtx.Err() == context.Canceled {
    if latest, _ := w.taskStore.Get(ctx, task.ID); latest != nil && latest.State == entity.TaskStateCancelled {
        _ = w.broker.Ack(ctx, task.QueueName, task.ID)
        metrics.TasksProcessed.WithLabelValues(task.QueueName, task.Type, "cancelled").Inc()
        return
    }
}
```

### 3.4 CancelTask 更新

**文件：** `internal/apiserver/domain/service/task_service_impl.go`

```go
func (s *TaskServiceImpl) CancelTask(ctx context.Context, taskID string) error {
    task, err := s.taskStore.Get(ctx, taskID)
    // ... 校验 ...

    task.State = entity.TaskStateCancelled
    now := time.Now()
    task.FinishedAt = &now
    if err := s.taskStore.Update(ctx, task); err != nil {
        return err
    }

    // Best-effort: 从 Redis 队列移除
    if err := s.broker.Remove(ctx, task.QueueName, taskID); err != nil {
        log.Errorf("remove cancelled task %s from queue: %v", taskID, err)
    }

    // Best-effort: 通知 Worker 取消正在执行的任务
    if err := s.broker.PublishCancel(ctx, taskID); err != nil {
        log.Errorf("publish cancel signal for task %s: %v", taskID, err)
    }

    return nil
}
```

## 四、竞态分析

| 场景 | 处理方式 |
|------|---------|
| 任务在 ready/delayed 队列中 | `Remove` 直接移除；即使 Remove 失败，Worker 取出后 MySQL 状态检查会跳过 |
| 任务已出队但尚未执行 | processTask 开始时的 MySQL 状态检查（第 228 行）会跳过已取消的任务 |
| 取消信号在 trackCancel 之前到达 | trackCancel 之后的二次 MySQL 状态检查（第 243 行）会发现并跳过 |
| 任务在取消信号到达前已完成 | untrackCancel 已清理 map 条目，信号找不到目标，无副作用；MySQL 状态已由 Worker 设为 Completed，不会被覆盖 |
| Worker 订阅 Pub/Sub 失败 | 仅打日志，不影响主流程；Running 任务会等超时自然结束；MySQL 状态已为 Cancelled |
| 多个 Worker 收到同一取消信号 | 只有实际执行该任务的 Worker 会在 cancels map 中找到匹配，其他 Worker O(1) 忽略 |

## 五、Handler 开发注意事项

Handler 必须检查 `ctx.Done()` 才能及时响应取消信号。示例：

```go
func (h *MyHandler) Handle(ctx context.Context, task *entity.Task) *entity.TaskResult {
    for i := 0; i < totalSteps; i++ {
        select {
        case <-ctx.Done():
            return &entity.TaskResult{Error: ctx.Err()}
        default:
        }
        // 执行一步业务逻辑 ...
    }
    return &entity.TaskResult{Output: "done"}
}
```

如果 Handler 内部调用了支持 context 的 API（如 HTTP 请求、数据库查询），context 取消会自动传播，无需额外处理。

不检查 `ctx.Done()` 的长时间运行 Handler 将无法被取消信号终止，只能等待超时。
