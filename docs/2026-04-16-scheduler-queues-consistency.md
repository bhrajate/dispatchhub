# SchedulerService workers 与 queues 数据一致性修复

> 日期：2026-04-16

## 一、问题描述

**文件：** `internal/scheduler/domain/service/scheduler.go`

`SchedulerService` 维护了两个状态字段：

```go
type SchedulerService struct {
    // ...
    mu      sync.RWMutex
    workers map[string]*entity.WorkerInfo  // 在线 worker 列表
    queues  []string                       // 所有 worker 监听的队列并集
}
```

`queues` 是 `workers` 的派生数据（所有在线 worker 监听的队列的并集），但二者的更新逻辑不同步：

| 方法 | 更新 `workers` | 更新 `queues` |
|------|:-:|:-:|
| `SyncWorkers()` | 是 | 是 |
| `HandleWorkerEvent()` | 是 | **否** |
| `DetectStaleWorkers()` | 是 | **否** |

### 1.1 调用链路

在 `SchedulerAppService.Run()` 中：

```go
func (s *SchedulerAppService) Run(ctx context.Context) error {
    s.domainSvc.SyncWorkers(ctx)  // 启动时调用一次，同步 workers + queues
    go s.watchWorkers(ctx)        // 后续通过 HandleWorkerEvent 更新 workers，不更新 queues
    go s.healthCheckLoop(ctx)     // 通过 DetectStaleWorkers 删除 workers，不更新 queues
    // ...
}
```

`SyncWorkers()` 仅在启动时调用一次，此后 `workers` 通过事件和心跳检测持续变化，但 `queues` 再也没有被更新。

### 1.2 不一致场景

**场景 1：新 Worker 加入，带来新队列**

Worker-X 注册，监听队列 `["default", "priority"]`，其中 `"priority"` 是新队列：

```
HandleWorkerEvent(Joined)
  → workers["X"] = WorkerInfo{Queues: ["default", "priority"]}  ✓
  → queues 未变，仍是 ["default"]                                 ✗
```

后果：`promoteDelayedLoop` 和 `metricsLoop` 遍历 `Queues()` 工作，`"priority"` 队列中的延迟任务不会被 promote，指标不会被采集。

**场景 2：Worker 离开，是某队列的唯一消费者**

Worker-Y 是唯一监听 `"special"` 队列的 Worker，下线：

```
HandleWorkerEvent(Left)
  → delete(workers, "Y")                    ✓
  → queues 未变，仍含 "special"              ✗
```

后果：Scheduler 持续对 `"special"` 队列做 PromoteDelayed、Stats 等无效 Redis 操作。

**场景 3：心跳超时移除 stale Worker**

```
DetectStaleWorkers()
  → delete(workers, id)                      ✓
  → queues 未变                               ✗
```

同场景 2。

## 二、方案选型

### 方案 A：提取 `rebuildQueues()` 辅助方法

在 `HandleWorkerEvent`、`DetectStaleWorkers`、`SyncWorkers` 末尾统一调用 `rebuildQueues()`。

- 优点：改动直观
- 缺点：依赖"每个修改 `workers` 的地方都记得调用"，未来新增修改入口容易遗漏

### 方案 B：移除 `queues` 字段，`Queues()` 从 `workers` 实时计算（采用）

`queues` 本质上就是 `workers` 的派生数据，可以消除这个冗余状态，在 `Queues()` 调用时动态计算。

- 优点：**从结构上杜绝不一致**，不存在遗漏的可能；减少代码量
- 缺点：每次调用 `Queues()` 有微量计算开销

**性能评估**：Worker 数量通常在个位数到两位数级别，遍历开销可忽略；`Queues()` 的最高调用频率为 1 次/秒（`promoteDelayedLoop`），不构成瓶颈。

**选择方案 B。**

## 三、修改内容

**文件：** `internal/scheduler/domain/service/scheduler.go`

### 3.1 删除 `queues` 字段

```go
// 修改前
type SchedulerService struct {
    // ...
    mu      sync.RWMutex
    workers map[string]*entity.WorkerInfo
    queues  []string                        // ← 删除
}

// 修改后
type SchedulerService struct {
    // ...
    mu      sync.RWMutex
    workers map[string]*entity.WorkerInfo
}
```

构造函数中同步移除 `queues` 的初始化。

### 3.2 简化 `SyncWorkers()`

移除队列收集逻辑，只维护 `workers`：

```go
// 修改前
func (s *SchedulerService) SyncWorkers(ctx context.Context) (int, error) {
    workers, err := s.registry.ListWorkers(ctx)
    // ...
    s.workers = make(map[string]*entity.WorkerInfo, len(workers))
    queueSet := make(map[string]struct{})
    for _, w := range workers {
        if w.State == entity.WorkerStateOnline {
            s.workers[w.ID] = w
            for _, q := range w.Queues {
                queueSet[q] = struct{}{}
            }
        }
    }
    s.queues = make([]string, 0, len(queueSet))
    for q := range queueSet {
        s.queues = append(s.queues, q)
    }
    if len(s.queues) == 0 {
        s.queues = []string{entity.DefaultQueueName}
    }
    return len(s.workers), nil
}

// 修改后
func (s *SchedulerService) SyncWorkers(ctx context.Context) (int, error) {
    workers, err := s.registry.ListWorkers(ctx)
    // ...
    s.workers = make(map[string]*entity.WorkerInfo, len(workers))
    for _, w := range workers {
        if w.State == entity.WorkerStateOnline {
            s.workers[w.ID] = w
        }
    }
    return len(s.workers), nil
}
```

### 3.3 重写 `Queues()`

从 `workers` 实时遍历计算队列集合：

```go
// 修改前
func (s *SchedulerService) Queues() []string {
    s.mu.RLock()
    defer s.mu.RUnlock()
    result := make([]string, len(s.queues))
    copy(result, s.queues)
    return result
}

// 修改后
func (s *SchedulerService) Queues() []string {
    s.mu.RLock()
    defer s.mu.RUnlock()

    queueSet := make(map[string]struct{})
    for _, w := range s.workers {
        for _, q := range w.Queues {
            queueSet[q] = struct{}{}
        }
    }

    if len(queueSet) == 0 {
        return []string{entity.DefaultQueueName}
    }

    queues := make([]string, 0, len(queueSet))
    for q := range queueSet {
        queues = append(queues, q)
    }
    return queues
}
```

无 worker 在线时回退到 `DefaultQueueName`，保持原有兜底行为。

## 四、修复效果

| 场景 | 修复前 | 修复后 |
|------|-------|-------|
| 新 Worker 带来新队列 | `queues` 不更新，新队列的延迟任务不被 promote | `Queues()` 立即包含新队列 |
| Worker 离开，队列无消费者 | `queues` 残留无效队列 | `Queues()` 自动排除 |
| 心跳超时移除 stale Worker | `queues` 不更新 | `Queues()` 自动排除 |
| 未来新增修改 `workers` 的方法 | 需要记得同步 `queues`，容易遗漏 | 无需额外操作，结构保证一致 |
