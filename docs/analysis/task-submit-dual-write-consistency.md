# 任务提交双写一致性分析

| 元信息 | 值 |
|--------|----|
| 日期 | 2026-05-15 |
| Git commit | `4353a12c44b0fc05ad82b972a8c738d39835fea9` (`4353a12`) |
| 分析范围 | `internal/apiserver/domain/service/task_service_impl.go`、`internal/scheduler/{domain,application}/`、`internal/shared/infrastructure/persistence/redis/queue_broker.go`、`internal/worker/application/service/worker_app_service.go` |

---

## 问题

DispatchHub 的 `SubmitTask` 路径需要把任务**同时写入** MySQL（持久化）与 Redis（队列）。这是经典的"双写"场景，本文回答：

1. 实际的写入顺序是什么？
2. 单边失败时如何保证一致性？
3. 哪些异常窗口仍未覆盖？

---

## 1. 提交流程：先 MySQL 再 Redis（**不是事务**）

源码：`internal/apiserver/domain/service/task_service_impl.go:92-104`

```go
// 1) 持久化到 MySQL
if err := s.taskStore.Create(ctx, task); err != nil {
    return fmt.Errorf("persist task: %w", err)        // ← 失败立即返回
}

// 2) 入 Redis 队列（错误被 _ = ... 忽略）
if task.ScheduleAt != nil || task.Delay.Duration > 0 {
    _ = s.broker.EnqueueDelayed(ctx, task.QueueName, task)
} else {
    _ = s.broker.Enqueue(ctx, task.QueueName, task)
}
return nil
```

第二步**故意吞掉了错误**。源码注释（`task_service_impl.go:96-99`）解释了原因：

> Enqueue to Redis. If this fails, the task is still persisted in MySQL and the scheduler's compensate loop will re-enqueue it within 30 seconds. We intentionally do NOT return error here to avoid client retries that would create duplicate tasks.

设计意图明确：**用最终一致性换吞吐与简单性**，不引入 2PC / Saga 这类分布式事务。

---

## 2. 双写一致性怎么保证

整体策略：**MySQL 是唯一真相之源 + Scheduler 异步补偿**。三种异常场景的处理：

### 场景 1：MySQL 写成功，Redis 写失败

- 客户端收到 `201 Created` / `OK`，认为提交成功
- Redis 队列里没有这个任务，Worker 不会拉到
- **补偿机制**：Scheduler Leader 每 30s 跑一次 `CompensateOrphanedTasks`
  - 配置：`internal/scheduler/application/scheduler_app_service.go:37`，`CompensateInterval = 30 * time.Second`
  - 实现：`internal/scheduler/domain/service/scheduler.go:130-156`

```go
// 找出 Pending 状态、updated_at 超过 30s 的任务
tasks := taskMaint.FindStaleByState(ctx, TaskStatePending, 30s, 100)
for _, task := range tasks {
    // 关键：用 Lua 原子检查 inflight，避免重复入队
    enqueued := broker.EnqueueIfNotInflight(ctx, queue, task)
    if enqueued {
        taskMaint.TouchUpdatedAt(ctx, task.ID)  // 防止下轮再补偿
    }
}
```

**最坏延迟**：~30s 后任务一定会被重新入队执行。

### 场景 2：MySQL 写失败

- 直接 `return` 错误给客户端，Redis 完全没写
- 不存在不一致——任何状态机都没启动

### 场景 3：Redis 入队成功，但消息丢失（Redis 崩溃 / AOF 未刷盘）

- MySQL 仍有 Pending 记录
- 走场景 1 的补偿路径

---

## 3. `EnqueueIfNotInflight` 的关键作用

这是补偿幂等性的核心，用 Redis Lua 脚本原子地"检查 inflight 哈希里是否已有该 task ID，没有才入队"。

源码：`internal/shared/infrastructure/persistence/redis/queue_broker.go:254-265`

```lua
if redis.call('HEXISTS', inflight_key, task_id) == 1 then
    return 0    -- worker 已经在跑了，跳过
end
redis.call('ZADD', ready_key, score, data)
return 1
```

**避免的坑**：如果用普通 `Enqueue`，可能 worker 已经把任务从 ready 拉到 inflight 但还没写完 MySQL（仍是 Pending），补偿循环会以为它"卡在 Pending"再塞一份回 ready，导致**任务重复执行**。

---

## 4. Worker 完成时的"反向"一致性

任务执行完同样是双写：先 MySQL 改终态、再 Redis Ack。

源码：`internal/worker/application/service/worker_app_service.go:322-333`

```go
task.State = TaskStateCompleted
taskStore.Update(ctx, task)    // 1) MySQL 改终态
broker.Ack(ctx, queue, taskID) // 2) Redis HDEL inflight
```

Ack 失败的话 inflight 哈希会残留 task ID——但**不影响正确性**，因为任务在 MySQL 已经是终态，下次补偿循环 `FindStaleByState(Pending, ...)` 也不会捞到它。inflight 残留只是一点数据漂移，可以靠定期清理或人工干预。

---

## 5. 取舍总结

| 维度 | 取舍 |
|------|------|
| **优点** | 入口路径无分布式事务，吞吐高；MySQL 单点决定真相，逻辑简单 |
| **代价** | 最坏 30s 延迟（`CompensateInterval`），不适合"必须秒级派发"的场景 |
| **强依赖** | MySQL 写必须成功；MySQL 是 SPOF，需要主从 |
| **容忍** | Redis 完全可丢（重启即可恢复），但 inflight 哈希残留需要兜底清理 |

---

## 6. 未覆盖的窗口 ⚠

API Server 在 `taskStore.Create` 后、`broker.Enqueue` 调用前**进程崩溃**，会出现：

- MySQL 有记录，状态 Pending
- Redis 没有记录
- 客户端**没收到响应**（连接被重置），可能会重试

补偿循环会在 30s 后捞到这个 Pending 任务并入队，**但如果客户端此时已经用相同业务参数重试**，就会出现两个 task ID 不同、payload 相同的任务。

要彻底防住这个，需要客户端传幂等键（业务侧 `idempotency_key`），目前 API 没暴露这个字段。这等于把"业务幂等"从系统层推到了客户端层。

### 改进建议

1. **加 `idempotency_key` 字段**：在 `TaskSpec` 增加可选幂等键，`tasks` 表加唯一索引；重复提交直接返回已有任务。
2. **缩短 `CompensateOlderThan`**：默认 30s 偏保守，对延迟敏感的场景可调到 5s（需评估补偿循环对 MySQL 的扫描压力）。
3. **inflight 残留监控**：在 Scheduler 增加一个低频清理循环，定时扫描 `inflight` 哈希中已是 MySQL 终态的 task ID 并 `HDEL`。

---

## 相关源码定位

| 关键点 | 文件 | 行号 |
|--------|------|------|
| 双写顺序与忽略 Redis 错误的设计注释 | `internal/apiserver/domain/service/task_service_impl.go` | 92–104 |
| `CompensateOrphanedTasks` 实现 | `internal/scheduler/domain/service/scheduler.go` | 130–156 |
| `compensateLoop` 周期触发 | `internal/scheduler/application/scheduler_app_service.go` | 78、170–185 |
| 补偿默认参数（30s / 100 batch） | `internal/scheduler/application/scheduler_app_service.go` | 31–46 |
| `EnqueueIfNotInflight` Lua 脚本 | `internal/shared/infrastructure/persistence/redis/queue_broker.go` | 254–282 |
| Worker 成功路径双写 | `internal/worker/application/service/worker_app_service.go` | 322–333 |
| Worker 失败路径双写 | `internal/worker/application/service/worker_app_service.go` | 335–360 |
