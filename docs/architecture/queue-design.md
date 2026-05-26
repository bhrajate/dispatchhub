# 队列设计

## 概览

DispatchHub 的队列系统基于 Redis 实现，支持以下能力：

- **优先级队列**：高优先级任务先被处理
- **延迟队列**：任务在指定时间后才可执行
- **原子出队**：Lua 脚本保证出队和状态转换的原子性
- **Inflight 追踪**：正在执行的任务可追踪和超时处理
- **多队列竞争**：Worker 可同时订阅多个队列

## Redis 数据结构

> 源码：`internal/shared/infrastructure/persistence/redis/queue_broker.go`

每个队列使用 5 个 Redis Key：

| Key 模式 | 类型 | 说明 |
|----------|------|------|
| `dispatchhub:queue:{name}:ready` | Sorted Set | 就绪队列，score = 负优先级 |
| `dispatchhub:queue:{name}:delayed` | Sorted Set | 延迟队列，score = 执行时间戳(ms) |
| `dispatchhub:queue:{name}:inflight` | Hash | 执行中的任务，field = taskID, value = taskJSON |
| `dispatchhub:queue:{name}:lease` | Sorted Set | 可见性 lease，member = taskID, score = deadline(ms) |
| `dispatchhub:queue:{name}:stats` | Hash | 统计计数器（enqueued / completed / failed） |

### Ready Queue（就绪队列）

使用 Redis Sorted Set，**score 为负的优先级值**。

```
ZADD dispatchhub:queue:default:ready -10 '{"id":"abc","priority":10,...}'
ZADD dispatchhub:queue:default:ready -8  '{"id":"def","priority":8,...}'
ZADD dispatchhub:queue:default:ready -5  '{"id":"ghi","priority":5,...}'
```

为什么使用负值？`ZPOPMIN` 取出 score 最小的成员，-10 < -8 < -5，所以优先级 10 的任务最先被取出。

### Delayed Queue（延迟队列）

使用 Redis Sorted Set，**score 为执行时间的 Unix 毫秒时间戳**。

```
ZADD dispatchhub:queue:default:delayed 1705312200000 '{"id":"xyz",...}'
```

Scheduler 每秒执行晋升扫描，将 score <= 当前时间的任务移入就绪队列。

### Inflight Set（执行中集合）

使用 Redis Hash，记录已出队正在执行的任务。

```
HSET dispatchhub:queue:default:inflight "task-id-abc" '{"id":"abc",...}'
```

任务完成后通过 Ack 删除；失败后通过 Nack 删除并重新入队。

### Lease Zset（可见性超时索引）

inflight Hash 的 `taskID → JSON` 适合 O(1) 查/删，但**不能按时间维度排序**。回收循环要找"在途超过可见性超时的任务"，必须用 Sorted Set 按 deadline 索引：

```
ZADD dispatchhub:queue:default:lease <dequeue_ts_ms + visibility_ms> "task-id-abc"
```

- `Dequeue` 时同时写 inflight Hash 和 lease Sorted Set（`dequeueScript` 在 Lua 内一次性完成）
- `Ack` / `Nack` / `Remove` 同时清理两份
- 回收循环 `ZRANGEBYSCORE lease -inf now LIMIT 0 N` 取出过期 taskID，逐个 HGET inflight 拿到原 JSON 再 ZADD 回 ready

这是 Asynq 的 `active hash + lease zset` 双结构方案：用 hash 保留补偿循环 / Stats / Remove 需要的 O(1) 查询，用 zset 保留按时间排序的回收能力。

#### 为什么需要 inflight 这一层？

`ready` 表示"待领"，`inflight` 表示"在途"，`Ack` / `Nack` 表示"完结"。少了 `inflight` 这一中间态，会同时丢掉四块能力：

**1. 防止 Worker 崩溃导致任务丢失（核心动机）**

`Dequeue` 使用 `ZPOPMIN`，任务一旦弹出就从 `ready` 消失。若 Worker 在执行过程中崩溃 / OOM kill / 网络断开，任务既不在 `ready` 也没被 `Ack`，将永久丢失。

引入 `inflight` + `lease` 后，scheduler 的 `reclaimInflightLoop` 每 5s 扫一次 lease zset，把超过 `DefaultVisibilityTimeout=30s` 的任务从 inflight 移回 ready 重投，实现见 `queue_broker.go` 的 `reclaimInflightScript`。这是 SQS、Sidekiq、Asynq 等队列的标准做法。

**2. 与 MySQL → Redis 补偿循环去重**

Scheduler 周期性扫描 MySQL 中 `pending` 状态的任务回填 Redis（防止 Redis 数据丢失后任务消失）。但任务可能已被 Worker 取走、正在执行，只是 `running` 状态尚未落库。

`EnqueueIfNotInflight` Lua 脚本（`queue_broker.go:252-263`）先 `HEXISTS inflight` 再决定是否 `ZADD ready`，避免补偿循环重复入队"已被取走但未更新到 MySQL"的任务，从而保证最终一致性收敛过程中的幂等性。

**3. 取消任务的覆盖面**

`removeScript`（`queue_broker.go:287-316`）需要同时清理 `ready` / `delayed` / `inflight` 三处。没有 `inflight`，就无法覆盖"已 dequeue 但未执行完"这一状态下的取消语义（实际终止还需 pub/sub 通知 Worker，但状态层必须有这条记录可清理）。

**4. 可观测性（Active 指标）**

`Stats` 通过 `HLEN inflight` 直接拿到"当前在跑多少任务"（`Active`），是运维面板里最关键的实时指标之一。如果没有这个集合，只能去 Worker 进程里聚合，跨进程统计精度差且时延高。

---

## 核心 Lua 脚本

### 原子出队脚本（Dequeue）

**问题**：如果使用 `ZPOPMIN` + `HSET` 两个独立命令，在两条命令之间进程崩溃会导致任务丢失。

**解决**：使用 Lua 脚本保证原子性。

```lua
-- KEYS: 多个就绪队列的 key
-- 返回: 任务 JSON 或 nil
local queues = KEYS
for i, queue_key in ipairs(queues) do
    local result = redis.call('ZPOPMIN', queue_key, 1)
    if #result > 0 then
        local data = result[1]
        local task = cjson.decode(data)
        -- 将 :ready 替换为 :inflight 得到 inflight key
        local inflight_key = string.gsub(queue_key, ':ready', ':inflight')
        -- 原子性: 出队 + 移入 inflight
        redis.call('HSET', inflight_key, task.id, data)
        return data
    end
end
return nil
```

**执行流程**：

1. 遍历传入的多个队列 key（按优先级排序）
2. 对每个队列执行 `ZPOPMIN`，取出 score 最小（优先级最高）的任务
3. 成功取出后，立即写入 inflight Hash
4. 返回任务 JSON，Worker 收到后开始执行
5. 如果所有队列都为空，返回 nil

### 延迟晋升脚本（Promote）

```lua
-- KEYS[1]: 延迟队列 key
-- KEYS[2]: 就绪队列 key
-- ARGV[1]: 当前时间戳 (毫秒)
local delayed_key = KEYS[1]
local ready_key = KEYS[2]
local now = tonumber(ARGV[1])

-- 取出所有到期任务 (score <= now), 每次最多 100 个
local tasks = redis.call('ZRANGEBYSCORE', delayed_key, '-inf', now, 'LIMIT', 0, 100)
if #tasks == 0 then
    return 0
end

-- 移入就绪队列
for _, data in ipairs(tasks) do
    local task = cjson.decode(data)
    local score = -(task.priority or 5)  -- 负优先级作为 ready score
    redis.call('ZADD', ready_key, score, data)
end

-- 从延迟队列中删除已晋升的任务
redis.call('ZREMRANGEBYSCORE', delayed_key, '-inf', now)

return #tasks
```

**执行流程**：

1. 查询延迟队列中 score <= 当前时间戳的任务（已到期）
2. 每次最多处理 100 个，避免长时间阻塞 Redis
3. 将每个任务以优先级 score 加入就绪队列
4. 批量删除延迟队列中已晋升的任务
5. 返回晋升任务数量

---

## 操作流程详解

### Enqueue（入队）

```go
func (q *QueueBroker) Enqueue(ctx, queue, task) error
```

1. 将 Task 序列化为 JSON
2. 计算 score = -priority（负值使高优先级排在前面）
3. Pipeline 执行：
   - `ZADD ready_key score taskJSON`
   - `HINCRBY stats_key "enqueued" 1`

### EnqueueDelayed（延迟入队）

```go
func (q *QueueBroker) EnqueueDelayed(ctx, queue, task) error
```

1. 将 Task 序列化为 JSON
2. 计算执行时间：
   - 如果有 `ScheduleAt`，使用绝对时间
   - 否则使用 `now + Delay`
3. `ZADD delayed_key executeAtMs taskJSON`

### Dequeue（出队）

```go
func (q *QueueBroker) Dequeue(ctx, queues) (*Task, error)
```

1. 构造所有队列的 ready key 列表
2. 执行 Lua 脚本（原子出队 + 移入 inflight）
3. 如果返回 nil，说明所有队列为空
4. 反序列化 JSON 为 Task 对象返回

### Ack（确认完成）

```go
func (q *QueueBroker) Ack(ctx, queue, taskID) error
```

Pipeline 执行：
1. `HDEL inflight_key taskID`（从 inflight 移除）
2. `HINCRBY stats_key "completed" 1`（递增完成计数）

### Nack（退回重试）

```go
func (q *QueueBroker) Nack(ctx, queue, task) error
```

Pipeline 执行：
1. `HDEL inflight_key taskID`（从 inflight 移除）
2. 根据重试策略选择重新入队方式：
   - **有 RetryBackoff**：加入延迟队列，score = now + backoff
   - **无 RetryBackoff**：立即加入就绪队列，score = -priority
   - **不可重试**：递增 failed 计数

### PromoteDelayed（晋升延迟任务）

```go
func (q *QueueBroker) PromoteDelayed(ctx, queue) (int64, error)
```

由 Scheduler 每秒调用一次：
1. 传入当前时间戳（毫秒）
2. 执行 Lua 脚本，将到期任务批量移入就绪队列
3. 返回晋升的任务数

---

## 多队列设计

### Worker 订阅

每个 Worker 可以订阅多个队列：

```yaml
worker:
  queues:
    - high-priority    # 先检查高优先级队列
    - default          # 再检查默认队列
    - batch            # 最后检查批处理队列
```

Dequeue 时按照配置顺序遍历队列，第一个有任务的队列会被消费。

### 队列隔离

不同业务可以使用不同队列实现隔离：

```
high-priority  ──▶  延迟敏感任务 (支付回调, 告警通知)
default        ──▶  普通任务 (邮件发送, 数据同步)
batch          ──▶  批量任务 (报表生成, 数据导出)
```

### 路由校验

Queue 和 Task Type 是正交维度——同一 Type 可走不同 Queue，同一 Queue 可承载多种 Type。为防止任务被投递到无法处理的队列，API Server 在提交时通过 RouteValidator 校验 queue+type 组合的可行性：

```
SubmitTask(type="video.transcode", queue="email-queue")
    ↓
RouteValidator: email-queue 上无 Worker 注册 "video.transcode" Handler
    ↓
拒绝: "no worker on queue "email-queue" handles task type "video.transcode""
```

RouteValidator 从 etcd 读取 Worker 拓扑（WorkerInfo.Queues + WorkerInfo.TaskTypes），构建 `queue → {types}` 映射。采用 fail-open 策略：无 Worker 在线或缓存刷新失败时放行，不阻塞提交。详见 [路由校验修复文档](../fixes/2026-04-17-queue-type-route-validation.md)。

### 队列统计

通过 Stats 方法获取队列实时状态：

```go
func (q *QueueBroker) Stats(ctx, queue) (*QueueStats, error)
```

Pipeline 查询：
- `ZCARD ready_key` → pending
- `ZCARD delayed_key` → scheduled
- `HLEN inflight_key` → active
- `HGET stats_key "completed"` → completed
- `HGET stats_key "failed"` → failed

---

## 可靠性保证

### 至少一次投递

通过 inflight 机制保证任务不会丢失：

```
                                          inflight
Redis Ready Queue                        Hash Map
┌──────────────┐    Lua Dequeue    ┌──────────────────┐
│  task-A (-10)│ ────────────────▶ │ task-A: {json}   │
│  task-B (-8) │     原子操作       │                  │
│  task-C (-5) │                  └────────┬─────────┘
└──────────────┘                           │
                                    Worker 处理完成
                                           │
                                    ┌──────▼─────────┐
                                    │   Ack: HDEL    │
                                    │   task-A       │
                                    └────────────────┘
```

- 出队和移入 inflight + lease 是原子操作（单条 Lua）
- 任务在 inflight 中直到被 Ack/Nack
- Worker 崩溃后，由 scheduler 的可见性超时回收循环重新调度（见下节）

### 可见性超时回收

> 实现：`reclaimInflightScript` + `SchedulerAppService.reclaimInflightLoop`

Worker `Dequeue` 时把任务的 deadline（`now + DefaultVisibilityTimeout`，默认 30s）写入 lease zset。Scheduler 每 5s 扫一次：

```lua
-- 简化版本
local expired = redis.call('ZRANGEBYSCORE', lease_key, '-inf', now, 'LIMIT', 0, batch)
for _, task_id in ipairs(expired) do
    local data = redis.call('HGET', inflight_key, task_id)
    redis.call('ZREM', lease_key, task_id)
    if data then
        redis.call('HDEL', inflight_key, task_id)
        local task = cjson.decode(data)
        redis.call('ZADD', ready_key, -(task.priority or 5), data)
    end
end
```

关键设计点：

- **batch 上限 100**：避免单次 Lua 阻塞 Redis 主循环过久
- **孤儿 lease 清理**：lease 有但 inflight 没有时，仍然 `ZREM`，避免 Remove 之后的残留条目反复触发回收
- **优先级保留**：从 inflight Hash 取出的 JSON 解析 priority，重新计算 score，保证回收任务在 ready 中仍按原优先级排序
- **至少一次语义的代价**：若 Worker 实际仍在执行只是慢了，回收会产生重复执行，**Handler 必须自身幂等**

回收日志使用 `Warn` 级别上报，触发即提示存在 worker 健康问题（崩溃 / 处理超时 / 网络分区）。

### 与 healthCheckLoop 的边界

| 循环 | 检测对象 | 失败时的行为 |
|---|---|---|
| `healthCheckLoop` | worker 心跳（30s 阈值） | 仅在 scheduler 内存中下线该 worker，**不动 inflight 任务** |
| `reclaimInflightLoop` | 任务 lease（30s 默认 visibility） | 把超时任务 HDEL inflight + ZADD ready 重投 |

二者职责互补：worker 离线检测决定"以后不再给它分新活"，可见性超时回收决定"它没干完的活让别的 worker 来"。

### 重试机制

```
失败 → Nack
  │
  ├─ CanRetry() == true
  │   ├─ 有 RetryBackoff → 延迟队列 (score = now + backoff)
  │   └─ 无 RetryBackoff → 就绪队列 (立即重试)
  │
  └─ CanRetry() == false
      └─ 标记 Failed, 递增 failed 计数
```

### 防惊群效应

重试使用指数退避 + 随机抖动（实现见 `queue_broker.go` 的 `computeRetryBackoff`）：

**公式**：`base * 2^(RetryCount-1) + jitter`，cap 到 `MaxRetryBackoff = 5min`

- `base` = 用户提交任务时设置的 `RetryBackoff`（API 字段，固定值）
- `RetryCount` 由 worker 在 `handleFailure` 中 `++` 后传入，代表"即将进行的这次重试是第几次"
- `jitter ∈ [0, base/4)`：用 base 而非当前 backoff 作为基准，是为了在退避被 cap 后仍保留足够的去同步空间

举例 `base = 1s`：

| RetryCount | backoff（不含 jitter） | jitter 范围 |
|---|---|---|
| 1 | 1s | 0~250ms |
| 2 | 2s | 0~250ms |
| 3 | 4s | 0~250ms |
| 4 | 8s | 0~250ms |
| ... | ... | ... |
| 9 | 256s | 0~250ms |
| 10+ | 5min（cap） | 0~250ms |

为什么需要 jitter：大量任务同时遇到下游故障 → 全部 Nack → 如果退避是确定值，N 秒后会被一起唤醒同时重试，再次打爆下游（thundering herd）。jitter 把唤醒时刻打散，避免同步重试。

为什么 cap 在 5min：业务能容忍的最长"再来一次"间隔；不 cap 的话 RetryCount=20 会算出 12 天的退避，等价于丢失任务。

---

## 性能特征

| 操作 | 复杂度 | 说明 |
|------|--------|------|
| Enqueue | O(log N) | ZADD 操作 |
| Dequeue | O(log N) | ZPOPMIN + HSET |
| Ack | O(1) | HDEL |
| Nack | O(log N) | HDEL + ZADD |
| PromoteDelayed | O(K log N) | K = 晋升数量, 每次最多 100 |
| Stats | O(1) | Pipeline 查询 |

Redis 单实例可支撑 10 万+ QPS 的队列操作。使用 Redis Cluster 可线性扩展。
