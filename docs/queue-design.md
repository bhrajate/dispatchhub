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

每个队列使用 4 个 Redis Key：

| Key 模式 | 类型 | 说明 |
|----------|------|------|
| `dispatchhub:queue:{name}:ready` | Sorted Set | 就绪队列，score = 负优先级 |
| `dispatchhub:queue:{name}:delayed` | Sorted Set | 延迟队列，score = 执行时间戳(ms) |
| `dispatchhub:queue:{name}:inflight` | Hash | 执行中的任务，field = taskID, value = taskJSON |
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

RouteValidator 从 etcd 读取 Worker 拓扑（WorkerInfo.Queues + WorkerInfo.TaskTypes），构建 `queue → {types}` 映射。采用 fail-open 策略：无 Worker 在线或缓存刷新失败时放行，不阻塞提交。详见 [路由校验修复文档](2026-04-17-queue-type-route-validation.md)。

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

- 出队和移入 inflight 是原子操作
- 任务在 inflight 中直到被 Ack/Nack
- Worker 崩溃后，inflight 中的任务可被重新调度

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

重试使用指数退避 + 随机抖动：

```
retry 0: 1s     + jitter(0~250ms)
retry 1: 2s     + jitter(0~500ms)
retry 2: 4s     + jitter(0~1s)
retry 3: 8s     + jitter(0~2s)
...
max:     5min   + jitter(0~75s)
```

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
