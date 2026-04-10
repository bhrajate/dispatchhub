# 任务分发消息队列选型分析

## 一、从代码提取的硬性需求

在选型之前，先从项目代码中梳理出队列必须满足的功能需求和非功能需求。

### 1.1 功能需求（来自 QueueBroker 接口）

> 源码：`pkg/store/store.go`

```go
type QueueBroker interface {
    Enqueue(ctx, queue, task) error           // R1: 按名称入队到指定队列
    EnqueueDelayed(ctx, queue, task) error    // R2: 延迟/定时入队
    Dequeue(ctx, queues) (*Task, error)       // R3: 从多个队列中取出优先级最高的任务
    Ack(ctx, queue, taskID) error             // R4: 确认消费成功
    Nack(ctx, queue, task) error              // R5: 消费失败退回，支持延迟重入队
    PromoteDelayed(ctx, queue) (int64, error) // R6: 批量将到期的延迟任务晋升到就绪队列
    Len(ctx, queue) (int64, error)            // R7: 实时查询队列长度
    Stats(ctx, queue) (*QueueStats, error)    // R8: 实时查询 pending/active/delayed/completed/failed 统计
}
```

从接口定义中提炼出 **8 项硬性需求**：

| 编号 | 需求 | 代码依据 |
|------|------|----------|
| **R1** | 多命名队列 | `Enqueue(ctx, queue, task)` — queue 是运行时动态参数 |
| **R2** | 延迟队列 / 定时触发 | `EnqueueDelayed` 用 `ScheduleAt` 或 `Delay` 计算未来时间点 |
| **R3** | **优先级排序出队** | `Dequeue` 注释明确要求 "pops the highest-priority task"；score = -priority |
| **R4** | 消费确认（Ack） | `Ack` 从 inflight 中移除，确保至少一次投递 |
| **R5** | 消费失败退回（Nack） + 延迟重试 | `Nack` 中判断 `CanRetry()` 和 `RetryBackoff`，支持立即重入队或延迟重入队 |
| **R6** | 延迟→就绪 批量原子晋升 | `PromoteDelayed` 通过 Lua 脚本一次性移动最多 100 个到期任务 |
| **R7** | 实时队列长度查询 | `Len` 用于背压判断和监控 |
| **R8** | 多维度实时统计 | `Stats` 同时返回 5 个维度的计数 |

### 1.2 非功能需求（来自 Worker 执行模型和三高目标）

> 源码：`pkg/worker/worker.go` fetchLoop，`pkg/scheduler/scheduler.go`

| 编号 | 需求 | 来源 |
|------|------|------|
| **N1** | 亚毫秒级出队延迟 | Worker fetchLoop 空转间隔仅 100ms，要求出队不能成为瓶颈 |
| **N2** | 10万+ QPS 入队/出队 | 三高场景目标 |
| **N3** | 原子性出队（取出+标记inflight 不可分割） | Lua 脚本 `ZPOPMIN + HSET` 在同一个原子操作内 |
| **N4** | 拉模型（Pull） | Worker 主动 `Dequeue`，不是 broker push |
| **N5** | 出队即消失（竞争消费） | 一个任务只能被一个 Worker 取走 |
| **N6** | 消费后即删 | 消费确认后任务不保留在队列中（区别于消息流） |
| **N7** | 运维简单，云原生友好 | 尽可能复用已有基础设施，少引入独立组件 |

---

## 二、候选方案逐一分析

### 2.1 候选清单

| 方案 | 类别 |
|------|------|
| **Redis Sorted Set** | 内存数据结构 |
| **Kafka** | 分布式消息流 |
| **RabbitMQ** | 传统消息队列 |
| **RocketMQ** | 阿里开源消息队列 |
| **Pulsar** | 流存储融合系统 |
| **MySQL 轮询** | 关系数据库 |

### 2.2 Redis Sorted Set（最终选择）

**实现方式**：ZSET score = -priority 实现优先级排序，ZSET score = timestamp 实现延迟队列，Lua 脚本保证原子出队。

```
ready   (ZSET)  ──ZPOPMIN──▶  inflight (Hash)  ──Ack/HDEL──▶  删除
delayed (ZSET)  ──Promote──▶  ready    (ZSET)
```

**逐项需求对照**：

| 需求 | Redis 实现 | 满足度 |
|------|-----------|--------|
| R1 多命名队列 | 每个 queue 一组独立 Key（`queue:{name}:ready`） | 完全满足，动态创建，无需预注册 |
| R2 延迟队列 | ZSET score = 执行时间戳，ZRANGEBYSCORE 扫描到期 | 完全满足，毫秒精度 |
| R3 **优先级排序** | ZSET score = -priority，ZPOPMIN 取最小值即最高优先级 | **完全满足，O(log N) 有序出队** |
| R4 Ack | HDEL inflight | 完全满足 |
| R5 Nack + 延迟重入队 | HDEL inflight + ZADD delayed/ready | 完全满足 |
| R6 批量原子晋升 | Lua 脚本内 ZRANGEBYSCORE + ZADD + ZREMRANGEBYSCORE | 完全满足，单线程原子执行 |
| R7 实时长度 | ZCARD，O(1) | 完全满足 |
| R8 多维统计 | Pipeline: ZCARD + HLEN + HGET 并行 | 完全满足 |
| N1 亚毫秒延迟 | 内存操作，P99 < 1ms | 完全满足 |
| N2 10万+ QPS | 单机 10万+ QPS，Cluster 线性扩展 | 完全满足 |
| N3 原子出队 | Lua 脚本在单线程中执行，天然原子 | 完全满足 |
| N4 拉模型 | 客户端主动调用 ZPOPMIN | 完全满足 |
| N5 竞争消费 | ZPOPMIN 原子弹出，不会重复 | 完全满足 |
| N6 消费后删 | 弹出即从 ZSET 移除 | 完全满足 |
| N7 运维简单 | 大多数系统已部署 Redis，无需额外组件 | 完全满足 |

**劣势**：
- 内存受限，只能存放活跃任务（项目用 MySQL 做持久化弥补）
- 非严格持久化（AOF fsync=always 可接近，但有性能代价）
- 无原生消费组（项目通过 Lua 脚本 + inflight Hash 自建）

### 2.3 Kafka

**模型**：分区有序消息流，Consumer Group 竞争消费。

| 需求 | Kafka 表现 | 满足度 |
|------|-----------|--------|
| R1 多命名队列 | Topic 对应队列，但创建 Topic 是重操作 | 部分满足（不适合大量动态队列） |
| R2 延迟队列 | **原生不支持**。需自建：时间轮 Topic 或外部定时器 | 不满足，需大量额外开发 |
| R3 **优先级排序** | **不支持**。Partition 内严格 FIFO，无法按 priority 插队 | **不满足** |
| R4 Ack | Consumer offset commit | 满足 |
| R5 Nack + 延迟重入队 | 无 Nack 语义。需 produce 到重试 Topic + DLQ | 部分满足，架构复杂 |
| R6 批量原子晋升 | 无此概念 | 不适用 |
| R7 实时长度 | Consumer lag 可查，但不等于"就绪任务数" | 部分满足 |
| R8 多维统计 | 需额外计算 | 不满足 |
| N1 亚毫秒延迟 | P99 通常 2~10ms（磁盘刷写） | 部分满足 |
| N2 10万+ QPS | 满足（Kafka 吞吐极高） | 完全满足 |
| N3 原子出队 | Partition 单消费者保证 | 满足 |
| N7 运维简单 | 依赖 ZooKeeper/KRaft，运维重 | 不满足 |

**结论**：Kafka 是消息流平台，核心优势在于高吞吐有序日志。但 DispatchHub 需要的是**任务队列**（优先级 + 延迟 + Ack/Nack），Kafka 的 FIFO 分区模型与优先级需求根本冲突。用 Kafka 做任务调度相当于拿日志系统当数据库用——方向不对。

### 2.4 RabbitMQ

**模型**：AMQP 协议，Exchange → Queue，支持 Push/Pull。

| 需求 | RabbitMQ 表现 | 满足度 |
|------|-------------|--------|
| R1 多命名队列 | Queue 声明轻量 | 完全满足 |
| R2 延迟队列 | 插件 `rabbitmq-delayed-message-exchange` 或 TTL+DLX | 满足（需插件或变通） |
| R3 **优先级排序** | `x-max-priority` 参数，最多 255 级 | **满足**，但实现有局限 |
| R4 Ack | 原生 Basic.Ack | 完全满足 |
| R5 Nack + 延迟重入队 | Basic.Nack + requeue=true | 满足（但延迟重入队需要 DLX 迂回） |
| R6 批量原子晋升 | 延迟插件自动处理 | 满足 |
| R7 实时长度 | Management API | 满足 |
| R8 多维统计 | Management API 提供多维度 | 满足 |
| N1 亚毫秒延迟 | P99 约 1~5ms | 部分满足 |
| N2 10万+ QPS | 单机约 2~5万 QPS，需集群 | **部分满足，达到10万需要较大集群** |
| N3 原子出队 | Prefetch + Ack 机制保证 | 满足 |
| N7 运维简单 | 独立组件，Erlang 运维有门槛 | 部分满足 |

**RabbitMQ 优先级队列的实际问题**：

RabbitMQ 的优先级队列虽然存在，但有工程限制：

1. **内存开销**：每个优先级在内部维护一个子队列，10 个级别 = 10 倍内存结构开销
2. **消费者预取干扰**：`prefetch` 机制会预取消息到客户端缓冲区，低优先级消息可能已被预取而不能被高优先级插队
3. **集群模式下表现退化**：镜像队列中优先级排序可能不一致
4. **Nack 延迟重入队**：不能直接 Nack 到延迟队列，需要 DLX+TTL 绕路实现，架构变复杂

**结论**：RabbitMQ 功能上最接近需求，但吞吐瓶颈明显（10万 QPS 需要大集群），优先级机制在高负载下有缺陷，且引入了独立的 Erlang 组件增加运维复杂度。

### 2.5 RocketMQ

**模型**：阿里开源，类 Kafka 设计 + 增强功能。

| 需求 | RocketMQ 表现 | 满足度 |
|------|-------------|--------|
| R1 多命名队列 | Topic + Tag | 满足 |
| R2 延迟队列 | 原生支持 18 个延迟级别（1s/5s/10s/30s/1m/...） | **部分满足，级别固定不灵活** |
| R3 **优先级排序** | **不支持**。Queue 内 FIFO | **不满足** |
| R4 Ack | Consumer offset | 满足 |
| R5 Nack + 延迟重入队 | 重试队列 `%RETRY%` 自动处理 | 满足（但延迟级别固定） |
| R7/R8 统计 | Console 提供 | 满足 |
| N1 亚毫秒延迟 | P99 约 1~5ms | 部分满足 |
| N2 10万+ QPS | 单机约 10万 QPS | 满足 |
| N7 运维简单 | NameServer + Broker，组件较多 | 部分满足 |

**结论**：RocketMQ 的延迟消息和重试队列有原生支持，比 Kafka 更适合任务场景。但致命问题与 Kafka 相同——**不支持优先级排序**，FIFO 模型无法让高优先级任务插队。延迟级别固定（18 档）也不满足 DispatchHub 任意精度延迟的需求。

### 2.6 Pulsar

**模型**：计算存储分离，BookKeeper 持久化。

| 需求 | Pulsar 表现 | 满足度 |
|------|-----------|--------|
| R2 延迟队列 | 原生支持任意精度延迟投递 | 完全满足 |
| R3 **优先级排序** | **不支持** | **不满足** |
| N2 10万+ QPS | 满足 | 满足 |
| N7 运维简单 | ZooKeeper + BookKeeper + Broker，最重 | **不满足** |

**结论**：Pulsar 架构先进但运维复杂度最高，同样不支持优先级排序。

### 2.7 MySQL 轮询

**模型**：`SELECT ... WHERE state=pending ORDER BY priority DESC LIMIT N FOR UPDATE`。

| 需求 | MySQL 表现 | 满足度 |
|------|-----------|--------|
| R1 多命名队列 | WHERE queue_name = ? | 满足 |
| R2 延迟队列 | WHERE schedule_at <= NOW() | 满足 |
| R3 **优先级排序** | ORDER BY priority DESC | **满足** |
| R4 Ack | UPDATE state = completed | 满足 |
| R5 Nack | UPDATE state = pending/retrying | 满足 |
| N1 亚毫秒延迟 | **不满足**。轮询间隔 + 锁竞争，P99 通常 10~100ms | **不满足** |
| N2 10万+ QPS | **不满足**。SELECT FOR UPDATE 锁竞争严重，实际 ~千QPS | **不满足** |
| N3 原子出队 | FOR UPDATE 行锁 | 满足但有性能代价 |

**结论**：MySQL 功能上全部满足（甚至优先级支持最自然），但性能是致命瓶颈。高并发下 `FOR UPDATE` 行锁竞争导致吞吐暴跌，轮询模型也引入额外延迟。适合日任务量 < 1万的场景，不适合三高。

---

## 三、横向对比矩阵

以项目 8 项功能需求 + 7 项非功能需求为评分标准：

| 需求 | 权重 | Redis ZSET | Kafka | RabbitMQ | RocketMQ | Pulsar | MySQL |
|------|------|-----------|-------|----------|----------|--------|-------|
| R1 多命名队列 | 中 | ✅ | ⚠️ | ✅ | ✅ | ✅ | ✅ |
| R2 延迟队列 | 高 | ✅ | ❌ | ⚠️ | ⚠️ | ✅ | ✅ |
| **R3 优先级排序** | **极高** | **✅** | **❌** | **⚠️** | **❌** | **❌** | **✅** |
| R4 Ack | 高 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| R5 Nack+延迟重试 | 高 | ✅ | ⚠️ | ⚠️ | ✅ | ⚠️ | ✅ |
| R6 批量原子晋升 | 中 | ✅ | — | — | — | — | ⚠️ |
| R7 实时长度 | 低 | ✅ | ⚠️ | ✅ | ✅ | ✅ | ✅ |
| R8 多维统计 | 低 | ✅ | ⚠️ | ✅ | ✅ | ✅ | ✅ |
| N1 亚毫秒延迟 | 高 | ✅ | ⚠️ | ⚠️ | ⚠️ | ⚠️ | ❌ |
| N2 10万+QPS | 高 | ✅ | ✅ | ⚠️ | ✅ | ✅ | ❌ |
| N3 原子出队 | 高 | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ |
| N4 拉模型 | 中 | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| N5 竞争消费 | 高 | ✅ | ✅ | ✅ | ✅ | ✅ | ⚠️ |
| N6 消费后删 | 中 | ✅ | ❌ | ✅ | ❌ | ❌ | ✅ |
| N7 运维简单 | 中 | ✅ | ❌ | ⚠️ | ⚠️ | ❌ | ✅ |
| **统计** | | **15/15** | **6/15** | **10/15** | **9/15** | **8/15** | **11/15** |

✅ 完全满足 ⚠️ 部分满足/需额外开发 ❌ 不满足

---

## 四、核心决策点分析

### 4.1 决定性因素：优先级排序（R3）

这是整个选型的**分水岭**。

DispatchHub 的核心调度语义是：

```go
// pkg/store/redis/queue.go:47
score := float64(-task.Priority)  // -10 < -8 < -5 < -1

// Dequeue 注释
// "atomically pops the highest-priority task from any of the given queues"
```

即**同一个队列中，优先级高的任务必须先被消费**，而不是先入先出。

这直接排除了 Kafka、RocketMQ、Pulsar——它们的设计哲学是 **Partition 内有序日志**，消息一旦写入，位置不可变，后写入的高优先级消息不可能插到前面。

虽然可以用多个 Partition/Topic 模拟优先级（如 `queue-p10`、`queue-p8`、`queue-p5`），但这会导致：
- 消费者需要复杂的多 Topic 轮询逻辑
- 优先级数量受 Partition/Topic 数量限制
- 跨 Partition 的全局排序无法保证
- 一个 Partition 空了才会消费下一个，不是严格按 priority 排序

### 4.2 排除 MySQL 后的唯一选择

MySQL 支持优先级排序（`ORDER BY priority DESC`），但：

```sql
-- 这个查询在 10万 QPS 下会崩溃
SELECT * FROM tasks
WHERE queue_name='default' AND state=0
ORDER BY priority DESC, created_at ASC
LIMIT 1
FOR UPDATE SKIP LOCKED;
```

- `FOR UPDATE` 行锁在高并发下引起锁等待和死锁
- 轮询间隔引入 10~100ms 额外延迟
- InnoDB 行锁粒度在热点行上严重退化

MySQL 适合做**持久化存储**（DispatchHub 确实这样用了），不适合做**高频出队的队列**。

### 4.3 Redis ZSET 为什么天然适配

| DispatchHub 需要什么 | Redis ZSET 提供什么 |
|---------------------|-------------------|
| 按优先级排序出队 | ZPOPMIN 取 score 最小成员，O(log N) |
| 延迟到指定时间 | ZRANGEBYSCORE 按时间戳范围查询 |
| 原子出队+状态转移 | Lua 脚本内多命令单线程执行 |
| 消费后从队列移除 | ZPOPMIN 弹出即删 |
| 实时查队列长度 | ZCARD O(1) |
| 多队列独立操作 | 不同 Key 完全隔离 |

Redis 的 Sorted Set 本质上是一个**可持久化的内存优先级队列**，数据结构层面与 DispatchHub 的需求完全对齐，无需任何 adapter 或 workaround。

---

## 五、Redis 方案的已知权衡与应对

选 Redis 不是没有代价，以下是项目中的应对策略：

### 5.1 内存受限

**问题**：Redis 数据全在内存，不能存全量历史任务。

**应对**（已实现）：

```
Redis 只存活跃任务 (pending + delayed + inflight)
          │
          │  任务完成/失败
          ▼
MySQL 存全量状态 (tasks 表 + task_events 表)
```

分工明确：Redis 是快速队列的"热路径"，MySQL 是持久化的"冷路径"。两者在 `SubmitTask` 时同步写入：

```go
// pkg/scheduler/scheduler.go
s.taskStore.Create(ctx, task)  // MySQL 持久化
s.broker.Enqueue(ctx, ...)     // Redis 入队
```

### 5.2 非严格持久化

**问题**：Redis 默认 AOF 每秒刷盘，极端情况可能丢失 1 秒数据。

**应对**（已实现）：

- 任务已经在 MySQL 中持久化，Redis 丢失后可从 MySQL 重建
- inflight 中的任务通过 Worker 心跳 + Scheduler 健康检查兜底

### 5.3 无原生消费组

**问题**：Redis ZSET 没有 Kafka Consumer Group 那样的消费组抽象。

**应对**（已实现）：

项目自建了完整的消费语义：

| 消费组功能 | 项目实现 |
|-----------|---------|
| 竞争消费 | Lua 脚本 ZPOPMIN 原子弹出，多 Worker 竞争同一个 ZSET |
| 消费确认 | inflight Hash 跟踪 + Ack/Nack |
| 重试 | Nack 将任务重新 ZADD 到 ready 或 delayed |
| 消费进度 | 不需要 offset（弹出即消费，无回溯需求） |

### 5.4 大 Key 风险

**问题**：单个 ZSET 元素过多时（>100万），操作延迟上升。

**应对**：

- 通过多队列分散（`high-priority`、`default`、`batch`）
- Redis Cluster 按 Key 自动分片
- 延迟晋升每次最多 100 个，避免大批量操作阻塞

---

## 六、结论

### 选型决策树

```
需求: 优先级排序出队?
  │
  ├── 不需要 → Kafka/RocketMQ/Pulsar (消息流场景)
  │
  └── 需要 → 需求: 10万+ QPS?
               │
               ├── 不需要 → MySQL 轮询 (最简单)
               │
               └── 需要 → 需求: 延迟队列 + 原子出队?
                            │
                            ├── 部分需要 → RabbitMQ (需要接受吞吐限制)
                            │
                            └── 完全需要 → Redis Sorted Set ✅
```

### 一句话总结

> DispatchHub 是**任务调度系统**而非**消息流平台**——优先级排序是核心需求，这直接排除了所有基于 FIFO 日志模型的 MQ（Kafka/RocketMQ/Pulsar）；在剩余选项中，Redis ZSET 以 O(log N) 有序出队 + Lua 原子操作 + 亚毫秒延迟 + 10万级 QPS 的组合完胜，配合 MySQL 持久化弥补内存短板，形成了最符合项目需求的方案。

### 同类项目验证

| 项目 | 队列选型 | 与 DispatchHub 的共同点 |
|------|---------|----------------------|
| [Asynq](https://github.com/hibiken/asynq) | Redis (List + ZSET) | 优先级队列、延迟任务、Ack/Nack |
| [Sidekiq](https://github.com/sidekiq/sidekiq) | Redis (List + ZSET) | 优先级、重试、调度 |
| [Bull](https://github.com/OptimalBits/bull) | Redis (List + ZSET) | 优先级、延迟、重试 |
| [Machinery](https://github.com/RichardKnop/machinery) | Redis / AMQP | 任务编排、重试 |

主流任务队列项目几乎全部选择 Redis 作为默认后端，原因一致：ZSET 天然适配任务调度的优先级 + 延迟语义。
