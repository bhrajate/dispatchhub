# DispatchHub 面试项目讲解

> STAR 原则组织，8-12 分钟。标注【深挖】的段落为面试官可能追问的方向。

---

## 一、Situation -- 项目背景

DispatchHub 是一个云原生分布式任务调度系统，处理邮件发送、数据导出、报表生成、定时对账等异步任务。核心挑战：**量大**（日均百万级）、**有优先级差异**（交易对账优先级远高于营销邮件）、**需要可靠执行**（不能丢任务、不能重复执行）。

技术调研结论：Kafka/RocketMQ 不支持优先级排序；MySQL 轮询有并发锁瓶颈；Asynq 缺少控制面/数据面分离和双写补偿。决定自研，目标同时满足高并发、高可用、高性能。

---

## 二、Task -- 我的职责

作为核心开发者，负责整体架构设计和核心模块编码：

1. 控制面/数据面分离的整体架构
2. Redis Sorted Set + Lua 原子脚本的优先级队列
3. etcd Leader 选举与 Worker 服务注册发现
4. Worker 拉模型执行引擎与 channel 信号量背压
5. MySQL-Redis 双写补偿机制
6. CronJob 定时任务调度
7. DDD 分层架构设计
8. Kubernetes 部署方案，含 HPA 自动伸缩

---

## 三、Action -- 技术方案

### 3.1 架构：控制面/数据面分离

借鉴 Kubernetes 设计哲学，三层分离：

**控制面 -- Scheduler：** 负责调度决策。部署 3 副本，etcd `concurrency.Election` 保证单 Leader 运行，15s TTL Lease，Standby 随时接管。Leader 运行 7 个后台循环：延迟晋升（promoteDelayedLoop, 每 1s）、补偿扫描（compensateLoop, 每 30s）、CronJob 触发（cronLoop, 每 1s）、Worker 健康检查（healthCheckLoop, 每 10s）、任务清理（cleanupLoop, 每 1h, 删除 7 天前终态任务）、metrics 发布、Worker Watch。Scheduler 仅暴露 :8080 运维端口（healthz/readyz/metrics），不对外暴露 gRPC。

**数据面 -- Worker：** 负责任务执行，完全无状态。Kubernetes HPA 根据 CPU 利用率和自定义指标 `dispatchhub_worker_active_tasks` 在 3-50 副本间伸缩。暴露 :8080（运维）和 :9091（metrics）。`terminationGracePeriodSeconds=60`，收到 SIGTERM 后停止拉取，等待 in-flight 任务完成（最多 30s ShutdownTimeout）。

**接入层 -- API Server：** 系统唯一外部入口，无状态网关。暴露 :8080 HTTP + :9090 gRPC + :9091 metrics，前置 Ingress。2+ 副本水平扩展。Scheduler 和 Worker 不暴露任务管理 API，所有任务提交/查询/取消都通过 API Server。

存储层职责分离：Redis 做队列热路径（入队/出队）、MySQL 做持久化冷路径（状态+审计）、etcd 做协调（选举+注册）。没有任何单一中间件能同时满足低延迟队列操作、持久化可靠性和强一致协调。

> 【深挖：Scheduler 和 Worker 之间怎么通信？】
>
> **零直接通信。** Redis、etcd、MySQL 作为中间层完成所有协调。Scheduler 把任务放进 Redis 队列，Worker 从 Redis 拉取。Worker 通过 etcd 注册自身信息（ID/Hostname/Queues/负载），Scheduler 通过 Watch etcd 感知 Worker 上下线。这种设计让两者完全解耦，Worker 可以独立伸缩，Scheduler 完全不需要感知具体有哪些 Worker 在消费。

---

### 3.2 队列选型：为什么选 Redis Sorted Set 而非消息队列

核心需求：**支持优先级排序的出队**。优先级为 10 的任务必须比优先级为 5 的先被取走。

排除主流 MQ 的原因：

| MQ | 排除原因 |
|-----|---------|
| Kafka | Partition 内严格 FIFO，消息位置不可变，高优先级无法插队 |
| RocketMQ | 同 FIFO 模型，仅有延迟消息，不支持优先级 |
| RabbitMQ | `x-max-priority` 受 prefetch 破坏（低优先级已预取到客户端缓冲区）；单机吞吐 2-5 万 QPS；Erlang 运维成本高 |
| MySQL | `ORDER BY priority` 天然支持，但 `SELECT ... FOR UPDATE` 并发出队时锁竞争严重，50+ Worker 后 QPS 不升反降 |

Redis Sorted Set 选型理由：
- `score = -priority`，`ZPOPMIN` 取最小 score 即最高优先级，O(log N)
- 延迟队列用独立 ZSET，`score = 执行时间戳`，`ZRANGEBYSCORE` 扫描到期任务
- Lua 脚本把 ZPOPMIN + HSET（inflight 标记）合并为原子操作，Redis 单线程无竞态
- 单机即可达 10 万+ QPS，Worker 数量增加对性能无影响

与业界一致：Asynq、Sidekiq、Bull 全部选择 Redis 作为默认后端。

> 【深挖：MySQL FOR UPDATE 的具体问题？】
>
> 四个层面：
> 1. **锁竞争热点**：所有 Worker 抢优先级最高的前 N 行，大量加锁/跳过操作
> 2. **间隙锁阻塞入队**：REPEATABLE READ 下 Next-Key Lock 锁住索引间隙，新任务 INSERT 被出队的 SELECT FOR UPDATE 阻塞。入队和出队本应互不干扰
> 3. **事务开销**：每次出队 BEGIN -> 事务 ID -> 索引扫描 -> 行锁 -> redo log fsync -> COMMIT，即使最简单操作也有磁盘 I/O
> 4. **空队列轮询**：队列空时 100 个 Worker 每秒产生上千次无效 SELECT

---

### 3.3 Lua 原子出队：防止重复消费

分布式场景下最怕一个任务被两个 Worker 同时取走。通过 Lua 脚本实现原子出队：

```lua
-- dequeueScript: 遍历多个队列, ZPOPMIN + HSET inflight 原子完成
local queues = KEYS
for i, queue_key in ipairs(queues) do
    local result = redis.call('ZPOPMIN', queue_key, 1)
    if #result > 0 then
        local data = result[1]
        local task = cjson.decode(data)
        local inflight_key = string.gsub(queue_key, ':ready', ':inflight')
        redis.call('HSET', inflight_key, task.id, data)
        return data
    end
end
return nil
```

Redis 单线程执行 Lua 脚本，ZPOPMIN 和 HSET 之间不会被其他命令打断。弹出后立即从 ZSET 删除 + 放入 inflight Hash，不存在"弹出了但没标记"的中间状态。

inflight Hash 追踪正在执行的任务：Worker 完成后 Ack 删除记录；Worker 崩溃则 Scheduler 健康检查发现超时，将 inflight 中的任务重新放回 ready 队列，实现 at-least-once 语义。

> 【深挖：Lua 脚本性能问题？】
>
> Lua 在 Redis 单线程中执行，长脚本会阻塞其他请求。做法：严格控制每个 Lua 脚本复杂度（出队脚本仅 2-3 个 Redis 命令）；延迟晋升脚本限制每次最多处理 100 个任务。Redis Cluster 模式下，Lua 操作的 Key 必须在同一个 slot，通过 Hash Tag（`{queue_name}`）保证。

---

### 3.4 MySQL-Redis 双写补偿

任务提交先写 MySQL 再写 Redis，两步非原子。Redis 写失败时任务卡在 MySQL Pending 状态，Worker 永远取不到。

**补偿机制 (compensateLoop)：**

1. 每 30 秒扫描 MySQL 中 `state=Pending AND updated_at < now()-30s` 的任务（留足正常入队的时间窗口）
2. 对每个疑似孤儿任务，执行 `EnqueueIfNotInflight` Lua 脚本原子检查：
   - 任务 ID 在 inflight Hash 中 -> 跳过（Worker 已取走正在处理）
   - 不在 inflight 中 -> ZADD 到 ready 队列（真正的孤儿，补偿入队）
3. 补偿成功后调用 `TouchUpdatedAt` 刷新 updated_at（不递增 version），防止下轮扫描重复命中，同时保证 Worker 拿到的 version 仍然有效

```lua
-- enqueueIfNotInflightScript
local inflight_key = KEYS[1]
local ready_key = KEYS[2]
local task_id = ARGV[1]
local score = tonumber(ARGV[2])
local data = ARGV[3]
if redis.call('HEXISTS', inflight_key, task_id) == 1 then
    return 0  -- 在 inflight 中, 跳过
end
redis.call('ZADD', ready_key, score, data)
return 1  -- 补偿入队成功
```

关键设计点：
- **ZADD 幂等**：任务已在 ready 队列时重复 ZADD 同一 member 是 no-op
- **Lua 原子检查**：HEXISTS + ZADD 在 Redis 单线程中原子执行，不存在"检查时不在 inflight，ZADD 前被 Worker 取走"的竞态
- **时间窗口保护**：只扫描 30 秒前的任务，避免误判正在正常入队流程中的任务

> 【深挖：为什么不用分布式事务？】
>
> XA/TCC 把 MySQL 和 Redis 绑定在一个事务边界内，每次提交多一轮协调，10 万+ QPS 场景代价不可接受。补偿扫描是最终一致方案：正常路径零额外开销，异常路径后台补偿兜底。实际上代码中 Redis 入队失败时故意不返回 error（`_ = s.broker.Enqueue(...)`），避免客户端重试产生重复任务，由补偿循环在 30 秒内恢复。

---

### 3.5 Worker 背压控制

Worker 采用 Pull 模型，通过 **channel 信号量**控制并发：

```
fetchLoop:
  1. sem <- struct{}{}    // 往 channel 写 token，channel 满则阻塞（停止从 Redis 取任务）
  2. Dequeue()            // 从 Redis 取一个任务
  3. go process(task)     // 起 goroutine 执行，完成后 <-sem 释放位置
```

channel 容量 = 最大并发数（默认 100，可通过 `worker.concurrency` 配置）。100 个 goroutine 全忙时 fetchLoop 阻塞在 step 1，Redis 中的任务安全留在队列。**满了就不取，腾出来就继续。**

空队列时指数退避（100ms -> 200ms -> 400ms -> ... -> 2s cap），避免空轮询消耗 Redis 连接。取到任务后立即 reset backoff。

> 【深挖：为什么用 Pull 不用 Push？】
>
> Push 模式下 Scheduler 需要知道每个 Worker 的负载才能推送，引入状态同步开销。Worker 崩溃时已推出去的任务可能丢失。Pull 模式下任务一直在 Redis 队列中，Worker 崩溃后其他 Worker 照常取走。新 Worker 上线立即开始拉取，Scheduler 完全不需要感知。弹性伸缩变得自然。

---

### 3.6 Leader 选举与故障转移

Scheduler 的调度循环（延迟晋升、补偿扫描、健康检查）如果多实例同时运行会导致重复调度，用 etcd `concurrency.Election` 实现 Leader 选举：

1. 3 个 Scheduler 副本同时 Campaign，仅一个当选 Leader
2. Leader 持有 **15 秒 TTL 的 etcd Lease**（`concurrency.WithTTL(15)`），Session 自动续约
3. 仅 Leader 运行 7 个后台调度循环（由 `OnStartedLeading` 回调触发）
4. Leader 崩溃时 Lease 到期不续约，**15 秒内**另一个 Standby 自动接管
5. Leader 正常退出时主动 Resign，切换时间更短

K8s 部署 3 副本 + pod anti-affinity 分散到不同节点，防止单节点故障导致所有 Scheduler 不可用。

> 【深挖：Leader 切换期间任务会丢失吗？】
>
> 不会。Leader 只负责"调度决策"（延迟晋升、补偿入队），不负责任务执行。任务在 Redis 队列中，Worker 的拉取完全不依赖 Scheduler。15 秒窗口内唯一影响是延迟任务晋升和补偿扫描暂停，但不会丢任务，已经在执行的任务不受影响。

---

### 3.7 CronJob 定时任务

Scheduler 的 `cronLoop` 每秒检查一次到期的 CronJob：

1. `FindDueCronJobs`：查询 MySQL 中 `enabled=true AND next_run_at <= now()` 的记录（每次最多 `cron_batch_size=100` 条）
2. **ConcurrencyPolicy** 检查：
   - `Allow`（默认）：允许并发执行，直接触发
   - `Forbid`：检查是否有同类型任务正在运行（`HasRunningTasks`），有则跳过本次但仍推进 next_run_at
3. 由 CronJob 模板生成新 Task，写 MySQL + 入队 Redis
4. 通过 `cronutil.NextRunTime`（基于 robfig/cron 库）计算下次执行时间，更新 `last_run_at` 和 `next_run_at`

Cron 表达式由 API Server 在创建时解析（`pkg/cronutil`），domain 层不依赖 cron 解析库，保持纯净。

---

### 3.8 DDD 分层架构

采用领域驱动设计（DDD）分层，核心约束：**domain 层纯净，不引入 log/metrics 等基础设施依赖。**

```
interfaces/     -> HTTP/gRPC handler, 参数校验, 协议转换
application/    -> 编排层, 组合领域服务, 管理后台循环
domain/         -> 纯业务逻辑, 仅依赖标准库和 entity/repository 接口
infrastructure/ -> 仓储实现, Leader 选举, 配置管理
```

基础设施关注点通过 **Hook 机制**注入 domain 层：
- `BeforeSubmitHook`：限流、配额检查（infrastructure 实现，注入 TaskServiceImpl）
- `AfterSubmitHook`：日志、metrics（同上）
- Worker 中间件链（Recovery -> Logging -> Timeout）：洋葱模型，业务 Handler 只关注业务逻辑

例如 `SchedulerService`（domain 层）只定义 `CompensateOrphanedTasks` 的业务规则，不关心定时器、日志、metrics。`SchedulerAppService`（application 层）负责用 ticker 驱动循环并记录日志。

---

### 3.9 gRPC + HTTP 双 API

通过 Protobuf 定义 API（`api/proto/dispatch.proto`），`make proto` 生成 Go 代码。API Server 同时暴露 gRPC（:9090）和 HTTP REST（:8080）两种接口。支持任务 CRUD、CronJob CRUD、队列统计查询。

---

### 3.10 其他关键设计

**任务状态机：** 8 个状态 Pending -> Scheduled -> Running -> Completed/Failed/Timeout/Retrying/Cancelled。MySQL Version 字段做乐观锁，防止并发状态更新冲突。

**指数退避重试：** 失败任务按 `RetryBackoff` 进入延迟队列（delayed ZSET），Worker 空队列轮询也采用指数退避（100ms-2s）。

**Worker 中间件链：** Recovery（捕获 panic）-> Logging（记录执行耗时）-> Timeout（超时控制）。可插拔洋葱模型。

**优雅停机：** Worker 收到 SIGTERM 停止拉取新任务，等待 in-flight 任务完成（ShutdownTimeout=30s）。K8s `terminationGracePeriodSeconds=60s` 留足余量。

**任务清理：** `cleanupLoop` 每小时运行一次，删除 7 天前的终态任务（Completed/Failed/Cancelled/Timeout），防止 MySQL 表无限增长。

**Worker 执行前终态检查：** 取到任务后先查 MySQL 最新状态，如果已被取消/完成则跳过执行，直接 Ack。防止用户已取消但任务仍被执行。

---

## 四、Result -- 成果

1. **性能**：队列入队/出队 10 万+ QPS，P99 < 1ms
2. **高可用**：Scheduler Leader 故障切换 < 15s，Worker HPA 3-50 副本自动伸缩，滚动更新零停机
3. **可靠性**：MySQL-Redis 双写 + inflight 追踪 + 补偿循环 + 乐观锁，实现 at-least-once 语义
4. **可观测性**：Prometheus metrics + Zap 结构化 JSON 日志，覆盖任务提交量/执行耗时/队列深度/Worker 活跃数
5. **云原生部署**：Kubernetes YAML + Helm Chart，HPA 基于 CPU + 自定义指标（active tasks）自动伸缩

---

## 五、常见追问

### Q1：Redis 宕机了怎么办？任务会丢吗？

不会丢。每个任务提交时先写 MySQL（持久化），再写 Redis（队列）。Redis 挂了，任务数据在 MySQL 中完好。Scheduler 的 compensateLoop 每 30 秒扫描一次 MySQL 中 Pending 状态的孤儿任务，Redis 恢复后自动补偿入队。最坏情况下任务延迟 30 秒被执行，但不会丢失。

### Q2：如何保证任务不被重复消费？

三层保障：
1. **Lua 原子出队**：ZPOPMIN 弹出后立即从 ZSET 删除，不存在两个 Worker 取到同一任务
2. **inflight Hash**：出队后放入 inflight，Ack 后删除。补偿循环通过 `EnqueueIfNotInflight` 避免重复入队
3. **MySQL 乐观锁**：状态更新检查 Version 字段，并发更新只有一个成功

### Q3：延迟任务怎么实现？

独立的 Redis ZSET（delayed），`score = 执行时间戳（毫秒）`。Scheduler 的 `promoteDelayedLoop` 每秒扫描一次，Lua 脚本把 `score <= 当前时间` 的任务批量（每次最多 100 个）原子地从 delayed ZSET 移动到 ready ZSET，移动时 score 变为 `-priority`。到期后 Worker 正常取走执行。

### Q4：任务取消怎么实现？

API Server 直接修改 MySQL 中任务状态为 Cancelled。Worker 在执行前会查 MySQL 最新状态（`processTask` 中的终态检查），发现已取消则跳过执行直接 Ack。如果任务正在执行中，当前版本不强制中断（需要 Handler 自行检查 context），但状态更新因乐观锁保证不会覆盖。

### Q5：Scheduler 和 Worker 之间怎么通信？

**零直接通信。** 三个中间件做解耦：
- **Redis**：Scheduler 入队，Worker 出队
- **etcd**：Worker 注册自身信息，Scheduler Watch 拓扑变更
- **MySQL**：任务状态的 source of truth

### Q6：如果让你重新设计，会有什么改进？

1. **认证授权**：当前假设可信 K8s 集群内部署，缺少 mTLS 和 RBAC
2. **DAG 工作流**：当前仅支持独立任务和 CronJob，可扩展支持任务依赖编排
3. **多租户隔离**：数据层已有 Namespace 字段，但缺少配额管理
4. **OpenTelemetry**：加入分布式链路追踪，串联任务全生命周期
5. **Redis Cluster**：当前单实例 Redis，生产环境应部署集群模式

### Q7：为什么不直接用 Asynq / Temporal？

Asynq 和 DispatchHub 思路类似（Redis 后端），但 Asynq 缺少控制面/数据面分离架构、缺少 etcd Leader 选举、没有 MySQL-Redis 双写补偿机制。Temporal 侧重工作流编排，简单任务调度场景过重。自研的优势是完全贴合需求，团队对核心调度逻辑有完整掌控力。

---

## 六、一句话总结

DispatchHub 是基于 Go 的云原生分布式任务调度系统，用 Redis Sorted Set + Lua 脚本实现支持优先级排序的高性能队列，etcd Leader 选举实现 Scheduler 高可用，Worker 拉模型 + channel 信号量实现背压控制，MySQL-Redis 双写 + 补偿循环保证任务可靠投递，采用 DDD 分层架构保持 domain 层纯净，Kubernetes + HPA 实现弹性伸缩。
