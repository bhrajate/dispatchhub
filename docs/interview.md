# DispatchHub 面试项目介绍逐字稿

> 基于 STAR 原则（Situation-Task-Action-Result）组织，预计讲述时间 8-12 分钟。标注【技术深挖点】的段落为面试官可能追问的方向，需要准备更深的回答。

---

## 一、Situation（项目背景）

> 面试官你好，我想介绍一下我做的一个项目——DispatchHub，这是一个云原生的分布式任务调度系统。

项目的业务背景是这样的：我们需要处理大量异步任务，比如邮件发送、数据导出、报表生成、定时对账等场景。这些任务有几个共同特点：**量大**（日均百万级）、**有优先级差异**（比如交易对账的优先级要比营销邮件高得多）、**需要可靠执行**（不能丢任务、不能重复执行）。

在做技术调研的时候，我发现市面上的方案要么是消息流平台（Kafka、RocketMQ），适合做事件驱动但不支持优先级排序；要么是简单的任务队列（基于 MySQL 轮询），功能够用但性能有天花板。所以我们决定自研一套，目标是同时满足**高并发**（10 万+ QPS）、**高可用**（故障自动切换）和**高性能**（亚毫秒级出队延迟）。

---

## 二、Task（我的职责）

我作为这个项目的核心开发者，负责**整体架构设计和核心模块的编码实现**。具体来说包括：

1. 设计整体的控制面/数据面分离架构
2. 实现基于 Redis Sorted Set + Lua 脚本的优先级队列
3. 实现基于 etcd 的 Leader 选举和服务注册发现
4. 设计 Worker 的拉模型执行引擎和背压控制机制
5. 制定 Kubernetes 部署方案，包括 HPA 自动伸缩策略

---

## 三、Action（核心技术方案与难点）

### 3.1 架构设计：控制面/数据面分离

我借鉴了 Kubernetes 的设计哲学，把系统分成三层：

- **控制面（Scheduler）**：负责任务调度决策、延迟任务晋升、Worker 健康检查。部署 3 副本，通过 etcd Leader 选举保证**只有一个 Leader 在运行调度逻辑**，其余 Standby 节点随时待命。
- **数据面（Worker）**：负责任务的实际执行，完全无状态，可以随意水平扩展。通过 Kubernetes HPA 根据 CPU 和活跃任务数自动在 3~50 个副本之间伸缩。
- **接入层（API Server）**：系统唯一的外部入口，无状态 HTTP/gRPC 网关。Scheduler 和 Worker 仅暴露运维端点（healthz/readyz/metrics），不对外暴露任务管理 API。

存储层也做了职责分离——**Redis** 做队列热路径，负责入队出队这种高频操作；**MySQL** 做持久化冷路径，保存任务状态和审计日志；**etcd** 做协调层，负责 Leader 选举和 Worker 服务注册。

> **【技术深挖点：为什么要三层存储而不是用一个中间件搞定？】**
>
> 因为没有任何一个中间件能同时满足高频队列操作的低延迟、任务状态的持久化可靠性、和分布式协调的强一致性。Redis 快但内存有限且非严格持久化；MySQL 可靠但高并发出队有锁瓶颈；etcd 强一致但不适合存大量数据。三者各司其职才能覆盖所有需求。

---

### 3.2 核心难点一：队列选型——为什么选 Redis Sorted Set？

这是整个项目**最关键的技术决策**。

我的核心需求是：**支持优先级排序的出队**。简单来说就是同一个队列里，优先级为 10 的任务必须比优先级为 5 的先被取走，而不是先进先出。

这个需求直接排除了三个主流 MQ：

| MQ | 为什么不行 |
|-----|-----------|
| **Kafka** | Partition 内严格 FIFO，消息写入后位置不可变，高优先级消息无法插队 |
| **RocketMQ** | 同样是 FIFO 模型，虽然有延迟消息但不支持优先级 |
| **Pulsar** | 和 Kafka 同类，不支持优先级排序 |

那为什么不用 RabbitMQ？RabbitMQ 有 `x-max-priority` 参数，确实支持优先级队列。但有三个工程问题：一是 `prefetch` 机制会预取消息到客户端缓冲区，低优先级消息已经被预取了，高优先级消息来了也插不了队；二是单机吞吐只有 2~5 万 QPS，达到 10 万 QPS 需要较大集群；三是 Erlang 技术栈运维成本高。

那 MySQL 呢？MySQL 的 `ORDER BY priority DESC` 天然支持优先级，但问题在于**并发出队时的锁竞争**。100 个 Worker 同时执行 `SELECT ... FOR UPDATE`，InnoDB 的行锁、间隙锁会严重退化性能。实测 Worker 超过 50 个后，QPS 不升反降，天花板大约在 5000 QPS。

> **【技术深挖点：MySQL FOR UPDATE 的问题具体是什么？】**
>
> 我可以展开说四个层面：
> 1. **锁竞争热点**：所有 Worker 都在抢优先级最高的那几行，前 N 行成为热点，大量加锁/跳过操作消耗资源
> 2. **间隙锁阻塞入队**：REPEATABLE READ 下 Next-Key Lock 不仅锁住行，还锁住索引间隙，导致新任务 INSERT 被出队的 SELECT FOR UPDATE 阻塞——入队和出队本应互不干扰
> 3. **事务开销**：每次出队都要经历 BEGIN → 分配事务 ID → 索引扫描 → 行锁获取 → redo log fsync → COMMIT，即使是最简单的操作也有磁盘 I/O
> 4. **空队列轮询**：队列空了 Worker 只能 sleep 后重试，100 个 Worker 每秒产生上千次无效 SELECT

最终选择了 **Redis Sorted Set**。原因是 ZSET 在数据结构层面与任务调度完美对齐：

- **score = -priority**，`ZPOPMIN` 取 score 最小值即最高优先级，O(log N) 复杂度
- **延迟队列**用另一个 ZSET，score = 执行时间戳，通过 `ZRANGEBYSCORE` 扫描到期任务
- **Lua 脚本**把 `ZPOPMIN`（出队）和 `HSET`（标记 inflight）合并为一个原子操作，Redis 单线程执行天然无竞态
- 单机即可达到 **10 万+ QPS**，且 Worker 数量增加对 QPS 几乎无影响

顺便说一下，这个选型方向和业界主流一致——Asynq、Sidekiq、Bull 这些知名任务队列框架全部选择 Redis 作为默认后端。

---

### 3.3 核心难点二：Lua 原子出队——如何避免任务被重复消费？

分布式场景下最怕的就是**一个任务被两个 Worker 同时拿走**。我通过 Redis Lua 脚本实现了原子出队：

```lua
-- 原子操作：从 ready 队列弹出 + 放入 inflight 追踪
local task = redis.call('ZPOPMIN', KEYS[1])  -- 从就绪队列弹出最高优先级
if #task == 0 then return nil end
redis.call('HSET', KEYS[2], task[1], task[1]) -- 放入 inflight Hash
return task[1]
```

关键点在于：Redis 是**单线程执行模型**，Lua 脚本在执行期间不会被其他命令打断。所以 `ZPOPMIN` 和 `HSET` 要么全部执行，要么全部不执行，不存在"弹出了但没标记"的中间状态。

inflight Hash 的作用是追踪正在执行的任务。Worker 完成后调用 `Ack` 删除 inflight 记录；如果 Worker 崩溃了，Scheduler 的健康检查会发现该 Worker 超时，然后把它 inflight 中的任务重新放回就绪队列——这就实现了 **at-least-once** 的投递语义。

> **【技术深挖点：Lua 脚本的性能问题？】**
>
> Lua 脚本在 Redis 单线程中执行，如果脚本太长会阻塞其他请求。所以我严格控制每个 Lua 脚本的复杂度，出队脚本只有 2 个 Redis 命令，延迟晋升脚本限制每次最多处理 100 个任务。此外在 Redis Cluster 模式下，Lua 脚本操作的 Key 必须在同一个 slot，我通过 Hash Tag（在 Key 名中加 `{queue_name}`）来保证。

---

### 3.4 核心难点三：Worker 背压控制——如何防止过载？

Worker 采用**拉模型**（Pull），而不是 Scheduler 推任务过来。好处是 Worker 可以根据自身处理能力控制消费速度，天然具备**背压能力**。

具体实现是一个**基于 channel 的协程池 + 信号量模式**：

```
fetchLoop 循环:
  1. 往 sem channel 写入一个 token（如果 channel 满了就阻塞 → 停止从 Redis 取任务）
  2. 从 Redis Dequeue 取一个任务
  3. 起一个 goroutine 执行任务，执行完后从 sem channel 读出 token（释放一个位置）
```

channel 的容量就是最大并发数（默认 100）。当 100 个 goroutine 都在忙的时候，fetchLoop 阻塞在 step 1，Redis 中的任务安全地留在队列里不会丢失，等有空闲 goroutine 了再继续取。

这比传统的固定线程池方案更优雅——没有拒绝策略的复杂度，也不需要估算队列缓冲区大小。**满了就不取，腾出来就继续**，非常简洁。

> **【技术深挖点：为什么用 Pull 不用 Push？】**
>
> Push 模式下 Scheduler 需要知道每个 Worker 的负载情况才能做推送决策，这引入了额外的状态同步开销。而且 Worker 崩溃后，已经推出去的任务可能丢失。Pull 模式下任务一直在 Redis 队列中，Worker 崩溃了其他 Worker 照样能取走，天然容错。新 Worker 上线后立即开始拉取，Scheduler 完全不需要感知——弹性伸缩变得非常自然。

---

### 3.5 核心难点四：Leader 选举与故障转移

Scheduler 是有状态的——延迟任务晋升、Worker 健康检查这些操作如果多个实例同时跑，会导致重复调度。所以我用 etcd 的 `concurrency.Election` 实现了 Leader 选举。

核心机制：

1. 3 个 Scheduler 副本同时 Campaign（竞选），只有一个当选 Leader
2. Leader 获得一个 **15 秒 TTL 的 etcd Lease**，每 5 秒续约一次
3. 只有 Leader 运行调度循环（延迟晋升、健康检查、指标发布）
4. 如果 Leader 崩溃，Lease 到期不续约，**15 秒内** Standby 会自动竞选接管

为什么选 etcd 而不是 ZooKeeper？因为 etcd 是 Kubernetes 的核心组件，和我们的云原生技术栈一致；它基于 gRPC 的 Watch 机制比 ZK 的 Watcher 更简洁；Lease 机制天然支持临时注册，不需要手动清理过期节点。

> **【技术深挖点：Leader 切换期间任务会丢失吗？】**
>
> 不会。Leader 只负责"调度决策"（比如把延迟任务晋升到就绪队列），不负责任务执行。任务本身在 Redis 队列里，Worker 的拉取完全不依赖 Scheduler。所以 Leader 切换的 15 秒窗口内，唯一的影响是延迟任务的晋升可能会延后几秒，但不会丢任务，已经在执行中的任务也不受影响。

---

### 3.6 核心难点五：MySQL-Redis 双写一致性补偿

任务提交时先写 MySQL 再写 Redis，这两步不是原子的。如果 MySQL 写成功但 Redis 写失败，任务会永远卡在 Pending 状态——MySQL 有记录但 Redis 队列里没有，Worker 永远取不到它。

我通过 Scheduler 的**补偿扫描循环**解决了这个问题：

1. 每 30 秒扫描 MySQL 中 `state=Pending AND updated_at < now()-30s` 的任务（给正常入队留足时间窗口）
2. 对每个疑似孤儿任务，通过 Lua 脚本**原子检查** Redis inflight Hash：
   - 如果任务 ID 在 inflight 中 → 跳过（Worker 已取走正在处理，只是还没更新 MySQL）
   - 如果不在 inflight 中 → ZADD 到 ready 队列（真正的孤儿，补偿入队）
3. 补偿成功后 Update 任务刷新 `updated_at`，防止下次扫描重复命中

关键设计点：
- **ZADD 幂等性**：如果任务已经在 ready 队列中（正常入队成功的情况），重复 ZADD 相同 member 是 no-op
- **Lua 原子检查**：`HEXISTS inflight + ZADD ready` 在 Redis 单线程中原子执行，不会出现"检查时不在 inflight，ZADD 前被 Worker 取走"的竞态
- **时间窗口保护**：只扫描 30 秒前的任务，避免把正在正常入队流程中的任务误判为孤儿

> **【技术深挖点：为什么不用分布式事务？】**
>
> XA 事务或 TCC 模式会把 MySQL 和 Redis 绑定在一个事务边界内，代价是每次提交都多一轮网络往返和协调开销。对于 10 万+ QPS 的场景，这个代价不可接受。补偿扫描是**最终一致**的方案——正常路径零额外开销，异常路径通过后台补偿兜底，对性能的影响几乎为零。

---

### 3.7 其他关键设计

**任务生命周期管理**：设计了 8 个状态（Pending → Scheduled → Running → Completed/Failed/Timeout/Retrying/Cancelled），通过 MySQL 的 Version 字段做乐观锁，防止并发状态更新冲突。

**指数退避重试**：失败任务的重试间隔按 `baseDelay × 2^retryCount` 指数增长，并叠加 25% 的随机抖动（jitter），避免大量失败任务在同一时刻重试造成**惊群效应**。

**Worker 中间件链**：参考洋葱模型设计了可插拔的中间件——Recovery（捕获 panic）、Logging（记录执行耗时）、Timeout（超时控制）。业务 Handler 只需关注业务逻辑，横切关注点由中间件统一处理。

**优雅停机**：Worker 收到 SIGTERM 后停止从队列拉取新任务，等待已有的 in-flight 任务完成（最多 30 秒），然后优雅退出。配合 Kubernetes 的 `terminationGracePeriodSeconds: 60s`，保证滚动更新时不丢任务。

---

## 四、Result（项目成果）

1. **性能指标**：队列入队/出队达到 **10 万+ QPS**，单次出队延迟 P99 < 1ms，满足高并发场景需求
2. **高可用**：Scheduler Leader 故障切换时间 < 15 秒，Worker 支持 HPA 自动伸缩（3~50 副本），实现了零停机的滚动更新
3. **可靠性**：通过 Redis + MySQL 双写 + inflight 追踪 + 指数退避重试，实现了 at-least-once 投递语义，任务不丢失
4. **可观测性**：集成 Prometheus 指标 + Zap 结构化日志 + Grafana 看板，涵盖任务提交量、执行耗时、队列深度、Worker 活跃数等核心指标
5. **云原生部署**：提供完整的 Kubernetes YAML 和 Helm Chart，支持一键部署，Worker 基于自定义指标自动伸缩

---

## 五、面试常见追问与回答要点

### Q1：Redis 宕机了怎么办？任务会丢吗？

不会。每个任务提交时会**同时写入 MySQL 和 Redis**——MySQL 做持久化，Redis 做队列。Redis 挂了，任务数据在 MySQL 中完好，Redis 恢复后可以从 MySQL 重建队列。另外 Redis 本身也有 AOF 持久化机制做兜底。

### Q2：如何保证任务不被重复消费？

三层保障：
1. **Lua 原子出队**：ZPOPMIN 是原子弹出操作，弹出后立即从 ZSET 中删除，不会有两个 Worker 拿到同一个任务
2. **inflight Hash 追踪**：出队后立即放入 inflight，Ack 后才删除
3. **MySQL 乐观锁**：状态更新时检查 Version 字段，并发更新只有一个能成功

### Q3：延迟任务是怎么实现的？

用另一个 Redis ZSET，score 是执行时间戳（毫秒）。Scheduler 每秒扫描一次，通过 Lua 脚本把 `score <= 当前时间` 的任务批量（每次最多 100 个）原子地从 delayed ZSET 移动到 ready ZSET。这样延迟任务到期后就会被 Worker 正常取走执行。

### Q4：Scheduler 和 Worker 之间是怎么通信的？

它们之间**没有直接通信**。Scheduler 把任务放进 Redis 队列，Worker 从 Redis 队列取任务——Redis 是它们之间的解耦层。Worker 通过 etcd 注册自己的信息（IP、状态、负载），Scheduler 通过 Watch etcd 感知 Worker 的上下线。这种设计让两者完全解耦，Worker 可以独立伸缩。

### Q5：如果让你重新设计，会有什么改进？

1. **补充认证授权**：当前版本假设部署在可信的 K8s 集群内，没有做 API 层的认证鉴权，生产环境需要加上 mTLS 和 RBAC
2. **任务编排**：当前只支持独立任务，可以扩展支持 DAG 工作流（任务 A 完成后才触发任务 B）
3. **多租户隔离**：数据层已经有 Namespace 字段，但缺少配额管理和资源隔离
4. **可观测性增强**：可以加入 OpenTelemetry 分布式链路追踪，串联任务从提交到完成的全链路

### Q6：为什么不直接用 Asynq / Temporal 这些现成方案？

Asynq 确实和 DispatchHub 的思路类似，都是基于 Redis 的任务队列。但 Asynq 缺少控制面/数据面分离的架构、缺少 etcd 级别的 Leader 选举、没有 MySQL-Redis 双写补偿机制。Temporal 更侧重工作流编排，对于简单的任务调度场景太重了。自研的好处是完全贴合业务需求，也让团队对核心调度逻辑有完整的掌控力。

---

## 六、一句话总结（收尾用）

> DispatchHub 是一个基于 Go 语言的云原生分布式任务调度系统，核心创新点是用 Redis Sorted Set + Lua 脚本实现了支持优先级排序的高性能队列，配合 etcd Leader 选举实现 Scheduler 高可用，Worker 拉模型+协程池实现背压控制，最终在 10 万+ QPS 的并发场景下保证了任务的可靠调度和执行。
