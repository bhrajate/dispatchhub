# 架构设计

## 整体架构

DispatchHub 采用**控制面 / 数据面**分离架构，借鉴了 Kubernetes 的设计哲学：

- **控制面（Scheduler）**：负责任务调度决策、Worker 拓扑管理、延迟任务晋升
- **数据面（Worker）**：负责任务的实际执行、结果上报、心跳维持
- **接入层（API Server）**：无状态网关，对外提供 HTTP/gRPC 接口

```
                         ┌──────────────────────┐
                         │    Client / CLI       │
                         └──────────┬───────────┘
                                    │ HTTP / gRPC
                         ┌──────────▼───────────┐
                         │     API Server (N)    │  无状态, 水平扩展
                         └──────────┬───────────┘
                                    │
            ┌───────────────────────┼───────────────────────┐
            │                       │                       │
   ┌────────▼─────────┐   ┌────────▼─────────┐   ┌────────▼─────────┐
   │ Scheduler (Leader)│   │Scheduler (Standby)│   │Scheduler (Standby)│
   └────────┬─────────┘   └──────────────────┘   └──────────────────┘
            │
     ┌──────┴──────┐
     │             │
┌────▼────┐  ┌────▼────┐
│  Redis  │  │  etcd   │
│ (队列)   │  │ (协调)   │
└────┬────┘  └────┬────┘
     │            │
     │    ┌───────┴──────────┐
     │    │   Worker Pool    │
     │    │  ┌──┐┌──┐┌──┐   │
     │    │  │W1││W2││W3│...│  HPA 自动伸缩
     │    │  └──┘└──┘└──┘   │
     │    └──────┬───────────┘
     │           │
┌────▼───────────▼────┐
│       MySQL         │
│   (持久化任务状态)     │
└─────────────────────┘
```

## 三高设计方案

### 高并发 (High Concurrency)

| 机制 | 实现 | 文件位置 |
|------|------|----------|
| 优先级队列 | Redis Sorted Set，score = 负优先级，ZPOPMIN 获取最高优先级任务 | `pkg/store/redis/queue.go` |
| 原子出队 | Lua 脚本实现 ZPOPMIN + HSET inflight 原子操作，避免竞态 | `pkg/store/redis/queue.go` |
| 协程池 | 基于 channel semaphore 的有界协程池，超出时自动阻塞（背压） | `pkg/worker/worker.go` |
| 令牌桶限流 | 每个队列独立的令牌桶限流器，精确控制入队速率 | `pkg/ratelimit/ratelimit.go` |
| Pipeline | Redis Pipeline 批量执行，减少网络往返 | `pkg/store/redis/queue.go` |
| 连接池 | Redis PoolSize=100，MySQL MaxOpenConns=50，避免连接瓶颈 | `pkg/config/config.go` |

**背压机制设计**：

```
fetchLoop:
  ┌─────────────────┐
  │ sem <- struct{}  │ ← 当协程池满时阻塞在这里, 停止从Redis取任务
  │ (获取令牌)        │    Redis 中的任务不会丢失, 等待有空闲协程后继续
  └────────┬────────┘
           │
  ┌────────▼────────┐
  │ broker.Dequeue() │ ← 从 Redis ZPOPMIN 取出一个任务
  └────────┬────────┘
           │
  ┌────────▼────────┐
  │ go processTask() │ ← 在新协程中执行, defer 释放令牌
  └─────────────────┘
```

### 高可用 (High Availability)

| 机制 | 实现 | 文件位置 |
|------|------|----------|
| Leader 选举 | 基于 etcd concurrency 包，类似 K8s client-go leaderelection | `pkg/election/election.go` |
| 多副本热备 | Scheduler 部署 3 副本，仅 Leader 运行调度循环，Standby 等待接管 | `cmd/scheduler/main.go` |
| 心跳检测 | Worker 每 5s 发送心跳，Scheduler 30s 未收到则摘除并重新调度任务 | `pkg/worker/worker.go` |
| 临时注册 | etcd Lease 机制，Worker 崩溃后 15s 内自动注销 | `pkg/store/etcd/registry.go` |
| 乐观锁 | MySQL Version 字段防止并发更新冲突 | `pkg/store/mysql/task_store.go` |
| 优雅停机 | 信号捕获 + WaitGroup 等待 in-flight 任务完成 | `pkg/worker/worker.go` |
| Pod 反亲和 | K8s podAntiAffinity 确保 Scheduler 副本分布在不同节点 | `deploy/kubernetes/` |

**Leader 选举流程**：

```
Scheduler-1                Scheduler-2              Scheduler-3
    │                          │                        │
    ├── Campaign ──────────────┼────────────────────────┤
    │   (竞选 Leader)           │                        │
    │                          │                        │
    ▼                          │                        │
 当选 Leader                    │                        │
    │                          │                        │
    ├── Run() 启动调度循环       ├── Campaign (阻塞等待)   ├── Campaign (阻塞等待)
    │   - promoteDelayed       │                        │
    │   - healthCheck          │                        │
    │   - metricsPublish       │                        │
    │                          │                        │
    ╳ 崩溃                      │                        │
                               │                        │
                               ▼                        │
                            当选 Leader                  │
                               │                        │
                               ├── Run() 接管调度         ├── Campaign (继续等待)
```

### 高性能 (High Performance)

| 机制 | 实现 | 文件位置 |
|------|------|----------|
| 一致性哈希 | CRC32 + 虚拟节点（默认150），Worker 增减时最小化任务重分配 | `pkg/hash/consistent.go` |
| gRPC 长连接 | Keepalive 心跳，连接复用，Protobuf 高效序列化 | `pkg/api/grpc/server.go` |
| 延迟晋升 | 每秒扫描一次延迟队列，批量移动到就绪队列（每次最多100个） | `pkg/scheduler/scheduler.go` |
| 批量操作 | BatchUpdateState 单次 SQL 更新多个任务状态 | `pkg/store/mysql/task_store.go` |
| 指数退避 | 重试间隔指数增长 + 25%随机抖动，避免惊群效应 | `pkg/retry/retry.go` |
| 无锁计数 | Worker 活跃任务数使用 atomic.Int64，避免锁竞争 | `pkg/worker/worker.go` |

## 设计决策

### 为什么选择 Redis Sorted Set 作为队列？

1. **优先级支持**：ZSET 天然支持按 score 排序，完美实现优先级队列
2. **原子操作**：Lua 脚本保证出队 + 移入 inflight 的原子性
3. **延迟队列**：利用 score 存储执行时间，ZRANGEBYSCORE 实现定时触发
4. **性能**：单机 10 万+ QPS，Redis Cluster 线性扩展

### 为什么选择 etcd 而非 ZooKeeper？

1. **云原生生态**：etcd 是 Kubernetes 的核心组件，与云原生技术栈一致
2. **简洁 API**：基于 gRPC 的 Watch 机制比 ZK 的 Watcher 更直观
3. **Lease 机制**：天然支持节点临时注册，无需手动清理
4. **强一致性**：Raft 协议保证数据一致，适合 Leader 选举场景

### 为什么 Scheduler 需要 Leader 选举？

1. **避免重复调度**：多个 Scheduler 同时运行会导致任务被重复分发
2. **状态一致性**：延迟晋升、健康检查等操作需要单点执行
3. **HA 保障**：Standby 节点随时可接管，故障切换时间 < 15s（Lease TTL）

### 为什么 Worker 使用拉模型（Pull）而非推模型（Push）？

1. **天然背压**：Worker 根据自身处理能力主动拉取，不会被过载
2. **简化调度**：Scheduler 无需维护 Worker 负载状态来做推送决策
3. **容错性**：Worker 崩溃后任务留在 Redis，其他 Worker 可接管
4. **弹性伸缩**：新 Worker 上线后立即开始拉取，无需 Scheduler 感知

## 数据流

### 任务提交到完成的完整流程

```
1. Client ──POST /api/v1/tasks──▶ API Server
                                       │
2.                                     ▼
                                  Scheduler.SubmitTask()
                                       │
3.                            ┌────────┴────────┐
                              │                 │
                              ▼                 ▼
                        MySQL.Create()    Redis.Enqueue()
                        (持久化)            (入队)
                              │                 │
4.                            │           Worker.fetchLoop()
                              │                 │
                              │                 ▼
                              │           Redis.Dequeue()  ← Lua 原子出队
                              │                 │
5.                            │           Worker.processTask()
                              │                 │
                              │           ┌─────┴─────┐
                              │           │           │
6.                            │      成功: Ack()   失败: Nack()
                              │           │           │
                              │           ▼           ▼
                              │     MySQL.Update   重入队列
                              │     (Completed)    (Retrying)
```

### 延迟任务流程

```
1. SubmitTask(delay=30s)
       │
       ▼
   Redis ZADD delayed_queue (score = now + 30s)
       │
       │  ... 30 秒后 ...
       │
2. Scheduler.promoteDelayedLoop() (每秒执行)
       │
       ▼
   Lua Script: ZRANGEBYSCORE <= now → ZADD ready_queue
       │
3.     ▼
   Worker.fetchLoop() 正常出队处理
```

## 可靠性保证

| 场景 | 处理方式 |
|------|----------|
| Worker 崩溃 | inflight 中的任务在 Lease 到期后由 Scheduler 检测并重新入队 |
| Scheduler Leader 崩溃 | Standby 在 15s 内接管 Leader，重新启动调度循环 |
| Redis 不可用 | 任务已持久化到 MySQL，Redis 恢复后可从 MySQL 重建队列 |
| MySQL 不可用 | 任务仍在 Redis 中可执行，状态更新暂缓直到恢复 |
| 任务执行超时 | context.WithTimeout 强制取消，进入重试流程 |
| Handler Panic | middleware.Recovery 捕获，转为 Failed 状态触发重试 |
| 网络分区 | etcd Lease 到期后 Worker 自动注销，避免脑裂 |
