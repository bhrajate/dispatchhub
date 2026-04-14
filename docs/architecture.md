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

## 组件职责

### API Server — 系统唯一的外部入口

> 源码：`cmd/apiserver/`、`internal/apiserver/`

API Server 是纯无状态网关，**系统中唯一对外暴露任务管理 API 的组件**。Scheduler 和 Worker 不对外暴露任务管理接口。

| 职责 | 说明 |
|------|------|
| HTTP REST API | `POST /api/v1/tasks`、`GET /api/v1/tasks/{id}`、`GET /api/v1/tasks`、`POST /api/v1/tasks/{id}/cancel`、`GET /api/v1/queues/{name}/stats` |
| gRPC API | SubmitTask、GetTask、ListTasks、CancelTask、QueueStats、WatchTasks |
| 健康探针 | `/healthz`、`/readyz`（K8s liveness/readiness） |
| 指标暴露 | `/metrics`（Prometheus） |

**依赖关系**：仅依赖 `internal/apiserver/` + `internal/shared/`（TaskServiceImpl），不引用 scheduler 或 worker 的任何代码。

**不做的事**：不参与 Leader 选举、不运行调度循环、不执行任务。可部署任意数量副本，前置 LoadBalancer/Ingress 即可。

---

### Scheduler — 调度控制面

> 源码：`cmd/scheduler/`、`internal/scheduler/`

Scheduler 是有状态的控制面组件，通过 etcd Leader 选举保证**同一时刻只有一个 Leader 运行调度循环**，其余副本 Standby 等待接管。

| 职责 | 说明 |
|------|------|
| 延迟任务晋升 | 每秒扫描 delayed ZSET，将到期任务移入 ready 队列 |
| 孤儿任务补偿 | 每 30s 扫描 MySQL 中 Pending 但未入 Redis 的任务，安全重新入队 |
| Worker 拓扑管理 | Watch etcd Worker 注册变更 |
| Worker 健康检查 | 每 10s 检测心跳，30s 无心跳的 Worker 自动摘除 |
| Leader 选举 | etcd concurrency.Election，3 副本仅 1 个 Leader 运行上述循环 |
| 队列指标采集 | 每 5s 发布队列深度（pending/active/scheduled）到 Prometheus |
| 运维端点 | 仅暴露 `/healthz`、`/readyz`、`/metrics` |

**依赖关系**：仅依赖 `internal/scheduler/` + `internal/shared/`。

**不做的事**：**不对外暴露 HTTP/gRPC 任务管理 API**、不执行任务。

**Scheduler 内部循环**：

| 循环 | 周期 | 功能 |
|------|------|------|
| `promoteDelayedLoop` | 1s | 扫描延迟队列，将到期任务移入就绪队列 |
| `healthCheckLoop` | 10s | 检测 Worker 心跳，摘除超时节点 |
| `metricsLoop` | 5s | 采集队列深度指标发布到 Prometheus |
| `watchWorkers` | 事件驱动 | Watch etcd Worker 变更事件，更新哈希环 |

---

### Worker — 执行数据面

> 源码：`cmd/worker/`、`internal/worker/`

Worker 是完全无状态的数据面组件，采用 **Pull 模型**从 Redis 队列拉取任务执行。通过 K8s HPA 可在 3~50 副本之间自动伸缩。

| 职责 | 说明 |
|------|------|
| 任务拉取 | 从 Redis 队列 Dequeue（Pull 模型），背压由 channel semaphore 控制 |
| 任务执行 | 查找 Handler → 应用中间件链（Recovery/Logging/Timeout）→ 执行 |
| 结果上报 | 成功 → Ack + MySQL 状态更新为 Completed；失败 → Nack 重入队列或标记 Failed |
| 心跳上报 | 每 5s 向 etcd 发送心跳（CPU、内存、活跃任务数） |
| 服务注册 | 启动时向 etcd 注册，关闭时注销 |
| 优雅停机 | SIGTERM → 停止拉取 → 等待 in-flight 完成（最长 30s）→ 注销 |
| 运维端点 | 仅暴露 `/healthz`、`/readyz`、`/metrics` |

**依赖关系**：仅依赖 `internal/worker/` + `internal/shared/`。

**不做的事**：**不对外暴露任务管理 API**、不参与调度决策。

---

### 组件间协作

三个组件之间**零直接通信**，全部通过基础设施解耦：

```
外部客户端 ──HTTP/gRPC──▶ APIServer ──TaskServiceImpl──▶ MySQL + Redis
                                                              │
Scheduler(Leader) ──reconciliation loops──────────────────────┤
  - promoteDelayed → Redis                                    │
  - healthCheck → etcd                                        │
  - watchWorkers → etcd                                       │
                                                              │
Worker(N) ──Dequeue────────────────────────── Redis ◀─────────┘
          ──Heartbeat──────────────────────── etcd
          ──Update─────────────────────────── MySQL
```

| 基础设施 | 角色 | 谁在读 | 谁在写 |
|----------|------|--------|--------|
| **Redis** | 任务队列 | Worker（Dequeue） | APIServer（Enqueue）、Scheduler（PromoteDelayed） |
| **etcd** | 服务协调 | Scheduler（Watch、健康检查） | Worker（Register、Heartbeat）、Scheduler（Leader 选举） |
| **MySQL** | 持久化 | APIServer（查询）、Worker（状态更新） | APIServer（Create）、Worker（Update） |

## 三高设计方案

### 高并发 (High Concurrency)

| 机制 | 实现 | 文件位置 |
|------|------|----------|
| 优先级队列 | Redis Sorted Set，score = 负优先级，ZPOPMIN 获取最高优先级任务 | `internal/shared/infrastructure/persistence/redis/queue_broker.go` |
| 原子出队 | Lua 脚本实现 ZPOPMIN + HSET inflight 原子操作，避免竞态 | `internal/shared/infrastructure/persistence/redis/queue_broker.go` |
| 协程池 | 基于 channel semaphore 的有界协程池，超出时自动阻塞（背压） | `internal/worker/application/service/worker_app_service.go` |
| 令牌桶限流 | 每个队列独立的令牌桶限流器，精确控制入队速率 | `pkg/ratelimit/ratelimit.go` |
| Pipeline | Redis Pipeline 批量执行，减少网络往返 | `internal/shared/infrastructure/persistence/redis/queue_broker.go` |
| 连接池 | Redis PoolSize=100，MySQL MaxOpenConns=50，避免连接瓶颈 | `internal/shared/infrastructure/config/config.go` |

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
| Leader 选举 | 基于 etcd concurrency 包，类似 K8s client-go leaderelection | `internal/scheduler/infrastructure/election/election.go` |
| 多副本热备 | Scheduler 部署 3 副本，仅 Leader 运行调度循环，Standby 等待接管 | `cmd/scheduler/main.go` |
| 心跳检测 | Worker 每 5s 发送心跳，Scheduler 30s 未收到则摘除并重新调度任务 | `internal/worker/application/service/worker_app_service.go` |
| 临时注册 | etcd Lease 机制，Worker 崩溃后 15s 内自动注销 | `internal/shared/infrastructure/persistence/etcd/worker_registry.go` |
| 乐观锁 | MySQL Version 字段防止并发更新冲突 | `internal/shared/infrastructure/persistence/mysql/task_repository.go` |
| 优雅停机 | 信号捕获 + WaitGroup 等待 in-flight 任务完成 | `internal/worker/application/service/worker_app_service.go` |
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
| gRPC 长连接 | Keepalive 心跳，连接复用，Protobuf 高效序列化 | `internal/apiserver/interfaces/grpc/server.go` |
| 延迟晋升 | 每秒扫描一次延迟队列，批量移动到就绪队列（每次最多100个） | `internal/scheduler/domain/service/scheduler.go` |
| 批量操作 | BatchUpdateState 单次 SQL 更新多个任务状态 | `internal/shared/infrastructure/persistence/mysql/task_repository.go` |
| 指数退避 | 重试间隔指数增长 + 25%随机抖动，避免惊群效应 | `internal/worker/application/service/worker_app_service.go` |
| 无锁计数 | Worker 活跃任务数使用 atomic.Int64，避免锁竞争 | `internal/worker/application/service/worker_app_service.go` |

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
                                  TaskServiceImpl.SubmitTask()
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
