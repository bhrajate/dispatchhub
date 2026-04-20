# DispatchHub 项目介绍（面试版）

> 云原生分布式任务调度系统 · Go 1.25 · DDD 架构

---

## 一、项目一句话介绍

**DispatchHub 是一个支持优先级排序、高可用、水平扩展的分布式任务调度系统**，对标 Asynq / Sidekiq / Celery，面向日均百万级异步任务场景（邮件/报表/对账/定时任务），解决了 Kafka 无法优先级插队、RabbitMQ 性能不足、MySQL 轮询锁竞争等主流方案的痛点。

---

## 二、业务背景与选型动机

### 2.1 业务场景

典型场景是业务系统需要处理不同优先级的异步任务：

| 优先级 | 任务类型 | 延迟要求 |
| --- | --- | --- |
| 10（高） | 交易对账、支付回调 | 秒级 |
| 5（中） | 营销邮件、通知推送 | 分钟级 |
| 1（低） | 日志归档、报表导出 | 小时级 |

### 2.2 为什么不用现成 MQ

| 方案 | 致命缺陷 |
| --- | --- |
| Kafka | Partition 内严格 FIFO，消息位置不可变，高优先级任务无法插队 |
| RocketMQ | 仅支持延迟消息，不支持任意优先级排序 |
| RabbitMQ | `x-max-priority` 会被 prefetch 破坏；2–5 万 QPS 上限 |
| MySQL 轮询 | 50+ Worker 并发时锁竞争 + 间隙锁阻塞，QPS 不升反降 |

**选型结论**：自研基于 **Redis Sorted Set + Lua 脚本**，单机 10 万+ QPS，完美支持优先级 O(log N) 插队。

---

## 三、技术栈

| 类别 | 选型 |
| --- | --- |
| 语言 | Go 1.25 |
| RPC | gRPC + Protocol Buffers |
| Web | 原生 net/http |
| ORM | GORM 1.31 |
| 日志 | Uber Zap |
| 指标 | Prometheus Client |
| 消息队列 | Redis 7 (Sorted Set + Lua) |
| 协调服务 | etcd 3.5 (Leader Election + 服务发现) |
| 持久化 | MySQL 8.0 |
| 部署 | Docker 多阶段构建 + Kubernetes + Helm |
| 监控 | Prometheus + Grafana + HPA |

---

## 四、整体架构：控制面 / 数据面分离

借鉴 Kubernetes 设计哲学，将编排与执行彻底分离。

```
    ┌────────────┐
    │  Client    │  HTTP :8080 / gRPC :9090
    └─────┬──────┘
          │
    ┌─────▼────────────┐   无状态，任意水平扩展
    │  API Server (N)  │   路由校验 + 限流
    └──┬─────────┬─────┘
       │         │
  ┌────▼──┐  ┌──▼─────┐
  │ Redis │  │ MySQL  │
  │ 热路径│  │ 冷路径 │
  └──┬────┘  └──┬─────┘
     │          │
     │   ┌──────▼─────────────┐
     │   │ Scheduler (Leader) │  etcd 选举单活
     │   │ 7 个 reconcile 循环│  Standby 秒级接管
     │   └──────┬─────────────┘
     │          │
  ┌──▼──────────▼────────────┐
  │  Worker Pool (HPA 3~50)  │  完全无状态
  │  Pull 模型 + 信号量背压   │  Handler 插件式
  └──────────────────────────┘
```

### 三个服务的职责

| 服务 | 部署形态 | 职责 |
| --- | --- | --- |
| **API Server** | Deployment, N 副本 | 唯一外部入口（HTTP/gRPC）、路由校验、限流 |
| **Scheduler** | StatefulSet, 单活 | Leader 选举 + 7 个调度循环（延迟晋升/补偿/CronJob/清理等） |
| **Worker** | Deployment + HPA | 任务执行、心跳上报、handler 注册 |

**关键设计**：三个服务通过 Redis / etcd / MySQL 协调，**无直接通信**，完全解耦。

---

## 五、DDD 分层架构

最近完成了 DDD 重构（commit `cc78f88`），目录结构清晰反映分层：

```
internal/
├── shared/                           # 三服务共享
│   ├── domain/
│   │   ├── entity/                   # Task / Worker / CronJob / Queue
│   │   └── repository/               # 仓储接口（纯接口，零实现）
│   └── infrastructure/               # MySQL / Redis / etcd 实现
│
├── apiserver/
│   ├── domain/service/               # TaskService + RouteValidator
│   └── interfaces/{http,grpc}/       # 接口层
│
├── scheduler/
│   ├── domain/service/               # 调度领域逻辑
│   ├── application/                  # 7 个 reconciliation 循环
│   └── infrastructure/election/      # etcd Leader 选举
│
└── worker/
    ├── application/service/          # 执行引擎
    └── interfaces/middleware/        # Recovery / Logging / Timeout
```

**依赖方向**：`Interfaces → Application → Domain ← Infrastructure`

- Domain 层零外部依赖
- Infrastructure 实现 Repository 接口，依赖倒置
- Shared 模块让三服务共享 Entity 和接口，零代码重复

---

## 六、核心技术难点（面试重点）

### 难点 1：优先级队列——Redis Sorted Set + Lua 原子脚本

**数据结构**：每个 queue 由三个 Redis key 组成

| Key | 类型 | 用途 |
| --- | --- | --- |
| `queue:{name}:ready` | ZSET | 就绪任务，score = `-priority`（越小越优先） |
| `queue:{name}:delayed` | ZSET | 延迟任务，score = `executeAt`（毫秒时间戳） |
| `queue:{name}:inflight` | HASH | 处理中任务，`taskID → taskJSON` |

**核心 Lua 脚本（原子操作）**：

```lua
-- dequeueScript: ZPOPMIN + HSET 原子完成，杜绝重复消费
local result = redis.call('ZPOPMIN', ready_key, 1)
if #result > 0 then
    redis.call('HSET', inflight_key, task.id, result[1])
    return result[1]
end

-- enqueueIfNotInflight: 补偿场景下的幂等重入队
if redis.call('HEXISTS', inflight_key, task_id) == 1 then
    return 0   -- 任务正在处理，跳过
end
redis.call('ZADD', ready_key, score, data)
return 1
```

**亮点**：Redis 单线程 + Lua 原子性，天然规避了分布式锁，单机 10 万+ QPS。

---

### 难点 2：Leader 选举的两个竞态修复

Scheduler 必须单活（否则会重复触发 CronJob、重复补偿），基于 `etcd concurrency.Election` 实现，Lease TTL 15s。最近修复了两个严重 bug：

#### Bug 2.1：Race Condition（commit `11081b9`）

**问题**：`session` 和 `election` 作为结构体字段被 `campaign()` 无锁写入、`observe()` 无锁读取；每次重试都启动新 observe goroutine，旧的不退出，导致 goroutine 泄漏 + 读到错误对象。

**修复**：移除共享字段，改为**局部变量 + 参数传递**

```go
func (le *LeaderElector) campaign(ctx context.Context) error {
    session, _ := concurrency.NewSession(le.client, concurrency.WithTTL(15))
    election := concurrency.NewElection(session, le.prefix)

    observeCtx, observeCancel := context.WithCancel(ctx)
    var wg sync.WaitGroup
    wg.Go(func() { le.observe(observeCtx, election) })  // 参数传递
    // ...
}
```

#### Bug 2.2：Defer Deadlock（commit `1fa57ef`）

**问题**：defer 按 LIFO 执行，原顺序为

```go
defer observeCancel()   // 注册#1 → 执行#2
// ...
defer wg.Wait()         // 注册#2 → 执行#1  ← 先 Wait，Cancel 永不执行
```

etcd session 过期时，observe goroutine 无法被取消，`Wait()` 永久阻塞 → **Scheduler 彻底卡死**。

**修复**：交换 defer 注册顺序，利用 LIFO 保证 cancel 先于 wait

```go
defer wg.Wait()         // 注册#1 → 执行#2
defer observeCancel()   // 注册#2 → 执行#1 ← 先取消 goroutine
```

> **面试价值**：这两个 bug 是经典的分布式并发问题，体现对 goroutine 生命周期、defer 执行顺序、channel/context 传播的深度理解。

---

### 难点 3：MySQL-Redis 双写最终一致性

**问题**：任务提交要做两件事

1. MySQL INSERT（持久化）
2. Redis ZADD（入队让 Worker 可见）

两步非原子。若 Redis 入队失败，任务会永远卡在 MySQL 的 `Pending` 状态。

**解决方案**：Scheduler 后台补偿循环（30s 周期）

```go
// 扫描 Pending 且 updated_at 早于 30s 的任务
tasks := taskRepo.FindStaleByState(ctx, Pending, 30*time.Second, 1000)

for _, task := range tasks {
    // Lua 脚本原子检查 + 重入队
    ok := broker.EnqueueIfNotInflight(ctx, queue, task)
    if ok {
        taskRepo.TouchUpdatedAt(ctx, task.ID)  // 防止下轮重复命中
    }
}
```

**关键设计**：

- **时间窗口**：只扫描 30s 前的任务，给正常路径留足时间
- **幂等性**：`EnqueueIfNotInflight` 通过 Lua 原子检查 `inflight` HASH，任务正在处理时跳过
- **不改 version**：`TouchUpdatedAt` 只刷新 updated_at，不递增 version，避免让 Worker 手里的任务失效
- **正常路径零开销**：补偿是异常路径，不影响主链路

---

### 难点 4：Scheduler 派生状态一致性（commit `aafba45`）

**问题**：`SchedulerService` 原本同时维护

```go
workers map[string]*WorkerInfo  // 源数据
queues  []string                // 派生数据：所有 worker 监听队列的并集
```

`SyncWorkers()` 启动时同步两者，但 `HandleWorkerEvent()` 和 `DetectStaleWorkers()` 只更新 workers，不更新 queues → 新 worker 带来的队列不被调度、下线的队列继续轮询。

**修复思路**：消除冗余派生状态，改为实时计算

```go
type SchedulerService struct {
    workers map[string]*WorkerInfo  // 唯一真实源
}

func (s *SchedulerService) Queues() []string {
    queueSet := make(map[string]struct{})
    for _, w := range s.workers {
        for _, q := range w.Queues {
            queueSet[q] = struct{}{}
        }
    }
    // ...
}
```

**启示**：**从结构上消除不一致**优于在多处同步维护，牺牲一点 CPU 换取正确性（worker 数量通常个位数）。

---

### 难点 5：Queue-Type 路由校验（commit `bfacfe6`）

**故障场景**：客户端提交 `type="video.transcode"` 到 `queue="email-queue"`，该队列的 Worker 只注册了 `email.send` handler → 任务静默失败 → 重试 3 次全部失败 → 浪费队列吞吐。

**解决**：API Server 层提交前校验 queue+type 可行性

1. `WorkerInfo` 新增 `TaskTypes` 字段，Worker 启动时自动从已注册 handlers 收集
2. 新增 `RouteValidator`，定期从 etcd 构建 `queue → {types}` 映射，10s 缓存
3. `SubmitTask` 调用前置校验，fail-open 策略（无 worker/etcd 不可用时放行，保护可用性）

```go
if s.routeValidator != nil {
    if err := s.routeValidator.Validate(ctx, task.QueueName, task.Type); err != nil {
        return fmt.Errorf("route validation: %w", err)
    }
}
```

**收益**：问题从"运行时静默失败"提前到"提交时快速拒绝"，客户端能及时感知配置错误。

---

## 七、高可用与可观测性

| 维度 | 实现 |
| --- | --- |
| **Scheduler HA** | etcd Leader 选举 + Lease TTL 15s，Standby 秒级接管 |
| **API Server HA** | 完全无状态，N 副本任意切换 |
| **Worker 弹性** | HPA 3–50 副本，基于 CPU + 自定义指标 `dispatchhub_worker_active_tasks` |
| **背压** | Worker 侧 channel 信号量控制并发，防止雪崩 |
| **故障恢复** | 双写补偿循环 + Worker 心跳超时剔除 + CronJob 幂等触发 |
| **日志** | Uber Zap 结构化 JSON 输出 |
| **指标** | Prometheus：队列深度、任务吞吐、worker 活跃数、执行耗时 |
| **健康检查** | `/healthz`、`/readyz`、`/metrics` 三端点 |

---

## 八、工程亮点

### 8.1 构建与部署

- **Docker 多阶段构建**：`golang:1.25-alpine` → `alpine:3.19`，最终镜像 < 50MB
- **静态链接**：`CGO_ENABLED=0`，无 glibc 依赖
- **版本注入**：ldflags 编译时写入 `VERSION / GIT_COMMIT / BUILD_DATE`
- **Helm Chart**：可配置化部署，values.yaml 控制副本数、资源限额
- **config 独立**：每个微服务有自己的 config（commit `735c8e4`），避免相互污染

### 8.2 测试策略

- `go test -race ./...` 启用竞态检测
- Repository 接口化便于 mock
- 核心 Lua 脚本在 Redis 沙箱环境做完整语义测试

---

## 九、可以在面试中讲的故事（STAR）

**S (Situation)**：业务侧有日均百万级异步任务，且优先级差异大，需要一个能支持优先级插队、高并发、高可用的调度平台。

**T (Task)**：作为核心开发，负责从 0 到 1 的架构设计和关键模块实现。

**A (Action)**：

1. 技术调研排除 Kafka/RabbitMQ/MySQL 方案，选型 Redis Sorted Set
2. 采用控制面/数据面分离架构，Scheduler 单活 + Worker 无状态
3. 通过 Lua 脚本保证出队原子性，通过 etcd 选举保证 Scheduler 高可用
4. 设计 MySQL-Redis 双写补偿实现最终一致性
5. 完成 DDD 分层重构，提升可维护性
6. 修复 4 个并发 bug（2 个 Leader 选举竞态、1 个派生状态不一致、1 个路由漏校验）

**R (Result)**：

- 单机 10 万+ QPS，Scheduler 故障 15s 内自动接管
- 代码架构清晰，38 个 Go 文件覆盖三个独立可部署服务
- 18 份技术文档，每个关键设计决策都有记录
- 积累了对分布式一致性、goroutine 生命周期、defer 执行顺序等的深度理解

---

## 十、可能被追问的问题与应答要点

| 追问 | 应答方向 |
| --- | --- |
| 为什么不用现有 Asynq？ | Asynq 不支持 queue+type 路由校验、补偿机制较弱；我们需要定制化的优先级策略 |
| Redis 单点故障怎么办？ | Redis Sentinel 或 Cluster 模式，生产环境部署哨兵 |
| 为什么 Scheduler 单活不分片？ | 调度逻辑（CronJob 触发、补偿扫描）天然全局性，分片会增加协调复杂度；单活 + 秒级接管已足够 |
| 如何保证 exactly-once？ | 目前是 at-least-once，业务 handler 需自己幂等（通过 task.ID 做去重） |
| 为什么选 etcd 不选 ZooKeeper？ | Go 生态原生集成，API 简洁；K8s 技术栈统一 |
| 延迟任务如何实现？ | 独立 delayed ZSET（score=executeAt），Scheduler 每秒扫描转移到 ready ZSET |
| 任务取消如何实现？ | CAS 状态机 + Worker 执行前二次校验，详见 `docs/2026-04-16-task-cancellation.md` |

---

## 十一、关键文件速查

| 主题 | 文件 |
| --- | --- |
| Redis 队列 + Lua | `internal/shared/infrastructure/persistence/redis/queue_broker.go` |
| Leader 选举 | `internal/scheduler/infrastructure/election/election.go` |
| 7 个调度循环 | `internal/scheduler/application/scheduler_app_service.go` |
| 执行引擎 | `internal/worker/application/service/worker_app_service.go` |
| 路由校验 | `internal/apiserver/domain/service/route_validator.go` |
| 架构详解 | `docs/architecture.md` |
| 队列选型 | `docs/queue-selection.md` / `docs/why-not-mysql-queue.md` |
| 技术修复记录 | `docs/2026-04-1{6,7}-*.md` |
