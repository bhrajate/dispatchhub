# DispatchHub 项目优化分析与修复方案

> 分析日期：2026-04-14
> 状态回顾：2026-04-20
> 分析范围：全量代码审查（DDD 架构、并发安全、性能、健壮性）
> 当前分支：refactor/ddd-architecture

---

## 状态一览（2026-04-20 回顾）

| 优先级 | 编号 | 问题 | 状态 |
|--------|------|------|------|
| P0 | 1.1 | 乐观锁 + 悲观锁混用 | ✅ 已修复 |
| P0 | 1.2 | Labels/Duration GORM 序列化 | ✅ 已修复（方案细节见 §1.2 注记） |
| P0 | 1.3 | leases map 并发不安全 | ✅ 已修复 |
| P0 | 1.4 | CancelTask 未移除队列 | ✅ 已修复（采用综合方案，详见 [2026-04-16-task-cancellation.md](2026-04-16-task-cancellation.md)） |
| P1 | 2.1 | Heartbeat 冗余读 etcd | ✅ 已修复 |
| P1 | 2.2 | fetchLoop 固定间隔空轮询 | ✅ 已修复（指数退避） |
| P1 | 2.3 | readyz 探针未检查依赖 | ✅ 已修复（注入 healthCheck 回调） |
| P1 | 2.4 | 双写失败返回 error | ✅ 已修复 |
| P2 | 3.1 | 初始化代码重复 | ✅ 已修复（`persistence/factory.go`） |
| P2 | 3.2 | Scheduler 队列列表静态 | ✅ 已修复（动态派生，详见 [2026-04-16-scheduler-queues-consistency.md](2026-04-16-scheduler-queues-consistency.md)） |
| P2 | 3.3 | env tag 未实际生效 | ✅ 已修复（`applyEnvOverrides`） |
| P2 | 3.4 | CronJob 无并发控制 | ✅ 已修复（`ConcurrencyPolicy`） |
| P3 | 4.1 | HTTP 错误泄露内部信息 | ✅ 已修复 |
| P3 | 4.2 | 缺少 Task TTL 清理 | ✅ 已修复（`cleanupLoop`） |
| P3 | 4.3 | Rate Limiter 忙等待 | ❌ **未修复**（见 TODO） |
| P3 | 4.4 | Worker version 硬编码 | ✅ 已修复 |
| P3 | 4.5 | Promote batch size 硬编码 | ✅ 已修复 |
| P3 | 4.6 | TouchUpdatedAt 错误静默 | ❌ **未修复**（见 TODO） |

> 下文保留原始分析，供回溯参考。每一节的"状态"行标注当前实际进展；未修复条目会同步记录在 [TODO.md](../TODO.md)。

---

## 一、P0 - 必须修复

### 1.1 乐观锁与悲观锁混用

**文件：** `internal/shared/infrastructure/persistence/mysql/task_repository.go:42-58`

**状态：** ✅ 已修复 — `clause.Locking{Strength:"UPDATE"}` 已移除，保留纯乐观锁（`WHERE id = ? AND version = ?`）。

**问题：**

```go
func (s *TaskRepository) Update(ctx context.Context, task *entity.Task) error {
    oldVersion := task.Version
    task.Version++

    result := s.db.WithContext(ctx).
        Model(task).
        Where("id = ? AND version = ?", task.ID, oldVersion).
        Clauses(clause.Locking{Strength: "UPDATE"}). // SELECT FOR UPDATE (悲观锁)
        Updates(task)
    // ...
}
```

- `WHERE version = ?` 是乐观锁策略，通过版本号检测冲突
- `clause.Locking{Strength: "UPDATE"}` 是悲观锁策略（SELECT FOR UPDATE）
- 两者同时使用是矛盾的：乐观锁的核心优势是无锁并发，加上悲观锁后反而增加了行级锁竞争
- 更关键的是，`Updates()` 生成的是 UPDATE 语句，SELECT FOR UPDATE 语义在此并不适用

**修复方案：**

```go
func (s *TaskRepository) Update(ctx context.Context, task *entity.Task) error {
    oldVersion := task.Version
    task.Version++

    result := s.db.WithContext(ctx).
        Model(task).
        Where("id = ? AND version = ?", task.ID, oldVersion).
        Updates(task) // 纯乐观锁，移除 Clauses

    if result.Error != nil {
        return result.Error
    }
    if result.RowsAffected == 0 {
        return fmt.Errorf("optimistic lock conflict: task %s version %d", task.ID, oldVersion)
    }
    return nil
}
```

---

### 1.2 Labels 和 Duration 缺少 GORM 序列化支持

**文件：** `internal/shared/domain/entity/task.go`

**状态：** ✅ 已修复。`Labels` 按 JSON 文本存储；`Duration` 实际实现采用 **int64 纳秒**（而非文档原方案中的 `String()` 文本），查询和计算更方便，具体代码见 `entity/task.go:109-184`。

**问题：**

`Labels`（`map[string]string`）和 `Duration`（包装 `time.Duration`）使用了 `gorm:"type:text"` 标签，但没有实现 `database/sql` 的 `Scanner`/`Valuer` 接口。GORM 无法正确将这些自定义类型序列化到 MySQL TEXT 列或从中反序列化，可能导致数据丢失或运行时 panic。

**修复方案：**

为 `Labels` 添加序列化支持：

```go
import (
    "database/sql/driver"
    "encoding/json"
    "fmt"
)

// Labels is a set of key-value pairs attached to a task (k8s-style).
type Labels map[string]string

func (l Labels) Value() (driver.Value, error) {
    if l == nil {
        return nil, nil
    }
    data, err := json.Marshal(l)
    if err != nil {
        return nil, fmt.Errorf("marshal labels: %w", err)
    }
    return string(data), nil
}

func (l *Labels) Scan(value interface{}) error {
    if value == nil {
        *l = nil
        return nil
    }
    var bytes []byte
    switch v := value.(type) {
    case string:
        bytes = []byte(v)
    case []byte:
        bytes = v
    default:
        return fmt.Errorf("unsupported labels type: %T", value)
    }
    return json.Unmarshal(bytes, l)
}
```

为 `Duration` 添加序列化支持：

```go
func (d Duration) Value() (driver.Value, error) {
    return d.String(), nil
}

func (d *Duration) Scan(value interface{}) error {
    if value == nil {
        d.Duration = 0
        return nil
    }
    var s string
    switch v := value.(type) {
    case string:
        s = v
    case []byte:
        s = string(v)
    default:
        return fmt.Errorf("unsupported duration type: %T", value)
    }
    dur, err := time.ParseDuration(s)
    if err != nil {
        return err
    }
    d.Duration = dur
    return nil
}
```

---

### 1.3 WorkerRegistry.leases map 缺少并发保护

**文件：** `internal/shared/infrastructure/persistence/etcd/worker_registry.go:20-26`

**状态：** ✅ 已修复 — `sync.RWMutex` 已加入，`Register / Deregister / Heartbeat` 均通过互斥访问 `leases` map。

**问题：**

```go
type WorkerRegistry struct {
    client *clientv3.Client
    leases map[string]clientv3.LeaseID // 无 mutex 保护
}
```

`Register()`、`Deregister()`、`Heartbeat()` 都会读写 `leases` map。`Register()` 中启动的 `KeepAlive` goroutine 和主 goroutine 对 `leases` 的读写存在 data race 风险。在 Go 中，并发读写 map 会直接导致 panic。

**修复方案：**

```go
type WorkerRegistry struct {
    client *clientv3.Client

    mu     sync.RWMutex
    leases map[string]clientv3.LeaseID
}

func (r *WorkerRegistry) Register(ctx context.Context, worker *entity.WorkerInfo) error {
    // ... grant, marshal, put ...

    r.mu.Lock()
    r.leases[worker.ID] = grant.ID
    r.mu.Unlock()

    // ... keepalive goroutine ...
}

func (r *WorkerRegistry) Deregister(ctx context.Context, workerID string) error {
    r.mu.Lock()
    leaseID, ok := r.leases[workerID]
    if ok {
        delete(r.leases, workerID)
    }
    r.mu.Unlock()

    if ok {
        _, _ = r.client.Revoke(ctx, leaseID)
    }
    _, err := r.client.Delete(ctx, workerKey(workerID))
    return err
}

func (r *WorkerRegistry) Heartbeat(ctx context.Context, hb *entity.Heartbeat) error {
    // ...
    r.mu.RLock()
    leaseID, ok := r.leases[hb.WorkerID]
    r.mu.RUnlock()

    if !ok {
        return fmt.Errorf("no lease for worker %s", hb.WorkerID)
    }
    // ...
}
```

---

### 1.4 CancelTask 没有从 Redis 队列移除任务

**文件：** `internal/apiserver/domain/service/task_service_impl.go:114-144`

**状态：** ✅ 已修复 — 最终采用**方案 A + B 综合方案**：CancelTask 同时调用 `broker.Remove`（从 ready/delayed/inflight 三处删除）和 `broker.PublishCancel`（通知正在执行的 Worker 取消 context），Worker 侧也增加了 MySQL 二次校验。完整设计见 [2026-04-16-task-cancellation.md](2026-04-16-task-cancellation.md)。

**问题：**

```go
func (s *TaskServiceImpl) CancelTask(ctx context.Context, taskID string) error {
    task, err := s.taskStore.Get(ctx, taskID)
    // ...
    task.State = entity.TaskStateCancelled
    now := time.Now()
    task.FinishedAt = &now
    return s.taskStore.Update(ctx, task) // 只更新了 MySQL，Redis 队列未处理
}
```

如果任务还在 Redis ready 队列中，Worker 仍会 dequeue 并执行。虽然执行完成后 MySQL update 会因 version 冲突失败（Cancel 已递增 version），但 Worker 已经浪费了计算资源执行了一个不需要的任务。

**修复方案（推荐方案 A：Worker 侧检查）：**

在 `processTask` 开始时检查 MySQL 中的最新状态，避免执行已取消的任务：

```go
func (w *WorkerAppService) processTask(ctx context.Context, task *entity.Task) {
    // 从 MySQL 获取最新状态，防止执行已取消的任务
    latest, err := w.taskWriter.(repository.TaskReader).Get(ctx, task.ID)
    if err == nil && latest != nil && latest.IsTerminal() {
        log.Infof("task %s already in terminal state %s, skipping", task.ID, latest.State)
        _ = w.broker.Ack(ctx, task.QueueName, task.ID)
        return
    }

    // ... 原有逻辑 ...
}
```

**修复方案（方案 B：QueueBroker 增加 Remove 方法）：**

在 `QueueBroker` 接口添加 `Remove` 方法，CancelTask 时主动移除：

```go
// repository/queue_broker.go
type QueueBroker interface {
    // ...
    Remove(ctx context.Context, queue string, taskID string) error
}

// redis/queue_broker.go - 用 Lua 遍历 ZSET 按 taskID 匹配移除
// 注意：Sorted Set 的 member 是完整 JSON，按 taskID 匹配需要扫描，效率较低
// 如果取消操作不频繁，方案 A 更简单
```

---

## 二、P1 - 性能与可靠性

### 2.1 Heartbeat 每次都要读 etcd

**文件：** `internal/shared/infrastructure/persistence/etcd/worker_registry.go:95-111`

**状态：** ✅ 已修复 — 采用方案一：`Heartbeat` 直接接收完整 `*entity.WorkerInfo`，Worker 本地维护 info，每次心跳只产生一次 etcd PUT。

**问题：**

```go
func (r *WorkerRegistry) Heartbeat(ctx context.Context, hb *entity.Heartbeat) error {
    worker, err := r.GetWorker(ctx, hb.WorkerID) // 每 5 秒一次 GET 请求
    // ... 更新字段后 PUT 回去 ...
}
```

每次心跳产生 1 GET + 1 PUT 的 etcd 调用。Worker 进程内已经持有 `WorkerInfo`，完全不需要从 etcd 读取。

**修复方案：**

方案一：让 Worker 在本地维护完整的 `WorkerInfo`，Heartbeat 直接传入完整对象：

```go
// 修改 WorkerRegistry 接口，Heartbeat 直接接收完整 WorkerInfo
func (r *WorkerRegistry) Heartbeat(ctx context.Context, worker *entity.WorkerInfo) error {
    data, err := json.Marshal(worker)
    if err != nil {
        return err
    }

    r.mu.RLock()
    leaseID, ok := r.leases[worker.ID]
    r.mu.RUnlock()

    if !ok {
        return fmt.Errorf("no lease for worker %s", worker.ID)
    }

    _, err = r.client.Put(ctx, workerKey(worker.ID), string(data), clientv3.WithLease(leaseID))
    return err
}
```

方案二（改动更小）：在 `WorkerAppService` 中维护 info，Heartbeat 时直接序列化本地 info：

```go
// worker_app_service.go heartbeatLoop
func (w *WorkerAppService) heartbeatLoop(ctx context.Context) {
    ticker := time.NewTicker(w.cfg.HeartbeatInterval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            cpuPct, memPct := systemStats()
            // 直接更新本地 info 并注册
            w.info.ActiveTasks = int(atomic.LoadInt64(&w.active))
            w.info.CPUUsage = cpuPct
            w.info.MemUsage = memPct
            w.info.LastHeartbeat = time.Now()
            w.info.State = entity.WorkerStateOnline

            if err := w.registry.HeartbeatFull(ctx, w.info); err != nil {
                log.Errorf("heartbeat failed: %v", err)
            }
        }
    }
}
```

**预期收益：** 每次心跳减少一次 etcd 读操作，在 100 个 Worker 实例、5 秒心跳间隔的场景下，每秒减少 20 次 etcd GET。

---

### 2.2 Worker fetchLoop 空轮询优化

**文件：** `internal/worker/application/service/worker_app_service.go:175-224`

**状态：** ✅ 已修复 — 指数退避已实现，minBackoff = 100ms，maxBackoff = 2s，拉到任务则重置退避。

**问题：**

```go
if task == nil {
    <-w.sem
    select {
    case <-ctx.Done():
        return
    case <-time.After(100 * time.Millisecond): // 固定 100ms 轮询
    }
    continue
}
```

在任务稀疏时，每秒产生 10 次无效的 Redis ZPOPMIN 调用。多 Worker 实例时 Redis 压力线性增长。

**修复方案（指数退避）：**

```go
func (w *WorkerAppService) fetchLoop(ctx context.Context) {
    backoff := 100 * time.Millisecond
    const maxBackoff = 2 * time.Second
    const minBackoff = 100 * time.Millisecond

    for {
        select {
        case <-ctx.Done():
            return
        case w.sem <- struct{}{}:
        }

        task, err := w.broker.Dequeue(ctx, w.cfg.Queues)
        if err != nil {
            <-w.sem
            if ctx.Err() != nil {
                return
            }
            log.Errorf("dequeue error: %v", err)
            time.Sleep(time.Second)
            continue
        }

        if task == nil {
            <-w.sem
            select {
            case <-ctx.Done():
                return
            case <-time.After(backoff):
            }
            // 指数退避：连续空结果时逐渐增大等待时间
            backoff = min(backoff*2, maxBackoff)
            continue
        }

        // 有任务，重置退避
        backoff = minBackoff

        w.wg.Add(1)
        go func() {
            defer func() {
                <-w.sem
                w.wg.Done()
            }()
            w.processTask(ctx, task)
        }()
    }
}
```

**预期收益：** 空闲时轮询频率从 10 次/秒降低到 0.5 次/秒，Redis 负载大幅下降。

---

### 2.3 readyz 探针未检查依赖健康

**文件：**
- `internal/apiserver/interfaces/http/server.go:326-336`
- `cmd/scheduler/main.go`
- `cmd/worker/main.go`

**状态：** ✅ 已修复 — Server 新增 `healthCheck func(ctx) error` 回调字段，由 `cmd/*/main.go` 在启动时注入 Redis/MySQL/etcd 的 Ping 检查函数；readyz 调用该回调，任一依赖不可用返回 503。

**问题：**

```go
func (s *Server) handleReadyz(w http.ResponseWriter, _ *http.Request) {
    writeJSON(w, http.StatusOK, map[string]string{"status": "ready"}) // 永远返回 ready
}
```

readyz 永远返回 200，即使 Redis/MySQL/etcd 断开，K8s 仍会将流量路由到该 Pod。

**修复方案（以 apiserver 为例）：**

```go
type Server struct {
    taskSvc     apisvc.TaskService
    mux         *http.ServeMux
    server      *http.Server
    redisClient redis.UniversalClient  // 注入依赖
    db          *gorm.DB               // 注入依赖
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
    ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
    defer cancel()

    checks := map[string]string{}

    // 检查 Redis
    if err := s.redisClient.Ping(ctx).Err(); err != nil {
        checks["redis"] = err.Error()
    } else {
        checks["redis"] = "ok"
    }

    // 检查 MySQL
    sqlDB, err := s.db.DB()
    if err != nil || sqlDB.PingContext(ctx) != nil {
        checks["mysql"] = "unavailable"
    } else {
        checks["mysql"] = "ok"
    }

    // 任一依赖不可用则返回 503
    for _, v := range checks {
        if v != "ok" {
            writeJSON(w, http.StatusServiceUnavailable, checks)
            return
        }
    }
    writeJSON(w, http.StatusOK, checks)
}
```

---

### 2.4 SubmitTask 双写错误处理策略

**文件：** `internal/apiserver/domain/service/task_service_impl.go:92-111`

**状态：** ✅ 已修复 — Redis 入队失败不再返回 error（用 `_ =` 丢弃），依赖 Scheduler 的 `CompensateOrphanedTasks` 循环（30s）兜底重入队，避免客户端重试产生重复任务。

**问题：**

```go
if err := s.taskStore.Create(ctx, task); err != nil { // MySQL 写入成功
    return fmt.Errorf("persist task: %w", err)
}
if err := s.broker.Enqueue(ctx, ...); err != nil { // Redis 写入可能失败
    return fmt.Errorf("enqueue: %w", err) // 对外返回错误
    // 但 MySQL 中已有记录，客户端重试会创建重复任务
}
```

MySQL 成功 + Redis 失败时，对外返回 error。客户端可能重试，导致 MySQL 中出现重复任务。虽然有补偿循环会最终将任务入队 Redis，但客户端不知道任务其实已经被持久化了。

**修复方案：**

```go
func (s *TaskServiceImpl) SubmitTask(ctx context.Context, task *entity.Task) error {
    // ... 参数设置 ...

    if s.beforeSubmit != nil {
        if err := s.beforeSubmit(task); err != nil {
            return err
        }
    }

    if err := s.taskStore.Create(ctx, task); err != nil {
        return fmt.Errorf("persist task: %w", err)
    }

    // MySQL 已持久化，Redis 入队失败不影响任务最终执行
    // 补偿循环会检测到 Pending 状态的 orphan 任务并重新入队
    if task.ScheduleAt != nil || task.Delay.Duration > 0 {
        if err := s.broker.EnqueueDelayed(ctx, task.QueueName, task); err != nil {
            log.Errorf("task %s: enqueue delayed failed (compensate loop will fix): %v", task.ID, err)
            // 不返回 error，任务已持久化
        }
    } else {
        if err := s.broker.Enqueue(ctx, task.QueueName, task); err != nil {
            log.Errorf("task %s: enqueue failed (compensate loop will fix): %v", task.ID, err)
            // 不返回 error，任务已持久化
        }
    }

    if s.afterSubmit != nil {
        s.afterSubmit(task)
    }

    return nil
}
```

> **注意：** 这个策略要求补偿循环 (`CompensateOrphanedTasks`) 及时且可靠地运行。如果补偿循环的延迟不可接受（当前默认 30 秒），可以考虑在返回前同步重试一次 Redis 入队。

---

## 三、P2 - 架构改进

### 3.1 基础设施初始化代码重复

**文件：**
- `cmd/apiserver/main.go`
- `cmd/scheduler/main.go`
- `cmd/worker/main.go`

**状态：** ✅ 已修复 — 共享工厂已提取到 `internal/shared/infrastructure/persistence/factory.go`，导出 `NewRedisClient / NewMySQLDB / NewEtcdClient` 供三个 main 文件复用。

**问题：**

三个入口文件中 Redis、MySQL、etcd 客户端初始化代码高度重复（各 30+ 行），且参数设置不一致：

| 参数 | apiserver | scheduler | worker |
|------|-----------|-----------|--------|
| Redis DialTimeout | 未设置 | 已设置 | 未设置 |
| Redis ReadTimeout | 未设置 | 已设置 | 未设置 |
| Redis WriteTimeout | 未设置 | 已设置 | 未设置 |
| etcd Username/Password | 不使用 etcd | 已设置 | 未设置 |

**修复方案：**

在 `internal/shared/infrastructure/` 下创建工厂函数：

```go
// internal/shared/infrastructure/factory.go
package infrastructure

import (
    "fmt"

    "github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/config"
    goredis "github.com/redis/go-redis/v9"
    clientv3 "go.etcd.io/etcd/client/v3"
    "gorm.io/driver/mysql"
    "gorm.io/gorm"
)

// NewRedisClient creates a Redis client (standalone or cluster) from config.
func NewRedisClient(cfg config.RedisConfig) goredis.UniversalClient {
    if cfg.ClusterMode {
        return goredis.NewClusterClient(&goredis.ClusterOptions{
            Addrs:        cfg.ClusterAddrs,
            Password:     cfg.Password,
            PoolSize:     cfg.PoolSize,
            MinIdleConns: cfg.MinIdleConns,
            DialTimeout:  cfg.DialTimeout,
            ReadTimeout:  cfg.ReadTimeout,
            WriteTimeout: cfg.WriteTimeout,
        })
    }
    return goredis.NewClient(&goredis.Options{
        Addr:         cfg.Addr,
        Password:     cfg.Password,
        DB:           cfg.DB,
        PoolSize:     cfg.PoolSize,
        MinIdleConns: cfg.MinIdleConns,
        DialTimeout:  cfg.DialTimeout,
        ReadTimeout:  cfg.ReadTimeout,
        WriteTimeout: cfg.WriteTimeout,
    })
}

// NewMySQLDB creates a GORM DB instance from config.
func NewMySQLDB(cfg config.MySQLConfig) (*gorm.DB, error) {
    db, err := gorm.Open(mysql.Open(cfg.DSN), &gorm.Config{})
    if err != nil {
        return nil, fmt.Errorf("connect mysql: %w", err)
    }
    sqlDB, err := db.DB()
    if err != nil {
        return nil, fmt.Errorf("get sql.DB: %w", err)
    }
    sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
    sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
    sqlDB.SetConnMaxLifetime(cfg.ConnMaxLifetime)
    return db, nil
}

// NewEtcdClient creates an etcd client from config.
func NewEtcdClient(cfg config.EtcdConfig) (*clientv3.Client, error) {
    return clientv3.New(clientv3.Config{
        Endpoints:   cfg.Endpoints,
        DialTimeout: cfg.DialTimeout,
        Username:    cfg.Username,
        Password:    cfg.Password,
    })
}
```

各 main 文件简化为：

```go
redisClient := infrastructure.NewRedisClient(cfg.Redis)
db, err := infrastructure.NewMySQLDB(cfg.MySQL)
etcdClient, err := infrastructure.NewEtcdClient(cfg.Etcd)
```

---

### 3.2 Scheduler 的 queues 列表是静态的

**文件：** `internal/scheduler/domain/service/scheduler.go:217-237`

**状态：** ✅ 已修复 — 最终采用方案二（动态发现）并更进一步：彻底删除了 `queues` 字段，`Queues()` 方法直接从 `workers` 实时派生，消除了冗余状态的不一致风险。详见 [2026-04-16-scheduler-queues-consistency.md](2026-04-16-scheduler-queues-consistency.md)。

**问题：**

```go
queues: []string{entity.DefaultQueueName}, // 只有 "default"
```

Scheduler 的 `promoteDelayedLoop` 和 `metricsLoop` 只处理 default 队列。如果 Worker 监听了 "high-priority" 或其他队列，这些队列的延迟任务不会被 promote，指标也不会被采集。

**修复方案：**

方案一（配置驱动）：在 config 中显式指定 Scheduler 管理的所有队列：

```yaml
scheduler:
  queues:
    - default
    - high-priority
    - batch
```

```go
type SchedulerConfig struct {
    Queues            []string      `yaml:"queues"`
    LeaseDuration     time.Duration `yaml:"lease_duration"`
    CronCheckInterval time.Duration `yaml:"cron_check_interval"`
    CronBatchSize     int           `yaml:"cron_batch_size"`
}
```

方案二（动态发现）：从 Worker 注册信息中收集队列列表：

```go
func (s *SchedulerService) SyncWorkers(ctx context.Context) (int, error) {
    workers, err := s.registry.ListWorkers(ctx)
    if err != nil {
        return 0, err
    }

    s.mu.Lock()
    defer s.mu.Unlock()

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

    // 动态更新队列列表
    s.queues = make([]string, 0, len(queueSet))
    for q := range queueSet {
        s.queues = append(s.queues, q)
    }
    if len(s.queues) == 0 {
        s.queues = []string{entity.DefaultQueueName}
    }

    return len(s.workers), nil
}
```

---

### 3.3 Config 的 env tag 未实际生效

**文件：** `internal/shared/infrastructure/config/config.go:117-147`

**状态：** ✅ 已修复 — `applyEnvOverrides` 已实现，覆盖 `DISPATCH_{GRPC_ADDR,HTTP_ADDR,REDIS_ADDR,REDIS_PASSWORD,MYSQL_DSN,ETCD_ENDPOINTS,LOG_LEVEL}` 等关键环境变量。

**问题：**

```go
type ServerConfig struct {
    GRPCAddr string `yaml:"grpc_addr" env:"DISPATCH_GRPC_ADDR"` // env tag 是装饰
    HTTPAddr string `yaml:"http_addr" env:"DISPATCH_HTTP_ADDR"`
}
```

`LoadFromFile` 只解析 YAML，不处理环境变量覆盖。在 K8s 部署中，通常通过环境变量注入敏感配置（如数据库密码），当前实现不支持。

**修复方案：**

在 `LoadFromFile` 后添加环境变量覆盖：

```go
import "os"

func LoadFromFile(path string) (*Config, error) {
    cfg := DefaultConfig()
    data, err := os.ReadFile(path)
    if err != nil {
        return nil, err
    }
    if err := yaml.Unmarshal(data, cfg); err != nil {
        return nil, err
    }
    applyEnvOverrides(cfg)
    return cfg, nil
}

func applyEnvOverrides(cfg *Config) {
    if v := os.Getenv("DISPATCH_GRPC_ADDR"); v != "" {
        cfg.Server.GRPCAddr = v
    }
    if v := os.Getenv("DISPATCH_HTTP_ADDR"); v != "" {
        cfg.Server.HTTPAddr = v
    }
    if v := os.Getenv("DISPATCH_REDIS_ADDR"); v != "" {
        cfg.Redis.Addr = v
    }
    if v := os.Getenv("DISPATCH_REDIS_PASSWORD"); v != "" {
        cfg.Redis.Password = v
    }
    if v := os.Getenv("DISPATCH_MYSQL_DSN"); v != "" {
        cfg.MySQL.DSN = v
    }
    if v := os.Getenv("DISPATCH_ETCD_ENDPOINTS"); v != "" {
        cfg.Etcd.Endpoints = strings.Split(v, ",")
    }
    if v := os.Getenv("DISPATCH_LOG_LEVEL"); v != "" {
        cfg.Log.Level = v
    }
}
```

> **替代方案：** 如果配置项未来会持续增长，可以引入 [viper](https://github.com/spf13/viper) 库统一管理 YAML + 环境变量 + 命令行参数。

---

### 3.4 CronJob 缺少并发执行控制

**文件：** `internal/scheduler/domain/service/scheduler.go:67-127`

**状态：** ✅ 已修复 — `CronJob.ConcurrencyPolicy` 已支持 `Allow/Forbid`，触发前通过 `taskMaint.HasRunningTasks` 判断；Forbid 策略下跳过本次但推进 `next_run_at`，避免重复触发累积。

**问题：**

`TriggerDueCronJobs` 在每次触发时无条件创建新 Task。如果上一次触发的任务还在运行中（如执行时间超过 cron 间隔），会产生重叠执行。对于幂等性较差的任务，这可能导致数据不一致。

**修复方案：**

在 `CronJob` 实体中添加并发策略字段：

```go
// entity/cronjob.go
type ConcurrencyPolicy string

const (
    ConcurrencyAllow   ConcurrencyPolicy = "Allow"   // 允许并发（默认）
    ConcurrencyForbid  ConcurrencyPolicy = "Forbid"  // 跳过本次触发
    ConcurrencyReplace ConcurrencyPolicy = "Replace"  // 取消上一个，创建新的
)

type CronJob struct {
    // ... 现有字段 ...
    ConcurrencyPolicy ConcurrencyPolicy `json:"concurrency_policy" gorm:"size:32;default:'Allow'"`
}
```

在触发逻辑中检查：

```go
func (s *SchedulerService) TriggerDueCronJobs(ctx context.Context, limit int) (int, error) {
    jobs, err := s.cronMaint.FindDueCronJobs(ctx, limit)
    // ...

    for _, job := range jobs {
        // 并发策略检查
        if job.ConcurrencyPolicy == entity.ConcurrencyForbid {
            // 查询是否有该 CronJob 的任务仍在运行
            running, _ := s.taskMaint.FindRunningByCronJobID(ctx, job.ID)
            if len(running) > 0 {
                log.Infof("cron job %s skipped: previous task still running", job.ID)
                // 仅更新 next_run_at，不创建新任务
                job.NextRunAt = &nextTime
                _ = s.cronMaint.UpdateCronJob(ctx, job)
                continue
            }
        }

        // ... 正常创建任务 ...
    }
}
```

---

## 四、P3 - 锦上添花

### 4.1 HTTP 错误处理泄露内部信息

**文件：** `internal/apiserver/interfaces/http/server.go:122-125`

**状态：** ✅ 已修复 — SubmitTask 错误已改为 `log.Errorf` + 通用提示：`writeError(w, 500, "failed to submit task")`。

**问题：**

```go
writeError(w, http.StatusInternalServerError, "submit task: %v", err)
// err 可能包含 MySQL DSN、SQL 语句、Redis 地址等敏感信息
```

**修复方案：**

```go
func (s *Server) handleSubmitTask(w http.ResponseWriter, r *http.Request) {
    // ...
    if err := s.taskSvc.SubmitTask(r.Context(), task); err != nil {
        log.Errorf("submit task: %v", err) // 详细错误写日志
        writeError(w, http.StatusInternalServerError, "failed to submit task")
        return
    }
}
```

---

### 4.2 缺少 Task 的 TTL / 自动清理机制

**状态：** ✅ 已修复 — `TaskRepository.DeleteTerminalOlderThan` 已实现，Scheduler 新增 `cleanupLoop`（默认每小时执行一次，清理 7 天前的终态任务）。

**问题：**

已完成/失败的任务永久留在 MySQL 中，Redis 的 inflight hash 也没有过期清理。随着时间推移，tasks 表会无限增长。

**修复方案：**

在 Scheduler 中添加清理循环：

```go
// repository 接口
type TaskCleaner interface {
    DeleteTerminalTasksOlderThan(ctx context.Context, olderThan time.Duration, limit int) (int64, error)
}

// MySQL 实现
func (s *TaskRepository) DeleteTerminalTasksOlderThan(ctx context.Context, olderThan time.Duration, limit int) (int64, error) {
    threshold := time.Now().Add(-olderThan)
    result := s.db.WithContext(ctx).
        Where("state IN ? AND finished_at < ?",
            []entity.TaskState{entity.TaskStateCompleted, entity.TaskStateFailed, entity.TaskStateCancelled, entity.TaskStateTimeout},
            threshold).
        Limit(limit).
        Delete(&entity.Task{})
    return result.RowsAffected, result.Error
}

// Scheduler 清理循环（可配置保留天数，默认 7 天）
func (s *SchedulerAppService) cleanupLoop(ctx context.Context) {
    ticker := time.NewTicker(1 * time.Hour)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            n, err := s.domainSvc.CleanupOldTasks(ctx, 7*24*time.Hour, 1000)
            if err != nil {
                log.Errorf("cleanup old tasks: %v", err)
            } else if n > 0 {
                log.Infof("cleaned up %d old tasks", n)
            }
        }
    }
}
```

---

### 4.3 Rate Limiter 的 Wait 方法使用忙等待

**文件：** `pkg/ratelimit/ratelimit.go:42-53`

**状态：** ❌ **未修复** — 当前仍使用 10ms 固定 polling。此问题已同步到 [TODO.md](../TODO.md)，优先级 P3，可在下一轮迭代中处理。

**问题：**

```go
func (l *Limiter) Wait(ctx context.Context) error {
    for {
        if l.Allow() {
            return nil
        }
        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-time.After(time.Millisecond * 10): // 10ms 忙等待
        }
    }
}
```

10ms 的 polling 间隔既浪费 CPU，又增加延迟（最坏情况下多等 10ms）。

**修复方案：**

计算精确等待时间，避免忙等待：

```go
func (l *Limiter) Wait(ctx context.Context) error {
    for {
        if l.Allow() {
            return nil
        }

        // 计算需要等待多久才能有 1 个 token
        l.mu.Lock()
        waitDuration := time.Duration(float64(time.Second) * (1.0 - l.tokens) / l.rate)
        l.mu.Unlock()

        if waitDuration < time.Millisecond {
            waitDuration = time.Millisecond
        }

        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-time.After(waitDuration):
        }
    }
}
```

> **替代方案：** 直接使用标准库 `golang.org/x/time/rate`，功能更完善，性能更好。

---

### 4.4 Worker version 硬编码

**文件：** `internal/worker/application/service/worker_app_service.go:123-133`

**状态：** ✅ 已修复 — `WorkerInfo.Version` 已改为 `version.Version`（由 ldflags 在编译时注入）。

**问题：**

```go
w.info = &entity.WorkerInfo{
    // ...
    Version: "v0.1.0", // 硬编码
}
```

Dockerfile 中通过 `-ldflags` 注入了构建版本号到 `version.Version`，但 Worker 的 `WorkerInfo` 没有使用它。

**修复方案：**

```go
import "github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/version"

w.info = &entity.WorkerInfo{
    // ...
    Version: version.Version,
}
```

---

### 4.5 PromoteDelayed Lua 脚本硬编码 batch size

**文件：** `internal/shared/infrastructure/persistence/redis/queue_broker.go:179-210`

**状态：** ✅ 已修复 — `PromoteDelayed(ctx, queue, batchSize)` 已参数化，Lua 脚本通过 `ARGV[2]` 接收 batch。

**问题：**

```lua
local tasks = redis.call('ZRANGEBYSCORE', delayed_key, '-inf', now, 'LIMIT', 0, 100)
-- batch size 100 硬编码
```

**修复方案：**

```lua
local delayed_key = KEYS[1]
local ready_key = KEYS[2]
local now = tonumber(ARGV[1])
local batch = tonumber(ARGV[2])  -- 新增参数
local tasks = redis.call('ZRANGEBYSCORE', delayed_key, '-inf', now, 'LIMIT', 0, batch)
-- ...
```

```go
func (q *QueueBroker) PromoteDelayed(ctx context.Context, queue string, batchSize int) (int64, error) {
    now := time.Now().UnixMilli()
    result, err := promoteScript.Run(ctx, q.client,
        []string{delayedKeyFor(queue), readyKeyFor(queue)},
        now, batchSize,
    ).Int64()
    // ...
}
```

---

### 4.6 TouchUpdatedAt 错误被静默忽略

**文件：** `internal/scheduler/domain/service/scheduler.go:147`

**状态：** ❌ **未修复** — 当前代码仍为 `_ = s.taskMaint.TouchUpdatedAt(ctx, task.ID)`。此问题已同步到 [TODO.md](../TODO.md)，优先级 P3。

**问题：**

```go
_ = s.taskMaint.TouchUpdatedAt(ctx, task.ID)
```

如果 `TouchUpdatedAt` 失败，下一次补偿循环会重复入队这个任务（因为 updated_at 没有被刷新），导致 Redis 中出现重复条目。虽然 ZADD 是幂等的（相同 member 会覆盖），但这仍然增加了不必要的 Redis 写入。

**修复方案：**

```go
if err := s.taskMaint.TouchUpdatedAt(ctx, task.ID); err != nil {
    log.Warnf("task %s: touch updated_at failed (may cause re-compensation): %v", task.ID, err)
}
```

---

## 五、总结

| 优先级 | 编号 | 问题 | 影响 |
|--------|------|------|------|
| **P0** | 1.1 | 乐观锁 + 悲观锁混用 | 性能下降、语义错误 |
| **P0** | 1.2 | Labels/Duration GORM 序列化 | 数据丢失或 panic |
| **P0** | 1.3 | leases map 并发不安全 | 运行时 panic |
| **P0** | 1.4 | CancelTask 未移除队列 | 已取消任务仍被执行 |
| **P1** | 2.1 | Heartbeat 冗余读 etcd | 不必要的网络开销 |
| **P1** | 2.2 | fetchLoop 固定间隔空轮询 | Redis 空闲负载高 |
| **P1** | 2.3 | readyz 探针未检查依赖 | K8s 路由到不健康 Pod |
| **P1** | 2.4 | 双写失败返回 error | 客户端重试致重复任务 |
| **P2** | 3.1 | 初始化代码重复 | 维护成本高、参数不一致 |
| **P2** | 3.2 | Scheduler 队列列表静态 | 非 default 队列未被管理 |
| **P2** | 3.3 | env tag 未实际生效 | 容器化部署不便 |
| **P2** | 3.4 | CronJob 无并发控制 | 任务重叠执行 |
| **P3** | 4.1 | HTTP 错误泄露内部信息 | 安全风险 |
| **P3** | 4.2 | 缺少 Task TTL 清理 | 数据库无限增长 |
| **P3** | 4.3 | Rate Limiter 忙等待 | CPU 浪费 |
| **P3** | 4.4 | Worker version 硬编码 | 版本信息不准确 |
| **P3** | 4.5 | Promote batch size 硬编码 | 无法调优 |
| **P3** | 4.6 | TouchUpdatedAt 错误静默 | 重复补偿 |
