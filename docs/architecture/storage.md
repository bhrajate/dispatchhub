# 存储层设计

## 总体分层

DispatchHub 采用**三层存储架构**，各层按访问频率与持久性要求选型：

```
┌─────────────────────────────────────────────────────────────┐
│                     应用层 (Scheduler / Worker)               │
└──────┬──────────────────┬──────────────────┬────────────────┘
       │                  │                  │
       ▼                  ▼                  ▼
 ┌───────────┐     ┌───────────┐     ┌───────────────┐
 │   Redis   │     │   etcd    │     │    MySQL      │
 │  快速队列  │     │  协调层    │     │   持久层       │
 ├───────────┤     ├───────────┤     ├───────────────┤
 │ 就绪队列   │     │ Leader选举 │     │ tasks 主表    │
 │ 延迟队列   │     │ Worker注册 │     │ task_events   │
 │ inflight  │     │ 拓扑Watch  │     │ dead_letters  │
 │ 队列统计   │     │           │     │ cron_jobs     │
 ├───────────┤     ├───────────┤     ├───────────────┤
 │ 读写 QPS:  │     │ 读写 QPS:  │     │ 读写 QPS:     │
 │ 10万+/s   │     │ 千级/s     │     │ 万级/s        │
 │ 延迟: <1ms │     │ 延迟: <5ms │     │ 延迟: 1~10ms  │
 └───────────┘     └───────────┘     └───────────────┘
   热路径数据          协调元数据         冷路径 + 持久化
```

---

## 存储选型对比

| 维度 | Redis | etcd | MySQL |
|------|-------|------|-------|
| **定位** | 高速队列 / 缓存 | 分布式协调 | 持久化存储 |
| **数据类型** | 任务队列、inflight 状态 | Leader 锁、Worker 注册 | 任务全量状态、定时任务、死信 |
| **一致性** | 最终一致 | 强一致 (Raft) | 强一致 (InnoDB) |
| **持久化** | AOF/RDB (可配置) | WAL + Snapshot | Redo Log + Binlog |
| **可用性** | Sentinel / Cluster | 3/5 节点 Raft 集群 | 主从 / MGR |
| **吞吐** | 10万+ QPS | 数千 QPS | 数万 QPS |
| **延迟** | 亚毫秒 | 1~5ms | 1~10ms |
| **数据量** | 内存受限 (活跃任务) | <1GB (元数据) | TB 级 (全量历史) |

### 为什么要三层而不是单一存储？

| 如果只用 MySQL | 如果只用 Redis | 三层方案 |
|---------------|---------------|---------|
| 队列吞吐受限 (~万 QPS) | 数据不够持久 | Redis 做队列热路径, 10万+ QPS |
| 排队/出队需加锁或 CAS | 没有事务保证 | MySQL 做持久化, 事务+乐观锁 |
| Leader 选举需自建 | Watch 能力不如 etcd | etcd 原生 Lease+Watch |
| 延迟队列实现复杂 | 内存有限, 历史数据丢失 | 各司其职, 互不牵制 |

---

## MySQL 表结构设计

> DDL 文件：`deploy/mysql/schema.sql`

### ER 图

```
┌─────────────────┐
│     tasks       │
│  (任务主表)      │
├─────────────────┤
│ PK: id          │
│ state           │
│ queue_name      │
│ priority        │
│ worker_id       │
│ version (乐观锁) │
└────────┬────────┘
         │ 失败归档
         │ 1 ── N
         ▼
┌─────────────────┐        ┌──────────────────┐        ┌──────────────────┐
│  dead_letters   │        │   task_events    │        │   cron_jobs      │
│  (死信表)        │        │  (审计日志)       │        │  (定时任务)       │
├─────────────────┤        ├──────────────────┤        ├──────────────────┤
│ task_id         │        │ task_id          │        │ PK: id           │
│ error           │        │ old_state        │        │ cron_expr        │
│ retry_count     │        │ new_state        │        │ next_run_at      │
│ redelivered     │        │ timestamp        │        │ concurrency_policy│
└─────────────────┘        └──────────────────┘        │ enabled          │
                                                       └──────────────────┘
```

> 注意：task_events 表的 DDL 存在于 schema.sql 中，但对应的 Go entity 已在 DDD 重构中移除，当前仅作为数据库端审计占位。

### 表 1: tasks（任务主表）

系统核心表，存储所有任务的完整生命周期数据。

```sql
CREATE TABLE `tasks` (
    `id`            VARCHAR(64)   NOT NULL,     -- UUID
    `name`          VARCHAR(255)  DEFAULT '',
    `namespace`     VARCHAR(128)  DEFAULT '',    -- 多租户隔离
    `group`         VARCHAR(128)  DEFAULT '',    -- 亲和性分组
    `type`          VARCHAR(128)  DEFAULT '',    -- Handler 类型
    `payload`       TEXT,                        -- JSON 载荷
    `labels`        TEXT,                        -- JSON 标签 (Scanner/Valuer)
    `priority`      TINYINT       DEFAULT 5,     -- 1~10
    `delay`         BIGINT        DEFAULT 0,     -- 纳秒
    `schedule_at`   DATETIME(3),
    `timeout`       BIGINT        DEFAULT 0,     -- 纳秒
    `max_retries`   INT           DEFAULT 3,
    `retry_count`   INT           DEFAULT 0,
    `retry_backoff` BIGINT        DEFAULT 0,     -- 纳秒
    `state`         TINYINT       DEFAULT 0,     -- 状态码 0~7
    `result`        TEXT,
    `error`         TEXT,
    `worker_id`     VARCHAR(128)  DEFAULT '',
    `queue_name`    VARCHAR(128)  DEFAULT 'default',
    `created_at`    DATETIME(3)   DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at`    DATETIME(3)   DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    `started_at`    DATETIME(3),
    `finished_at`   DATETIME(3),
    `version`       BIGINT        DEFAULT 1,     -- 乐观锁

    PRIMARY KEY (`id`),
    KEY `idx_queue_state_priority` (`queue_name`, `state`, `priority`, `created_at`),
    KEY `idx_ns_type_state` (`namespace`, `type`, `state`),
    KEY `idx_worker_state` (`worker_id`, `state`),
    KEY `idx_created_at` (`created_at`)
) ENGINE=InnoDB;
```

> Go entity (`entity.Task`) 中不包含 `cron_expr` 和 `deadline` 字段。schema.sql DDL 中虽有这两列，但 GORM AutoMigrate 以 struct tag 为准，实际运行时不会使用它们。

**字段设计决策**：

| 决策 | 理由 |
|------|------|
| `id` VARCHAR(64) | 兼容 UUID (36 字符) 和 Snowflake ID 等分布式 ID 方案 |
| `priority` TINYINT | 1~10 够用，1 字节存储，索引高效 |
| `delay`/`timeout`/`retry_backoff` BIGINT | 存储纳秒精度的 `time.Duration`，Go 原生兼容 |
| `labels` TEXT + Scanner/Valuer | map[string]string 通过 JSON 序列化存储，GORM 自动调用 `Value()` / `Scan()` |
| `state` TINYINT | 8 种状态，1 字节，枚举值比字符串更紧凑高效 |
| `version` BIGINT | 乐观锁，64 位不会溢出，支持频繁更新 |

**索引策略**：

| 索引 | 列 | 覆盖场景 |
|------|-----|----------|
| PRIMARY | `id` | 主键查询 |
| `idx_queue_state_priority` | `queue_name, state, priority, created_at` | **核心调度查询**：按队列+状态查询，按优先级排序 |
| `idx_ns_type_state` | `namespace, type, state` | 多租户维度查询任务列表 |
| `idx_worker_state` | `worker_id, state` | 查询某 Worker 的活跃/历史任务 |
| `idx_created_at` | `created_at` | 时间范围查询、数据归档 |

**最核心的组合索引**：

```sql
KEY `idx_queue_state_priority` (`queue_name`, `state`, `priority`, `created_at`)
```

覆盖调度系统最高频查询：

```sql
SELECT * FROM tasks
WHERE queue_name = 'default' AND state = 0
ORDER BY priority DESC, created_at ASC
LIMIT 100;
```

MySQL 通过索引前缀定位 `queue_name + state`，在索引内按 `priority + created_at` 排序，无需回表扫描。

**MySQL Repository 方法清单**（源码：`internal/shared/infrastructure/persistence/mysql/task_repository.go`）：

| 方法 | 接口 | 说明 |
|------|------|------|
| `Create` | TaskWriter | INSERT 新任务 |
| `Get` | TaskReader | 按 ID 查询，`gorm.ErrRecordNotFound` 时返回 nil |
| `Update` | TaskWriter | **纯乐观锁**：`WHERE id=? AND version=?`，无 `SELECT FOR UPDATE` |
| `List` | TaskReader | 支持多条件过滤 + 分页，排序 `priority DESC, created_at ASC` |
| `FindStaleByState` | TaskCompensator | 按 `updated_at` 查找过期任务（补偿循环用） |
| `TouchUpdatedAt` | TaskCompensator | 仅刷新 `updated_at`，**不递增 version** |
| `HasRunningTasks` | TaskCompensator | 查询指定 type+namespace 是否有 Running 态任务（cron Forbid 策略用） |
| `DeleteTerminalOlderThan` | TaskCompensator | 按 `finished_at` 批量清理终态任务 |

乐观锁实现：

```go
func (s *TaskRepository) Update(ctx context.Context, task *entity.Task) error {
    oldVersion := task.Version
    task.Version++
    result := s.db.Where("id = ? AND version = ?", task.ID, oldVersion).Updates(task)
    if result.RowsAffected == 0 {
        return fmt.Errorf("optimistic lock conflict: task %s version %d", task.ID, oldVersion)
    }
    return nil
}
```

生成的 SQL：

```sql
UPDATE tasks SET state=?, worker_id=?, version=3, ...
WHERE id = 'abc' AND version = 2;
-- RowsAffected == 0 → 版本冲突
```

### 表 2: task_events（事件审计表）

```sql
CREATE TABLE `task_events` (
    `id`         VARCHAR(64)   NOT NULL,
    `task_id`    VARCHAR(64)   NOT NULL,
    `type`       VARCHAR(64)   DEFAULT '',
    `old_state`  TINYINT       DEFAULT 0,
    `new_state`  TINYINT       DEFAULT 0,
    `worker_id`  VARCHAR(128)  DEFAULT '',
    `message`    TEXT,
    `timestamp`  DATETIME(3)   DEFAULT CURRENT_TIMESTAMP(3),

    PRIMARY KEY (`id`),
    KEY `idx_task_timestamp` (`task_id`, `timestamp`),
    KEY `idx_timestamp` (`timestamp`)
) ENGINE=InnoDB;
```

**当前状态**：DDL 保留在 schema.sql 中，但 Go 代码中对应的 entity 已在 DDD 重构中移除。该表仅作为数据库端审计预留，当前代码不会写入。

### 表 3: dead_letters（死信表）

```sql
CREATE TABLE `dead_letters` (
    `id`            VARCHAR(64)   NOT NULL,
    `task_id`       VARCHAR(64)   NOT NULL,
    `queue_name`    VARCHAR(128)  DEFAULT '',
    `type`          VARCHAR(128)  DEFAULT '',
    `payload`       TEXT,
    `error`         TEXT,
    `retry_count`   INT           DEFAULT 0,
    `max_retries`   INT           DEFAULT 0,
    `failed_at`     DATETIME(3)   DEFAULT CURRENT_TIMESTAMP(3),
    `redelivered`   TINYINT(1)    DEFAULT 0,
    `redelivered_at` DATETIME(3),

    PRIMARY KEY (`id`),
    KEY `idx_task_id` (`task_id`),
    KEY `idx_redelivered` (`redelivered`, `failed_at`)
) ENGINE=InnoDB;
```

使用场景：

- 任务用尽所有重试后，最终快照写入 dead_letters
- 运维人员通过 `redelivered = 0` 过滤未处理的死信
- 排查问题后将 `redelivered` 设为 1 并重新投递任务

### 表 4: cron_jobs（定时任务定义表）

每触发一次 cron 周期，调度器调用 `CronJob.ToTask()` 生成一个独立的 Task 实例入队。

```sql
CREATE TABLE `cron_jobs` (
    `id`                 VARCHAR(64)   NOT NULL,
    `name`               VARCHAR(255)  DEFAULT '',
    `namespace`          VARCHAR(128)  DEFAULT '',
    `type`               VARCHAR(128)  NOT NULL,
    `payload`            TEXT,
    `labels`             TEXT,
    `cron_expr`          VARCHAR(128)  NOT NULL,
    `queue_name`         VARCHAR(128)  DEFAULT 'default',
    `priority`           TINYINT       DEFAULT 5,
    `timeout`            BIGINT        DEFAULT 0,
    `max_retries`        INT           DEFAULT 3,
    `retry_backoff`      BIGINT        DEFAULT 0,
    `concurrency_policy` VARCHAR(32)   DEFAULT 'Allow',  -- Allow / Forbid
    `enabled`            TINYINT(1)    DEFAULT 1,
    `last_run_at`        DATETIME(3),
    `next_run_at`        DATETIME(3),
    `created_at`         DATETIME(3)   DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at`         DATETIME(3)   DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

    PRIMARY KEY (`id`),
    KEY `idx_enabled_next` (`enabled`, `next_run_at`)
) ENGINE=InnoDB;
```

**concurrency_policy 语义**：

| 策略 | 行为 |
|------|------|
| `Allow` | 允许并发执行，即使上次触发的任务尚未完成也继续投递新任务 |
| `Forbid` | 跳过本次触发，如果同 type+namespace 存在 Running 态任务（通过 `HasRunningTasks` 查询） |

**核心查询**：

```sql
-- Scheduler 定时扫描：查找已到期且启用的 cron_jobs
SELECT * FROM cron_jobs
WHERE enabled = 1 AND next_run_at IS NOT NULL AND next_run_at <= NOW()
ORDER BY next_run_at ASC
LIMIT 100;
```

**CronJob Repository 方法清单**（源码：`internal/shared/infrastructure/persistence/mysql/cronjob_repository.go`）：

| 方法 | 说明 |
|------|------|
| `CreateCronJob` | INSERT 新定时任务 |
| `GetCronJob` | 按 ID 查询 |
| `UpdateCronJob` | 全量更新 (`Save`) |
| `DeleteCronJob` | 按 ID 删除 |
| `ListCronJobs` | 按 namespace 过滤 + 分页，排序 `created_at DESC` |
| `FindDueCronJobs` | 查找 `enabled=1 AND next_run_at <= NOW()` 的到期任务 |

### 表 5: scheduler_locks（分布式锁表，etcd 备选）

```sql
CREATE TABLE `scheduler_locks` (
    `lock_name`   VARCHAR(128)  NOT NULL,
    `holder`      VARCHAR(255)  DEFAULT '',
    `acquired_at` DATETIME(3)   DEFAULT CURRENT_TIMESTAMP(3),
    `expires_at`  DATETIME(3)   NOT NULL,
    `version`     BIGINT        DEFAULT 1,

    PRIMARY KEY (`lock_name`)
) ENGINE=InnoDB;
```

当无法部署 etcd 时，可基于此表实现 MySQL 分布式锁：

```sql
UPDATE scheduler_locks
SET holder = 'scheduler-abc', acquired_at = NOW(),
    expires_at = DATE_ADD(NOW(), INTERVAL 15 SECOND), version = version + 1
WHERE lock_name = 'leader' AND (expires_at < NOW() OR holder = 'scheduler-abc');
```

---

## Redis 数据结构设计

> 源码：`internal/shared/infrastructure/persistence/redis/queue_broker.go`

### Key 命名规范

```
dispatchhub:queue:{queue_name}:{type}
```

每个队列对应 4 个 Key：

| Key | Redis 类型 | 说明 |
|-----|-----------|------|
| `dispatchhub:queue:{name}:ready` | Sorted Set | 就绪队列，score = -priority（负值，ZPOPMIN 取最高优先级） |
| `dispatchhub:queue:{name}:delayed` | Sorted Set | 延迟队列，score = 执行时间戳 (毫秒) |
| `dispatchhub:queue:{name}:inflight` | Hash | 执行中，field = taskID, value = taskJSON |
| `dispatchhub:queue:{name}:stats` | Hash | 统计计数器 (enqueued / completed / failed) |

### 数据流转

```
                    score = -priority              score = executeAt_ms
                  ┌─────────────────┐            ┌─────────────────┐
  Enqueue ──────▶ │   :ready (ZSET) │            │ :delayed (ZSET) │ ◀── EnqueueDelayed
                  │  -10: task_A    │            │ 1705312200000:  │
                  │  -8:  task_B    │            │   task_X        │
                  │  -5:  task_C    │  ◀─────────│                 │
                  └───────┬─────────┘  Promote   └─────────────────┘
                          │ (Lua 原子)     (每秒)
                          ▼
                  ┌─────────────────┐
                  │ :inflight (Hash)│
                  │ task_A: {json}  │ ──── Ack ────▶ HDEL + stats.completed++
                  │                 │ ──── Nack ───▶ 回到 :ready 或 :delayed
                  └─────────────────┘

                  ┌─────────────────┐
                  │  :stats (Hash)  │
                  │ enqueued: 10583 │
                  │ completed: 9821 │
                  │ failed:     37  │
                  └─────────────────┘
```

### Lua 脚本详解

系统使用 4 个 Lua 脚本保证 Redis 操作的原子性：

**1. enqueueWithCapScript — 带容量检查的入队**

```
KEYS: [ready_key, stats_key]
ARGV: [score, data, max_size]
逻辑: if max_size > 0 && ZCARD >= max_size → return -1 (队列满)
      else ZADD ready + HINCRBY stats.enqueued
```

max_size = 0 表示不限容量（默认行为，`Enqueue` 方法传 0）。

**2. dequeueScript — 原子出队**

```
KEYS: [ready_key_1, ready_key_2, ...]  (多队列按顺序尝试)
逻辑: for each queue:
        ZPOPMIN → 取出 score 最小的任务 (即优先级最高)
        cjson.decode 解析 task.id
        HSET inflight_key, task.id, data
        return data
      若所有队列为空 → return nil
```

关键细节：inflight_key 通过 `string.gsub(queue_key, ':ready', ':inflight')` 从 ready key 推导，无需额外传参。

**3. promoteScript — 延迟任务晋升**

```
KEYS: [delayed_key, ready_key]
ARGV: [now_ms, batchSize]
逻辑: ZRANGEBYSCORE delayed_key -inf now LIMIT 0 batchSize
      for each task:
        score = -(task.priority or 5)
        ZADD ready_key score data
        ZREM delayed_key data        ← 逐个 ZREM，不是 ZREMRANGEBYSCORE
      return 晋升数量
```

使用逐个 `ZREM`（而非 `ZREMRANGEBYSCORE`）的原因：需要为每个任务重新计算 ready 队列的 score（基于 priority），这要求逐个解析 JSON。batchSize 参数控制单次晋升上限，避免 Lua 脚本长时间阻塞 Redis。

**4. enqueueIfNotInflightScript — 补偿入队（幂等）**

```
KEYS: [inflight_key, ready_key]
ARGV: [task_id, score, task_json]
逻辑: if HEXISTS inflight_key task_id → return 0 (跳过)
      else ZADD ready_key → return 1 (已入队)
```

供 Scheduler 补偿循环使用：MySQL 中 Pending 但 Redis 中不存在的任务，需要安全地重新入队。先检查 inflight Hash 避免重复投递正在执行中的任务。

### Nack 重入队逻辑

Nack 操作根据任务状态决定重入队策略：

```
task.CanRetry() && RetryBackoff > 0  →  ZADD delayed (score = now + backoff)
task.CanRetry() && RetryBackoff == 0 →  ZADD ready   (score = -priority)
!task.CanRetry()                     →  HINCRBY stats.failed
```

三条路径通过 Pipeline 批量执行，先 HDEL inflight 再执行对应的 ZADD/HINCRBY。

---

## etcd 数据结构设计

> 源码：`internal/shared/infrastructure/persistence/etcd/worker_registry.go`、`internal/scheduler/infrastructure/election/election.go`

### Key 空间

| Key 模式 | 用途 | TTL |
|----------|------|-----|
| `/dispatchhub/workers/{worker_id}` | Worker 注册信息 (JSON) | 15s (Lease) |
| `/dispatchhub/scheduler/leader` | Leader 选举键 | Session TTL |

### Worker 注册 Value

```json
{
    "id": "node-1-a3b2c1d4",
    "hostname": "worker-pod-xyz",
    "ip": "10.244.1.15",
    "port": 8080,
    "state": 0,
    "labels": {"gpu": "true"},
    "queues": ["default", "high-priority"],
    "concurrency": 100,
    "active_tasks": 42,
    "completed_total": 1582,
    "failed_total": 23,
    "cpu_usage": 65.2,
    "mem_usage": 78.1,
    "started_at": "2024-01-15T10:00:00Z",
    "last_heartbeat": "2024-01-15T10:05:35Z",
    "version": "v0.1.0"
}
```

### Worker 注册生命周期

```
Worker 启动
    │
    ▼
Register()
    │
    ├── client.Grant(ctx, 15)              ← 创建 15s Lease
    │
    ├── client.Put(key, json, WithLease)   ← 绑定 Lease 写入
    │
    ├── mu.Lock → leases[workerID] = leaseID → mu.Unlock
    │       ↑
    │       └── sync.RWMutex 保护 leases map
    │
    └── client.KeepAlive(ctx, leaseID)     ← 后台 goroutine 持续续约
              │
              ├── 正常运行: 消费 KeepAlive channel, Lease 持续续约
              │
              ╳ Worker 崩溃 (KeepAlive 停止)
              │
              │  ... 15 秒后 ...
              │
              ▼
              etcd 自动删除 Key → Scheduler Watch 收到 DELETE 事件
```

### Heartbeat 实现

Heartbeat 复用 `*entity.WorkerInfo` 结构体（而非独立的心跳结构），通过带 Lease 的 PUT 操作更新 worker key：

```go
func (r *WorkerRegistry) Heartbeat(ctx context.Context, worker *entity.WorkerInfo) error {
    data, _ := json.Marshal(worker)
    r.mu.RLock()
    leaseID, ok := r.leases[worker.ID]
    r.mu.RUnlock()
    _, err = r.client.Put(ctx, workerKey(worker.ID), string(data), clientv3.WithLease(leaseID))
    return err
}
```

每次心跳覆写完整 WorkerInfo（含 ActiveTasks、CPUUsage、MemUsage 等实时指标），Scheduler Watch 端会收到 Updated 事件。

### WatchWorkers 事件流

```go
// Watch 使用 WithPrefix + WithPrevKV
watchCh := client.Watch(ctx, "/dispatchhub/workers/", WithPrefix(), WithPrevKV())
```

事件类型映射：

| etcd 事件 | WorkerEventType | 触发条件 |
|-----------|-----------------|----------|
| `EventTypePut` + `IsCreate()` | `Joined (0)` | 新 Worker 首次注册 |
| `EventTypePut` + 非 Create | `Updated (2)` | 心跳更新 WorkerInfo |
| `EventTypeDelete` | `Left (1)` | Worker 注销或 Lease 过期；`PrevKv` 携带最后一次 WorkerInfo |

### Leader 选举

基于 `go.etcd.io/etcd/client/v3/concurrency` 包的 `Election`：

```
/dispatchhub/scheduler/leader
```

Scheduler 部署 3 副本，仅 Leader 运行调度循环（promoteDelayed、healthCheck 等），Standby 阻塞在 `Campaign()` 等待接管。Leader 崩溃后 Standby 在 Lease TTL 内自动竞选成功。

### 存储量估算

| 项目 | 数量 | 大小/个 | 总量 |
|------|------|---------|------|
| Worker 注册 | 50 个 | ~500B | ~25KB |
| Leader 键 | 1 个 | ~50B | ~50B |
| **合计** | - | - | **< 1MB** |

etcd 建议总数据量 < 2GB，DispatchHub 的使用量远低于此限制。

---

## 并发控制

### 乐观锁（MySQL tasks 表）

纯乐观锁模式，不使用 `SELECT FOR UPDATE`：

```sql
UPDATE tasks SET state=?, worker_id=?, version=3, ...
WHERE id = 'abc' AND version = 2;
-- RowsAffected == 0 → 版本冲突，返回 error
```

适用场景：任务状态更新（Worker 完成/失败上报）频率不高，冲突概率低，乐观锁比悲观锁开销更小。

`TouchUpdatedAt` 方法是例外——仅刷新 `updated_at` 字段，**不递增 version**，用于补偿循环标记任务活跃。

### 原子操作（Redis Lua 脚本）

所有需要跨 Key 的操作均通过 Lua 脚本保证原子性：

| 操作 | 原子性要求 |
|------|-----------|
| Enqueue | ZCARD 检查 + ZADD 入队 + HINCRBY 统计必须一体完成，避免超出容量 |
| Dequeue | ZPOPMIN + HSET inflight 不能分开执行，否则任务可能丢失 |
| Promote | ZRANGEBYSCORE + ZADD + ZREM 必须一体，否则任务可能被重复晋升 |
| EnqueueIfNotInflight | HEXISTS + ZADD 必须一体，否则补偿循环可能重复投递 |

### Lease 机制（etcd 自动注销）

15s TTL Lease 是 Worker 存活的唯一凭证。KeepAlive goroutine 持续续约；Worker 进程崩溃后续约停止，15 秒内 etcd 自动删除 Key，Scheduler Watch 立即感知并执行任务回收。

---

## 存储接口抽象

> 源码：`internal/shared/domain/repository/`

所有存储操作通过 DDD Repository 接口抽象，遵循接口隔离原则 (ISP) 拆分为细粒度接口：

```go
// 任务读取 (MySQL)
type TaskReader interface {
    Get(ctx, id) (*Task, error)
    List(ctx, filter) ([]*Task, int64, error)
}

// 任务写入 (MySQL)
type TaskWriter interface {
    Create(ctx, task) error
    Update(ctx, task) error    // 乐观锁
}

// 任务读写组合
type TaskStore interface {
    TaskReader
    TaskWriter
}

// 补偿操作 (MySQL, 供 Scheduler 后台循环使用)
type TaskCompensator interface {
    FindStaleByState(ctx, state, olderThan, limit) ([]*Task, error)
    TouchUpdatedAt(ctx, id) error
    HasRunningTasks(ctx, taskType, namespace) (bool, error)
    DeleteTerminalOlderThan(ctx, olderThan, limit) (int64, error)
}

// CronJob 读写 (MySQL)
type CronJobReader interface {
    GetCronJob(ctx, id) (*CronJob, error)
    ListCronJobs(ctx, namespace, limit, offset) ([]*CronJob, int64, error)
    FindDueCronJobs(ctx, limit) ([]*CronJob, error)
}
type CronJobWriter interface {
    CreateCronJob(ctx, job) error
    UpdateCronJob(ctx, job) error
    DeleteCronJob(ctx, id) error
}
type CronJobStore interface { CronJobReader; CronJobWriter }

// 快速队列 (Redis)
type QueueBroker interface {
    Enqueue(ctx, queue, task) error
    EnqueueDelayed(ctx, queue, task) error
    Dequeue(ctx, queues) (*Task, error)
    Ack(ctx, queue, taskID) error
    Nack(ctx, queue, task) error
    PromoteDelayed(ctx, queue, batchSize) (int64, error)
    Len(ctx, queue) (int64, error)
    Stats(ctx, queue) (*QueueStats, error)
    EnqueueIfNotInflight(ctx, queue, task) (bool, error)
}

// 服务注册 (etcd)
type WorkerRegistry interface {
    Register(ctx, worker) error
    Deregister(ctx, workerID) error
    Heartbeat(ctx, worker) error       // 参数为 *WorkerInfo，非独立结构
    GetWorker(ctx, workerID) (*WorkerInfo, error)
    ListWorkers(ctx) ([]*WorkerInfo, error)
    WatchWorkers(ctx) (<-chan WorkerEvent, error)
}
```

如需替换存储实现（例如将 Redis 替换为 Kafka，或将 etcd 替换为 Consul），只需实现对应接口即可，上层代码无需修改。

---

## 数据量级与容量规划

### 小型（日任务 < 10万）

| 组件 | 配置 |
|------|------|
| MySQL | 单机 4C8G，50GB SSD |
| Redis | 单机 2C4G |
| etcd | 单机（开发）或 3 节点 |
| Worker | 3~5 个 |

### 中型（日任务 10万 ~ 1000万）

| 组件 | 配置 |
|------|------|
| MySQL | 8C32G 主从，200GB SSD，读写分离 |
| Redis | 单机 4C16G 或 3 主 3 从 Cluster |
| etcd | 3 节点集群 |
| Worker | 10~50 个，HPA |

tasks 表数据量：按保留 30 天算，约 3 亿行。建议：
- 按 `created_at` 月分区
- 终态任务（Completed/Failed/Cancelled/Timeout）通过 `DeleteTerminalOlderThan` 定期清理
- task_events 按月分区，保留 7 天热数据

### 大型（日任务 > 1000万）

| 组件 | 配置 |
|------|------|
| MySQL | 16C64G MGR 集群，1TB SSD，按 queue_name 分库 |
| Redis | 6 主 6 从 Cluster，64GB+ 总内存 |
| etcd | 5 节点集群，SSD |
| Worker | 50~500 个，多集群 |

需要额外措施：
- MySQL 按 `queue_name` 或 `namespace` 分库分表（ShardingSphere / Vitess）
- Redis 大 Key 拆分，单队列超 100 万任务时按 hash slot 分桶
- task_events 考虑切换到 ClickHouse / Elasticsearch

### 容量估算公式

```
MySQL tasks 表:
  行大小 ≈ 500B ~ 2KB (取决于 payload)
  日增量 = 日任务数 × 行大小
  月容量 = 日增量 × 30
  示例: 100万/天 × 1KB = 1GB/天 = 30GB/月

Redis 内存:
  仅存活跃任务 (pending + delayed + inflight)
  峰值任务数 × 平均 JSON 大小
  示例: 10万活跃 × 1KB = 100MB
```

### Redis 内存估算参考

单个任务 JSON 约 500B ~ 2KB（取决于 Payload 大小）。

| 队列规模 | 内存估算 | 说明 |
|----------|---------|------|
| 1 万任务 | ~10MB | 小型场景 |
| 10 万任务 | ~100MB | 中型场景 |
| 100 万任务 | ~1GB | 大型场景，建议 Redis Cluster |
| 1000 万任务 | ~10GB | 超大型，需分片 + 多队列分散 |

> Redis 仅存储**活跃任务**（pending + delayed + inflight），已完成的任务仅保留在 MySQL 中。

---

## 数据归档策略

### tasks 表归档

系统内置 `DeleteTerminalOlderThan` 方法，可通过定时任务调用进行批量清理：

```sql
DELETE FROM tasks
WHERE state IN (4, 5, 6, 7)  -- Completed/Failed/Cancelled/Timeout
  AND finished_at < DATE_SUB(NOW(), INTERVAL 30 DAY)
LIMIT 10000;  -- 分批删除，避免长事务
```

如需保留历史数据，可先迁移到归档表：

```sql
CREATE TABLE tasks_archive LIKE tasks;

INSERT INTO tasks_archive
SELECT * FROM tasks
WHERE state IN (4, 5, 6, 7)
  AND finished_at < DATE_SUB(NOW(), INTERVAL 30 DAY);
```

### 推荐分区方案

```sql
ALTER TABLE tasks PARTITION BY RANGE (TO_DAYS(created_at)) (
    PARTITION p202401 VALUES LESS THAN (TO_DAYS('2024-02-01')),
    PARTITION p202402 VALUES LESS THAN (TO_DAYS('2024-03-01')),
    PARTITION p202403 VALUES LESS THAN (TO_DAYS('2024-04-01')),
    PARTITION pmax    VALUES LESS THAN MAXVALUE
);
```

分区后：
- `DROP PARTITION` 秒级删除整月数据
- 查询自动分区裁剪，只扫描相关分区
- 配合定时 Job 自动创建新分区、删除旧分区
