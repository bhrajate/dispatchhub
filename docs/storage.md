# 存储层设计

## 总体分层

DispatchHub 采用**三层存储架构**，每层负责不同的数据类别，按访问频率和持久性要求选型：

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
| **数据类型** | 任务队列、inflight状态 | Leader锁、Worker注册 | 任务全量状态、事件审计 |
| **一致性** | 最终一致 | 强一致 (Raft) | 强一致 (InnoDB) |
| **持久化** | AOF/RDB (可配置) | WAL + Snapshot | Redo Log + Binlog |
| **可用性** | Sentinel / Cluster | 3/5节点 Raft 集群 | 主从 / MGR |
| **吞吐** | 10万+ QPS | 数千 QPS | 数万 QPS |
| **延迟** | 亚毫秒 | 1~5ms | 1~10ms |
| **数据量** | 内存受限 (活跃任务) | <1GB (元数据) | TB级 (全量历史) |

### 为什么要三层而不是单一存储？

| 如果只用 MySQL | 如果只用 Redis | 三层方案 |
|---------------|---------------|---------|
| 队列吞吐受限 (~万QPS) | 数据不够持久 | Redis 做队列热路径, 10万+ QPS |
| 排队/出队需加锁或CAS | 没有事务保证 | MySQL 做持久化, 事务+乐观锁 |
| Leader选举需自建 | Watch能力不如etcd | etcd 原生 Lease+Watch |
| 延迟队列实现复杂 | 内存有限, 历史数据丢失 | 各司其职, 互不牵制 |

---

## MySQL 表结构设计

> DDL 文件：`deploy/mysql/schema.sql`

### ER 图

```
┌─────────────────┐        ┌──────────────────┐
│     tasks       │ 1 ── N │   task_events    │
│  (任务主表)      │        │  (事件审计)       │
├─────────────────┤        ├──────────────────┤
│ PK: id          │───────▶│ FK: task_id      │
│ state           │        │ type             │
│ queue_name      │        │ old_state        │
│ priority        │        │ new_state        │
│ worker_id       │        │ timestamp        │
│ version (乐观锁) │        └──────────────────┘
└────────┬────────┘
         │ 失败
         │ 1 ── N
         ▼
┌─────────────────┐        ┌──────────────────┐
│  dead_letters   │        │   cron_jobs      │
│  (死信表)        │        │  (定时任务)       │
├─────────────────┤        ├──────────────────┤
│ task_id         │        │ PK: id           │
│ error           │        │ cron_expr        │
│ retry_count     │        │ next_run_at      │
│ redelivered     │        │ enabled          │
└─────────────────┘        └──────────────────┘
```

### 表 1: tasks（任务主表）

系统核心表，存储所有任务的完整生命周期数据。

```sql
CREATE TABLE `tasks` (
    `id`            VARCHAR(64)   NOT NULL,     -- UUID
    `name`          VARCHAR(255)  DEFAULT '',
    `namespace`     VARCHAR(128)  DEFAULT '',    -- 多租户隔离
    `group`         VARCHAR(128)  DEFAULT '',    -- 亲和性分组
    `type`          VARCHAR(128)  DEFAULT '',    -- Handler类型
    `payload`       TEXT,                        -- JSON载荷
    `labels`        TEXT,                        -- JSON标签
    `priority`      TINYINT       DEFAULT 5,     -- 1~10
    `delay`         BIGINT        DEFAULT 0,     -- 纳秒
    `schedule_at`   DATETIME(3),
    `timeout`       BIGINT        DEFAULT 0,     -- 纳秒
    `max_retries`   INT           DEFAULT 3,
    `retry_count`   INT           DEFAULT 0,
    `retry_backoff` BIGINT        DEFAULT 0,     -- 纳秒
    `state`         TINYINT       DEFAULT 0,     -- 状态码
    `result`        TEXT,
    `error`         TEXT,
    `worker_id`     VARCHAR(128)  DEFAULT '',
    `queue_name`    VARCHAR(128)  DEFAULT 'default',
    `created_at`    DATETIME(3)   DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at`    DATETIME(3)   DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    `started_at`    DATETIME(3),
    `finished_at`   DATETIME(3),
    `version`       BIGINT        DEFAULT 1,     -- 乐观锁

    PRIMARY KEY (`id`)
) ENGINE=InnoDB;
```

**字段设计决策**：

| 决策 | 理由 |
|------|------|
| `id` VARCHAR(64) | 兼容 UUID (36字符) 和 Snowflake ID 等分布式ID方案 |
| `priority` TINYINT | 1~10 够用，1字节存储，索引高效 |
| `delay`/`timeout`/`retry_backoff` BIGINT | 存储纳秒精度的 `time.Duration`，Go 原生兼容 |
| `schedule_at` DATETIME(3) | 毫秒精度，满足调度时间要求 |
| `state` TINYINT | 8种状态，1字节，枚举值比字符串更紧凑高效 |
| `payload`/`labels` TEXT | 可变长 JSON，最大 64KB，大载荷可改用 MEDIUMTEXT |
| `version` BIGINT | 乐观锁，64位不会溢出，支持频繁更新 |

**索引策略**：

| 索引 | 列 | 覆盖场景 |
|------|-----|----------|
| PRIMARY | `id` | 主键查询 |
| `idx_queue_state_priority` | `queue_name, state, priority, created_at` | **核心调度查询**：按队列+状态查询，按优先级排序 |
| `idx_ns_type_state` | `namespace, type, state` | 多租户维度查询任务列表 |
| `idx_worker_state` | `worker_id, state` | 查询某 Worker 的活跃/历史任务 |
| `idx_created_at` | `created_at` | 时间范围查询、数据归档 |
| `idx_name` | `name` | 按名称模糊搜索 |

**最核心的组合索引**：

```sql
KEY `idx_queue_state_priority` (`queue_name`, `state`, `priority`, `created_at`)
```

这个索引覆盖了系统最高频的查询模式：

```sql
-- 调度查询: 从某队列取 Pending 任务，按优先级排序
SELECT * FROM tasks
WHERE queue_name = 'default' AND state = 0
ORDER BY priority DESC, created_at ASC
LIMIT 100;
```

MySQL 可以直接通过索引定位 + 索引内排序，无需回表扫描。

### 表 2: task_events（事件审计表）

```sql
CREATE TABLE `task_events` (
    `id`         VARCHAR(64)   NOT NULL,
    `task_id`    VARCHAR(64)   NOT NULL,
    `type`       VARCHAR(64)   DEFAULT '',    -- created/started/completed/failed/...
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

**设计要点**：

- **仅追加 (Append-Only)**：只有 INSERT，没有 UPDATE/DELETE
- **按任务查事件流**：`idx_task_timestamp` 支持 `WHERE task_id=? ORDER BY timestamp DESC`
- **按时间范围归档**：`idx_timestamp` 支持 `WHERE timestamp < ?` 批量清理
- **高写入量**：每次任务状态变更都会写入，建议按月分区

### 表 3: dead_letters（死信表）

```sql
CREATE TABLE `dead_letters` (
    `id`            VARCHAR(64)   NOT NULL,
    `task_id`       VARCHAR(64)   NOT NULL,
    `queue_name`    VARCHAR(128)  DEFAULT '',
    `type`          VARCHAR(128)  DEFAULT '',
    `payload`       TEXT,                       -- 载荷快照
    `error`         TEXT,                       -- 最后错误
    `retry_count`   INT           DEFAULT 0,
    `max_retries`   INT           DEFAULT 0,
    `failed_at`     DATETIME(3)   DEFAULT CURRENT_TIMESTAMP(3),
    `redelivered`   TINYINT(1)    DEFAULT 0,    -- 是否已人工重投
    `redelivered_at` DATETIME(3),

    PRIMARY KEY (`id`),
    KEY `idx_task_id` (`task_id`),
    KEY `idx_redelivered` (`redelivered`, `failed_at`)
) ENGINE=InnoDB;
```

**使用场景**：

- 任务用尽所有重试后，Worker 将最终快照写入 dead_letters
- 运维人员通过 `redelivered = 0` 过滤未处理的死信
- 排查问题后可将 `redelivered` 设为 1 并重新投递任务

### 表 4: cron_jobs（定时任务定义表）

```sql
CREATE TABLE `cron_jobs` (
    `id`            VARCHAR(64)   NOT NULL,
    `name`          VARCHAR(255)  DEFAULT '',
    `namespace`     VARCHAR(128)  DEFAULT '',
    `type`          VARCHAR(128)  NOT NULL,     -- Handler类型
    `payload`       TEXT,                       -- 载荷模板
    `cron_expr`     VARCHAR(128)  NOT NULL,     -- Cron表达式
    `queue_name`    VARCHAR(128)  DEFAULT 'default',
    `priority`      TINYINT       DEFAULT 5,
    `timeout`       BIGINT        DEFAULT 0,
    `max_retries`   INT           DEFAULT 3,
    `enabled`       TINYINT(1)    DEFAULT 1,
    `last_run_at`   DATETIME(3),
    `next_run_at`   DATETIME(3),
    `created_at`    DATETIME(3)   DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at`    DATETIME(3)   DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

    PRIMARY KEY (`id`),
    KEY `idx_enabled_next` (`enabled`, `next_run_at`)
) ENGINE=InnoDB;
```

**核心查询**：

```sql
-- Scheduler 每秒扫描: 查找已到期且启用的 cron_jobs
SELECT * FROM cron_jobs
WHERE enabled = 1 AND next_run_at <= NOW()
LIMIT 100;
```

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
-- 尝试获取锁 (CAS)
UPDATE scheduler_locks
SET holder = 'scheduler-abc', acquired_at = NOW(), expires_at = DATE_ADD(NOW(), INTERVAL 15 SECOND), version = version + 1
WHERE lock_name = 'leader' AND (expires_at < NOW() OR holder = 'scheduler-abc');
```

---

## Redis 数据结构设计

> 源码：`internal/shared/infrastructure/persistence/redis/queue_broker.go`

### Key 命名规范

```
dispatchhub:queue:{queue_name}:{type}
```

| Key | Redis 类型 | 说明 |
|-----|-----------|------|
| `dispatchhub:queue:default:ready` | Sorted Set | 就绪队列，score = -priority |
| `dispatchhub:queue:default:delayed` | Sorted Set | 延迟队列，score = 执行时间戳(ms) |
| `dispatchhub:queue:default:inflight` | Hash | 执行中，field=taskID, value=taskJSON |
| `dispatchhub:queue:default:stats` | Hash | 统计计数器 |

### 数据流转

```
                    score = -priority              score = timestamp_ms
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
                  │ task_A: {json}  │ ──── Ack ────▶ 删除
                  │                 │ ──── Nack ───▶ 回到 :ready 或 :delayed
                  └─────────────────┘

                  ┌─────────────────┐
                  │  :stats (Hash)  │
                  │ enqueued: 10583 │
                  │ completed: 9821 │
                  │ failed:     37  │
                  └─────────────────┘
```

### 内存估算

单个任务 JSON 约 500B ~ 2KB（取决于 Payload 大小）。

| 队列规模 | 内存估算 | 说明 |
|----------|---------|------|
| 1万 任务 | ~10MB | 小型场景 |
| 10万 任务 | ~100MB | 中型场景 |
| 100万 任务 | ~1GB | 大型场景，建议 Redis Cluster |
| 1000万 任务 | ~10GB | 超大型，需分片 + 多队列分散 |

> 注意：Redis 仅存储**活跃任务**（pending + delayed + inflight），已完成的任务仅保留在 MySQL 中。

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
    "queues": ["default", "high-priority"],
    "concurrency": 100,
    "active_tasks": 42,
    "cpu_usage": 65.2,
    "mem_usage": 78.1,
    "state": 0,
    "started_at": "2024-01-15T10:00:00Z",
    "last_heartbeat": "2024-01-15T10:05:35Z",
    "version": "v0.1.0"
}
```

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

```go
// internal/shared/infrastructure/persistence/mysql/task_repository.go
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

执行的 SQL：

```sql
UPDATE tasks SET state=?, worker_id=?, version=3, ...
WHERE id = 'abc' AND version = 2;
-- RowsAffected == 0 表示版本冲突
```

适用场景：任务状态更新（Worker 完成/失败上报）频率不高，冲突概率低。

### 原子操作（Redis Lua 脚本）

出队操作必须保证原子性：ZPOPMIN + HSET inflight 不能分开执行。

```lua
-- 原子出队: 从 ready 取出 → 放入 inflight
local result = redis.call('ZPOPMIN', queue_key, 1)
local data = result[1]
local task = cjson.decode(data)
redis.call('HSET', inflight_key, task.id, data)
return data
```

### Lease 机制（etcd 自动注销）

```
Worker 注册 → PUT /dispatchhub/workers/w1 (Lease TTL=15s)
              │
              ├── KeepAlive (持续续约)
              │
              ╳ Worker 崩溃 (KeepAlive 停止)
              │
              │  ... 15秒后 ...
              │
              ▼
              etcd 自动删除 Key → Scheduler Watch 收到 DELETE 事件
```

---

## 数据量级与容量规划

### 小型（日任务 < 10万）

| 组件 | 配置 |
|------|------|
| MySQL | 单机 4C8G，50GB SSD |
| Redis | 单机 2C4G |
| etcd | 单机（开发）或 3节点 |
| Worker | 3~5 个 |

### 中型（日任务 10万 ~ 1000万）

| 组件 | 配置 |
|------|------|
| MySQL | 8C32G 主从，200GB SSD，读写分离 |
| Redis | 单机 4C16G 或 3主3从 Cluster |
| etcd | 3节点集群 |
| Worker | 10~50 个，HPA |

tasks 表数据量：按保留 30天算，~3亿行。建议：
- 按 `created_at` 月分区
- 终态任务（completed/failed）定期归档到 `tasks_archive`
- task_events 按月分区，保留 7天热数据

### 大型（日任务 > 1000万）

| 组件 | 配置 |
|------|------|
| MySQL | 16C64G MGR 集群，1TB SSD，按 queue_name 分库 |
| Redis | 6主6从 Cluster，64GB+ 总内存 |
| etcd | 5节点集群，SSD |
| Worker | 50~500 个，多集群 |

需要额外措施：
- MySQL 按 `queue_name` 或 `namespace` 分库分表（ShardingSphere / Vitess）
- Redis 大 Key 拆分，单队列超 100万任务时按 hash slot 分桶
- task_events 考虑切换到 ClickHouse / Elasticsearch

### 容量估算公式

```
MySQL tasks 表:
  行大小 ≈ 500B ~ 2KB (取决于 payload)
  日增量 = 日任务数 × 行大小
  月容量 = 日增量 × 30
  示例: 100万/天 × 1KB = 1GB/天 = 30GB/月

MySQL task_events 表:
  每个任务平均 3~5 个事件 (created → running → completed)
  行大小 ≈ 200B
  日增量 = 日任务数 × 4 × 200B
  示例: 100万/天 × 4 × 200B = 800MB/天

Redis 内存:
  仅存活跃任务 (pending + delayed + inflight)
  峰值任务数 × 平均 JSON 大小
  示例: 10万活跃 × 1KB = 100MB
```

---

## 数据归档策略

### tasks 表归档

```sql
-- 1. 创建归档表 (结构相同)
CREATE TABLE tasks_archive LIKE tasks;

-- 2. 将30天前的终态任务迁移到归档表
INSERT INTO tasks_archive
SELECT * FROM tasks
WHERE state IN (4, 5, 6, 7)  -- Completed/Failed/Cancelled/Timeout
  AND finished_at < DATE_SUB(NOW(), INTERVAL 30 DAY);

-- 3. 删除已归档数据
DELETE FROM tasks
WHERE state IN (4, 5, 6, 7)
  AND finished_at < DATE_SUB(NOW(), INTERVAL 30 DAY)
LIMIT 10000;  -- 分批删除，避免长事务
```

### task_events 表归档

```sql
-- 保留7天，其余归档到冷存储或直接删除
DELETE FROM task_events
WHERE timestamp < DATE_SUB(NOW(), INTERVAL 7 DAY)
LIMIT 50000;
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
- `DROP PARTITION` 可以秒级删除整月数据
- 查询自动分区裁剪，只扫描相关分区
- 配合定时 Job 自动创建新分区、删除旧分区

---

## 存储接口抽象

> 源码：`internal/shared/domain/repository/`

所有存储操作通过接口抽象，方便替换实现：

```go
// 持久化 (MySQL)
type TaskRepository interface {
    Create(ctx, task) error
    Get(ctx, id) (*Task, error)
    Update(ctx, task) error              // 乐观锁
    Delete(ctx, id) error
    List(ctx, filter) ([]*Task, int64, error)
}

// 快速队列 (Redis)
type QueueBroker interface {
    Enqueue(ctx, queue, task) error
    EnqueueDelayed(ctx, queue, task) error
    Dequeue(ctx, queues) (*Task, error)  // Lua原子出队
    Ack(ctx, queue, taskID) error
    Nack(ctx, queue, task) error
    PromoteDelayed(ctx, queue) (int64, error)
    Len(ctx, queue) (int64, error)
    Stats(ctx, queue) (*QueueStats, error)
}

// 服务注册 (etcd)
type WorkerRegistry interface {
    Register(ctx, worker) error          // Lease注册
    Deregister(ctx, workerID) error
    Heartbeat(ctx, heartbeat) error
    GetWorker(ctx, workerID) (*WorkerInfo, error)
    ListWorkers(ctx) ([]*WorkerInfo, error)
    WatchWorkers(ctx) (<-chan WorkerEvent, error)
}
```

如需替换存储实现（例如将 Redis 替换为 Kafka，或将 etcd 替换为 Consul），只需实现对应接口即可，上层代码无需修改。
