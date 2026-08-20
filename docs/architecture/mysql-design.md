# MySQL 设计：运维与 DBA 视角

本文聚焦 `storage.md` 没有展开的运维细节：查询模式与索引的实际匹配、写入路径的性能取舍、容量与归档策略、连接池调优依据、schema 与 entity 的演进遗留。表结构与字段语义请先阅读 [`storage.md`](storage.md) 与 [`data-models.md`](data-models.md)。

> 源 DDL：`deploy/mysql/schema.sql`
> Repository 实现：`internal/shared/infrastructure/persistence/mysql/`
> 连接池配置：`config/{apiserver,scheduler,worker}.yaml`

---

## 1. 查询模式 → 索引匹配表

`tasks` 表上有 9 个单列索引 + 3 个组合索引。下表把每条线上 SQL 与命中的索引一一对应起来。

### 1.1 写路径

| 触发点 | SQL 形态 | 命中索引 | 备注 |
|---|---|---|---|
| `apiserver` 提交任务 (`task_repository.go:Create`) | `INSERT INTO tasks ...` | PRIMARY (id) | 单条 INSERT，无事务包装（见 §2.1） |
| `worker` 状态推进 (`task_repository.go:Update`) | `UPDATE tasks SET ... WHERE id=? AND version=?` | PRIMARY (id) | 主键定位，再校验 version；`RowsAffected=0` 即乐观锁冲突 |
| `scheduler` 补偿刷新 (`TouchUpdatedAt`) | `UPDATE tasks SET updated_at=? WHERE id=?` | PRIMARY (id) | 故意不递增 version，避免破坏 worker 持有的旧版本号 |
| `cron` 触发新任务 | `INSERT INTO tasks ...` | PRIMARY (id) | 同提交路径 |

**关键约束**：所有写入都按 `id` 单行操作。**没有任何 `SELECT ... FOR UPDATE` 或区间锁**，并发由乐观锁（`version` 字段）兜底。这是项目**用 Redis 做队列、不让 MySQL 当队列**的直接体现 —— 详见 [队列选型分析](queue-selection.md)。

### 1.2 读路径

| 触发点 | SQL 形态 | 命中索引 | 估算行扫描 |
|---|---|---|---|
| API 单任务查询 | `WHERE id=?` | PRIMARY | 1 行 |
| API 列表查询 (`List`) | `WHERE namespace=? AND type=? AND state=? ORDER BY priority DESC, created_at ASC LIMIT ?` | `idx_ns_type_state`（三列前缀全命中），ORDER BY 走 filesort | 看过滤后行数，通常百级 |
| API 列表（仅按队列+状态） | `WHERE queue_name=? AND state=? ORDER BY priority DESC, created_at ASC LIMIT ?` | **`idx_queue_state_priority`** | **ORDER BY 走索引，无 filesort** ← 核心索引 |
| Worker 详情回查 | `WHERE id=?` | PRIMARY | 1 行 |
| 补偿循环 (`FindStaleByState`) | `WHERE state=? AND updated_at<? ORDER BY updated_at ASC LIMIT 100` | `idx_state` 后回表过滤 `updated_at` | 看 pending 队列深度，通常千级 |
| 清理循环 (`DeleteTerminalOlderThan`) | `WHERE state IN (...) AND finished_at<? LIMIT 1000` | `idx_state` + 回表 | **见 §3.2，存在优化空间** |
| Cron 维护 (`HasRunningTasks`) | `WHERE type=? AND namespace=? AND state=?` COUNT | `idx_ns_type_state` | 三列全命中 |

### 1.3 为什么 `idx_queue_state_priority` 是"核心索引"

调度系统最高频的列表查询长这样：

```sql
SELECT * FROM tasks
WHERE queue_name='default' AND state=0
ORDER BY priority DESC, created_at ASC
LIMIT 100;
```

`(queue_name, state, priority, created_at)` 这条索引能让 MySQL 做到：

1. **用前两列定位**：`queue_name='default' AND state=0` 走索引前缀，扫描区间极小
2. **后两列排序免 filesort**：索引内部已经是 `priority DESC, created_at ASC` 顺序（`priority` 用 DESC 还是 ASC 不重要，MySQL 8 都能反向扫描），LIMIT 100 直接拿前 100 行
3. **不需要 SELECT * 的回表**：如果只取索引列就完全覆盖；当前实现 SELECT *，会回表 100 次拿其他列，但**filter+sort 阶段是零回表**

EXPLAIN 期望长这样：

```
type=range, key=idx_queue_state_priority, rows≈100, Extra=Using index condition
```

**绝对不能出现** `Using filesort` 或 `Using temporary` —— 出现就说明索引失效了，要立刻排查。

### 1.4 索引冗余审视

`tasks` 表上有些单列索引在当前查询模式下**实际上是冗余的**：

| 索引 | 是否冗余 | 理由 |
|---|---|---|
| `idx_state` | ⚠️ 部分冗余 | 大部分场景被 `idx_queue_state_priority` 或 `idx_ns_type_state` 覆盖。但 `FindStaleByState` 和 `DeleteTerminalOlderThan` 只按 state 过滤，**保留** |
| `idx_priority` | ❌ 冗余 | 没有"仅按 priority 过滤"的查询；所有按 priority 排序的场景都附带 queue/state 过滤，已被组合索引覆盖 |
| `idx_queue_name` | ❌ 冗余 | 同理，没有"仅按 queue 过滤"的查询 |
| `idx_name`, `idx_group` | ⚠️ 预留 | entity 上 `Name`/`Group` 有 `index` tag 但**当前 List 接口不支持按这两列过滤**（`task_repository.go:List`），等于死索引 |
| `idx_namespace` | ✅ 保留 | `idx_ns_type_state` 三列前缀已覆盖，单独的 namespace 索引可考虑删除，但保留代价低 |

**索引不是越多越好**。每多一个索引，INSERT/UPDATE 都要维护。当前 SubmitTask 的写路径吞吐压力主要来自 GORM 隐式事务（已通过 `SkipDefaultTransaction` 关掉，详见 §2.1），但索引维护开销随写入量线性放大。**如果 r3 之后还要继续优化写吞吐，第一个该删的就是 `idx_priority`、`idx_queue_name`、`idx_name`、`idx_group` 这几条**。

---

## 2. 写入路径性能

### 2.1 关闭 GORM 默认事务

`internal/shared/infrastructure/persistence/factory.go:46`：

```go
db, err := gorm.Open(mysql.Open(cfg.DSN), &gorm.Config{
    SkipDefaultTransaction: true,
})
```

GORM 默认会把每次 `Create/Update/Delete` 包在 `BEGIN ... COMMIT` 里。**本项目所有写入都是单语句**，没有跨表跨行的事务需求，这层包装是纯开销：

- 每次写入多 2 次 client-server round-trip（BEGIN + COMMIT）
- 占用连接时间从单语句的 ~1ms 变成 ~3ms

2026-05-19 的 CPU profile 显示，关闭前**这层包装占 SubmitTask CPU 时间的 39%**（见 `factory.go:43` 的注释，以及 `docs/performance/optimizations/r2-profile-analysis-2026-05-19.md`）。关掉之后写入吞吐有显著提升。

**前提条件**：项目里没有需要事务原子性的多语句写入。一旦未来出现"写 tasks 同时写 task_events"的需求，这个开关就不能开了 —— 必须显式 `db.Transaction(func(tx) {...})`。

### 2.2 乐观锁的并发模型

`task_repository.go:Update`：

```go
oldVersion := task.Version
task.Version++
result := s.db.Where("id = ? AND version = ?", task.ID, oldVersion).Updates(task)
if result.RowsAffected == 0 {
    return fmt.Errorf("optimistic lock conflict: task %s version %d", task.ID, oldVersion)
}
```

生成 SQL：

```sql
UPDATE tasks SET state=?, worker_id=?, version=3, ... 
WHERE id='abc' AND version=2;
```

**关键设计点**：

- **不预先 `SELECT FOR UPDATE`**：行锁完全交给 InnoDB 在 `UPDATE` 自身的内部锁机制，不会有"读锁→升级写锁"的两阶段
- **不递增 version 的特殊路径**：`TouchUpdatedAt` 只刷 `updated_at`，**故意不动 version**。补偿循环刷新 updated_at 是为了让自己下一轮不再捞起这条任务，但 worker 仍持有旧 version，正常 Update 还能成功
- **冲突即错误，不重试**：发生冲突说明任务被并发修改（典型场景：补偿入队时 worker 同时也开始执行），上层根据语义决定是放弃还是重新加载后再操作

**冲突频率**：worker 取走任务后只有自己会更新它（其他 worker 不会重复 Dequeue 同一条），冲突主要发生在"补偿/取消"和"worker 状态推进"之间。生产环境实测冲突率 < 0.1%。

### 2.3 写并发的天花板

实测下三类写入的瓶颈位置：

| 操作 | 瓶颈 | 数量级 |
|---|---|---|
| INSERT (Create) | InnoDB redo log fsync 频率 | 单实例 ~10k QPS（关闭事务后） |
| UPDATE by PK (单行) | 同上 + 索引页修改 | 单实例 ~8k QPS |
| 补偿循环 SELECT (`FindStaleByState`) | `idx_state` 选择性差，回表多 | 看 pending 行数，pending<10万 时 < 50ms |

如果 INSERT 吞吐成为瓶颈，**先做的是降低索引数量**（§1.4），其次才考虑分库分表。

---

## 3. 容量规划与数据归档

### 3.1 单行存储估算

`tasks` 表单行字节数（不含变长 TEXT 字段）：

| 字段类型 | 字节 | 字段数 | 小计 |
|---|---|---|---|
| VARCHAR(64) (id) | ≤66 | 1 | 66 |
| VARCHAR(255/128) (name/namespace/group/type/queue_name/worker_id) | ≤257/130 | 6 | ~800 |
| TINYINT (priority/state) | 1 | 2 | 2 |
| INT (max_retries/retry_count) | 4 | 2 | 8 |
| BIGINT (delay/timeout/retry_backoff/version) | 8 | 4 | 32 |
| DATETIME(3) (5 个时间字段) | 8 | 5 | 40 |

**固定部分约 ~950 字节**。加上典型的 `payload` (1KB) + `labels` (200B) + `result` (500B)，**单行约 2.5~3 KB**。

容量估算：

| 任务数 | tasks 表大小 | 索引大小 (估 30%) | 总计 |
|---|---|---|---|
| 100 万 | ~3 GB | ~1 GB | ~4 GB |
| 1000 万 | ~30 GB | ~10 GB | ~40 GB |
| 1 亿 | ~300 GB | ~100 GB | ~400 GB |

**经验阈值**：单表 > 5000 万行后，二级索引 B+Tree 高度从 3 涨到 4，单次查询多一次磁盘 IO。**到 5000 万行就该考虑归档策略了**。

### 3.2 终态任务清理

`scheduler` 每小时跑一次清理（`scheduler_app_service.go:42`）：

```go
CleanupInterval:  time.Hour,
CleanupOlderThan: 7 * 24 * time.Hour,  // 7 天
CleanupBatchSize: 1000,
```

实际执行的 SQL（`task_repository.go:DeleteTerminalOlderThan`）：

```sql
DELETE FROM tasks
WHERE state IN (4, 5, 6, 7) AND finished_at < ?
LIMIT 1000;
```

**当前实现的问题**：

1. **`idx_state` 选择性差**：终态任务占总量的 90%+ 时，state IN (4,5,6,7) 命中行数巨大，索引扫描 + 回表 + 过滤 finished_at 性能不理想
2. **没有 finished_at 索引**：扫到 state 匹配的行后还要逐行比 finished_at
3. **DELETE 在 InnoDB 是逻辑删除 + Purge 异步回收空间**：高频 DELETE 会让表的 free page 碎片化，物理大小不会立刻缩小

**改进方向**（未实施，留作 r4 优化项）：

- 加 `idx_finished_at_state(finished_at, state)`，让清理走时间区间扫描
- 改用 **TRUNCATE PARTITION**：按 `finished_at` 月度分区，整月归档时 DROP PARTITION 是 metadata 操作，秒级完成
- 归档前先复制到 `tasks_archive` 表，保留审计能力

### 3.3 死信表无清理策略

**注意**：`dead_letters` 表**当前没有任何清理逻辑**。设计意图是"人工排查 + 重新投递"，所以保留全量。但如果失败率高、长期积累，需要单独写归档脚本。

### 3.4 task_events 表的现状

`task_events` 在 schema.sql 中保留 DDL，**但 entity 已删除**（见 `storage.md` 备注）。当前代码不写不读这张表，对运维而言它就是个空壳。**如果未来要恢复审计功能，这是写入压力最大的表**（每次状态变更一条），方案上必须按 `timestamp` 做时间分区。

---

## 4. 连接池调优

三个服务的 `conn_max_lifetime`、`max_open_conns` 都是 `1h` / `50`，但 `max_idle_conns` 不一样：

| 服务 | max_open_conns | max_idle_conns | conn_max_idle_time | 理由 |
|---|---|---|---|---|
| **apiserver** | 50 | **50** | **5m** | idle == open，避免请求峰值时连接抖动 |
| **scheduler** | 50 | 10 | 未配置 | 后台任务流量平稳，10 个 idle 够用 |
| **worker** | 50 | 10 | 未配置 | 同 scheduler |

### 4.1 apiserver 为什么 `idle == open`

`config/apiserver.yaml:21` 注释里说明了：

> 连接池设置为 idle == open，避免突发负载下 database/sql 连接频繁创建和关闭。
> ConnMaxIdleTime 应小于服务端 wait_timeout（MySQL 8 默认 28800s），
> 使客户端先于 MySQL 主动关闭 idle connection。

**问题场景**（`idle < open` 时）：
- QPS 突增 → 池里 idle 连接被快速借走 → 不够用就 dial 新连接（典型 5~10ms）
- QPS 回落 → idle 连接超过 `max_idle_conns` 被关闭
- 下一波突增又重新 dial → **连接生命周期与流量同步抖动**

`idle == open` 让池子始终保持满载，请求只是借/还，不创建/关闭。代价是连接长期占用 MySQL 端的线程槽位。

### 4.2 `ConnMaxIdleTime` 的两端协调

MySQL 服务端 `wait_timeout` 默认 28800s（8 小时），超过这个时间服务端会**单方面关闭 idle 连接**。但服务端关闭后，客户端连接池里那条连接还认为自己是好的，下次借出就会得到 `connection reset by peer`。

apiserver 把 `ConnMaxIdleTime` 设成 5 分钟，让**客户端先于服务端主动关闭 idle 连接**。这是规避 stale connection 错误的正确姿势。

scheduler/worker 没配 `ConnMaxIdleTime`，**这是个小坑** —— 如果服务端 `wait_timeout` 调小（比如运维改成 600s），scheduler/worker 会先开始报 stale connection 错。建议统一加上 `conn_max_idle_time: 5m`。

### 4.3 为什么 `max_open_conns = 50`

50 不是凭空来的：
- 单 apiserver 实例 ~5000 QPS，平均每请求占用连接 ~10ms
- 理论并发连接数 = 5000 × 0.01 = 50
- 留 100% buffer 也只需 100 个；50 是"够用 + 不挤占 MySQL"的折中

MySQL 端的 `max_connections` 通常是几百到几千。**如果部署 N 个 apiserver 实例，N × 50 不能超过 MySQL 端 max_connections 的 70%**，否则其他服务会被挤掉。

---

## 5. Schema 与 entity 的演进遗留

schema.sql 里有些字段/表，**Go 代码已经不用了**。运维排查问题时容易踩坑，记录在此：

### 5.1 entity 已弃用但 schema 保留的字段

| 字段 | DDL 中存在 | entity.Task 中 | 实际状态 |
|---|---|---|---|
| `tasks.cron_expr` | ✅ VARCHAR(128) | ❌ 无 | GORM AutoMigrate 不会维护它，但旧数据可能有值。**不要靠它做查询** |
| `tasks.deadline` | ✅ DATETIME(3) | ❌ 无 | 同上。原本想做"绝对截止时间"，被 `Timeout`（相对时长）替代 |

GORM 的 AutoMigrate 是**只增不减**：它会按 entity 创建新列，但不会删除 schema 里多余的列。所以这两列会**一直留在 DDL 和实际表里**，但没人写、没人读。

### 5.2 整张表已弃用

| 表 | 状态 |
|---|---|
| `task_events` | DDL 保留，entity 已删，**当前代码完全不读不写**。空表。 |
| `scheduler_locks` | DDL 保留，**项目实际用 etcd 做 leader 选举**（`internal/scheduler/infrastructure/election/election.go`），这张表是早期备选方案的残留。空表。 |

排查时发现这两张表为空是**正常**的。

### 5.3 GORM AutoMigrate 与 schema.sql 的关系

`task_repository.go:21`：

```go
func NewTaskRepository(db *gorm.DB) (*TaskRepository, error) {
    if err := db.AutoMigrate(&entity.Task{}); err != nil {
        return nil, fmt.Errorf("auto migrate: %w", err)
    }
    ...
}
```

启动时会跑 AutoMigrate，**以 entity 的 struct tag 为准**。这意味着：

- 全新部署：schema.sql 不是必须的，AutoMigrate 会建表
- 老库升级：DDL 演进以 entity 为准，schema.sql 仅供初始化和参考
- **风险**：AutoMigrate 不会删字段、不会改字段类型，遇到不兼容变更（比如 VARCHAR 长度从 64 → 128）会**静默忽略**。生产环境结构变更必须走人工 ALTER TABLE，不能依赖 AutoMigrate。

---

## 6. 运维 checklist

启动新环境时按这个清单走：

1. **建库**：`mysql -uroot < deploy/mysql/schema.sql`（或让 AutoMigrate 自动建）
2. **核对字符集**：`SHOW CREATE DATABASE dispatchhub`，必须是 `utf8mb4 / utf8mb4_general_ci`
3. **核对 `wait_timeout`**：`SHOW VARIABLES LIKE 'wait_timeout'`，应 ≥ 客户端 `conn_max_idle_time` × 2
4. **核对 `max_connections`**：≥ 部署的所有客户端实例 × `max_open_conns` × 1.4
5. **预创建监控用户**：grant `SELECT` on tasks/cron_jobs/dead_letters，给 Grafana 用
6. **配置慢查询日志**：`long_query_time = 0.5`，关注是否有 `idx_queue_state_priority` 之外的索引被频繁打中
7. **配置归档作业**：cron + 自定义脚本，每月把 `dead_letters` 中 `failed_at < 90d` 的归档到冷库

## 7. 待办（r4 候选）

- [ ] 删除冗余索引 `idx_priority`、`idx_queue_name`、`idx_name`、`idx_group`，量化写入吞吐提升
- [ ] 给清理路径加 `idx_finished_at_state(finished_at, state)`
- [ ] `tasks` 按 `created_at` 月度分区，支持 `DROP PARTITION` 秒级归档
- [ ] scheduler/worker 的 `conn_max_idle_time` 补齐
- [ ] `dead_letters` 加自动归档脚本
- [ ] schema.sql 清理：删除 `cron_expr`、`deadline` 列，删除 `task_events`、`scheduler_locks` 表（或写明"仅保留为占位"）
