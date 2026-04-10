# 为什么不用 MySQL 做任务队列——FOR UPDATE 锁机制深度分析

## 一、问题背景

DispatchHub 的核心操作是**出队**——多个 Worker 并发地从同一个队列中取出优先级最高的任务来执行。用 SQL 描述就是：

```sql
-- 取出 default 队列中优先级最高的一个待处理任务
SELECT * FROM tasks
WHERE queue_name = 'default' AND state = 0  -- 0 = pending
ORDER BY priority DESC, created_at ASC
LIMIT 1;
```

这条 SQL 本身没问题，问题在于 **100 个 Worker 同时执行它时会发生什么**。

---

## 二、不加锁的灾难

### 场景设定

tasks 表中有一条待处理任务：

```
| id     | state   | priority | queue_name |
|--------|---------|----------|------------|
| task-1 | pending | 10       | default    |
```

3 个 Worker 同时执行出队。

### 方案 A：裸查询 + UPDATE（无任何保护）

```sql
-- 每个 Worker 都执行这两步
-- Step 1: 查出优先级最高的待处理任务
SELECT * FROM tasks
WHERE queue_name = 'default' AND state = 0
ORDER BY priority DESC LIMIT 1;
-- 结果: task-1

-- Step 2: 标记为 running
UPDATE tasks SET state = 2, worker_id = 'me' WHERE id = 'task-1';
```

时序：

```
时间    Worker-A                  Worker-B                  Worker-C
────────────────────────────────────────────────────────────────────────
T1     SELECT → task-1
T2                                SELECT → task-1
T3                                                          SELECT → task-1
T4     UPDATE state=2,            UPDATE state=2,           UPDATE state=2,
       worker_id='A'              worker_id='B'             worker_id='C'
       WHERE id='task-1'          WHERE id='task-1'         WHERE id='task-1'
```

**结果**：三个 Worker 都拿到了 task-1，**三个都执行了同一个任务**。最后一个 UPDATE 的 worker_id 覆盖前面的，MySQL 不报错，但业务上已经重复消费。

**问题**：**重复消费**——同一个任务被执行了 3 次。

### 方案 B：CAS 保护（WHERE state = pending）

加一层状态检查：

```sql
-- Step 1: 查
SELECT * FROM tasks WHERE queue_name = 'default' AND state = 0
ORDER BY priority DESC LIMIT 1;
-- 结果: task-1

-- Step 2: CAS 更新
UPDATE tasks SET state = 2, worker_id = 'me'
WHERE id = 'task-1' AND state = 0;  -- 只有 state 仍是 pending 才更新
-- 检查 RowsAffected：0 = 被别人抢走了，1 = 抢到了
```

时序：

```
时间    Worker-A                  Worker-B                  Worker-C
────────────────────────────────────────────────────────────────────────
T1     SELECT → task-1
T2                                SELECT → task-1
T3                                                          SELECT → task-1
T4     UPDATE WHERE state=0
       RowsAffected=1 ✅
       (抢到了)
T5                                UPDATE WHERE state=0
                                  RowsAffected=0 ❌
                                  (state 已被 A 改成 2)
T6                                                          UPDATE WHERE state=0
                                                            RowsAffected=0 ❌
```

**重复消费的问题解决了**，但带来了新问题：

#### 问题 1：空转浪费

Worker-B 和 Worker-C **白跑了一轮**——查出来了 task-1，花了 SELECT 的开销，结果抢不到。在高并发下（100 个 Worker），可能 99 个都在空转，只有 1 个成功。

#### 问题 2：全体扎堆

更严重的场景——队列里有 100 个任务，100 个 Worker 同时查询：

```sql
SELECT ... ORDER BY priority DESC LIMIT 1;
```

**所有 Worker 都查到同一条**（优先级最高的），然后 99 个 CAS 失败。下一轮又全部扎堆到第二条......

本质上退化成了**串行处理**——虽然有 100 个任务和 100 个 Worker，但每一轮只有 1 个 Worker 成功取到任务。

#### 问题 3：轮询风暴

CAS 失败的 Worker 怎么办？只能 sleep 一会再重试。假设 sleep 100ms：

- 100 个 Worker × 每秒 10 次轮询 = **1000 QPS 的无效 SELECT**
- 这些查询全部走索引扫描，对 MySQL 造成不必要的负载

---

## 三、FOR UPDATE 解决什么

### 方案 C：SELECT ... FOR UPDATE SKIP LOCKED

```sql
BEGIN;

-- Step 1: 查询并锁定（跳过已锁的行）
SELECT * FROM tasks
WHERE queue_name = 'default' AND state = 0
ORDER BY priority DESC
LIMIT 1
FOR UPDATE SKIP LOCKED;

-- Step 2: 更新状态
UPDATE tasks SET state = 2, worker_id = 'me' WHERE id = 'task-1';

COMMIT;
```

关键词解释：

| 关键词 | 作用 |
|--------|------|
| `FOR UPDATE` | 对查询结果集中的行加**排他锁（X Lock）**，其他事务不能读取（FOR UPDATE）或修改这些行 |
| `SKIP LOCKED` | 遇到已被其他事务锁定的行时，**不等待，直接跳过**，继续找下一行 |

时序：

```
时间    Worker-A                    Worker-B                    Worker-C
──────────────────────────────────────────────────────────────────────────
T1     BEGIN
       SELECT ... FOR UPDATE
       SKIP LOCKED
       → 锁住 task-1
       → 返回 task-1

T2                                  BEGIN
                                    SELECT ... FOR UPDATE
                                    SKIP LOCKED
                                    → task-1 被锁 → 跳过
                                    → 锁住 task-2
                                    → 返回 task-2 ✅

T3                                                              BEGIN
                                                                SELECT ... FOR UPDATE
                                                                SKIP LOCKED
                                                                → task-1,2 被锁 → 跳过
                                                                → 锁住 task-3
                                                                → 返回 task-3 ✅

T4     UPDATE task-1 → running
       COMMIT (释放锁)

T5                                  UPDATE task-2 → running
                                    COMMIT

T6                                                              UPDATE task-3 → running
                                                                COMMIT
```

**效果**：三个 Worker **各取各的，互不干扰**。不重复，不空转，真正的并发出队。

### FOR UPDATE 的三种模式对比

| 模式 | 行为 | 效果 |
|------|------|------|
| 不加锁 | 所有 Worker 查到同一行 | 重复消费 / 空转浪费 |
| `FOR UPDATE`（不加 SKIP） | 遇到已锁的行**阻塞等待**，直到锁释放 | Worker 排队等锁，退化成串行 |
| `FOR UPDATE SKIP LOCKED` | 遇到已锁的行**跳过**，找下一个可用行 | 各取各的，真正并发 |

`SKIP LOCKED` 是 MySQL 8.0+ 才有的特性，之前的版本只能用 `FOR UPDATE` 阻塞等待，性能更差。

---

## 四、FOR UPDATE 仍然无法根治的问题

虽然 `FOR UPDATE SKIP LOCKED` 是 MySQL 做队列的最佳方案，但它仍有**结构性缺陷**，这些是关系数据库做实时队列的天花板：

### 4.1 锁竞争热点

```
100 个 Worker 同时执行:
  SELECT ... FOR UPDATE SKIP LOCKED ORDER BY priority DESC LIMIT 1
```

InnoDB 的执行过程：

1. 按索引找到第一行 → 尝试加锁
2. 如果已锁 → 跳过 → 找第二行 → 尝试加锁
3. 如果已锁 → 跳过 → 找第三行 → ...
4. 直到找到一个未锁的行

当并发 Worker 数量很大时，优先级最高的前 N 行成为**热点**，大量加锁/跳过操作消耗 CPU 和锁管理资源。

### 4.2 索引间隙锁（Gap Lock）

InnoDB 在 REPEATABLE READ 隔离级别下使用 Next-Key Lock：不仅锁住找到的行，还锁住索引中行之间的**间隙**。

```
索引 idx_queue_state_priority 上的锁范围:

     priority=10  priority=8  priority=5
         │            │           │
         ▼            ▼           ▼
    ─────[task-1]────[task-2]────[task-3]────
         ▲                                ▲
         │    Next-Key Lock 覆盖范围       │
         └────────────────────────────────┘

这个范围内的 INSERT（新任务入队）也会被阻塞！
```

**入队被出队阻塞**——Enqueue（INSERT）和 Dequeue（SELECT FOR UPDATE）产生锁冲突，二者本应互不干扰。

### 4.3 事务开销

每次出队是一个完整的 InnoDB 事务：

```
BEGIN
  → 分配事务 ID
  → 创建一致性读视图
  → 索引扫描 + 行锁获取
  → UPDATE 写 redo log
  → 写 undo log
COMMIT
  → redo log 刷盘（fsync）
  → 释放行锁
  → 清理事务元数据
```

即使最简单的出队操作，也要经历上述全部步骤。redo log 的 `fsync` 是最大开销——每次 COMMIT 至少一次磁盘写入。

对比 Redis Lua 脚本：

```
ZPOPMIN + HSET
  → 内存操作
  → 单线程无锁
  → 完成
```

### 4.4 轮询空转

队列为空时：

```sql
SELECT ... FOR UPDATE SKIP LOCKED WHERE state = 0;
-- 结果: 空
```

Worker 只能 `sleep(100ms)` 后再查。这意味着：

- **额外延迟**：新任务入队后最多等 100ms 才会被取走
- **无效查询**：空队列时每秒仍然有 N_worker × 10 次 SELECT

Redis 的 ZPOPMIN 返回 nil 几乎是零开销（O(1) 内存操作），空轮询无压力。

### 4.5 连接数耗尽

每个 Worker 出队时持有一个 MySQL 连接和事务。100 个 Worker = 100 个并发连接 + 100 个活跃事务。

MySQL 默认 `max_connections = 151`，被出队操作占满后，其他业务查询（API 查任务列表、管理后台）无连接可用。

---

## 五、性能实测对比

| 指标 | Redis ZPOPMIN (Lua) | MySQL FOR UPDATE SKIP LOCKED |
|------|--------------------|-----------------------------|
| **单次出队延迟** | < 0.1ms | 1~10ms |
| **10 Worker 并发 QPS** | 10万+ | 8000~12000 |
| **100 Worker 并发 QPS** | 10万+ | 2000~5000 |
| **空队列轮询开销** | O(1) 返回 nil | 全索引扫描 |
| **入队是否受出队影响** | 完全独立 | 可能被间隙锁阻塞 |
| **事务开销** | 无 | 每次 fsync |
| **连接占用** | 1 个 Redis 连接 | 1 个 MySQL 连接 + 1 个事务 |

Worker 数量增加时的 QPS 变化：

```
QPS
│
│  Redis ────────────────────────── 10万+（基本恒定）
│
│
│       MySQL
│       ╱╲
│      ╱  ╲
│     ╱    ╲──────────── 下降
│    ╱
│   ╱
│──╱
└────────────────────────────────── Worker 数量
   1   10   50   100  200

Redis: Worker 增加对 QPS 几乎无影响（无锁竞争）
MySQL: Worker 超过 50 后 QPS 反而下降（锁竞争加剧）
```

---

## 六、MySQL 适合做什么

虽然 MySQL 不适合做高频出队的队列热路径，但它在 DispatchHub 中承担了同样重要的角色：

### 适合 MySQL 的操作

| 操作 | 频率 | 原因 |
|------|------|------|
| 任务创建（持久化） | 与入队同频 | 一次 INSERT，无锁竞争 |
| 状态更新（完成/失败） | 每任务 1~2 次 | 单行 UPDATE，version 乐观锁 |
| 任务查询（列表/详情） | 低频 | 走索引，读操作无锁 |
| 事件审计写入 | 每任务 3~5 次 | Append-only INSERT |
| 批量归档 | 定时 | 非实时，可在低峰执行 |

### 项目中的分工

```
任务提交:
  ├── MySQL: INSERT tasks（持久化，保证不丢）
  └── Redis: ZADD ready（入队，保证快速消费）

任务消费:
  ├── Redis: ZPOPMIN（出队，10万+ QPS）
  └── MySQL: UPDATE state=running（状态变更，低频）

任务完成:
  ├── Redis: HDEL inflight（移除追踪）
  └── MySQL: UPDATE state=completed（最终记录）
```

---

## 七、总结

```
不加锁:              100 Worker 查到同一条 → 重复消费
CAS (WHERE state=0): 99 个白跑 → 串行化 → 吞吐暴跌
FOR UPDATE:          排队等锁 → 串行化 → 慢
FOR UPDATE           各跳各的 → 能并发了
  SKIP LOCKED:       但锁竞争 + 间隙锁 + 事务开销 → 上限约 5000 QPS

Redis ZPOPMIN:       原子弹出 → 无锁 → 纯内存 → 0.1ms / 10万+ QPS
```

**`FOR UPDATE` 是 MySQL 做队列的"最优解"，同时也是"天花板"**。它解决了正确性问题（不重复消费），但关系数据库的事务模型、磁盘 I/O、锁管理机制决定了它的吞吐上限远低于内存数据结构。

对于 DispatchHub 的三高场景，正确的做法是让 MySQL 和 Redis 各司其职：

- **Redis**：队列热路径——入队、出队、inflight 跟踪
- **MySQL**：持久化冷路径——任务状态、事件审计、历史查询
