# 写路径异步化：设计与权衡

承接 `2026-05-15-baseline-driven.md` §2.3 B 的简述，这里展开"写路径异步化"的完整设计。

一句话：**把"写 MySQL"从用户请求的同步路径里挪出来，让客户端只等 Redis 入队就返回**。

## 一、现在的写路径（同步双写）

```
HTTP 请求 ───┐
             ▼
    apiserver.SubmitTask
             │
             ├─ 1. MySQL INSERT (sync, ~5-8 ms 含 fsync)
             │      ↓ 等 fsync 完
             ├─ 2. Redis ZADD via Lua  (~0.5 ms)
             │      ↓
             └─ 3. 200 OK 返回客户端
```

客户端要等两个 I/O 都完成。MySQL fsync 是同步路径上最慢的一步，**直接决定吞吐上限**（实测 ~650 RPS）。

`internal/apiserver/domain/service/task_service_impl.go:92-104` 的代码就是这个顺序：先 `taskStore.Create`，再 `broker.Enqueue`。

## 二、改造后（Redis 优先 + MySQL 异步落盘）

```
HTTP 请求 ───┐
             ▼
    apiserver.SubmitTask
             │
             ├─ 1. Redis ZADD via Lua  (~0.5 ms, sync)
             │      [task 完整 JSON 写进 Redis 队列]
             │      ↓
             ├─ 2. push 到内存 channel  (~1 µs, 非阻塞)
             │      ↓
             └─ 3. 200 OK 返回客户端  (端到端 ~3 ms)

后台独立 goroutine ────────────────────────────────────
    for batch := range channel:
        攒满 50 条 / 等到 5 ms 超时
        ↓
        MySQL 一次 batch INSERT（多行 VALUES，1 次 fsync 摊给 50 条）
```

**改动核心三点**：

1. **顺序翻转**：Redis ZADD 先做，MySQL Insert 后做。
2. **MySQL 写入移出请求路径**：变成"内存队列 → 后台批量 flush"。
3. **批量摊销 fsync**：50 条一批 → 单条 fsync 成本从 5 ms 摊到 0.1 ms。

## 三、为什么语义上行得通

DispatchHub 的设计里 **Redis 已经是任务的实际工作 source-of-truth**——scheduler 和 worker 全部通过 Redis 队列流转，MySQL 只在以下场景用：

- 列表查询 `GET /api/v1/tasks`
- 单条状态查询 `GET /api/v1/tasks/{id}`
- scheduler 的 compensate 兜底（task 在 inflight 太久没人完成 → 从 MySQL 回查 → 重新入队）
- 审计 / 历史记录

**核心洞察**：客户端拿到 200 OK 后，任务马上就被 worker 消费了——这条链路全程不依赖 MySQL。MySQL 在这里更像"延迟写入的审计表"，而非"数据落地的唯一真相"。

## 四、需要处理的边界情况

异步化最大的风险是 **Redis 在了 / MySQL 还没来** 的时间窗口（几毫秒到几秒）：

| 场景 | 现在的行为 | 异步化后 |
|---|---|---|
| 客户端立刻 `GET /tasks/{id}` 查询 | MySQL 命中（同步写过了） | **可能 404**（MySQL 还没 flush） |
| Worker 完成任务回写 MySQL `state=completed` | MySQL 行已存在，Update 即可 | **行还不存在**，Update 会失败 |
| apiserver 进程崩溃 | Redis 在 / MySQL 在，安全 | 内存 channel 里没 flush 的丢失 |

对应处理方案：

1. **`GET /tasks/{id}` 查不到时回查 Redis**：Redis ready/inflight key 里就有完整 task JSON，作为 fallback 数据源。
2. **Worker Update 失败 → 退化为 Insert**：用 `INSERT ... ON DUPLICATE KEY UPDATE` 即可，本来就是幂等写。
3. **apiserver 崩溃丢失内存 channel**：因为 Redis 已经有了（步骤 1 是 sync），调度照常进行。受影响的只有 MySQL 审计记录的完整性——可以通过"scheduler compensate loop 反向扫 Redis 同步到 MySQL"修复，或接受小概率丢审计。

## 五、实际代码改动量

骨架大概这样（伪代码，~150 行）：

```go
type TaskServiceImpl struct {
    // ...原有字段...
    insertCh chan *entity.Task    // 缓冲 1024
}

func (s *TaskServiceImpl) SubmitTask(ctx, task) error {
    // ...校验、填默认值...
    if err := s.broker.Enqueue(ctx, task.QueueName, task); err != nil {
        return err  // Redis 失败才真失败
    }
    select {
    case s.insertCh <- task:
    default:
        // channel 满 → 同步退化（保留兜底）
        return s.taskStore.Create(ctx, task)
    }
    return nil
}

// 启动时跑一个 goroutine
func (s *TaskServiceImpl) flushLoop() {
    batch := make([]*entity.Task, 0, 50)
    ticker := time.NewTicker(5 * time.Millisecond)
    for {
        select {
        case t := <-s.insertCh:
            batch = append(batch, t)
            if len(batch) >= 50 {
                s.taskStore.BatchCreate(ctx, batch)
                batch = batch[:0]
            }
        case <-ticker.C:
            if len(batch) > 0 {
                s.taskStore.BatchCreate(ctx, batch)
                batch = batch[:0]
            }
        }
    }
}
```

`taskStore` 加一个 `BatchCreate(ctx, []*Task) error`：一条 SQL `INSERT INTO tasks (...) VALUES (?, ?, ...), (?, ?, ...), ...`。

## 六、收益估算

写路径同步部分：原 ~10 ms → ~3 ms（去掉 fsync 5-7 ms + GORM 反射 100 µs）。

饱和 RPS：650 → 预计 **1500-2000**。再做完连接池调整 + gRPC keepalive，~2500 RPS 是合理目标。

## 七、为什么不要先做这个

写路径异步化是工作量最大、风险最高的方案（一周 + 崩溃恢复设计）。**先做 §2.3 A**（一行 MySQL 配置 `innodb_flush_log_at_trx_commit=2`）就能拿到 50-100% 收益，并**用实测数据回答"瓶颈到底是不是 fsync"**：

- 改一行配置，跑一次 trim 矩阵（90 min）
- 如果 RPS 从 650 → 1500：fsync 是大头确认无误，**那时再决定**要不要花一周做异步化拿剩下的部分
- 如果 RPS 只到 700-800：fsync 不是大头，异步化也救不了多少，转去做日志 + GORM 优化

异步化的价值在于"把那一秒丢失风险也消除"——崩溃时 `flush_log_at_trx_commit=2` 会丢最近 1 秒，异步化崩溃只丢内存 channel 里没 flush 的部分（也是 ~毫秒级，但语义更可控，因为 Redis 仍是真相）。是否值这一周的工作量，要看做完 fsync=2 后的实际数字。

## 八、决策路径

```
                    跑 trim 矩阵（基线 650 RPS）
                                │
                fsync=2 改成"每秒 fsync"
                                │
                       再跑一次 trim
                                │
                ┌───────────────┴───────────────┐
                ▼                               ▼
           RPS → 1500+                     RPS → 700-800
                │                               │
        瓶颈确实在 fsync                   瓶颈不在 fsync
                │                               │
        是否做完整异步化？               转去做日志降级 + GORM
                │                            prepared stmt
        ┌───────┴───────┐
        ▼               ▼
    生产能接受          生产要求
    1 秒丢失风险？      "0 丢失"，
        │               必须做异步化
        ▼               + Redis 兜底回查
    停在 fsync=2，
    收益已经够好
```

## 九、回归验证清单

异步化做完后必须验证的几个场景（除了常规吞吐 / p99）：

- **崩溃恢复**：apiserver 启动时主动扫 Redis 所有 ready / delayed / inflight key，对每个 task ID 做 `INSERT ... ON DUPLICATE KEY UPDATE`，把丢失的内存 channel 数据补回 MySQL。
- **Worker 完成时的 Update 退化**：现有 `taskStore.Update` 在行不存在时返回 "optimistic lock conflict"（见 `task_repository.go:54-56`），需要改成 upsert 或在 Update 失败时退化为 Insert。
- **GET /tasks/{id} 的回查**：增加单元测试覆盖"提交后 1 ms 内查询"的场景，确认走 Redis fallback 而非返回 404。
- **审计完整性**：跑 1 小时压测后比对"提交总数（metric）"与"MySQL 行数"，差值应在 channel 缓冲量级（1024 以内）。
