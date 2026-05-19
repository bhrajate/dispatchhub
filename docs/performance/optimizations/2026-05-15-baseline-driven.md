# 基于 baseline-2026-05-15 的性能优化分析

输入：`docs/performance/reports/baseline-2026-05-15.md`、`test/perf/results/2026-05-15/`。

原则：**只针对压测里看得到的瓶颈做优化**，每条建议给"动几行 / 收益预估 / 风险"。

## 一、压测数据画像

| 接口 | 1 VU 服务时间 | 饱和 RPS | 饱和后表现 |
|---|---|---|---|
| HTTP `POST /api/v1/tasks` | ~10 ms (loopback) | ~655 (100-500 VU) | 加并发 → p99 拉到秒级、RPS 不再增 |
| gRPC `SubmitTask` | ~33 ms (loopback) | ~604@100 VU，1000 VU 跌至 401 | 高 VU **倒退**，疑似 k6 客户端 HoL |
| `GET /api/v1/tasks/{id}` (MySQL) | ~12 ms | ~1,600 (100+ VU 后平台) | 1000 VU p99 3 s |
| `GET /queues/{name}/stats` (Redis) | ~7 ms | ~5,600 | 唯一接近线性的接口 |

**关键观察**：

1. 四种网络场景下 HTTP 写路径饱和 RPS 几乎一致（655 / 690 / 655 / 523），证明**瓶颈在服务端 CPU + I/O，而非网络**。
2. 1 VU 写入 10 ms 服务时间里，loopback 网络几乎为 0，所以 10 ms 全部花在：`JSON 解码 → Hooks → MySQL Insert → Redis ZADD/Lua → AfterSubmit log + metric`。
3. apiserver 日志 80 分钟产生 **329 万行**（其中 86.6 万行是 `task submitted: id=...`），即每次写入都强制同步落盘一条带 caller stacktrace 的结构化日志。

## 二、代码层面的实证瓶颈

### 2.1 同步路径上的隐形日志（Tier 1）

`cmd/apiserver/main.go:108-114`

```go
taskSvc.SetAfterSubmit(func(task *entity.Task) {
    metrics.TasksSubmitted.WithLabelValues(...).Inc()
    log.Infof("task submitted: id=%s type=%s queue=%s priority=%d",
        task.ID, task.Type, task.QueueName, task.Priority)
})
```

`pkg/log/logger.go:51`

```go
logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zapcore.ErrorLevel))
```

每次提交都进入 zap.Sugar 的 Infof：fmt.Sprintf 拼字符串 → JSON encode 6 个字段 → `runtime.Caller(skip)` 解析 PC（AddCaller 强制） → write(2) 到 stdout。在 100 VU 档约 650 次/秒，加上 stdout 在容器/重定向下未必带缓冲，I/O 开销直接进同步路径。

**收益估计**：纯日志开销在每条 ~10 µs 量级（zap 文档实测），650 RPS = 6.5 ms/s CPU 等价。看似不大，但**写路径自身只有 10 ms**，10% 的削减直接=10% 的吞吐头部空间。

**改法**（互斥三选一）：

A. **降级到 Debug**（最简单，1 行）：

```go
log.Debugf("task submitted: id=%s type=%s queue=%s priority=%d", ...)
```

生产仍可通过调级别打开。

B. **删掉**（更激进，metric 已经覆盖统计需求）：

```go
taskSvc.SetAfterSubmit(func(task *entity.Task) {
    metrics.TasksSubmitted.WithLabelValues(...).Inc()
})
```

C. **批量异步**：投递到 channel，独立 goroutine 攒 1 ms 一批刷盘。复杂度高，不建议为此一行改动写。

**额外收益**：去掉 `zap.AddCaller()` 同样省 runtime.Caller 调用。如果想保留 caller，可以只在 Error 及以上级别上加（zap 支持 per-level）。

### 2.2 GORM Create 在写路径热点上（Tier 1）

`internal/shared/infrastructure/persistence/mysql/task_repository.go:26-28`

```go
func (s *TaskRepository) Create(ctx context.Context, task *entity.Task) error {
    return s.db.WithContext(ctx).Create(task).Error
}
```

GORM 的 `Create` 走反射构造 INSERT、扫描字段标签、callback 链、autocommit 事务。对 entity.Task 这种字段多（version / labels json / timeout duration）的 struct，单次开销在 50-200 µs 量级。

**改法**：保留 GORM 处理 schema migration，热路径换原生 prepared statement。可以放在 `TaskRepository` 内部缓存：

```go
type TaskRepository struct {
    db          *gorm.DB
    insertStmt  *sql.Stmt   // 启动时一次性 db.DB().Prepare(...)
}

func (s *TaskRepository) Create(ctx context.Context, task *entity.Task) error {
    payload, _ := json.Marshal(task.Payload)
    labels,  _ := json.Marshal(task.Labels)
    _, err := s.insertStmt.ExecContext(ctx,
        task.ID, task.Name, task.Namespace, task.Group, task.Type,
        payload, labels, task.Priority, task.QueueName, task.MaxRetries,
        int64(task.Timeout.Duration), task.State, task.CreatedAt, task.UpdatedAt,
        task.Version)
    return err
}
```

**收益估计**：单次 -100 µs ≈ 写路径 -1%。配合下一条改动收益更大。

### 2.3 MySQL 单连接事务 = 写路径吞吐天花板（Tier 2）

`internal/shared/infrastructure/persistence/mysql/task_repository.go` 每次 Create 是独立 autocommit 事务 → 一次 redo log fsync。在容器化 MySQL 默认 `innodb_flush_log_at_trx_commit=1` 下，每次事务都要等磁盘 fsync 完成。

**两条路**：

A. **降低 fsync 安全等级**（最快，profile 用，不改业务代码）：`innodb_flush_log_at_trx_commit=2` + `sync_binlog=0`，把"每事务 fsync"换成"每秒 fsync"。崩溃风险窗口 1 秒。压测场景下值得；生产看业务需求。

B. **写路径异步化** —— 关键架构决策：

```
现在：HTTP → MySQL Insert (sync) → Redis ZADD → 200 OK
改成：HTTP → Redis ZADD (sync, 含完整 task JSON) → 200 OK
             ↘ 异步 goroutine batch insert MySQL（每 50 条 / 5 ms 一批）
```

语义上 Redis 已经是新的 source-of-truth；MySQL 沦为审计表 + scheduler 失败补偿回查依据。代码已经有"MySQL 失败时 scheduler compensate loop 会重试入队"的设计（见 `task_service_impl.go:96-104` 的注释），反向"Redis 在 / MySQL 还没来"的窗口同样可以由 worker 完成时回写覆盖。

**收益估计**：MySQL fsync 从同步路径剥离，写路径 10 ms → ~3 ms（去掉 fsync ~5-7 ms），饱和 RPS 从 650 → 1500-2000 是合理预期。但**改动面大**，需要设计崩溃恢复细节。

**建议**：先做 A 验证理论上限（profile 用），再决定要不要做 B。

### 2.4 Redis Lua 已经是合理的（Tier 0）

`enqueueWithCapScript` 一次 Lua 把 ZADD + HINCRBY 合并；queue_broker 用 Pipeline。这部分无需改动。loopback 上单次 Lua RTT < 200 µs，不是瓶颈。

### 2.5 HTTP handler 微优化（Tier 3，收益 < 5%）

`internal/apiserver/interfaces/http/server.go:74-132` 用 `json.NewDecoder` 然后逐字段拷贝到 entity.Task；`Timeout` / `Delay` 是字符串再 ParseDuration。

可以一次性改成 entity.Task 的 JSON tag 直接 unmarshal，去掉中间结构。但是当前 wrapper struct 更稳健（不暴露内部字段、抗 schema 演化），收益小、改动大，**不建议作为优先项**。

### 2.6 routeValidator 缓存命中（Tier 0）

`internal/apiserver/domain/service/route_validator.go:39-68` 已经做了 10 s 缓存 + RWMutex；refresh 走 etcd。压测期间 worker 注册稳定，命中率 100%，不是瓶颈。无需改动。

## 三、连接池与资源限制

`test/perf/configs/apiserver.yaml`：

```yaml
mysql:
  max_open_conns: 50      # ⚠️ 500-1000 VU 下不够
  max_idle_conns: 10
redis:
  pool_size: 100          # 够用
```

**MySQL 连接池**：500 VU 时如果每次写入需 ~25 ms（含 fsync），单连接吞吐 1000/25 ≈ 40 RPS，50 连接 × 40 = 2000 RPS 理论上限——和实测的 1,600 单点读平台、655 写入平台基本对得上（写有 fsync 拖累）。

**改法**：调到 200 + 把 MySQL 容器的 `max_connections` 同步上调（默认 151）。但**先做 §2.3 异步化**收益更大；只调连接池只是把瓶颈从"等连接"变成"等 fsync"。

## 四、gRPC 高并发跌倒（Tier 2）

实测数据：

- 100 VU：HTTP 641 RPS / gRPC 604 RPS（差 6%）
- 1000 VU：HTTP 633 RPS / gRPC 401 RPS（**差 36%**，gRPC 倒退 33%）

gRPC server 起在 `internal/apiserver/interfaces/grpc/server.go`，没有显式设 keepalive 或 max_concurrent_streams。**怀疑是两个独立问题之和**：

1. **k6 客户端单连接 HTTP/2 stream HoL**：1000 VU 共享一条 keepalive 连接时，server-side handler 阻塞会让所有 stream 排队。这是客户端问题，但服务端 `MaxConcurrentStreams` 默认 100，超过的 stream 等待，p99 飙高。
2. **goroutine per stream**：1000 stream 在阻塞 fsync 上时，每条占着一个 server goroutine，runtime 调度 + GC 压力上升。

**改法**：

```go
import "google.golang.org/grpc"
import "google.golang.org/grpc/keepalive"

grpcServer := grpc.NewServer(
    grpc.MaxConcurrentStreams(2000),
    grpc.KeepaliveParams(keepalive.ServerParameters{
        Time:    30 * time.Second,
        Timeout: 10 * time.Second,
    }),
)
```

**验证手段**：用 `ghz` 替换 k6 重测。ghz 默认按 connection-pool 模式发请求，能区分"k6 客户端反压"和"服务端真实 gRPC 瓶颈"。如果 ghz 测出 1000 并发 RPS ≈ HTTP 的 600+，就证明跌倒在 k6 客户端，**服务端无需改动**。

## 五、读路径优化（Tier 3）

### 5.1 `GET /api/v1/tasks/{id}`

实测 1,600 RPS 平台。`task_repository.go:30` 的 GORM `First` 同样走反射。对一个高频读、行已经在 InnoDB buffer pool 里的查询，反射开销占比就高了。

**改法**：单点查询用 prepared `SELECT * FROM tasks WHERE id=? LIMIT 1`，预期 2× 提升（→ 3,000+ RPS）。但**优先级低**——读路径生产场景通常前面有 Redis 缓存。

### 5.2 `GET /queues/{name}/stats`

5,600 RPS 已经很高了，不需要优化。如果未来需要更高，可以把 stats 结果 1 秒缓存（业务上完全可以容忍）。

## 六、推荐执行顺序

按"收益/工作量比"排：

| # | 改动 | 改动量 | 预计收益（写路径 RPS） | 风险 |
|---|---|---|---|---|
| 1 | `AfterSubmit` 日志降级到 Debug 或删除（§2.1） | 1-2 行 | +5-10% | 极低 |
| 2 | 调 MySQL `innodb_flush_log_at_trx_commit=2`（§2.3 A） | 配置项 | +50-100% | 1 秒崩溃窗口 |
| 3 | gRPC server 显式设 MaxConcurrentStreams + keepalive（§四） | ~10 行 | gRPC 高 VU 不再倒退 | 低 |
| 4 | MySQL `max_open_conns` 50 → 200，配合容器 `max_connections=500`（§三） | 2 行配置 | +20-30%（仅在解决 §2.3 后） | 低 |
| 5 | GORM `Create` → prepared stmt（§2.2） | ~30 行 | +5-10% | 中（schema 演化时要同步改） |
| 6 | MySQL 异步化 / Redis 优先（§2.3 B） | 一周 | +100-200% | 高，需要崩溃恢复设计 |
| 7 | GORM `First` → prepared stmt（§5.1） | ~10 行 | 读 +50-100% | 低 |

**先做 1-3，做完再跑一次 baseline 对比**——单跑 trim 矩阵 90 分钟，足以验证每条改动的实际增益、避免决策依赖估算。

## 七、不建议做的"优化"

- **加更大的 buffer pool / 改 hash 算法 / 重写 JSON 库**：基线没有数据支持这些方向是瓶颈。
- **拆 apiserver 出多实例**：单实例 650 RPS 远未达 16 vCPU 极限（实际 CPU 应在 200% 以下，待 §五 资源数据补齐后确认），先纵向调优。
- **缓存 task_id → task**：业务上 task 状态会变，缓存失效成本高于一次 MySQL Buffer Pool 命中。

## 八、回归测试

每次改动后跑 `MATRIX=trim DURATION=60s bash test/perf/run.sh`，对比 `test/perf/results/<date>/index.csv` 的关键 cell：

- HTTP submit 100 VU loopback RPS（基线 641）
- HTTP submit 100 VU loopback p99（基线 461 ms）
- HTTP submit 500 VU loopback p99（基线 3420 ms）
- gRPC submit 1000 VU loopback RPS（基线 401，目标修复后接近 HTTP）

`docs/performance/optimizations/` 目录下按 `YYYY-MM-DD-<改动名>.md` 记录，形成"改了什么 → 实测前后差"的可追踪线。
