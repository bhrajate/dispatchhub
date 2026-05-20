# 性能优化路线图

> 这份文档列出从 r1 之后的候选优化点，按"预期收益 / 改动成本 / 依赖前置" 排序，并给出每一项的**判断依据**与**何时不该做**。
>
> **不是承诺要做的事**，是路线图——每条都标注了"是否需要先做某件事/先有某个数据"，避免按表盲做。
>
> 配套：
> - [positioning-and-targets.md](./positioning-and-targets.md) —— 决定哪些项值得做（取决于目标定位）
> - [reports/r1-i5-13500h-2026-05-19.md](./reports/r1-i5-13500h-2026-05-19.md) —— 当前性能基线（所有"预期 RPS"以此为起点）
> - [optimizations/r1-postmortem-2026-05-19-write-path.md](./optimizations/r1-postmortem-2026-05-19-write-path.md) —— r1 优化的方法论（profile + A/B），下面每项都应按同样流程验证
> - [../playbooks/bottleneck-investigation.md](../playbooks/bottleneck-investigation.md) —— 优化执行的标准流程

## 当前位置（2026-05-19, r2）

```
HTTP 写路径 (POST /api/v1/tasks):
  100 RPS  PASS  p99=48ms   CPU=15%
  300 RPS  PASS  p99=142ms  CPU=60%   ← r2 后首次通过
  600 RPS  FAIL  actual=429 p99=18s   CPU=78%   errors=0%   ← 下一个目标
  1000 RPS FAIL  actual=392 p99=36s   CPU=69%   errors=0%

读路径: 1k RPS 全 PASS，未压顶
```

r1 → r2 关键变化：300 RPS 阶梯 errors 从 10.9% 降到 0%、verdict 从 FAIL 翻到 PASS。**下一刀的判断标准变了**：现在 600 RPS errors 已经是 0%，瓶颈纯粹是"actual_rps 不达标"——单核 CPU 处理速度限制，需要重新抓 profile 找新热点。

## 一、必修优化（影响正确性 / 可观测性）

> 不修就上生产会出事，与 RPS 数字无关。

### F1. 写路径每请求一行 INFO 日志的代价

**位置**：`cmd/apiserver/main.go:112` `log.Infof("task submitted: id=%s ...")`

**问题**：当前用 zap sugared logger + JSON encoder + **无 buffer 的 stdout sink**。
profile 显示这条同步路径占 SubmitTask 9% CPU，量级不大；但**生产规模下危险**：

- 1k RPS = 每秒 1k 行 JSON + 1k 次 `write(2)` syscall
- 容器 stdout 经常是 pipe 到 docker daemon 的日志 driver；driver buffer 满时反向阻塞 apiserver
- 这条日志的内容（task ID + type + queue + priority）已经在 response body 里返回，**运维上可被 access log 替代**

**改法**（任选）：
- a. 改 `log.Debug` 直接降级（最小改动）
- b. 删掉，依赖 access log（最干净，需要先有 access log）
- c. 改 zap 结构化字段 + 添加 `bufio.Writer` 包装 stdout（约 20 行）

**预期收益**：300 RPS errors 不会因这个改善（不是当前瓶颈），但**避免生产被日志 IO 拖死**。

**做的前置条件**：option b/c 都需要先确认运维侧用的日志方案。option a 无依赖。

**何时不该做**：当前 dev 环境内永远不会触发 buffer 阻塞，可以推迟。但**上线前必须做某种降级**。

---

### F2. 写路径 300 RPS errors 来源未定位 ✅ 已完成（r2）

**结论**：100% 是 `database/sql` 的 `driver: bad connection`，根因是 `MaxIdleConns=10` 远低于 `MaxOpenConns=50` 导致连接 churn + TCP TIME_WAIT 堆积。详见 [r2 postmortem](./optimizations/r2-postmortem-pool-tuning-2026-05-19.md)。

修复见下面 D。

---

## 二、容量类优化（直接提 RPS）

### B. MySQL INSERT + Redis Enqueue 并发执行

**位置**：`internal/apiserver/domain/service/task_service_impl.go:92-104`

**问题**：当前 SubmitTask 串行执行 `taskStore.Create` → `broker.Enqueue`，两次同步 IO 互不依赖（Redis 不依赖 MySQL 的返回值），但被串起来。

**改法**：用 `errgroup` 并发执行：
```go
g, gctx := errgroup.WithContext(ctx)
g.Go(func() error { return s.taskStore.Create(gctx, task) })
g.Go(func() error { return s.broker.Enqueue(gctx, task.QueueName, task) })
err := g.Wait()
```

**预期收益**（基于 [r2 profile](./optimizations/r2-profile-analysis-2026-05-19.md)）：MySQL 8.54s / Redis 3.95s 串行总和占 SubmitTask 84%。并发后理论最低 = max(MySQL, Redis) = 8.54s。
**单请求时延 −22%**，对应 RPS **+20-30%**。比 r1 时估的 +15-20% 更高，原因：r1 时 Redis 占比仅 17%，r2 后升到 27%（MySQL 那部分被事务优化吃掉了）。

**做的前置条件**：
- 必须先想清楚**语义变化**：当前是"MySQL 成功 → 尝试 Redis（失败靠 scheduler 30s 内补偿）"。并发后两路同时跑：
  - 如果 MySQL 失败、Redis 成功：现在是直接 return，并发后会留一条 Redis 队列里"找不到对应 MySQL 行"的孤儿任务。worker 拿到后会怎样？需要看 worker 代码。
  - 如果 MySQL 成功、Redis 失败：与当前行为相同（fail-silent + scheduler 补偿）。
- 看 [analysis/task-submit-dual-write-consistency.md](../analysis/task-submit-dual-write-consistency.md) 里对双写一致性的设计是否覆盖了这个并发场景。

**何时不该做**：如果一致性分析显示并发后语义变化不可接受，跳过这一项；改去做 D（连接池调优）。

**预计工作量**：30 分钟代码改动，2 小时一致性 review，1 小时压测验证。

---

### B'. Redis Enqueue 的 json.Marshal 优化

**位置**：`internal/shared/infrastructure/persistence/redis/queue_broker.go:61`

**问题**：r2 profile 显示 `json.Marshal(task)` 占 1.23s（SubmitTask 总时间的 8%）。
`entity.Task` 字段较多（metadata、payload、retry 配置等），每次 enqueue 全量序列化。

**改法**（任选）：
- a. 减字段：worker 真正需要的字段只是 ID + payload + 路由信息，可定义一个 narrow DTO 只 marshal 必要字段
- b. 用 `easyjson` 或 `json-iterator` 替代 `encoding/json`
- c. `sync.Pool` 复用 buffer

**预期收益**：序列化时间 −50%，对应 RPS **+4-5%**。

**做的前置条件**：B 完成后再做，否则会被 B 的并发 IO 收益吞没。

**何时不该做**：B 之后 profile 重抓如果 json.Marshal 占比下降（因为时延变短），收益相应下降。

---

### C. 异步化 afterSubmit hook（指标 + 日志）

**位置**：`cmd/apiserver/main.go:108-114`

**问题**：当前 `afterSubmit` 在请求 goroutine 同步执行 metrics.Inc + log.Infof。
profile 显示 1.37s / 14.93s = 9%。

**改法**：channel + 单 worker goroutine 消费。或更简单——只把日志去掉（即 F1 的 b 选项）。

**预期收益**：单请求耗 CPU 时间 −9%，对应 RPS **+5-10%**。

**做的前置条件**：F1（如果选 F1.b 删日志，C 收益就消失了）。**先做 F1 再决定 C 是否仍需要**。

**何时不该做**：F1 选了 b（删日志），C 自动失效。

---

### D. MySQL 连接池调优 ✅ 已完成（r2）

**改动**：`max_idle_conns: 10 → 50`（与 max_open_conns 同），新增 `conn_max_idle_time: 5m`。

**实测**（300 RPS 阶梯）：
- errors: **10.9% → 0%**（verdict FAIL → PASS）
- p99: **2.2s → 142ms**（−94%）
- CPU: 90% → 60%（retry 路径释放出来的 CPU）

详见 [r2 报告](./reports/r2-i5-13500h-2026-05-19.md)。

**剩余建议**：生产 `MaxOpenConns` 应根据 MySQL 实例的 `max_connections` 与 apiserver 副本数调优。当前 50 的设置假设 ≤ 5 个 apiserver 副本（共 250 连接）；副本数更多时需要相应调整。

---

### E. Redis 连接池调优

**位置**：`test/perf/configs/apiserver.yaml:11`

**问题**：当前 `pool_size: 100`、`min_idle_conns: 10`。go-redis 默认每个 host 50 个连接，对 1k RPS 一般够。

**改法**：观察 `go_redis_pool_*` metrics（如已暴露）；如未暴露，先添加。再决定调不调。

**预期收益**：通常不是瓶颈；**只在 F2 显示是 Redis 连接竞争时才动**。

**做的前置条件**：F2 + `/metrics` 里有 redis pool 相关指标。

---

## 三、稳定性优化（影响 p99 / 抖动）

### G. RouteValidator 加 singleflight

**位置**：`internal/apiserver/domain/service/route_validator.go:39-49`

**问题**：cache TTL 10s，stale 时**多个并发请求各自调一次 etcd**（thundering herd）。profile 显示这一项只占 0.5% CPU，**不是 RPS 瓶颈**，但每 10s 制造一次 etcd 群轰，是 p99 抖动来源。

**改法**：
```go
import "golang.org/x/sync/singleflight"

type RouteValidator struct {
    sf singleflight.Group
    // ...
}

func (v *RouteValidator) Validate(...) error {
    // ...
    if stale {
        v.sf.Do("refresh", func() (interface{}, error) {
            return nil, v.refresh(ctx)
        })
    }
    // ...
}
```

**预期收益**：稳态 RPS 不变，**p99 抖动收敛**，每 10s 那次毛刺消失。

**做的前置条件**：无。

**预计工作量**：10 分钟代码 + 1 小时压测验证。

---

### H. RouteValidator 改后台周期 refresh

**位置**：同 G

**问题**：进一步优化——写路径完全不应触发 etcd 调用。启动时起一个后台 goroutine 周期 refresh，写路径只 RLock 读 cache。

**改法**：在 `NewRouteValidator` 起 `go v.refreshLoop(ctx, ticker)`，写路径保留只读 + fail-open 语义。

**预期收益**：与 G 类似，但更彻底；写路径完全去掉 etcd 调用机会。

**做的前置条件**：G（增量演进；或直接做 H 跳过 G）。

**何时不该做**：G 已足够时跳过 H。

---

### I. 给写路径加超时 / 熔断

**位置**：`task_service_impl.go::SubmitTask`

**问题**：当前 SubmitTask 没有自己的超时，依赖 client 的 ctx。Redis Enqueue 失败也是 silent ignore（依赖 scheduler 30s 内补偿）。在 Redis hang 时单请求可以拖到 client 侧 timeout（30s+），占着 goroutine。

**改法**：
- 给整个 SubmitTask 加 5s timeout（`context.WithTimeout`）
- Redis Enqueue 错误率 > 阈值时短期跳过（circuit breaker），让 scheduler 兜底

**预期收益**：饱和段 errors 类型从"客户端 timeout"变成"快速 5xx"，**业务体验更好**（5xx 可以快速重试，timeout 客户端会卡）。**RPS 数字可能反而下降**，但**业务可用性提升**。

**做的前置条件**：F2（先确认现在 timeout 现象是否严重）。

---

## 四、架构类优化（数量级提升的前提）

> 下面这些都是"做了能让 RPS 上一个量级，但改动大、风险高"。**只有在业务峰值真的需要才值得做**。参 [positioning-and-targets.md](./positioning-and-targets.md) §五。

### J. 写路径 batch 化

**问题**：当前每请求一次 `INSERT` + 一次 `ZADD`。MySQL `INSERT INTO ... VALUES (...), (...)` 与 Redis pipeline 都能 batch。

**改法**：apiserver 内开 ring buffer，每 5ms / 100 条 flush 一次。

**预期收益**：吞吐 **3-10x**（取决于 batch size），**单请求 p99 增加 ~5ms**（等待 batch 关闭）。

**做的前置条件**：业务能接受 +5ms 提交确认延迟（大多数异步任务都能）。

**何时不该做**：业务对单任务确认延迟敏感（< 5ms 要求）；或当前架构演进期，不想引入 batch 复杂度。

---

### K. apiserver 多副本横向扩

**问题**：单核 CPU bound 架构，单实例上限大约就是当前优化能逼近的极限。

**改法**：
- 部署多个 apiserver 副本，前面挂负载均衡（k8s service / nginx / envoy）
- worker 注册 / RouteValidator cache 通过 etcd 已经是共享的，无需改造
- MySQL / Redis 连接池每副本独立，注意总连接数不要超过后端上限

**预期收益**：3 副本下集群 RPS ≈ 3x 单实例（线性度需要测）。

**做的前置条件**：
- 单实例 RPS 已经稳定（先做完 F1 / F2 / B）
- 部署侧可以跑多副本（k8s 或 docker-compose 改造）
- 一份"多副本压测" cell 在 `test/perf/run.sh` 里

**何时不该做**：业务峰值 < 100 RPS 时，单副本已够。

---

### L. 持久化层分离（写直通 Redis，MySQL 异步落盘）

**问题**：当前 SubmitTask 先 MySQL 后 Redis；MySQL 是同步 IO 主导。

**改法**：
- 写路径只 ZADD 到 Redis + 同步写本地 WAL
- 后台 worker 从 WAL 异步刷到 MySQL
- 读路径如果命中 hot cache（Redis）则不查 MySQL

**预期收益**：写路径吞吐 **5-10x**（Redis 单实例 50k+ ops/s vs MySQL 几 k）。

**做的前置条件**：
- 一致性模型变更（apiserver crash 时 WAL 丢失会丢任务），需要明确 SLO
- 这是**架构级改造**，不是优化，1-2 个月工作量
- 与 [analysis/task-submit-dual-write-consistency.md](../analysis/task-submit-dual-write-consistency.md) 的设计要重新对齐

**何时不该做**：单实例 + 多副本横向（K）已够业务需求。

---

## 五、推荐执行顺序（r2 后修订）

按"低风险高回报先做"：

```
✅ F2 (找 errors 根因)            r2 完成
✅ D  (连接池调优)                r2 完成 — 300 RPS PASS
✅ 重新抓 CPU profile @ 600 RPS   见 optimizations/r2-profile-analysis-2026-05-19.md
                                    结论: MySQL 58% / Redis 27% / log 9%；IO bound
  ↓
B  (并发 IO)         ← 当前位置；profile 显示串行两路 IO 占 84%，并发后预期 +20-30%
  ↓
F1 (改 INFO log) ─┐
                  ├→ 重新 baseline
G  (singleflight) ┘
  ↓
重新 profile + A/B
  ↓
[根据 profile 顶部] C (异步 hook) / I (超时)
  ↓
multi-instance 验证 (K)
  ↓
[业务峰值需要时] J (batch) / L (架构改造)
```

每一刀都按 [playbook](../playbooks/bottleneck-investigation.md) 流程：**改 → 同矩阵重测 → 对比 actual / p99 / CPU% / errors → 写一份 `reports/r{n}-i5-13500h-{date}.md`**。

## 六、什么情况下要重看这份文档

- 业务实际峰值 QPS 估算变化（见 positioning §五）
- 重新 profile 后 top 函数变了（之前的 ROI 估算失效）
- 新增了 r2、r3 等优化轮次（更新当前位置）
- 引入新依赖（比如 Kafka 替代 Redis 队列）——很多假设要重做

## 七、不在路线图上的事

- **去掉 zap，换其他日志库**：日志库不是瓶颈，零和游戏。
- **改 Go 版本 / 升级 GORM**：看不到明确收益证据。
- **加 HTTP/2 / gRPC streaming**：当前请求都是单次小请求，HTTP/1.1 keep-alive 足够。
- **预先优化"将来可能成为瓶颈"的地方**：见 playbook §0 一句话原则——profile 没显示之前不动。
