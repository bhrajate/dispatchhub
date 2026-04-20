# DispatchHub 简历版项目描述

> 用于直接复制到简历中的不同版本（精简 / 标准 / 详细），可根据目标 JD 裁剪。

---

## 版本 A：一句话超精简版（适合简历项目列表首行）

**DispatchHub · 云原生分布式任务调度系统**（Go 1.25 / Redis / etcd / MySQL / K8s）
对标 Asynq / Sidekiq / Celery，面向日均百万级异步任务场景，基于 Redis Sorted Set + Lua 脚本实现优先级队列，etcd Leader 选举保证 Scheduler 高可用，单机 10 万+ QPS。

---

## 版本 B：标准简历版（推荐，6–8 行，核心项目位）

### DispatchHub — 分布式任务调度系统（个人项目 / 核心开发）
**技术栈**：Go 1.25 · gRPC · Redis 7 · etcd 3.5 · MySQL 8 · Prometheus · Docker · Kubernetes + Helm + HPA

- **架构设计**：借鉴 K8s 控制面 / 数据面分离思想，拆分 API Server（无状态 HTTP/gRPC 网关）、Scheduler（Leader 选举单活 + 7 个 reconciliation 循环）、Worker（拉模型 + 信号量背压）三个独立可部署微服务，通过 Redis / etcd / MySQL 协调，彻底解耦。
- **核心队列**：基于 Redis Sorted Set + Lua 原子脚本自研优先级队列，解决 Kafka 无法插队、RabbitMQ 吞吐不足、MySQL 轮询锁竞争等痛点，支持 O(log N) 优先级排序与延迟任务，**单机 10 万+ QPS**。
- **高可用**：基于 etcd `concurrency.Election` 实现 Scheduler Leader 选举（Lease TTL 15s），Standby 秒级接管；Worker 支持 HPA 3–50 副本动态扩缩容；双写最终一致性通过 30s 周期补偿循环 + Lua 幂等重入队实现。
- **DDD 分层重构**：按 `Interfaces → Application → Domain ← Infrastructure` 依赖方向组织代码，Shared 模块统一三服务的 Entity / Repository 接口，依赖倒置，零代码重复。
- **关键 bug 修复**：定位并修复 2 个 Leader 选举竞态（goroutine 泄漏 + defer LIFO 死锁）、派生状态不一致、Queue-Type 路由漏校验等分布式并发问题。
- **工程化**：Docker 多阶段构建（CGO_ENABLED=0，最终镜像 < 50MB）、ldflags 版本注入、Helm Chart 可配置化部署、`go test -race` 全量竞态检测、Prometheus 全链路指标。

---

## 版本 C：详细版（适合项目经验展开页 / 个人主页）

### DispatchHub — 云原生分布式任务调度系统

**项目定位**：对标 Asynq / Sidekiq / Celery，面向日均百万级异步任务（交易对账、营销通知、报表导出、定时 Cron 等）场景，解决主流方案在"优先级插队 + 高并发 + 高可用"三重约束下的痛点。

**技术栈**：Go 1.25、gRPC + Protobuf、原生 net/http、GORM、Uber Zap、Prometheus Client、Redis 7（Sorted Set + Lua）、etcd 3.5、MySQL 8、Docker 多阶段构建、Kubernetes + Helm + HPA、Grafana。

**我的职责**：从 0 到 1 的架构设计与核心模块实现，涵盖队列选型、分布式一致性、并发安全、DDD 分层、容器化部署与可观测性建设。

#### 核心贡献

1. **技术选型**：系统对比 Kafka / RocketMQ / RabbitMQ / MySQL 轮询四种方案，发现均无法同时满足优先级插队 + 10 万 QPS + 低延迟，最终自研基于 **Redis Sorted Set + Lua 原子脚本** 的优先级队列，通过 `ZPOPMIN + HSET` 单条 Lua 保证出队与 inflight 登记的原子性，天然规避分布式锁。

2. **控制面 / 数据面分离架构**：
   - API Server：无状态 HTTP/gRPC 网关，Deployment + Ingress 接入，支持任意水平扩展；
   - Scheduler：StatefulSet 部署 3 副本，基于 etcd 选举单活，运行延迟晋升、补偿扫描、CronJob 触发、清理回收等 7 个 reconciliation 循环；
   - Worker：Deployment + HPA（3–50），拉模型 + channel 信号量背压控制并发，Handler 插件式注册。

3. **最终一致性设计**：MySQL 持久化 + Redis 入队双写非原子，通过 Scheduler 30s 周期扫描 `Pending` 且 `updated_at` 早于阈值的任务，调用 `EnqueueIfNotInflight` Lua 脚本做幂等重入队，正常路径零开销。

4. **分布式并发 bug 修复**（体现对 goroutine 生命周期、defer 执行顺序、context 传播的深度理解）：
   - **选举 Race Condition**：`session` / `election` 作为结构体字段被 `campaign` 与 `observe` 无锁读写 → 改为局部变量 + 参数传递；
   - **Defer LIFO 死锁**：`defer wg.Wait()` 注册早于 `defer observeCancel()`，etcd session 过期时导致 Scheduler 永久卡死 → 交换 defer 注册顺序，利用 LIFO 保证 cancel 先于 wait；
   - **派生状态不一致**：`queues` 作为 `workers` 的派生集合，多处同步维护导致遗漏 → 消除冗余状态，改为实时计算（worker 数量通常个位数，CPU 开销可忽略）；
   - **Queue-Type 路由漏校验**：任务 type 与队列 worker 不匹配时静默失败 → API Server 层接入 `RouteValidator`，从 etcd 构建 `queue → {types}` 映射做 10s 缓存 + fail-open 策略。

5. **DDD 分层重构**：按 `Interfaces → Application → Domain ← Infrastructure` 组织代码，Domain 层零外部依赖，Repository 接口在 Domain 层定义、Infrastructure 层实现，依赖倒置；`internal/shared` 统一三服务共享的 Entity、Repository、Infra 实现，避免代码重复。

6. **工程化建设**：
   - Docker 多阶段构建，`golang:1.25-alpine` → `alpine:3.19`，`CGO_ENABLED=0` 静态链接，最终镜像 < 50MB；
   - 通过 `--build-arg` + `-ldflags` 注入 `VERSION / GIT_COMMIT / BUILD_DATE`；
   - Helm Chart 可配置化部署，values.yaml 控制副本数、资源限额、HPA 阈值；
   - 三个服务独立配置文件（`apiserver.yaml` / `scheduler.yaml` / `worker.yaml`），支持环境变量覆盖；
   - `go test -race` 全量竞态检测 + Repository 接口化 mock + Lua 脚本沙箱语义测试；
   - Prometheus 全链路指标（队列深度、吞吐、active tasks、执行耗时）+ `/healthz`、`/readyz` 健康检查。

#### 项目成果

- 单机 10 万+ QPS，Scheduler 故障 15s 内自动接管，Worker HPA 弹性 3–50 副本；
- 三个独立可部署微服务，代码结构清晰反映 DDD 分层；
- 18 份技术文档覆盖架构、队列选型、存储选型、API、部署、故障修复复盘等；
- 积累了对分布式一致性（最终一致性 / 幂等 / 补偿）、Go 并发原语（goroutine / context / defer）、etcd / Redis 深度使用的实战经验。

---

## 简历关键词汇总（ATS 友好，可按 JD 筛选）

**语言 / 框架**：Go、Golang、gRPC、Protocol Buffers、GORM、net/http、Zap

**分布式 / 中间件**：Redis、Sorted Set、Lua 脚本、etcd、Leader Election、Lease、Watch、MySQL、乐观锁、最终一致性、幂等性、补偿机制、背压、心跳、服务发现

**架构 / 方法**：DDD、领域驱动设计、微服务、控制面 / 数据面分离、CQRS（可选强调）、依赖倒置、仓储模式

**云原生 / DevOps**：Docker、多阶段构建、Kubernetes、Helm、HPA、StatefulSet、Deployment、Ingress、Prometheus、Grafana、可观测性

**并发 / 性能**：goroutine、channel、context、sync、race detector、信号量、优先级调度、O(log N)

---

## 填入建议

- **校招 / 实习简历**：选 **版本 B**，保留 6 条要点，突出"自研优先级队列 + 单机 10 万 QPS + bug 修复"这三个最有记忆点的内容。
- **社招 3 年以下**：选 **版本 B** + 面试时用 `docs/project-introduction.md` 的 STAR 故事展开。
- **社招 3 年以上 / Go 架构岗**：选 **版本 C**，强调 DDD 重构、etcd 竞态修复、最终一致性设计等体现系统思考的点。
- **非 Go 岗位（如通用后端）**：只保留 **版本 A** + 版本 B 的前 3 条即可，避免过多技术细节喧宾夺主。

---

## 面试预热清单（简历交付前自检）

- [ ] 能画出 README 里的完整架构图（凭记忆）
- [ ] 能手写 `dequeueScript` 的伪代码并解释为何必须放在 Lua 里
- [ ] 能讲清楚 Leader 选举两次竞态的根因与修复
- [ ] 能解释为什么不选 Kafka / RabbitMQ / MySQL 做队列
- [ ] 能说明 MySQL-Redis 双写补偿为什么不改 `version` 字段
- [ ] 能说出 HPA 弹性扩缩容的自定义指标是什么
- [ ] 能讲清楚 DDD 分层里 Domain 层为什么要零外部依赖
