# Queue-Type 路由校验

> 日期：2026-04-17

## 一、问题描述

### 1.1 Queue 与 Type 之间缺乏约束

DispatchHub 的调度模型中，Queue（队列）和 Type（任务类型）是两个正交维度：

- **Queue** 决定任务被哪些 Worker 消费（运维调度维度）
- **Type** 决定 Worker 内部使用哪个 Handler 处理（业务逻辑维度）

这种解耦设计是合理的——同一 Type 可以走不同优先级的 Queue，同一 Queue 可以承载多种 Type。但问题在于：**提交任务时没有校验目标 Queue 上是否存在能处理该 Type 的 Worker**。

### 1.2 故障场景

当 `video.transcode` 类型的任务被投递到 `email-queue` 队列，而消费 `email-queue` 的 Worker 只注册了 `email.send` Handler 时：

```
Client: SubmitTask(type="video.transcode", queue="email-queue")
    ↓
API Server: MySQL.Create() ✓, Redis.Enqueue("email-queue") ✓  -- 提交成功
    ↓
Worker (queues: ["email-queue"], handlers: {"email.send": ...}):
    Dequeue() → task{type="video.transcode"}
    handlers["video.transcode"] → nil
    → "no handler for type: video.transcode" → 标记失败
    → 重试 3 次后永久 Failed
```

**后果**：

1. 任务**静默失败**——客户端收到提交成功响应，但任务永远不会被正确执行
2. **浪费重试次数**——每次重试仍然被同一个没有 Handler 的 Worker 消费，结果相同
3. **消耗队列吞吐**——无效任务占用出队/入队资源，影响同队列中其他有效任务

### 1.3 根因分析

- `WorkerInfo` 只上报 `Queues`，不上报 `TaskTypes`，系统无法得知每个 Worker 能处理什么类型
- `TaskServiceImpl.SubmitTask()` 只做限流检查，不做路由可行性检查
- API Server 不连接 etcd，无法获取 Worker 拓扑信息

## 二、设计动机

### 2.1 为什么 Queue 和 Type 是正交解耦的？

这不是设计缺陷，而是刻意为之。所有主流任务系统（Celery / Sidekiq / Asynq / Bull）都采用同样的模式。原因：

| 场景 | 需要正交解耦的理由 |
|------|-------------------|
| **同 Type 不同优先级** | `email.send` 的密码重置走 `critical` 队列，营销邮件走 `default` 队列 |
| **不同 Type 共享资源池** | CPU 密集型任务 (`report.generate`, `video.transcode`) 共享高配 Worker 池 |
| **弹性伸缩** | 运维按队列扩容，不需要理解业务的 Type 分类 |
| **多租户隔离** | 同一 Type 在不同租户队列中，按队列做限流和资源隔离 |

### 2.2 缺失的是什么？

正交解耦没有错，缺失的是 **提交时的可行性校验**——在任务进入队列前，检查目标 Queue 上是否有能力处理该 Type 的 Worker。

类比：Kubernetes 允许 Pod 调度到任何 NodePool（正交解耦），但 kube-scheduler 会检查 nodeSelector / taints / tolerations 确保 Pod 能在目标节点上运行。

## 三、修复方案

### 3.1 WorkerInfo 增加 TaskTypes 字段

**文件：** `internal/shared/domain/entity/worker.go`

```go
type WorkerInfo struct {
    // ...existing fields...
    Queues         []string          `json:"queues"`
    TaskTypes      []string          `json:"task_types,omitempty"` // 新增
    Concurrency    int               `json:"concurrency"`
    // ...
}
```

Worker 注册到 etcd 时，同时上报自己支持的任务类型列表。由于 `WorkerInfo` 通过 JSON 序列化存储在 etcd 中，新增字段带 `omitempty` tag，**向后兼容**——旧版 Worker 的 JSON 不包含该字段，反序列化后为 nil slice。

### 3.2 Worker 启动时自动填充 TaskTypes

**文件：** `internal/worker/application/service/worker_app_service.go`

```go
func (w *WorkerAppService) Run(ctx context.Context) error {
    // 从已注册的 handlers map 收集所有 task type
    w.mu.RLock()
    taskTypes := make([]string, 0, len(w.handlers))
    for t := range w.handlers {
        taskTypes = append(taskTypes, t)
    }
    w.mu.RUnlock()

    w.info = &entity.WorkerInfo{
        // ...
        Queues:    w.cfg.Queues,
        TaskTypes: taskTypes,   // 新增
        // ...
    }
    // ...
}
```

Handler 注册发生在 `Run()` 之前（`cmd/worker/main.go` 中先 `RegisterFunc` 再 `Run`），因此此时 handlers map 已经是完整的。TaskTypes 随 Register/Heartbeat 写入 etcd，Scheduler 和 API Server 均可读取。

### 3.3 创建 RouteValidator 路由校验器

**文件（新建）：** `internal/apiserver/domain/service/route_validator.go`

RouteValidator 从 WorkerRegistry 拉取在线 Worker 拓扑，构建 `queue → {types}` 映射，带定时缓存刷新。

```go
type RouteValidator struct {
    registry     repository.WorkerRegistry
    refreshEvery time.Duration

    mu          sync.RWMutex
    queueTypes  map[string]map[string]struct{} // queue -> set of task types
    lastRefresh time.Time
}
```

#### 校验策略（fail-open）

| 场景 | 行为 | 原因 |
|------|------|------|
| Queue + Type 组合有效 | 放行 | 正常路径 |
| Queue 不存在 | 拒绝 | `no online worker is consuming queue "xxx"` |
| Queue 存在但无对应 Type Handler | 拒绝 | `no worker on queue "xxx" handles task type "yyy"` |
| **无 Worker 在线** | **放行** | 冷启动容忍——Worker 可能尚未注册 |
| **缓存刷新失败** | **放行** | fail-open——etcd 短暂不可用时不阻塞提交 |

选择 fail-open 而非 fail-close，是因为路由校验是**防御性增强**而非核心约束。任务已持久化到 MySQL，即使校验暂时不可用，最终一致性由补偿循环保证。

#### 缓存刷新策略

- 默认刷新间隔 10s（可配置）
- 惰性刷新：在 `Validate()` 调用时检查是否过期
- 不使用 etcd Watch，避免 API Server 维护长连接状态

### 3.4 TaskServiceImpl 集成路由校验

**文件：** `internal/apiserver/domain/service/task_service_impl.go`

路由校验作为**可选依赖**注入，通过 `SetRouteValidator()` 方法设置。执行位置在限流检查之后、MySQL 持久化之前：

```go
func (s *TaskServiceImpl) SubmitTask(ctx context.Context, task *entity.Task) error {
    // 1. 填充默认值
    // 2. BeforeSubmit hook (限流检查)
    // 3. 路由校验 (新增)
    if s.routeValidator != nil {
        if err := s.routeValidator.Validate(ctx, task.QueueName, task.Type); err != nil {
            return fmt.Errorf("route validation: %w", err)
        }
    }
    // 4. MySQL 持久化
    // 5. Redis 入队
    // 6. AfterSubmit hook
}
```

**为什么放在持久化之前？** 无效的 queue+type 组合不应该被写入 MySQL，否则会产生必然失败的脏数据，同时占用补偿循环的扫描资源。

### 3.5 API Server 接入 etcd

**文件：** `cmd/apiserver/main.go`

API Server 新增 etcd 连接，创建 WorkerRegistry 和 RouteValidator：

```go
etcdClient, err := persistence.NewEtcdClient(cfg.Etcd)
// ...
registry := etcdstore.NewWorkerRegistry(etcdClient)
taskSvc.SetRouteValidator(apisvc.NewRouteValidator(registry, 10*time.Second))
```

**架构影响**：API Server 原本不连接 etcd（见 architecture.md 中 "为什么 API Server 不连接 etcd?"）。此次变更引入了对 etcd 的**只读轻量依赖**：

- 仅调用 `ListWorkers()`，不使用 Watch/Lease/Campaign
- 带缓存（10s 刷新），不会对 etcd 造成压力
- fail-open 策略——etcd 不可用时不影响任务提交

## 四、变更文件清单

| 文件 | 变更类型 | 说明 |
|------|---------|------|
| `internal/shared/domain/entity/worker.go` | 修改 | WorkerInfo 增加 `TaskTypes` 字段 |
| `internal/worker/application/service/worker_app_service.go` | 修改 | Run() 启动时从 handlers 收集 TaskTypes |
| `internal/apiserver/domain/service/route_validator.go` | 新增 | RouteValidator 路由校验器 |
| `internal/apiserver/domain/service/task_service_impl.go` | 修改 | 集成 RouteValidator，SubmitTask 增加校验步骤 |
| `cmd/apiserver/main.go` | 修改 | 接入 etcd，创建并注入 RouteValidator |

## 五、向后兼容性

| 场景 | 行为 |
|------|------|
| 旧版 Worker（无 TaskTypes 字段） | etcd 中的 JSON 不包含 `task_types`，反序列化后为 nil → RouteValidator 构建映射时该 Worker 的 types 集合为空 → 其所在的 Queue 存在但 types 为空 → **所有 Type 被拒绝** |
| 旧版 Worker + 新版 API Server | 需要一起升级 Worker，否则路由校验会过度拒绝 |
| 新版 Worker + 旧版 API Server | API Server 无 RouteValidator → 行为与修复前完全相同 |
| 无 Worker 在线 | RouteValidator 放行所有请求 → 行为与修复前完全相同 |

**建议升级顺序**：先升级 Worker（开始上报 TaskTypes），再升级 API Server（开启路由校验）。
