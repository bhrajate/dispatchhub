# API 参考

DispatchHub 提供两种 API 接入方式：

- **HTTP REST API** -- 默认端口 8080，适用于外部客户端和调试
- **gRPC API** -- 默认端口 9090，适用于内部服务间高性能通信

两套 API 功能完全对等，共 9 个操作：任务 4 个、队列 1 个、CronJob 4 个。

---

## HTTP REST API

> 源码：`internal/apiserver/interfaces/http/server.go`

基础路径：`http://<host>:8080`

### 任务管理

#### 提交任务

```
POST /api/v1/tasks
```

**请求体**：

```json
{
    "name": "send-welcome-email",
    "namespace": "user-service",
    "group": "emails",
    "type": "email.send",
    "payload": {
        "to": "user@example.com",
        "template": "welcome",
        "vars": {"name": "Alice"}
    },
    "labels": {
        "env": "production",
        "team": "growth"
    },
    "priority": 8,
    "queue_name": "high-priority",
    "max_retries": 3,
    "timeout": "30s",
    "delay": "5m"
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `type` | string | **是** | Handler 类型标识 |
| `name` | string | 否 | 任务可读名称 |
| `namespace` | string | 否 | 命名空间 |
| `group` | string | 否 | 逻辑分组 |
| `payload` | object | 否 | 任务载荷（任意 JSON） |
| `labels` | object | 否 | K8s 风格标签 |
| `priority` | int | 否 | 优先级 1/5/8/10 |
| `queue_name` | string | 否 | 目标队列 |
| `max_retries` | int | 否 | 最大重试次数 |
| `timeout` | string | 否 | 执行超时（Go duration 格式，如 `"30s"`、`"5m"`） |
| `delay` | string | 否 | 延迟执行时长（Go duration 格式） |

**响应** `201 Created`：

```json
{
    "task_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
    "task": {
        "id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
        "name": "send-welcome-email",
        "type": "email.send",
        "state": 0,
        "priority": 8,
        "queue_name": "high-priority",
        "created_at": "2024-01-15T10:30:00Z"
    }
}
```

| 状态码 | 场景 |
|--------|------|
| 201 | 创建成功 |
| 400 | 请求体格式错误或缺少 `type` 字段 |
| 500 | 持久化或入队失败（错误信息不泄露内部细节） |

---

#### 查询任务

```
GET /api/v1/tasks/{id}
```

**响应** `200 OK`：

```json
{
    "id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
    "name": "send-welcome-email",
    "namespace": "user-service",
    "type": "email.send",
    "state": 4,
    "result": "email sent successfully",
    "worker_id": "node-1-a3b2c1d4",
    "retry_count": 0,
    "priority": 8,
    "queue_name": "high-priority",
    "created_at": "2024-01-15T10:30:00Z",
    "started_at": "2024-01-15T10:30:01Z",
    "finished_at": "2024-01-15T10:30:02Z",
    "version": 3
}
```

| 状态码 | 场景 |
|--------|------|
| 200 | 成功 |
| 404 | 任务不存在 |

---

#### 列出任务

```
GET /api/v1/tasks?namespace=user-service&type=email.send&limit=20&offset=0
```

**查询参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `namespace` | string | 按命名空间过滤 |
| `group` | string | 按分组过滤 |
| `type` | string | 按 Handler 类型过滤 |
| `queue` | string | 按队列过滤 |
| `limit` | int | 分页大小 |
| `offset` | int | 分页偏移 |

**响应** `200 OK`：

```json
{
    "tasks": [
        {
            "id": "...",
            "name": "...",
            "type": "email.send",
            "state": 4
        }
    ],
    "total": 156
}
```

---

#### 取消任务

```
POST /api/v1/tasks/{id}/cancel
```

**响应** `200 OK`：

```json
{
    "status": "cancelled"
}
```

| 状态码 | 场景 |
|--------|------|
| 200 | 取消成功 |
| 500 | 任务不存在或已处于终态 |

> 只能取消非终态的任务。已 Completed、Failed、Cancelled、Timeout 的任务不可取消。

---

### 队列管理

#### 查看队列统计

```
GET /api/v1/queues/{name}/stats
```

**响应** `200 OK`：

```json
{
    "name": "default",
    "pending": 42,
    "active": 8,
    "scheduled": 15,
    "retrying": 2,
    "completed": 10583,
    "failed": 37
}
```

| 字段 | 说明 |
|------|------|
| `pending` | 就绪队列中等待处理的任务数 |
| `active` | 已出队正在执行的任务数（inflight） |
| `scheduled` | 延迟队列中尚未到期的任务数 |
| `retrying` | 等待重试的任务数 |
| `completed` | 累计完成任务总数 |
| `failed` | 累计失败任务总数 |

---

### CronJob 管理

#### 创建 CronJob

```
POST /api/v1/cronjobs
```

**请求体**：

```json
{
    "name": "daily-report",
    "namespace": "analytics",
    "type": "report.generate",
    "payload": {"format": "pdf"},
    "labels": {"team": "data"},
    "cron_expr": "0 2 * * *",
    "queue_name": "batch",
    "priority": 5,
    "max_retries": 3,
    "timeout": "10m",
    "retry_backoff": "1s"
}
```

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `type` | string | **是** | Handler 类型标识 |
| `cron_expr` | string | **是** | cron 表达式（如 `"*/5 * * * *"`、`"0 2 * * *"`） |
| `name` | string | 否 | 可读名称 |
| `namespace` | string | 否 | 命名空间 |
| `payload` | object | 否 | 任务载荷 |
| `labels` | object | 否 | 标签 |
| `queue_name` | string | 否 | 目标队列 |
| `priority` | int | 否 | 优先级 |
| `max_retries` | int | 否 | 最大重试次数 |
| `timeout` | string | 否 | 执行超时 |
| `retry_backoff` | string | 否 | 重试退避时长 |

**响应** `201 Created`：返回完整的 CronJob 对象（含计算好的 `next_run_at`）。

| 状态码 | 场景 |
|--------|------|
| 201 | 创建成功 |
| 400 | 缺少 `type`/`cron_expr`，或 cron 表达式无效 |
| 500 | 持久化失败 |

---

#### 查询 CronJob

```
GET /api/v1/cronjobs/{id}
```

| 状态码 | 场景 |
|--------|------|
| 200 | 成功 |
| 404 | CronJob 不存在 |

---

#### 列出 CronJob

```
GET /api/v1/cronjobs?namespace=analytics&limit=20&offset=0
```

| 参数 | 类型 | 说明 |
|------|------|------|
| `namespace` | string | 按命名空间过滤 |
| `limit` | int | 分页大小（默认 100） |
| `offset` | int | 分页偏移 |

**响应** `200 OK`：

```json
{
    "cron_jobs": [ ... ],
    "total": 12
}
```

---

#### 删除 CronJob

```
DELETE /api/v1/cronjobs/{id}
```

**响应** `200 OK`：

```json
{
    "status": "deleted"
}
```

---

### 可观测性端点

#### 健康检查

```
GET /healthz
```

返回 `{"status": "ok"}`，用于 Kubernetes livenessProbe，判断进程是否存活。始终返回 200（只要进程在运行）。

#### 就绪检查

```
GET /readyz
```

返回 `{"status": "ready"}` 或 `{"status": "not ready", "error": "..."}`。用于 Kubernetes readinessProbe，实际检查 Redis 和 MySQL 连通性（3s 超时）。

| 状态码 | 说明 |
|--------|------|
| 200 | 所有依赖就绪 |
| 503 | 某个依赖不可用 |

#### Prometheus 指标

```
GET /metrics
```

返回 Prometheus 格式的指标数据。

---

### 错误响应格式

所有错误响应统一为以下格式，**不泄露内部实现细节**（SQL 语句、堆栈等仅记录到日志）：

```json
{
    "error": "failed to submit task"
}
```

---

## gRPC API

> 源码：`api/proto/dispatch.proto`、`internal/apiserver/interfaces/grpc/server.go`

端口 9090，使用 Protocol Buffers 定义接口。

### 服务定义

```protobuf
service DispatchService {
    // 任务操作
    rpc SubmitTask(SubmitTaskRequest)     returns (SubmitTaskResponse);
    rpc GetTask(GetTaskRequest)           returns (GetTaskResponse);
    rpc ListTasks(ListTasksRequest)       returns (ListTasksResponse);
    rpc CancelTask(CancelTaskRequest)     returns (CancelTaskResponse);

    // 队列操作
    rpc GetQueueStats(GetQueueStatsRequest) returns (GetQueueStatsResponse);

    // CronJob 操作
    rpc CreateCronJob(CreateCronJobRequest) returns (CreateCronJobResponse);
    rpc GetCronJob(GetCronJobRequest)       returns (GetCronJobResponse);
    rpc ListCronJobs(ListCronJobsRequest)   returns (ListCronJobsResponse);
    rpc DeleteCronJob(DeleteCronJobRequest) returns (DeleteCronJobResponse);
}
```

### 消息类型

#### TaskSpec（任务规格）

```protobuf
message TaskSpec {
    string name = 1;
    string namespace = 2;
    string group = 3;
    string type = 4;
    bytes payload = 5;                        // JSON bytes
    map<string, string> labels = 6;
    TaskPriority priority = 7;
    google.protobuf.Duration delay = 8;
    google.protobuf.Timestamp schedule_at = 9;
    google.protobuf.Duration timeout = 10;
    int32 max_retries = 11;
    google.protobuf.Duration retry_backoff = 12;
    string queue_name = 13;
}
```

#### Task（完整任务）

```protobuf
message Task {
    string id = 1;
    TaskSpec spec = 2;
    TaskState state = 3;
    string result = 4;
    string error = 5;
    string worker_id = 6;
    int32 retry_count = 7;
    google.protobuf.Timestamp created_at = 8;
    google.protobuf.Timestamp started_at = 9;
    google.protobuf.Timestamp finished_at = 10;
    int64 version = 11;
}
```

#### CronJob

```protobuf
message CronJob {
    string id = 1;
    string name = 2;
    string namespace = 3;
    string type = 4;
    bytes payload = 5;
    map<string, string> labels = 6;
    string cron_expr = 7;
    string queue_name = 8;
    TaskPriority priority = 9;
    google.protobuf.Duration timeout = 10;
    int32 max_retries = 11;
    google.protobuf.Duration retry_backoff = 12;
    bool enabled = 13;
    google.protobuf.Timestamp last_run_at = 14;
    google.protobuf.Timestamp next_run_at = 15;
    google.protobuf.Timestamp created_at = 16;
}
```

### 请求/响应消息

| RPC | Request | Response |
|-----|---------|----------|
| SubmitTask | `TaskSpec spec` | `string task_id` + `Task task` |
| GetTask | `string task_id` | `Task task` |
| ListTasks | namespace, group, type, state, labels, queue_name, limit, offset | `repeated Task tasks` + `int64 total` |
| CancelTask | `string task_id` | `Task task` |
| GetQueueStats | `string queue_name` | queue_name, pending, active, scheduled, retrying, completed, failed |
| CreateCronJob | name, namespace, type, payload, labels, cron_expr, queue_name, priority, timeout, max_retries, retry_backoff | `CronJob cron_job` |
| GetCronJob | `string id` | `CronJob cron_job` |
| ListCronJobs | namespace, limit, offset | `repeated CronJob cron_jobs` + `int64 total` |
| DeleteCronJob | `string id` | (空消息) |

### 枚举类型

#### TaskPriority

| 名称 | 值 |
|------|-----|
| TASK_PRIORITY_UNSPECIFIED | 0 |
| TASK_PRIORITY_LOW | 1 |
| TASK_PRIORITY_DEFAULT | 5 |
| TASK_PRIORITY_HIGH | 8 |
| TASK_PRIORITY_CRITICAL | 10 |

#### TaskState

| 名称 | 值 |
|------|-----|
| TASK_STATE_UNSPECIFIED | 0 |
| TASK_STATE_PENDING | 1 |
| TASK_STATE_SCHEDULED | 2 |
| TASK_STATE_RUNNING | 3 |
| TASK_STATE_RETRYING | 4 |
| TASK_STATE_COMPLETED | 5 |
| TASK_STATE_FAILED | 6 |
| TASK_STATE_CANCELLED | 7 |
| TASK_STATE_TIMEOUT | 8 |

> 注意：gRPC proto 中 TaskState 从 1 开始（0 为 UNSPECIFIED），Go 实体中 TaskState 从 0 开始（Pending=0）。两者在 gRPC handler 中进行转换。

### 附加 gRPC 服务

| 服务 | 说明 |
|------|------|
| `grpc.health.v1.Health` | gRPC 标准健康检查 |
| `grpc.reflection.v1` | 服务反射（开发调试用，可通过 grpcurl 发现服务） |

---

## Prometheus 指标

> 源码：`pkg/metrics/metrics.go`

所有指标使用 `dispatchhub` 作为 namespace。

### Scheduler 指标

| 指标名 | 类型 | 标签 | 说明 |
|--------|------|------|------|
| `dispatchhub_scheduler_tasks_submitted_total` | Counter | queue, type, priority | 提交任务总数 |
| `dispatchhub_scheduler_tasks_scheduled_total` | Counter | queue, worker | 调度分发任务总数 |
| `dispatchhub_scheduler_schedule_latency_seconds` | Histogram | queue | 任务提交到调度的延迟 |
| `dispatchhub_scheduler_loop_duration_seconds` | Histogram | - | 单次调度循环耗时 |
| `dispatchhub_scheduler_leader_elections_total` | Counter | - | Leader 选举次数 |

### Worker 指标

| 指标名 | 类型 | 标签 | 说明 |
|--------|------|------|------|
| `dispatchhub_worker_tasks_processed_total` | Counter | queue, type, status | 处理任务总数 |
| `dispatchhub_worker_task_duration_seconds` | Histogram | queue, type | 任务执行耗时分布 |
| `dispatchhub_worker_active_count` | Gauge | - | 当前在线 Worker 数 |
| `dispatchhub_worker_active_tasks` | Gauge | queue | 各队列正在执行的任务数 |
| `dispatchhub_worker_heartbeats_total` | Counter | worker | 心跳发送总数 |

### Queue 指标

| 指标名 | 类型 | 标签 | 说明 |
|--------|------|------|------|
| `dispatchhub_queue_depth` | Gauge | queue, state | 队列深度（pending/active/scheduled） |

### Grafana 查询示例

```promql
# 每秒任务提交速率
rate(dispatchhub_scheduler_tasks_submitted_total[5m])

# 任务处理 P99 延迟
histogram_quantile(0.99, rate(dispatchhub_worker_task_duration_seconds_bucket[5m]))

# 队列积压告警
dispatchhub_queue_depth{state="pending"} > 1000

# Worker 利用率
dispatchhub_worker_active_tasks / on() dispatchhub_worker_active_count
```
