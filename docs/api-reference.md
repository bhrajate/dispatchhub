# API 参考

DispatchHub 提供两种 API 接入方式：

- **HTTP REST API**：端口 8080，适用于外部客户端和调试
- **gRPC API**：端口 9090，适用于内部服务间高性能通信

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
    "delay": "5m",
    "cron_expr": ""
}
```

| 字段 | 类型 | 必填 | 默认值 | 说明 |
|------|------|------|--------|------|
| `name` | string | 否 | "" | 任务可读名称 |
| `namespace` | string | 否 | "" | 命名空间 |
| `group` | string | 否 | "" | 逻辑分组 |
| `type` | string | **是** | - | Handler 类型标识 |
| `payload` | object | 否 | null | 任务载荷（任意 JSON） |
| `labels` | object | 否 | null | K8s 风格标签 |
| `priority` | int | 否 | 5 | 优先级 1-10 |
| `queue_name` | string | 否 | "default" | 目标队列 |
| `max_retries` | int | 否 | 3 | 最大重试次数 |
| `timeout` | string | 否 | "5m" | 执行超时（Go duration 格式） |
| `delay` | string | 否 | "" | 延迟执行时长 |
| `cron_expr` | string | 否 | "" | Cron 表达式 |

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
        "created_at": "2024-01-15T10:30:00Z",
        ...
    }
}
```

**错误响应**：

| 状态码 | 场景 |
|--------|------|
| 400 | 请求体格式错误或缺少 `type` 字段 |
| 500 | 持久化或入队失败 |

---

#### 查询任务

```
GET /api/v1/tasks/{id}
```

**路径参数**：

| 参数 | 说明 |
|------|------|
| `id` | 任务 UUID |

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
| `limit` | int | 分页大小，默认 100 |
| `offset` | int | 分页偏移 |

**响应** `200 OK`：

```json
{
    "tasks": [
        {
            "id": "...",
            "name": "...",
            "type": "email.send",
            "state": 4,
            ...
        }
    ],
    "total": 156
}
```

结果按 `priority DESC, created_at ASC` 排序。

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

> 注意：只能取消 Pending / Running / Retrying 状态的任务。已完成、已失败、已超时的任务不可取消。

---

### 队列管理

#### 查看队列统计

```
GET /api/v1/queues/{name}/stats
```

**路径参数**：

| 参数 | 说明 |
|------|------|
| `name` | 队列名称，如 `default`、`high-priority` |

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

### 可观测性端点

#### 健康检查

```
GET /healthz
```

**响应** `200 OK`：

```json
{"status": "ok"}
```

用于 Kubernetes livenessProbe，判断进程是否存活。

#### 就绪检查

```
GET /readyz
```

**响应** `200 OK`：

```json
{"status": "ready"}
```

用于 Kubernetes readinessProbe，判断服务是否就绪接受流量。

#### Prometheus 指标

```
GET /metrics
```

返回 Prometheus 格式的指标数据，详见下方 [Prometheus 指标](#prometheus-指标) 章节。

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
    rpc PauseQueue(PauseQueueRequest)       returns (PauseQueueResponse);
    rpc ResumeQueue(ResumeQueueRequest)     returns (ResumeQueueResponse);

    // Worker 操作
    rpc ListWorkers(ListWorkersRequest)     returns (ListWorkersResponse);

    // 流式事件
    rpc WatchTasks(WatchTasksRequest)       returns (stream TaskEvent);
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
    string cron_expr = 10;
    google.protobuf.Duration timeout = 11;
    google.protobuf.Timestamp deadline = 12;
    int32 max_retries = 13;
    google.protobuf.Duration retry_backoff = 14;
    string queue_name = 15;
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

#### WorkerInfo

```protobuf
message WorkerInfo {
    string id = 1;
    string hostname = 2;
    string ip = 3;
    repeated string queues = 4;
    int32 concurrency = 5;
    int32 active_tasks = 6;
    double cpu_usage = 7;
    double mem_usage = 8;
    string state = 9;
    google.protobuf.Timestamp started_at = 10;
    google.protobuf.Timestamp last_heartbeat = 11;
}
```

#### TaskEvent（流式事件）

```protobuf
message TaskEvent {
    string task_id = 1;
    string event_type = 2;     // created / started / completed / failed / ...
    TaskState old_state = 3;
    TaskState new_state = 4;
    string worker_id = 5;
    google.protobuf.Timestamp timestamp = 6;
}
```

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

### 附加 gRPC 服务

| 服务 | 说明 |
|------|------|
| `grpc.health.v1.Health` | gRPC 标准健康检查 |
| `grpc.reflection.v1` | 服务反射（开发调试用） |

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

### Histogram Buckets

- `schedule_latency_seconds`：指数分布，从 1ms 到约 32s
- `loop_duration_seconds`：指数分布，从 0.1ms 到约 3.2s
- `task_duration_seconds`：指数分布，从 10ms 到约 327s

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
