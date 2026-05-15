# API 参考

DispatchHub 由三个独立微服务组成，对外/对内暴露的所有接口都在本文中列出。

| 服务 | 进程入口 | 暴露接口 | 端口（默认） |
|------|----------|----------|--------------|
| **API Server** | `cmd/apiserver` | HTTP REST、gRPC、运维端点 | `:8080` HTTP / `:9090` gRPC / `:9091` metrics |
| **Scheduler** | `cmd/scheduler` | 仅运维端点（无业务 API） | `:8080` HTTP（healthz/readyz/metrics） |
| **Worker** | `cmd/worker` | 仅运维端点（无业务 API） | `:8080` HTTP（healthz/readyz/metrics） |

> 业务 API 全部位于 **API Server**。Scheduler 与 Worker 是无外部 API 的后台服务，仅暴露 healthcheck 与 Prometheus 指标。
>
> 端口默认值在 [`docs/reference/configuration.md`](configuration.md) 中可调整；Helm 部署的实际端口请以 `deploy/helm/dispatchhub/values.yaml` 为准。

---

## 目录

- [API Server — 业务接口](#api-server--业务接口)
  - [HTTP REST API](#http-rest-api)
    - [任务管理](#1-任务管理)
    - [队列查询](#2-队列查询)
    - [CronJob 管理](#3-cronjob-管理)
    - [运维端点](#4-运维端点-api-server)
  - [gRPC API](#grpc-api)
- [Scheduler — 运维接口](#scheduler--运维接口)
- [Worker — 运维接口](#worker--运维接口)
- [枚举与公共类型](#枚举与公共类型)
- [错误响应规范](#错误响应规范)

---

## API Server — 业务接口

源码：`internal/apiserver/interfaces/{http,grpc}/server.go`、`api/proto/dispatch.proto`。

HTTP REST 与 gRPC **功能完全对等**，共 9 个业务方法 + 3 个运维端点。两套 API 共享同一个 `TaskService` 实现（`internal/apiserver/domain/service/task_service_impl.go`），因此行为完全一致；区别仅在编码方式与错误码映射。

### HTTP REST API

- 基础路径：`http://<host>:8080`
- 内容类型：所有请求/响应都使用 `application/json`
- 路由实现：Go 1.22+ 标准库 `http.ServeMux` 模式语法（`POST /api/v1/tasks`、`GET /api/v1/tasks/{id}`）

### 1. 任务管理

#### Task 对象字段（响应体复用结构）

任务管理类接口（提交、查询、列表、取消）的响应中均包含完整 `Task` 对象。字段定义如下（实体见 `internal/shared/domain/entity/task.go`）：

| 字段 | 类型 | 何时为空 | 含义 |
|------|------|----------|------|
| `id` | string (UUID) | 始终有值 | 任务唯一标识，由服务端生成 |
| `name` | string | 创建时未传则为 `""` | 可读名称 |
| `namespace` | string | 创建时未传则为 `""` | 命名空间 |
| `group` | string | 创建时未传则为 `""` | 逻辑分组 |
| `type` | string | 始终有值 | Handler 类型 |
| `payload` | raw JSON | 创建时未传则为 `null` | 任务载荷（gRPC 中为 bytes） |
| `labels` | map\<string,string\> | 创建时未传则为 `null` | 标签 |
| `priority` | int (1–10) | 始终有值 | 优先级，参见 [TaskPriority](#taskpriority) |
| `delay` | duration string | 即时任务为 `"0s"` | 延迟入队时长 |
| `schedule_at` | RFC3339 | 仅延迟任务有 | 绝对调度时间 |
| `timeout` | duration string | 始终有值，默认 `"5m"` | 单次执行超时 |
| `max_retries` | int | 始终有值 | 最大重试次数 |
| `retry_count` | int | 始终有值 | 已重试次数 |
| `retry_backoff` | duration string | 默认 `"0s"` | 重试退避基础时长 |
| `state` | int (0–7) | 始终有值 | 状态，参见 [TaskState](#taskstate) |
| `result` | string | 仅成功后有 | Handler 返回的输出（字符串化） |
| `error` | string | 仅失败/超时有 | 错误信息 |
| `worker_id` | string | 被 Worker 拉取后有 | 执行该任务的 Worker ID |
| `queue_name` | string | 始终有值，默认 `"default"` | 所属队列 |
| `created_at` | RFC3339 | 始终有值 | 入库时刻 |
| `updated_at` | RFC3339 | 始终有值 | 上次更新时刻 |
| `started_at` | RFC3339 | 进入 Running 后有 | 开始执行时刻 |
| `finished_at` | RFC3339 | 进入终态后有 | 终态时刻 |
| `version` | int64 | 始终有值，从 1 起 | 乐观锁版本号 |

#### 1.1 提交任务

```
POST /api/v1/tasks
```

**作用**：创建任务并入队。任务被持久化到 MySQL 后写入 Redis（即时任务进 ready 队列；带 `delay` 或 `schedule_at` 的任务进 delayed 队列）。Redis 写入失败不阻断响应——Scheduler 的补偿循环会在 30s 内重新入队，避免客户端重试造成重复任务。

**请求体**：

| 字段 | 类型 | 必填 | 默认 | 说明 |
|------|------|------|------|------|
| `name` | string | 否 | `""` | 任务可读名称 |
| `namespace` | string | 否 | `""` | 命名空间，多租户隔离 |
| `group` | string | 否 | `""` | 逻辑分组，可用于亲和性调度 |
| `type` | string | **是** | — | Handler 类型，Worker 用它路由到对应处理函数 |
| `payload` | object/raw JSON | 否 | `null` | 任务载荷，原样透传 |
| `labels` | map\<string,string\> | 否 | `null` | K8s 风格标签 |
| `priority` | int | 否 | `5` (Default) | 1=Low、5=Default、8=High、10=Critical |
| `queue_name` | string | 否 | `"default"` | 目标队列 |
| `max_retries` | int | 否 | `3` | 最大重试次数 |
| `timeout` | duration string | 否 | `"5m"` | Go duration 格式（`30s`、`5m`、`1h`） |
| `delay` | duration string | 否 | `"0s"` | 延迟入队，与 `schedule_at` 二选一 |
| `retry_backoff` | duration string | 否 | `"0s"` | 重试退避基础时长 |

> ⚠ HTTP 接口当前**未读取** `schedule_at` 字段（参见 `internal/apiserver/interfaces/http/server.go:74-120`），如需绝对时间调度请使用 gRPC。

**请求示例**：

```bash
curl -X POST http://localhost:8080/api/v1/tasks \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "send-welcome-email",
    "namespace": "user-service",
    "group": "emails",
    "type": "email.send",
    "payload": {"to": "alice@example.com", "template": "welcome"},
    "labels": {"env": "prod", "team": "growth"},
    "priority": 8,
    "queue_name": "high-priority",
    "max_retries": 3,
    "timeout": "30s",
    "delay": "5m"
  }'
```

**响应结构（201 Created）**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `task_id` | string | 服务端生成的任务 ID（与 `task.id` 重复，便于客户端只取 ID 时省略解析整体） |
| `task` | object | 完整 Task 对象，字段见 [Task 对象字段](#task-对象字段响应体复用结构) |

**响应示例**：

```json
{
  "task_id": "550e8400-e29b-41d4-a716-446655440000",
  "task": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "name": "send-welcome-email",
    "namespace": "user-service",
    "group": "emails",
    "type": "email.send",
    "payload": {"to": "alice@example.com", "template": "welcome"},
    "labels": {"env": "prod", "team": "growth"},
    "priority": 8,
    "delay": "5m0s",
    "timeout": "30s",
    "max_retries": 3,
    "retry_count": 0,
    "retry_backoff": "0s",
    "state": 0,
    "queue_name": "high-priority",
    "created_at": "2026-05-15T10:00:00.123+08:00",
    "updated_at": "2026-05-15T10:00:00.123+08:00",
    "version": 1
  }
}
```

**错误**：
- `400 Bad Request` — 请求体非法 JSON 或缺 `type`
- `500 Internal Server Error` — 限流拒绝、MySQL 写失败、`RouteValidator` 校验失败（语义上限流应返回 `429`，详见[错误响应规范](#错误响应规范)）

---

#### 1.2 查询单个任务

```
GET /api/v1/tasks/{id}
```

**作用**：根据任务 ID 读取最新状态。从 MySQL 读取，Worker 完成任务后会异步更新该表。

**路径参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `id` | string | 任务 UUID |

**请求示例**：

```bash
curl http://localhost:8080/api/v1/tasks/550e8400-e29b-41d4-a716-446655440000
```

**响应结构（200 OK）**：响应体直接是一个 [Task 对象](#task-对象字段响应体复用结构)，无外层包装。

**响应示例**：

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "name": "send-welcome-email",
  "namespace": "user-service",
  "group": "emails",
  "type": "email.send",
  "payload": {"to": "alice@example.com", "template": "welcome"},
  "priority": 8,
  "timeout": "30s",
  "max_retries": 3,
  "retry_count": 0,
  "state": 4,
  "result": "{\"message_id\":\"abc123\"}",
  "worker_id": "worker-7d8c9b-x4k2",
  "queue_name": "high-priority",
  "created_at": "2026-05-15T10:00:00.123+08:00",
  "started_at": "2026-05-15T10:00:01.456+08:00",
  "finished_at": "2026-05-15T10:00:02.789+08:00",
  "version": 4
}
```

**错误**：
- `404 Not Found` — 任务不存在
- `500 Internal Server Error` — 数据库读失败

---

#### 1.3 任务列表查询

```
GET /api/v1/tasks?namespace=&group=&type=&queue=&limit=&offset=
```

**作用**：按多维度过滤任务。所有过滤字段都可选，组合 AND 条件。

**Query 参数**：

| 参数 | 类型 | 默认 | 说明 |
|------|------|------|------|
| `namespace` | string | — | 精确匹配 |
| `group` | string | — | 精确匹配 |
| `type` | string | — | 精确匹配 |
| `queue` | string | — | 精确匹配队列名 |
| `limit` | int | 0（无限制） | 返回条数上限 |
| `offset` | int | 0 | 偏移 |

> 提示：HTTP 路径未暴露 `state` 与 `worker_id` 过滤（仅 gRPC 可按 `state` 过滤）。

**请求示例**：

```bash
curl "http://localhost:8080/api/v1/tasks?namespace=user-service&type=email.send&limit=20&offset=0"
```

**响应结构（200 OK）**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `tasks` | array of object | 命中的 Task 对象数组，单个元素结构见 [Task 对象字段](#task-对象字段响应体复用结构)；当 `total` > `len(tasks)` 时需要分页继续拉取 |
| `total` | int64 | 命中条件下的总数（不受 `limit`/`offset` 影响） |

**响应示例**：

```json
{
  "tasks": [
    {"id": "...", "type": "email.send", "state": 4, "...": "..."},
    {"id": "...", "type": "email.send", "state": 1, "...": "..."}
  ],
  "total": 124
}
```

---

#### 1.4 取消任务

```
POST /api/v1/tasks/{id}/cancel
```

**作用**：将任务标记为已取消。处理流程：

1. 写入 MySQL：`state = TaskStateCancelled`、`finished_at = now()`
2. 尽力从 Redis ready/delayed/inflight 队列移除（防止 Worker 拉取）
3. 通过 Redis Pub/Sub `dispatchhub:task:cancel` 通知正在执行该任务的 Worker（Worker 收到后取消 ctx）

如果任务已处于终态（Completed/Failed/Cancelled/Timeout），返回错误。

**路径参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `id` | string | 待取消的任务 UUID |

**请求示例**：

```bash
curl -X POST http://localhost:8080/api/v1/tasks/550e8400-e29b-41d4-a716-446655440000/cancel
```

**响应结构（200 OK）**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `status` | string | 固定字符串 `"cancelled"` |

> ⚠ HTTP 与 gRPC 的取消响应**结构不同**：HTTP 仅返回 `{status:"cancelled"}`；gRPC 返回完整的更新后 Task 对象（见 `CancelTaskResponse`）。如果客户端需要拿到取消后的 Task 状态，建议在 HTTP 取消之后再调一次 [GET 单任务](#12-查询单个任务)，或改用 gRPC。

**响应示例**：

```json
{ "status": "cancelled" }
```

**错误**：
- `500` — 任务已是终态、不存在，或后端写失败

---

### 2. 队列查询

#### 2.1 队列实时统计

```
GET /api/v1/queues/{name}/stats
```

**作用**：从 Redis 一次 pipeline 读出指定队列的实时统计。

**路径参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `name` | string | 队列名（默认队列为 `default`） |

**响应结构（200 OK）**：

| 字段 | 类型 | 来源 Redis 键 | 含义 |
|------|------|---------------|------|
| `name` | string | — | 队列名 |
| `pending` | int64 | `ZCARD dispatchhub:queue:<name>:ready` | 等待调度的任务数 |
| `active` | int64 | `HLEN dispatchhub:queue:<name>:inflight` | 已被 Worker 取走、执行中的任务数 |
| `scheduled` | int64 | `ZCARD dispatchhub:queue:<name>:delayed` | 延迟队列中尚未到时的任务数 |
| `retrying` | int64 | — | 当前实现固定为 0（重试任务复用 delayed） |
| `completed` | int64 | `HGET dispatchhub:queue:<name>:stats completed` | 历史累计完成数 |
| `failed` | int64 | `HGET dispatchhub:queue:<name>:stats failed` | 历史累计失败数 |

**请求示例**：

```bash
curl http://localhost:8080/api/v1/queues/default/stats
```

**响应示例**：

```json
{
  "name": "default",
  "pending": 42,
  "active": 7,
  "scheduled": 3,
  "retrying": 0,
  "completed": 12345,
  "failed": 12
}
```

---

### 3. CronJob 管理

CronJob 是周期性任务模板：Scheduler 的 cron 检查循环每秒扫描 `next_run_at <= now()` 的 CronJob，按其 spec 创建一个新 Task 实例并入队，然后更新 `next_run_at` 为下一次触发时刻。

#### CronJob 对象字段（响应体复用结构）

CronJob 类接口（创建、查询、列表）的响应中均包含完整 `CronJob` 对象。字段定义如下（实体见 `internal/shared/domain/entity/cronjob.go`）：

| 字段 | 类型 | 何时为空 | 含义 |
|------|------|----------|------|
| `id` | string (UUID) | 始终有值 | CronJob 唯一标识 |
| `name` | string | 创建时未传则为 `""` | 可读名称 |
| `namespace` | string | 创建时未传则为 `""` | 命名空间 |
| `type` | string | 始终有值 | 触发后所创建任务的 Handler 类型 |
| `payload` | raw JSON | 创建时未传则为 `null` | 触发时透传给 Task 的载荷 |
| `labels` | map\<string,string\> | 创建时未传则为 `null` | 标签 |
| `cron_expr` | string | 始终有值 | Cron 表达式 |
| `queue_name` | string | 始终有值，默认 `"default"` | 触发任务所投递的队列 |
| `priority` | int (1–10) | 始终有值，默认 `5` | 触发任务的优先级 |
| `timeout` | duration string | 始终有值，默认 `"5m"` | 触发任务的执行超时 |
| `max_retries` | int | 始终有值，默认 `3` | 触发任务的最大重试次数 |
| `retry_backoff` | duration string | 默认 `"0s"` | 触发任务的重试退避 |
| `concurrency_policy` | string | 始终为 `"Allow"` | 并发策略，参见 [ConcurrencyPolicy](#cronjob-的-concurrencypolicy)。当前 API 不支持创建为 `"Forbid"` |
| `enabled` | bool | 始终有值，默认 `true` | 是否启用，禁用后 Scheduler 不再触发 |
| `last_run_at` | RFC3339 | 从未触发时为 `null` | 上次触发时刻 |
| `next_run_at` | RFC3339 | 始终有值 | 下次触发时刻 |
| `created_at` | RFC3339 | 始终有值 | 创建时刻 |
| `updated_at` | RFC3339 | 始终有值 | 上次更新时刻 |

#### 3.1 创建 CronJob

```
POST /api/v1/cronjobs
```

**作用**：创建一个 Cron 调度模板。HTTP 处理层会立即解析 `cron_expr` 并设置初始 `next_run_at`，表达式非法直接 400。

**请求体**：

| 字段 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `name` | string | 否 | 可读名 |
| `namespace` | string | 否 | 命名空间 |
| `type` | string | **是** | 任务 Handler 类型 |
| `payload` | raw JSON | 否 | 触发时透传给 Task |
| `labels` | map\<string,string\> | 否 | 标签 |
| `cron_expr` | string | **是** | 标准 5/6 字段 cron 表达式（实现见 `pkg/cronutil`） |
| `queue_name` | string | 否 | 默认 `"default"` |
| `priority` | int | 否 | 默认 `5` |
| `timeout` | duration | 否 | 默认 `"5m"` |
| `max_retries` | int | 否 | 默认 `3` |
| `retry_backoff` | duration | 否 | 默认 `"0s"` |

**请求示例**：

```bash
curl -X POST http://localhost:8080/api/v1/cronjobs \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "daily-report",
    "namespace": "analytics",
    "type": "report.daily",
    "payload": {"format": "pdf"},
    "cron_expr": "0 9 * * *",
    "queue_name": "default",
    "priority": 5,
    "timeout": "10m"
  }'
```

**响应结构（201 Created）**：响应体直接是一个 [CronJob 对象](#cronjob-对象字段响应体复用结构)，无外层包装。

**响应示例**：

```json
{
  "id": "660e8400-e29b-41d4-a716-446655440111",
  "name": "daily-report",
  "namespace": "analytics",
  "type": "report.daily",
  "payload": {"format": "pdf"},
  "cron_expr": "0 9 * * *",
  "queue_name": "default",
  "priority": 5,
  "timeout": "10m0s",
  "max_retries": 3,
  "retry_backoff": "0s",
  "concurrency_policy": "Allow",
  "enabled": true,
  "next_run_at": "2026-05-16T09:00:00+08:00",
  "created_at": "2026-05-15T15:30:00+08:00",
  "updated_at": "2026-05-15T15:30:00+08:00"
}
```

**错误**：
- `400 Bad Request` — 缺 `type` / `cron_expr`，或 cron 表达式解析失败

---

#### 3.2 查询单个 CronJob

```
GET /api/v1/cronjobs/{id}
```

**路径参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `id` | string | CronJob UUID |

**响应结构（200 OK）**：响应体直接是一个 [CronJob 对象](#cronjob-对象字段响应体复用结构)。`last_run_at` 在被触发过至少一次后才会有值。

**错误**：
- `404 Not Found` — CronJob 不存在

---

#### 3.3 CronJob 列表

```
GET /api/v1/cronjobs?namespace=&limit=&offset=
```

**Query 参数**：

| 参数 | 默认 | 说明 |
|------|------|------|
| `namespace` | — | 精确匹配；为空时返回全部 |
| `limit` | 100 | 上限 |
| `offset` | 0 | 偏移 |

**响应结构（200 OK）**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `cron_jobs` | array of object | 命中的 CronJob 对象数组，单个元素结构见 [CronJob 对象字段](#cronjob-对象字段响应体复用结构) |
| `total` | int64 | 命中条件下的总数（不受 `limit`/`offset` 影响） |

**响应示例**：

```json
{
  "cron_jobs": [
    {"id": "...", "name": "daily-report", "...": "..."}
  ],
  "total": 5
}
```

---

#### 3.4 删除 CronJob

```
DELETE /api/v1/cronjobs/{id}
```

**路径参数**：

| 参数 | 类型 | 说明 |
|------|------|------|
| `id` | string | 待删除的 CronJob UUID |

**响应结构（200 OK）**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `status` | string | 固定字符串 `"deleted"` |

**响应示例**：

```json
{ "status": "deleted" }
```

> ⚠ 删除只移除 CronJob 模板本身，**不会**取消已经触发并提交的任务实例。

---

### 4. 运维端点 (API Server)

源码：`internal/apiserver/interfaces/http/server.go:58-60`

| 路径 | 方法 | 状态码 | 说明 |
|------|------|--------|------|
| `/healthz` | GET | 200 固定 `{"status":"ok"}` | 进程存活探针，不查依赖 |
| `/readyz` | GET | 200 / 503 | 就绪探针，检查 Redis ping + MySQL ping，3s 超时 |
| `/metrics` | GET | 200 | Prometheus 文本格式指标，由 `promauto` 暴露 |

**`/healthz` 响应结构（200 OK）**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `status` | string | 固定字符串 `"ok"` |

**`/readyz` 响应结构**：

| 状态码 | 字段 | 类型 | 说明 |
|--------|------|------|------|
| 200 OK | `status` | string | 固定字符串 `"ready"` |
| 503 Service Unavailable | `status` | string | 固定字符串 `"not ready"` |
| 503 Service Unavailable | `error` | string | 失败原因描述（首个失败的依赖+错误） |

**`/readyz` 响应示例**：

```json
{ "status": "ready" }
```

或失败：

```json
{ "status": "not ready", "error": "redis: dial tcp 127.0.0.1:6379: connect: connection refused" }
```

**`/metrics` 响应结构**：Prometheus 文本暴露格式（`Content-Type: text/plain; version=0.0.4`），不是 JSON。每行一条样本：

```
# HELP dispatchhub_scheduler_tasks_submitted_total Total number of tasks submitted
# TYPE dispatchhub_scheduler_tasks_submitted_total counter
dispatchhub_scheduler_tasks_submitted_total{queue="default",type="email.send",priority="5"} 12345
```

**`/metrics` 关键指标**（实现见 `pkg/metrics/metrics.go`）：

| 指标 | 类型 | 标签 | 含义 |
|------|------|------|------|
| `dispatchhub_scheduler_tasks_submitted_total` | Counter | queue,type,priority | 累计提交任务数 |
| `dispatchhub_queue_depth` | Gauge | queue,state | 队列深度（如导出） |
| `grpc_server_handled_total` | Counter | method,code | gRPC 已处理 RPC 数（来自 `grpc-prometheus`） |

---

### gRPC API

- 基础地址：`<host>:9090`
- 服务名：`dispatch.v1.DispatchService`
- 协议文件：[`api/proto/dispatch.proto`](../../api/proto/dispatch.proto)
- 反射：已注册 `reflection.Register(srv)`，可用 `grpcurl` 直接调用
- 健康检查：标准 `grpc.health.v1.Health` 协议
- 拦截器链：`grpc-prometheus` → `recovery` → `logging`
- Keepalive：`MaxConnectionIdle=5m`、`MaxConnectionAge=30m`、`Time=10s`、`Timeout=3s`
- `MaxRecvMsgSize` = 16 MiB

#### gRPC 方法一览

| RPC | 对应 HTTP | 说明 |
|-----|-----------|------|
| `SubmitTask` | `POST /api/v1/tasks` | 提交任务 |
| `GetTask` | `GET /api/v1/tasks/{id}` | 查询单个任务 |
| `ListTasks` | `GET /api/v1/tasks` | 任务列表（**比 HTTP 多支持 `state` 过滤**） |
| `CancelTask` | `POST /api/v1/tasks/{id}/cancel` | 取消任务（返回完整 Task 而非状态字符串） |
| `GetQueueStats` | `GET /api/v1/queues/{name}/stats` | 队列统计 |
| `CreateCronJob` | `POST /api/v1/cronjobs` | 创建 CronJob |
| `GetCronJob` | `GET /api/v1/cronjobs/{id}` | 查询 CronJob |
| `ListCronJobs` | `GET /api/v1/cronjobs` | CronJob 列表 |
| `DeleteCronJob` | `DELETE /api/v1/cronjobs/{id}` | 删除 CronJob |

#### 各 RPC 的 Request / Response 字段

> 字段编号即 protobuf field number；类型用 protobuf 表示法。

**SubmitTask**

| Request 字段 | 类型 | 必填 | 说明 |
|--------------|------|------|------|
| `spec` | `TaskSpec` | 是 | 任务规格，结构见下方 [TaskSpec](#消息结构核心) |

| Response 字段 | 类型 | 说明 |
|---------------|------|------|
| `task_id` | string | 服务端生成的任务 ID |
| `task` | `Task` | 完整 Task 对象，结构见 [Task](#消息结构核心) |

**GetTask**

| Request 字段 | 类型 | 必填 | 说明 |
|--------------|------|------|------|
| `task_id` | string | 是 | 任务 UUID |

| Response 字段 | 类型 | 说明 |
|---------------|------|------|
| `task` | `Task` | 完整 Task 对象 |

**ListTasks**

| Request 字段 | 类型 | 必填 | 说明 |
|--------------|------|------|------|
| `namespace` | string | 否 | 命名空间精确匹配 |
| `group` | string | 否 | 分组精确匹配 |
| `type` | string | 否 | 类型精确匹配 |
| `state` | `TaskState` enum | 否 | 状态过滤；传 `TASK_STATE_UNSPECIFIED` 时不过滤（**HTTP 不支持此字段**） |
| `labels` | map\<string,string\> | 否 | proto 字段已定义但当前 server 端**未实现**该过滤 |
| `queue_name` | string | 否 | 队列精确匹配 |
| `limit` | int32 | 否 | 返回条数上限，0=无限制 |
| `offset` | int32 | 否 | 偏移 |

| Response 字段 | 类型 | 说明 |
|---------------|------|------|
| `tasks` | repeated `Task` | 命中的 Task 对象数组 |
| `total` | int64 | 命中条件下的总数 |

**CancelTask**

| Request 字段 | 类型 | 必填 | 说明 |
|--------------|------|------|------|
| `task_id` | string | 是 | 待取消的任务 UUID |

| Response 字段 | 类型 | 说明 |
|---------------|------|------|
| `task` | `Task` | 取消后的 Task 完整对象（与 HTTP 仅返回 `{status:"cancelled"}` 不同） |

**GetQueueStats**

| Request 字段 | 类型 | 必填 | 说明 |
|--------------|------|------|------|
| `queue_name` | string | 是 | 队列名 |

| Response 字段 | 类型 | 说明 |
|---------------|------|------|
| `queue_name` | string | 队列名 |
| `pending` | int64 | 等待调度的任务数 |
| `active` | int64 | 执行中的任务数 |
| `scheduled` | int64 | 延迟队列中尚未到时的任务数 |
| `retrying` | int64 | 当前实现固定为 0 |
| `completed` | int64 | 累计完成数 |
| `failed` | int64 | 累计失败数 |

**CreateCronJob**

| Request 字段 | 类型 | 必填 | 说明 |
|--------------|------|------|------|
| `name` | string | 否 | 可读名 |
| `namespace` | string | 否 | 命名空间 |
| `type` | string | 是 | Handler 类型 |
| `payload` | bytes | 否 | 任务载荷 |
| `labels` | map\<string,string\> | 否 | 标签 |
| `cron_expr` | string | 是 | Cron 表达式 |
| `queue_name` | string | 否 | 默认 `"default"` |
| `priority` | `TaskPriority` enum | 否 | 默认 `TASK_PRIORITY_DEFAULT` |
| `timeout` | `Duration` | 否 | 默认 5 分钟 |
| `max_retries` | int32 | 否 | 默认 3 |
| `retry_backoff` | `Duration` | 否 | 默认 0 |

| Response 字段 | 类型 | 说明 |
|---------------|------|------|
| `cron_job` | `CronJob` | 新建的 CronJob 对象 |

**GetCronJob**

| Request 字段 | 类型 | 必填 | 说明 |
|--------------|------|------|------|
| `id` | string | 是 | CronJob UUID |

| Response 字段 | 类型 | 说明 |
|---------------|------|------|
| `cron_job` | `CronJob` | CronJob 对象 |

**ListCronJobs**

| Request 字段 | 类型 | 必填 | 说明 |
|--------------|------|------|------|
| `namespace` | string | 否 | 命名空间过滤 |
| `limit` | int32 | 否 | 上限 |
| `offset` | int32 | 否 | 偏移 |

| Response 字段 | 类型 | 说明 |
|---------------|------|------|
| `cron_jobs` | repeated `CronJob` | CronJob 数组 |
| `total` | int64 | 总数 |

**DeleteCronJob**

| Request 字段 | 类型 | 必填 | 说明 |
|--------------|------|------|------|
| `id` | string | 是 | CronJob UUID |

| Response 字段 | 类型 | 说明 |
|---------------|------|------|
| — | — | `DeleteCronJobResponse` 为空消息（仅靠 gRPC status 表达成败） |

#### 消息结构（核心）

```protobuf
message TaskSpec {
  string name = 1;
  string namespace = 2;
  string group = 3;
  string type = 4;                                     // 必填
  bytes payload = 5;                                   // gRPC 用 bytes，HTTP 用 JSON
  map<string, string> labels = 6;
  TaskPriority priority = 7;                           // enum
  google.protobuf.Duration delay = 8;
  google.protobuf.Timestamp schedule_at = 9;           // 仅 gRPC 真正读取
  google.protobuf.Duration timeout = 10;
  int32 max_retries = 11;
  google.protobuf.Duration retry_backoff = 12;
  string queue_name = 13;
}

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

完整定义见 [`api/proto/dispatch.proto`](../../api/proto/dispatch.proto)。

#### 调用示例（grpcurl）

提交任务：

```bash
grpcurl -plaintext \
  -d '{
    "spec": {
      "type": "email.send",
      "queue_name": "high-priority",
      "priority": "TASK_PRIORITY_HIGH",
      "max_retries": 3,
      "timeout": "30s",
      "payload": "eyJ0byI6ImFsaWNlQGV4YW1wbGUuY29tIn0="
    }
  }' \
  localhost:9090 dispatch.v1.DispatchService/SubmitTask
```

> `payload` 是 Base64 编码的字节串（对应 proto 的 `bytes` 类型）；上面解码后是 `{"to":"alice@example.com"}`。

响应：

```json
{
  "taskId": "550e8400-e29b-41d4-a716-446655440000",
  "task": {
    "id": "550e8400-e29b-41d4-a716-446655440000",
    "spec": { "type": "email.send", "...": "..." },
    "state": "TASK_STATE_PENDING",
    "createdAt": "2026-05-15T02:00:00.123Z",
    "version": "1"
  }
}
```

按状态过滤任务列表（HTTP 不支持）：

```bash
grpcurl -plaintext \
  -d '{"namespace": "user-service", "state": "TASK_STATE_RUNNING", "limit": 50}' \
  localhost:9090 dispatch.v1.DispatchService/ListTasks
```

#### gRPC 状态码映射

| 业务情况 | gRPC code | 触发位置 |
|----------|-----------|----------|
| 缺 `spec` / `type` / `cron_expr` | `INVALID_ARGUMENT` | 入参校验 |
| Cron 表达式非法 | `INVALID_ARGUMENT` | `CreateCronJob` |
| 任务/CronJob 不存在 | `NOT_FOUND` | `GetTask` / `GetCronJob` |
| 限流、MySQL 写失败、Redis 异常等 | `INTERNAL` | 所有业务方法兜底 |
| panic 被 recovery 捕获 | `INTERNAL` "internal error" | 拦截器 |

> ⚠ 当前实现把限流也映射成 `INTERNAL`（`server.go:137`），按 gRPC 语义应使用 `RESOURCE_EXHAUSTED`。详见[错误响应规范](#错误响应规范)。

#### gRPC 健康检查

```bash
grpc_health_probe -addr=localhost:9090
# 或：
grpcurl -plaintext localhost:9090 grpc.health.v1.Health/Check
```

---

## Scheduler — 运维接口

源码：`cmd/scheduler/main.go:128-150`

Scheduler 是后台调度循环（Leader 选举单活），**没有任何外部业务 API**。它通过 etcd Leader 选举决定哪个副本执行调度循环（`cron_check_interval` 默认 1s），其它副本待命。仅暴露三个 HTTP 端点用于运维：

| 路径 | 方法 | 状态码 | 说明 |
|------|------|--------|------|
| `/healthz` | GET | 200 | 进程存活，固定返回 `{"status":"ok"}` |
| `/readyz` | GET | 200 | 进程就绪，固定返回 `{"status":"ready"}`（**不区分是否为 Leader**，因此 Standby 副本也会返回 ready） |
| `/metrics` | GET | 200 | Prometheus 指标（文本格式） |

**`/healthz` 与 `/readyz` 响应结构**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `status` | string | `/healthz` 固定 `"ok"`；`/readyz` 固定 `"ready"` |

**响应示例**：

```bash
$ curl http://scheduler:8080/healthz
{"status":"ok"}

$ curl http://scheduler:8080/readyz
{"status":"ready"}
```

**Scheduler 关键指标**：

| 指标 | 类型 | 含义 |
|------|------|------|
| `dispatchhub_scheduler_loop_duration_seconds` | Histogram | 单次调度循环耗时 |
| `dispatchhub_scheduler_schedule_latency_seconds` | Histogram | 任务从提交到被调度的延迟 |
| `dispatchhub_scheduler_tasks_scheduled_total{queue,worker}` | Counter | 累计派发到 Worker 的任务数 |
| `dispatchhub_scheduler_leader_elections_total` | Counter | Leader 当选次数（每次重新当选 +1） |

---

## Worker — 运维接口

源码：`cmd/worker/main.go:107-130`

Worker 是无状态执行节点，从 Redis 拉任务、并发执行、回写结果。它**没有任何接收外部请求的业务 API**——任务派发完全通过 Redis 队列、进度通过 etcd 心跳。仅暴露：

| 路径 | 方法 | 状态码 | 说明 |
|------|------|--------|------|
| `/healthz` | GET | 200 | 进程存活，固定返回 `{"status":"ok"}` |
| `/readyz` | GET | 200 | 进程就绪，固定返回 `{"status":"ready"}` |
| `/metrics` | GET | 200 | Prometheus 指标（文本格式） |

**`/healthz` 与 `/readyz` 响应结构**：

| 字段 | 类型 | 说明 |
|------|------|------|
| `status` | string | `/healthz` 固定 `"ok"`；`/readyz` 固定 `"ready"` |

**Worker 关键指标**：

| 指标 | 类型 | 标签 | 含义 |
|------|------|------|------|
| `dispatchhub_worker_tasks_processed_total` | Counter | queue,type,status | 累计处理任务数（status: success/failed/timeout/panic） |
| `dispatchhub_worker_task_duration_seconds` | Histogram | queue,type | Handler 执行耗时 |
| `dispatchhub_worker_active_count` | Gauge | — | 该进程内活跃 Worker 数（通常 1） |
| `dispatchhub_worker_active_tasks` | Gauge | queue | 该进程当前并发执行中的任务数 |
| `dispatchhub_worker_heartbeats_total` | Counter | worker | etcd 心跳次数 |

> Worker 自身在 etcd 注册的服务实例信息（`WorkerInfo`，含 `id`、`hostname`、`ip`、`queues`、`active_tasks`、`completed_total` 等字段）目前**没有 HTTP 接口暴露**，需要直接读 etcd（key 前缀见 `internal/shared/infrastructure/persistence/etcd/worker_registry.go`）。

---

## 枚举与公共类型

### TaskState

| 值 | 名称 | proto enum | 含义 |
|----|------|------------|------|
| 0 | Pending | `TASK_STATE_PENDING` | 已入库，等待调度 |
| 1 | Scheduled | `TASK_STATE_SCHEDULED` | 已分派给 Worker，未开始执行 |
| 2 | Running | `TASK_STATE_RUNNING` | 正在执行 |
| 3 | Retrying | `TASK_STATE_RETRYING` | 失败后等待重试（在 delayed 队列） |
| 4 | Completed | `TASK_STATE_COMPLETED` | 成功结束（终态） |
| 5 | Failed | `TASK_STATE_FAILED` | 重试用尽（终态） |
| 6 | Cancelled | `TASK_STATE_CANCELLED` | 用户取消（终态） |
| 7 | Timeout | `TASK_STATE_TIMEOUT` | 执行超时（终态） |

> proto enum 第 0 项是 `TASK_STATE_UNSPECIFIED`，整数值与 entity 枚举有 +1 偏移；REST API 返回的 `state` 字段是 entity 整数值（Pending=0，…，Timeout=7）。

### TaskPriority

| 名称 | 数值 | proto enum | 说明 |
|------|------|------------|------|
| Low | 1 | `TASK_PRIORITY_LOW` | 后台低优先级 |
| Default | 5 | `TASK_PRIORITY_DEFAULT` | 默认 |
| High | 8 | `TASK_PRIORITY_HIGH` | 业务关键 |
| Critical | 10 | `TASK_PRIORITY_CRITICAL` | 紧急任务（如告警） |

> 数值越大越优先。Redis Sorted Set 的 score 取负值（`-priority`），保证 `ZPOPMIN` 拿到优先级最高的任务。

### Duration 字符串

请求/响应中所有 `*_at` 时间字段是 RFC3339；时长字段（`timeout`、`delay`、`retry_backoff`）使用 Go duration 格式：

| 示例 | 含义 |
|------|------|
| `"30s"` | 30 秒 |
| `"5m"` | 5 分钟 |
| `"1h30m"` | 1 小时 30 分 |
| `"500ms"` | 500 毫秒 |

### CronJob 的 ConcurrencyPolicy

| 取值 | 含义 |
|------|------|
| `"Allow"` | 默认，允许同一 CronJob 的多个实例并发 |
| `"Forbid"` | 上一次还没结束时跳过本轮触发 |

> 当前 HTTP/gRPC API 创建 CronJob 时**未暴露**该字段，固定按 `Allow` 写入。如需 `Forbid` 行为，需直连 MySQL 修改 `concurrency_policy` 列。

---

## 错误响应规范

### HTTP 错误响应

所有错误统一返回 JSON：

```json
{ "error": "<message>" }
```

| 状态码 | 触发条件 |
|--------|----------|
| `400 Bad Request` | 请求体 JSON 解析失败、缺必填字段、cron 表达式非法 |
| `404 Not Found` | 任务/CronJob 不存在 |
| `500 Internal Server Error` | 后端依赖异常、限流拒绝、`RouteValidator` 校验失败 |
| `503 Service Unavailable` | `/readyz` 在依赖不可达时返回 |

### 已知错误码语义问题 ⚠

下面两个映射在语义上不准确，但目前的实现就是如此（在 `test/perf/results/.../REPORT.md` 的性能测试中可以观测到）：

| 实际场景 | 当前返回 | 语义上应该是 |
|----------|----------|--------------|
| `MultiQueueLimiter` 限流拒绝 | HTTP `500` / gRPC `INTERNAL` | HTTP `429 Too Many Requests` / gRPC `RESOURCE_EXHAUSTED` |
| `RouteValidator` 找不到匹配 Worker（无 Worker 订阅该队列+类型组合） | HTTP `500` / gRPC `INTERNAL` | HTTP `422 Unprocessable Entity` 或 `503` / gRPC `FAILED_PRECONDITION` |

客户端在判断"是否可重试"时不应仅看状态码，建议同时解析 `error` 文本中是否含 `rate limit exceeded` / `route validation`。

---

## 相关文档

- [配置参考](configuration.md) — 三个服务的 YAML 配置项与环境变量
- [部署指南](deployment.md) — Helm / Kubernetes 部署
- [Proto 定义](../../api/proto/dispatch.proto) — gRPC 协议源
- [架构设计](../architecture/) — 控制面/数据面分离、Leader 选举、补偿机制
