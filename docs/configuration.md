# 配置参考

> 源码：`internal/shared/infrastructure/config/`

## 配置架构

DispatchHub 的三个服务使用**独立的配置文件**，每个服务只包含自己需要的配置项：

| 服务 | 配置类型 | 默认函数 | 加载函数 | 示例文件 |
|------|---------|---------|---------|---------|
| API Server | `APIServerConfig` | `DefaultAPIServerConfig()` | `LoadAPIServerConfig()` | `config/apiserver.yaml` |
| Scheduler | `SchedulerServiceConfig` | `DefaultSchedulerServiceConfig()` | `LoadSchedulerServiceConfig()` | `config/scheduler.yaml` |
| Worker | `WorkerServiceConfig` | `DefaultWorkerServiceConfig()` | `LoadWorkerServiceConfig()` | `config/worker.yaml` |

共享的基础设施类型（`RedisConfig`、`MySQLConfig`、`EtcdConfig`、`LogConfig`、`MetricsConfig`）定义在 `config.go` 中，各服务配置按需组合引用。

## 配置加载方式

每个服务通过 `--config` 参数指定各自的配置文件：

```bash
# 使用各自的配置文件
./apiserver  --config=/etc/dispatchhub/apiserver.yaml
./scheduler  --config=/etc/dispatchhub/scheduler.yaml
./worker     --config=/etc/dispatchhub/worker.yaml

# 使用默认配置（不指定 --config）
./apiserver
```

配置文件与默认值合并：文件中指定的字段覆盖默认值，未指定的字段使用默认值。

---

## 各服务配置项

### API Server

> 源码：`config/apiserver.go`

API Server 需要：server（gRPC + HTTP）、etcd（路由校验）、redis（队列操作）、mysql（持久化）。

```yaml
server:
  grpc_addr: ":9090"        # gRPC 监听地址
  http_addr: ":8080"        # HTTP 监听地址

etcd:
  endpoints:
    - localhost:2379
  dial_timeout: 5s

redis:
  addr: localhost:6379
  pool_size: 100

mysql:
  dsn: "root:@tcp(localhost:3306)/dispatchhub?charset=utf8mb4&parseTime=true&loc=Local"
  max_open_conns: 50

metrics:
  enabled: true
  addr: ":9091"
  path: "/metrics"

log:
  level: info
  format: json
  output: stdout
```

### Scheduler

> 源码：`config/scheduler.go`

Scheduler 需要：server（仅 HTTP 运维端点）、scheduler（选举和调度参数）、etcd（选举 + Worker 拓扑）、redis（队列操作）、mysql（任务补偿）。

```yaml
server:
  http_addr: ":8080"        # 仅 HTTP 运维端点，无 gRPC

scheduler:
  lease_duration: 15s       # Leader Lease 有效期
  cron_check_interval: 1s   # CronJob 检查间隔
  cron_batch_size: 100      # 每次最多触发的 CronJob 数量

etcd:
  endpoints:
    - localhost:2379
  dial_timeout: 5s

redis:
  addr: localhost:6379
  pool_size: 100

mysql:
  dsn: "root:@tcp(localhost:3306)/dispatchhub?charset=utf8mb4&parseTime=true&loc=Local"
  max_open_conns: 50

metrics:
  enabled: true
  addr: ":9091"
  path: "/metrics"

log:
  level: info
  format: json
  output: stdout
```

**调优建议**：

- `lease_duration`：越小故障切换越快，但对 etcd 压力越大。推荐 10s~30s

### Worker

> 源码：`config/worker.go`

Worker 需要：server（仅 HTTP 运维端点）、worker（队列订阅和并发参数）、etcd（注册/心跳）、redis（出队/确认）、mysql（状态读写）。

```yaml
server:
  http_addr: ":8080"        # 仅 HTTP 运维端点，无 gRPC

worker:
  id: ""                    # 留空自动生成 {hostname}-{uuid8}
  queues:
    - high-priority         # 顺序决定消费优先级
    - default
    - batch
  concurrency: 100          # 最大并发任务数
  heartbeat_interval: 5s    # 心跳发送间隔
  shutdown_timeout: 30s     # 优雅停机等待超时
  task_timeout: 5m          # 任务默认执行超时

etcd:
  endpoints:
    - localhost:2379
  dial_timeout: 5s

redis:
  addr: localhost:6379
  pool_size: 100

mysql:
  dsn: "root:@tcp(localhost:3306)/dispatchhub?charset=utf8mb4&parseTime=true&loc=Local"
  max_open_conns: 50

metrics:
  enabled: true
  addr: ":9091"
  path: "/metrics"

log:
  level: info
  format: json
  output: stdout
```

**调优建议**：

- `concurrency`：取决于任务类型。CPU 密集型设为 CPU 核数；IO 密集型可设为 100~1000
- `queues`：顺序决定消费优先级，排在前面的队列优先被检查
- `shutdown_timeout`：应大于最长任务的执行时间
- `heartbeat_interval`：推荐 `lease_duration / 3`

---

## 共享配置项详解

### server - 服务端口

| 字段 | 类型 | 默认值 | 使用服务 | 说明 |
|------|------|--------|---------|------|
| `grpc_addr` | string | `:9090` | API Server | gRPC 监听地址 |
| `http_addr` | string | `:8080` | 全部 | HTTP 监听地址 |

### etcd - etcd 连接配置

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `endpoints` | []string | `["localhost:2379"]` | etcd 节点地址列表 |
| `dial_timeout` | duration | `5s` | 连接超时 |
| `username` | string | `""` | 认证用户名 |
| `password` | string | `""` | 认证密码 |
| `tls_cert` | string | `""` | TLS 客户端证书路径 |
| `tls_key` | string | `""` | TLS 客户端私钥路径 |
| `tls_ca` | string | `""` | TLS CA 证书路径 |

### redis - Redis 连接配置

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `addr` | string | `localhost:6379` | 单机模式地址 |
| `password` | string | `""` | 认证密码 |
| `db` | int | `0` | 数据库编号（单机模式） |
| `pool_size` | int | `100` | 连接池大小 |
| `min_idle_conns` | int | `10` | 最小空闲连接数 |
| `dial_timeout` | duration | `5s` | 连接超时 |
| `read_timeout` | duration | `3s` | 读超时 |
| `write_timeout` | duration | `3s` | 写超时 |
| `cluster_mode` | bool | `false` | 是否使用 Cluster 模式 |
| `cluster_addrs` | []string | `[]` | Cluster 节点地址列表 |

**单机模式**：

```yaml
redis:
  addr: localhost:6379
  pool_size: 100
```

**Cluster 模式**：

```yaml
redis:
  cluster_mode: true
  cluster_addrs:
    - redis-0:6379
    - redis-1:6379
    - redis-2:6379
```

**调优建议**：

- `pool_size`：建议设为 Worker concurrency 的 1~2 倍
- `min_idle_conns`：设为 `pool_size` 的 10%，避免冷启动延迟
- 生产环境推荐使用 Cluster 模式，提供更高的吞吐和可用性

### mysql - MySQL 连接配置

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `dsn` | string | `root:@tcp(localhost:3306)/dispatchhub?...` | 数据源名称 |
| `max_open_conns` | int | `50` | 最大打开连接数 |
| `max_idle_conns` | int | `10` | 最大空闲连接数 |
| `conn_max_lifetime` | duration | `1h` | 连接最大存活时间 |

**DSN 参数说明**：

| 参数 | 推荐值 | 说明 |
|------|--------|------|
| `charset` | `utf8mb4` | 完整 UTF-8 支持 |
| `parseTime` | `true` | 自动解析 TIME/DATE 列为 time.Time |
| `loc` | `Local` | 使用本地时区 |

**调优建议**：

- `max_open_conns`：设为预期并发查询数。过大会导致 MySQL 连接数耗尽
- `max_idle_conns`：设为 `max_open_conns` 的 20%~50%
- `conn_max_lifetime`：设为小于 MySQL `wait_timeout`（默认 8h），推荐 1h

### metrics - 指标导出配置

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `enabled` | bool | `true` | 是否启用 Prometheus 指标 |
| `addr` | string | `:9091` | 指标 HTTP 监听地址 |
| `path` | string | `/metrics` | 指标端点路径 |

### log - 日志配置

| 字段 | 类型 | 默认值 | 可选值 | 说明 |
|------|------|--------|--------|------|
| `level` | string | `info` | `debug` / `info` / `warn` / `error` | 日志级别 |
| `format` | string | `json` | `json` / `console` | 输出格式 |
| `output` | string | `stdout` | `stdout` / `stderr` / 文件路径 | 输出目标 |

**格式对比**：

JSON 格式（生产推荐）：
```json
{"level":"info","ts":"2024-01-15T10:30:00.000Z","caller":"scheduler/scheduler.go:95","msg":"task submitted","task_id":"abc123","type":"email.send"}
```

Console 格式（开发调试）：
```
2024-01-15T10:30:00.000Z  INFO  scheduler/scheduler.go:95  task submitted  {"task_id":"abc123","type":"email.send"}
```

---

## 环境变量覆盖

以下环境变量可覆盖配置文件中的对应字段（所有服务通用）：

| 环境变量 | 覆盖字段 |
|---------|---------|
| `DISPATCH_GRPC_ADDR` | `server.grpc_addr` |
| `DISPATCH_HTTP_ADDR` | `server.http_addr` |
| `DISPATCH_REDIS_ADDR` | `redis.addr` |
| `DISPATCH_REDIS_PASSWORD` | `redis.password` |
| `DISPATCH_MYSQL_DSN` | `mysql.dsn` |
| `DISPATCH_ETCD_ENDPOINTS` | `etcd.endpoints`（逗号分隔） |
| `DISPATCH_LOG_LEVEL` | `log.level` |

在 Kubernetes 中，推荐使用 ConfigMap 挂载配置文件，敏感信息（密码等）通过 Secret 注入为环境变量：

```yaml
# Pod spec 示例
containers:
  - name: apiserver
    command: ["./apiserver", "--config=/etc/dispatchhub/apiserver.yaml"]
    volumeMounts:
      - name: config
        mountPath: /etc/dispatchhub
    env:
      - name: DISPATCH_MYSQL_DSN
        valueFrom:
          secretKeyRef:
            name: dispatchhub-secrets
            key: mysql-dsn
volumes:
  - name: config
    configMap:
      name: dispatchhub-apiserver-config
```

---

## 各服务配置依赖矩阵

| 配置项 | API Server | Scheduler | Worker |
|--------|:---:|:---:|:---:|
| `server.grpc_addr` | Y | - | - |
| `server.http_addr` | Y | Y | Y |
| `scheduler.*` | - | Y | - |
| `worker.*` | - | - | Y |
| `etcd.*` | Y | Y | Y |
| `redis.*` | Y | Y | Y |
| `mysql.*` | Y | Y | Y |
| `metrics.*` | Y | Y | Y |
| `log.*` | Y | Y | Y |
