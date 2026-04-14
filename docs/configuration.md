# 配置参考

> 源码：`internal/shared/infrastructure/config/config.go`

## 配置加载方式

DispatchHub 支持两种配置方式：

1. **YAML 文件**：通过 `--config` 参数指定
2. **默认值**：未指定配置文件时使用内置默认值

```bash
# 使用配置文件
./scheduler --config=/etc/dispatchhub/config.yaml

# 使用默认配置
./scheduler
```

配置文件与默认值合并：文件中指定的字段覆盖默认值，未指定的字段使用默认值。

---

## 完整配置项

### server - 服务端口

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `grpc_addr` | string | `:9090` | gRPC 监听地址 |
| `http_addr` | string | `:8080` | HTTP 监听地址 |

```yaml
server:
  grpc_addr: ":9090"
  http_addr: ":8080"
```

---

### scheduler - 调度器配置

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `lease_duration` | duration | `15s` | Leader Lease 有效期 |

```yaml
scheduler:
  lease_duration: 15s
```

**调优建议**：

- `lease_duration`：越小故障切换越快，但对 etcd 压力越大。推荐 10s~30s

---

### worker - Worker 配置

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `id` | string | `{hostname}-{uuid8}` | Worker 唯一标识，自动生成 |
| `queues` | []string | `["default"]` | 订阅的队列列表 |
| `concurrency` | int | `CPU核数 * 10` | 最大并发任务数 |
| `heartbeat_interval` | duration | `5s` | 心跳发送间隔 |
| `shutdown_timeout` | duration | `30s` | 优雅停机等待超时 |
| `task_timeout` | duration | `5m` | 任务默认执行超时 |

```yaml
worker:
  id: ""                        # 留空自动生成
  queues:
    - high-priority
    - default
    - batch
  concurrency: 100
  heartbeat_interval: 5s
  shutdown_timeout: 30s
  task_timeout: 5m
```

**调优建议**：

- `concurrency`：取决于任务类型。CPU 密集型设为 CPU 核数；IO 密集型可设为 100~1000
- `queues`：顺序决定消费优先级，排在前面的队列优先被检查
- `shutdown_timeout`：应大于最长任务的执行时间
- `heartbeat_interval`：推荐 `lease_duration / 3`

---

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

```yaml
etcd:
  endpoints:
    - etcd-0.etcd-headless:2379
    - etcd-1.etcd-headless:2379
    - etcd-2.etcd-headless:2379
  dial_timeout: 5s
  username: ""
  password: ""
  tls_cert: ""
  tls_key: ""
  tls_ca: ""
```

---

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
  password: ""
  db: 0
  pool_size: 100
  min_idle_conns: 10
  dial_timeout: 5s
  read_timeout: 3s
  write_timeout: 3s
```

**Cluster 模式**：

```yaml
redis:
  cluster_mode: true
  cluster_addrs:
    - redis-0:6379
    - redis-1:6379
    - redis-2:6379
    - redis-3:6379
    - redis-4:6379
    - redis-5:6379
  password: ""
  pool_size: 100
  min_idle_conns: 10
```

**调优建议**：

- `pool_size`：建议设为 Worker concurrency 的 1~2 倍
- `min_idle_conns`：设为 `pool_size` 的 10%，避免冷启动延迟
- 生产环境推荐使用 Cluster 模式，提供更高的吞吐和可用性

---

### mysql - MySQL 连接配置

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `dsn` | string | `root:@tcp(localhost:3306)/dispatchhub?...` | 数据源名称 |
| `max_open_conns` | int | `50` | 最大打开连接数 |
| `max_idle_conns` | int | `10` | 最大空闲连接数 |
| `conn_max_lifetime` | duration | `1h` | 连接最大存活时间 |

```yaml
mysql:
  dsn: "user:password@tcp(mysql-host:3306)/dispatchhub?charset=utf8mb4&parseTime=true&loc=Local"
  max_open_conns: 50
  max_idle_conns: 10
  conn_max_lifetime: 1h
```

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

---

### metrics - 指标导出配置

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `enabled` | bool | `true` | 是否启用 Prometheus 指标 |
| `addr` | string | `:9091` | 指标 HTTP 监听地址 |
| `path` | string | `/metrics` | 指标端点路径 |

```yaml
metrics:
  enabled: true
  addr: ":9091"
  path: "/metrics"
```

---

### log - 日志配置

| 字段 | 类型 | 默认值 | 可选值 | 说明 |
|------|------|--------|--------|------|
| `level` | string | `info` | `debug` / `info` / `warn` / `error` | 日志级别 |
| `format` | string | `json` | `json` / `console` | 输出格式 |
| `output` | string | `stdout` | `stdout` / `stderr` / 文件路径 | 输出目标 |

```yaml
log:
  level: info
  format: json
  output: stdout
```

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

## 完整示例配置

```yaml
server:
  grpc_addr: ":9090"
  http_addr: ":8080"

scheduler:
  lease_duration: 15s

worker:
  queues:
    - high-priority
    - default
    - batch
  concurrency: 100
  heartbeat_interval: 5s
  shutdown_timeout: 30s
  task_timeout: 5m

etcd:
  endpoints:
    - etcd-0:2379
    - etcd-1:2379
    - etcd-2:2379
  dial_timeout: 5s

redis:
  cluster_mode: true
  cluster_addrs:
    - redis-0:6379
    - redis-1:6379
    - redis-2:6379
  pool_size: 100
  min_idle_conns: 10
  dial_timeout: 5s
  read_timeout: 3s
  write_timeout: 3s

mysql:
  dsn: "dispatchhub:password@tcp(mysql:3306)/dispatchhub?charset=utf8mb4&parseTime=true&loc=Local"
  max_open_conns: 50
  max_idle_conns: 10
  conn_max_lifetime: 1h

metrics:
  enabled: true
  addr: ":9091"
  path: "/metrics"

log:
  level: info
  format: json
  output: stdout
```

---

## 环境变量

当前版本配置通过 YAML 文件加载。在 Kubernetes 中，推荐使用 ConfigMap 挂载配置文件，敏感信息（密码等）通过 Secret 注入为环境变量后在 DSN 中引用。

```yaml
# ConfigMap 中的 config.yaml
mysql:
  dsn: "${MYSQL_USER}:${MYSQL_PASSWORD}@tcp(mysql:3306)/dispatchhub?..."

# Pod 中注入环境变量
env:
  - name: MYSQL_PASSWORD
    valueFrom:
      secretKeyRef:
        name: dispatchhub-secrets
        key: mysql-password
```
