# DispatchHub

云原生分布式任务调度系统。控制面/数据面分离架构，基于 Redis Sorted Set + Lua 原子脚本实现优先级队列，etcd Leader 选举保证 Scheduler 高可用，Worker 拉模型 + 协程池信号量实现背压控制。

## 架构

```
                       ┌─────────────────┐
                       │  Client / CLI   │
                       └────────┬────────┘
                                │ HTTP / gRPC
                       ┌────────▼────────┐
                       │   API Server    │  无状态网关, Ingress 接入
                       │ :8080  :9090    │  HTTP + gRPC + Metrics(:9091)
                       └────────┬────────┘
                                │
          ┌─────────────────────┼─────────────────────┐
          │                     │                     │
 ┌────────▼─────────┐ ┌────────▼─────────┐ ┌────────▼─────────┐
 │ Scheduler(Leader) │ │ Scheduler(Standby)│ │ Scheduler(Standby)│
 │ 延迟晋升/补偿/cron │ │ 等待选举          │ │ 等待选举          │
 │ 仅暴露 :8080 运维  │ └──────────────────┘ └──────────────────┘
 └────────┬─────────┘
          │  etcd Leader Election (15s TTL)
 ┌────────┴──────────────┐
 │  Redis Queue Broker   │  Sorted Set 优先级队列 + 延迟队列
 └────────┬──────────────┘
          │  Pull (背压)
  ┌───────┼───────┼───────┐
  │       │       │       │
┌─▼──┐ ┌─▼──┐ ┌─▼──┐ ┌─▼──┐
│ W1 │ │ W2 │ │ W3 │ │ WN │  HPA 3-50 副本
└─┬──┘ └─┬──┘ └─┬──┘ └─┬──┘
  │      │      │      │
  └──────┴──┬───┴──────┘
            │ Heartbeat
     ┌──────▼──────┐   ┌────────┐
     │    etcd     │   │ MySQL  │
     │  注册 & 选举  │   │ 持久化  │
     └─────────────┘   └────────┘
```

**组件职责**

| 组件 | 角色 | K8s 部署 | 端口 |
|------|------|----------|------|
| API Server | 唯一外部入口，无状态 HTTP/gRPC 网关 | 2+ replicas, Ingress | :8080 HTTP, :9090 gRPC, :9091 metrics |
| Scheduler | Leader 选举单活，运行后台调度循环 | 3 replicas, pod anti-affinity | :8080 (healthz/readyz/metrics) |
| Worker | 拉模型执行引擎，完全无状态 | 5 replicas + HPA(3-50) | :8080 ops, :9091 metrics |
| Redis | Sorted Set 优先级队列 + Lua 原子脚本 | - | - |
| etcd | Leader 选举 + Worker 服务注册发现 | - | - |
| MySQL | 任务状态持久化 + 乐观锁并发控制 | - | - |

## 项目结构

```
dispatchhub/
├── cmd/{apiserver,scheduler,worker}/   # 三个进程入口
├── internal/
│   ├── shared/                         # 跨服务共享
│   │   ├── domain/
│   │   │   ├── entity/                 # Task, Worker, CronJob, Queue
│   │   │   ├── repository/             # 仓储接口 (QueueBroker, TaskStore, WorkerRegistry)
│   │   │   └── service/                # 领域服务接口
│   │   └── infrastructure/
│   │       ├── config/                 # 配置管理
│   │       ├── version/                # 版本信息 (ldflags 注入)
│   │       └── persistence/
│   │           ├── mysql/              # TaskStore 实现
│   │           ├── redis/              # QueueBroker 实现 (Lua 脚本)
│   │           └── etcd/               # WorkerRegistry 实现
│   ├── apiserver/                      # API Server
│   │   ├── domain/service/             # TaskServiceImpl + Hooks
│   │   └── interfaces/{grpc,http}/     # gRPC + REST 接口
│   ├── scheduler/                      # Scheduler
│   │   ├── domain/service/             # SchedulerService (纯领域逻辑)
│   │   ├── application/                # 应用层编排 (reconciliation loops)
│   │   └── infrastructure/election/    # etcd Leader 选举
│   └── worker/                         # Worker
│       ├── application/service/        # 执行引擎 + 背压控制
│       └── interfaces/middleware/      # Recovery / Logging / Timeout
├── pkg/{log,metrics,ratelimit,retry,signals,cronutil}/
├── api/proto/                          # Protobuf 定义
├── deploy/{kubernetes,helm}/           # K8s YAML + Helm Chart
├── config/                             # 各服务配置文件
│   ├── apiserver.yaml
│   ├── scheduler.yaml
│   └── worker.yaml
├── Dockerfile                          # 多阶段构建
└── Makefile                            # 构建自动化
```

## 构建与部署

**前置依赖:** Go 1.25+, Redis 7.0+, etcd 3.5+, MySQL 8.0+

```bash
# 编译 -- 产出 bin/apiserver, bin/scheduler, bin/worker
make build

# 运行测试
make test          # 全量 (含 -race)
make test-unit     # 仅单元测试 (-short)
make lint          # golangci-lint

# 生成 protobuf 代码
make proto

# Docker 多阶段构建 (golang:1.25-alpine, CGO_ENABLED=0, ldflags 注入版本)
make docker        # 构建全部镜像
make push          # 推送全部镜像

# 本地运行 (需本地 Redis/etcd/MySQL)
make run-scheduler
make run-worker
make run-apiserver

# Kubernetes 部署
make helm-install                      # Helm
kubectl apply -f deploy/kubernetes/    # 或原生 YAML
```

**Docker 构建细节:** `FROM golang:1.25-alpine` 多阶段构建，`CGO_ENABLED=0`，通过 `--build-arg` 传入 VERSION/GIT_COMMIT/BUILD_DATE，`-ldflags` 注入 `version` 包。运行时镜像 `alpine:3.19`，非 root 用户运行。

## 配置

三个服务使用独立的配置文件，支持环境变量覆盖 (`DISPATCH_MYSQL_DSN`, `DISPATCH_REDIS_PASSWORD` 等)。

```bash
./apiserver  --config=config/apiserver.yaml
./scheduler  --config=config/scheduler.yaml
./worker     --config=config/worker.yaml
```

每个配置文件只包含该服务需要的配置项（如 Worker 配置无 `scheduler` 段，API Server 配置无 `worker` 段），共享基础设施段（redis、mysql、etcd、log、metrics）。详见 [配置参考文档](docs/configuration.md)。

## API 示例

```bash
# 提交即时任务
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "send-welcome-email",
    "type": "email.send",
    "queue_name": "default",
    "priority": 8,
    "max_retries": 3,
    "timeout": "30s",
    "payload": {"to": "user@example.com", "template": "welcome"}
  }'

# 查询任务状态
curl http://localhost:8080/api/v1/tasks/{task_id}

# 队列统计
curl http://localhost:8080/api/v1/queues/default/stats
```

## 技术栈

| 类别 | 选型 |
|------|------|
| 语言 | Go 1.25 |
| 接口 | gRPC + HTTP/REST, proto codegen |
| 队列 | Redis Sorted Set + Lua 原子脚本 |
| 协调 | etcd (concurrency.Election + Watch) |
| 持久化 | MySQL + GORM |
| 监控 | Prometheus + Grafana |
| 日志 | Zap 结构化 JSON |
| 容器化 | Docker 多阶段构建, K8s + Helm + HPA |

## 文档

| 文档 | 内容 |
|------|------|
| [架构设计](docs/architecture.md) | 整体架构与设计决策 |
| [核心组件](docs/components.md) | Scheduler / Worker / 存储层 |
| [数据模型](docs/data-models.md) | Task / Worker / CronJob 数据结构与状态机 |
| [存储层设计](docs/storage.md) | MySQL / Redis / etcd 选型与容量规划 |
| [队列选型分析](docs/queue-selection.md) | 6 种 MQ 方案对比与决策推导 |
| [队列设计](docs/queue-design.md) | 优先级队列 / 延迟队列 / Lua 脚本详解 |
| [API 参考](docs/api-reference.md) | REST + gRPC 接口文档 |
| [部署指南](docs/deployment.md) | K8s 部署 / Helm Chart / 运维手册 |
| [配置参考](docs/configuration.md) | 全部配置项说明 |
| [面试指南](docs/interview.md) | 项目介绍与常见追问 |

## License

MIT
