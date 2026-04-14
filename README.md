# DispatchHub

云原生分布式任务调度系统，专为高并发、高可用、高性能场景设计。

## 系统简介

DispatchHub 是一个生产级别的分布式任务调度平台，参考了 Kubernetes Scheduler、Temporal、Asynq 等知名开源项目的设计理念，采用控制面/数据面分离架构，具备以下核心能力：

- **高并发**：基于 Redis Sorted Set 的优先级队列 + Lua 原子脚本，Worker 协程池背压控制
- **高可用**：etcd Leader 选举实现 Scheduler 多副本热备，Worker 心跳检测与自动摘除
- **高性能**：gRPC 长连接通信，延迟任务定时晋升，批量状态转换

## 架构总览

```
                         ┌──────────────────────┐
                         │    Client / CLI       │
                         └──────────┬───────────┘
                                    │ HTTP/gRPC
                         ┌──────────▼───────────┐
                         │     API Server        │  ← 无状态网关, 可水平扩展
                         │  (REST + gRPC + Metrics)│
                         └──────────┬───────────┘
                                    │
              ┌─────────────────────┼─────────────────────┐
              │                     │                     │
     ┌────────▼─────────┐ ┌────────▼─────────┐ ┌────────▼─────────┐
     │  Scheduler (Leader)│ │ Scheduler (Standby)│ │ Scheduler (Standby)│
     │  - 任务分发        │ │  - 等待选举        │ │  - 等待选举        │
     │  - 延迟晋升        │ └──────────────────┘ └──────────────────┘
     │  - 健康检查        │
     └────────┬─────────┘
              │  etcd Leader Election
    ┌─────────┼──────────────┐
    │   Redis Queue Broker   │  ← 优先级队列 + 延迟队列
    └─────────┬──────────────┘
              │  Dequeue (背压)
   ┌──────────┼──────────┼──────────┐
   │          │          │          │
┌──▼──┐  ┌───▼──┐  ┌───▼──┐  ┌───▼──┐
│Worker│  │Worker│  │Worker│  │Worker│  ← HPA 自动伸缩
└──────┘  └──────┘  └──────┘  └──────┘
    │         │         │         │
    └─────────┴────┬────┴─────────┘
                   │  Heartbeat
              ┌────▼─────┐    ┌────────┐
              │   etcd   │    │  MySQL  │  ← 持久化存储
              │ (注册中心) │    │ (任务状态) │
              └──────────┘    └────────┘
```

## 核心组件

| 组件 | 角色 | 说明 |
|------|------|------|
| **Scheduler** | 控制面 | Leader 选举保证单活，负责任务入队、延迟晋升、Worker 健康检查 |
| **Worker** | 数据面 | 从队列拉取任务执行，协程池限流，心跳上报，优雅停机 |
| **API Server** | 接入层 | 无状态 HTTP/gRPC 网关，可独立水平扩展 |
| **Redis** | 快速队列 | Sorted Set 优先级队列，Lua 原子出队，延迟任务暂存 |
| **etcd** | 协调层 | Leader 选举、Worker 服务注册与发现、Watch 拓扑变更 |
| **MySQL** | 持久层 | 任务状态持久化、乐观锁并发控制、事件审计 |

## 项目结构

```
dispatchhub/
├── cmd/
│   ├── apiserver/              # API 网关入口
│   ├── scheduler/              # 调度器入口
│   └── worker/                 # 工作节点入口
├── internal/
│   ├── shared/                 # 跨服务共享
│   │   ├── domain/
│   │   │   ├── entity/         # 实体 & 值对象 (Task, Worker, Queue)
│   │   │   ├── repository/     # 仓储接口 (TaskRepository, QueueBroker, WorkerRegistry)
│   │   │   └── service/        # 服务接口 (TaskService)
│   │   └── infrastructure/
│   │       ├── config/         # 配置管理
│   │       ├── version/        # 版本信息
│   │       └── persistence/
│   │           ├── mysql/      # MySQL 持久化实现
│   │           ├── redis/      # Redis 队列实现
│   │           └── etcd/       # etcd 注册中心实现
│   ├── apiserver/              # API Server 服务
│   │   └── interfaces/
│   │       ├── grpc/           # gRPC 接口
│   │       └── http/           # REST API + 健康检查
│   ├── scheduler/              # Scheduler 服务
│   │   ├── domain/service/     # 调度领域服务 (核心算法)
│   │   ├── application/        # 应用编排 (reconciliation loops)
│   │   ├── infrastructure/
│   │   │   └── election/       # etcd Leader 选举
│   │   └── interfaces/
│   │       ├── grpc/           # gRPC 接口
│   │       └── http/           # REST API
│   └── worker/                 # Worker 服务
│       ├── application/service/# Worker 执行引擎
│       └── interfaces/
│           └── middleware/     # 中间件 (日志/恢复/超时)
├── pkg/                        # 通用工具包
│   ├── log/                    # 结构化日志
│   ├── metrics/                # Prometheus 指标
│   ├── ratelimit/              # 令牌桶限流器
│   ├── retry/                  # 指数退避重试
│   └── signals/                # 信号处理
├── api/proto/                  # Protobuf 定义
├── deploy/
│   ├── kubernetes/             # K8s 原生 YAML
│   └── helm/                   # Helm Chart
├── config.yaml                 # 示例配置
├── Dockerfile                  # 多阶段构建
└── Makefile                    # 构建自动化
```

## 快速开始

### 前置依赖

- Go 1.22+
- Redis 7.0+
- etcd 3.5+
- MySQL 8.0+

### 本地构建

```bash
# 编译所有组件
make build

# 运行调度器
make run-scheduler

# 运行 Worker
make run-worker

# 运行 API Server
make run-apiserver
```

### Docker 构建

```bash
# 构建所有镜像
make docker

# 推送镜像
make push
```

### Kubernetes 部署

```bash
# 使用 Helm
make helm-install

# 或直接使用 kubectl
kubectl apply -f deploy/kubernetes/
```

### 提交任务示例

```bash
# 通过 HTTP API 提交任务
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

# 查看队列统计
curl http://localhost:8080/api/v1/queues/default/stats
```

## 文档目录

| 文档 | 内容 |
|------|------|
| [架构设计](docs/architecture.md) | 整体架构、三高设计方案、设计决策 |
| [核心组件](docs/components.md) | Scheduler、Worker、存储层详细说明 |
| [数据模型](docs/data-models.md) | Task/Worker/Queue 数据结构、状态机 |
| [存储层设计](docs/storage.md) | 表结构、Redis/etcd/MySQL 选型、容量规划、归档策略 |
| [队列选型分析](docs/queue-selection.md) | 6 种 MQ 方案逐一对照分析、决策推导过程 |
| [为什么不用 MySQL 做队列](docs/why-not-mysql-queue.md) | FOR UPDATE 锁机制、并发出队问题、性能天花板分析 |
| [API 参考](docs/api-reference.md) | REST API 和 gRPC 接口完整文档 |
| [队列设计](docs/queue-design.md) | 优先级队列、延迟队列、Lua 脚本详解 |
| [部署指南](docs/deployment.md) | Kubernetes 部署、Helm Chart、运维手册 |
| [配置参考](docs/configuration.md) | 全部配置项说明与默认值 |

## 技术栈

| 类别 | 技术选型 |
|------|----------|
| 语言 | Go 1.22 |
| RPC | gRPC + HTTP/REST |
| 队列 | Redis Sorted Set + Lua |
| 协调 | etcd (Leader Election + Service Discovery) |
| 持久化 | MySQL + GORM |
| 监控 | Prometheus + Grafana |
| 日志 | Zap (结构化 JSON) |
| 容器化 | Docker 多阶段构建 |
| 编排 | Kubernetes + Helm + HPA |

## License

MIT
