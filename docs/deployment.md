# 部署指南

## 部署架构

```
                    ┌─────────────┐
                    │   Ingress   │
                    │  (Nginx)    │
                    └──────┬──────┘
                           │
                    ┌──────▼──────┐
                    │ API Server  │  Deployment (replicas: 2)
                    │  :8080/:9090│
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
       ┌──────▼──────┐    │     ┌──────▼──────┐
       │  Scheduler  │    │     │   Worker    │  Deployment + HPA
       │ (3 replicas)│    │     │ (3~50 pods) │
       └──────┬──────┘    │     └──────┬──────┘
              │            │            │
         ┌────▼────┐  ┌───▼───┐  ┌────▼────┐
         │  etcd   │  │ Redis │  │  MySQL  │
         │(3-node) │  │Cluster│  │         │
         └─────────┘  └───────┘  └─────────┘
```

## 前置依赖

| 组件 | 最低版本 | 推荐配置 |
|------|---------|---------|
| Go | 1.22+ | 编译用 |
| Redis | 7.0+ | 单机或 Cluster 模式 |
| etcd | 3.5+ | 3 节点集群 |
| MySQL | 8.0+ | 主从/MGR |
| Kubernetes | 1.26+ | 生产部署 |
| Helm | 3.0+ | Chart 部署 |

## 本地开发部署

### 编译

```bash
# 编译全部组件
make build

# 编译产物
ls bin/
# scheduler  worker  apiserver
```

### 启动基础设施

使用 Docker Compose 快速启动依赖：

```bash
docker run -d --name etcd \
  -p 2379:2379 \
  quay.io/coreos/etcd:v3.5.9 \
  etcd --advertise-client-urls=http://0.0.0.0:2379 \
       --listen-client-urls=http://0.0.0.0:2379

docker run -d --name redis \
  -p 6379:6379 \
  redis:7-alpine

docker run -d --name mysql \
  -p 3306:3306 \
  -e MYSQL_ROOT_PASSWORD=root \
  -e MYSQL_DATABASE=dispatchhub \
  mysql:8.0
```

### 启动服务

```bash
# 终端 1: 启动调度器
make run-scheduler

# 终端 2: 启动 Worker
make run-worker

# 终端 3: 启动 API Server（可选，scheduler 已包含 API）
make run-apiserver
```

### 验证

```bash
# 健康检查
curl http://localhost:8080/healthz

# 提交测试任务
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{"type":"example.echo","payload":{"msg":"hello"}}'

# 查看 Prometheus 指标
curl http://localhost:9091/metrics
```

---

## Docker 构建

### 构建镜像

```bash
# 构建所有组件
make docker

# 单独构建
make docker-scheduler
make docker-worker
make docker-apiserver
```

### Dockerfile 特点

- **多阶段构建**：编译阶段使用 `golang:1.22-alpine`，运行阶段使用 `alpine:3.19`
- **静态二进制**：`CGO_ENABLED=0`，无外部依赖
- **非 root 用户**：使用 `dispatchhub` 用户运行
- **版本注入**：通过 `--build-arg` 注入 Version/GitCommit/BuildDate
- **依赖缓存**：先 COPY go.mod/go.sum 下载依赖，利用 Docker 构建缓存

### 镜像大小

| 组件 | 预估大小 |
|------|---------|
| scheduler | ~30MB |
| worker | ~30MB |
| apiserver | ~30MB |

---

## Kubernetes 部署

### 方式一：kubectl 直接部署

```bash
# 创建命名空间和基础资源
kubectl apply -f deploy/kubernetes/namespace.yaml
kubectl apply -f deploy/kubernetes/configmap.yaml

# 部署 Scheduler（3 副本 + Leader 选举）
kubectl apply -f deploy/kubernetes/scheduler-deployment.yaml

# 部署 Worker（5 副本 + HPA）
kubectl apply -f deploy/kubernetes/worker-deployment.yaml
```

### 方式二：Helm Chart 部署

```bash
# 安装
helm upgrade --install dispatchhub deploy/helm/dispatchhub \
  --namespace dispatchhub \
  --create-namespace

# 自定义配置
helm upgrade --install dispatchhub deploy/helm/dispatchhub \
  --namespace dispatchhub \
  --set worker.replicas=10 \
  --set worker.config.concurrency=200 \
  --set redis.addr=redis-master:6379

# 查看模板（dry run）
make helm-template
```

### Scheduler 部署细节

```yaml
apiVersion: apps/v1
kind: Deployment
spec:
  replicas: 3                          # 3 副本实现高可用
  template:
    spec:
      terminationGracePeriodSeconds: 30
      containers:
        - name: scheduler
          ports:
            - containerPort: 8080      # HTTP API
            - containerPort: 9090      # gRPC API
            - containerPort: 9091      # Prometheus Metrics
          livenessProbe:
            httpGet:
              path: /healthz
              port: 8080
          readinessProbe:
            httpGet:
              path: /readyz
              port: 8080
          resources:
            requests: { cpu: 200m, memory: 256Mi }
            limits:   { cpu: "1", memory: 512Mi }
      affinity:
        podAntiAffinity:               # 反亲和: 分散到不同节点
          preferredDuringSchedulingIgnoredDuringExecution:
            - topologyKey: kubernetes.io/hostname
```

关键点：
- **3 副本**：etcd Leader 选举保证只有 1 个 Leader 运行调度循环，其余 Standby
- **Pod 反亲和**：Scheduler 副本分布在不同节点，避免单节点故障
- **存活探针**：`/healthz` 检测进程健康
- **就绪探针**：`/readyz` 检测服务就绪

### Worker 部署细节

```yaml
apiVersion: apps/v1
kind: Deployment
spec:
  replicas: 5
  strategy:
    rollingUpdate:
      maxSurge: 2                       # 滚动更新: 最多多 2 个 Pod
      maxUnavailable: 1                 # 最多少 1 个 Pod
  template:
    spec:
      terminationGracePeriodSeconds: 60 # 等待 in-flight 任务完成
```

### HPA 自动伸缩

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
spec:
  minReplicas: 3
  maxReplicas: 50
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          averageUtilization: 70
    - type: Pods
      pods:
        metric:
          name: dispatchhub_worker_active_tasks  # 自定义指标
        target:
          averageValue: "80"
  behavior:
    scaleUp:
      stabilizationWindowSeconds: 30     # 快速扩容
      policies:
        - type: Percent
          value: 100                     # 30s 内可翻倍
          periodSeconds: 30
    scaleDown:
      stabilizationWindowSeconds: 300    # 缓慢缩容
      policies:
        - type: Percent
          value: 10                      # 每分钟最多缩 10%
          periodSeconds: 60
```

扩缩策略：
- **扩容**：CPU 超过 70% 或活跃任务数 > 80 时快速扩容（30s 窗口，可翻倍）
- **缩容**：缓慢缩容（5 分钟稳定窗口，每分钟最多缩 10%）
- 自定义指标需要部署 Prometheus Adapter

### RBAC 配置

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: dispatchhub

---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
rules:
  - apiGroups: [""]
    resources: ["configmaps"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list"]
```

---

## 监控与告警

### Prometheus 采集

所有组件通过 Pod annotations 实现自动服务发现：

```yaml
annotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "9091"
  prometheus.io/path: "/metrics"
```

### 关键告警规则建议

```yaml
groups:
  - name: dispatchhub
    rules:
      # 队列积压告警
      - alert: QueueBacklogHigh
        expr: dispatchhub_queue_depth{state="pending"} > 1000
        for: 5m
        labels:
          severity: warning

      # Worker 全部离线
      - alert: NoActiveWorkers
        expr: dispatchhub_worker_active_count == 0
        for: 1m
        labels:
          severity: critical

      # 任务失败率过高
      - alert: HighTaskFailureRate
        expr: >
          rate(dispatchhub_worker_tasks_processed_total{status="failed"}[5m])
          /
          rate(dispatchhub_worker_tasks_processed_total[5m])
          > 0.1
        for: 5m
        labels:
          severity: warning

      # Leader 选举过于频繁
      - alert: FrequentLeaderElections
        expr: rate(dispatchhub_scheduler_leader_elections_total[10m]) > 0.1
        for: 5m
        labels:
          severity: warning

      # 调度延迟过高
      - alert: HighScheduleLatency
        expr: >
          histogram_quantile(0.99,
            rate(dispatchhub_scheduler_schedule_latency_seconds_bucket[5m])
          ) > 5
        for: 5m
        labels:
          severity: warning
```

---

## 运维操作

### 滚动更新

```bash
# Worker 滚动更新（自动 drain + graceful shutdown）
kubectl set image deployment/dispatchhub-worker \
  worker=dispatchhub/worker:v0.2.0 \
  -n dispatchhub

# Scheduler 更新（Leader 自动切换）
kubectl set image deployment/dispatchhub-scheduler \
  scheduler=dispatchhub/scheduler:v0.2.0 \
  -n dispatchhub
```

Worker 更新流程：
1. K8s 发送 SIGTERM 给旧 Pod
2. Worker 停止拉取新任务
3. 等待 in-flight 任务完成（最多 60s）
4. 从 etcd 注销
5. Pod 终止

### 扩缩容

```bash
# 手动扩容 Worker
kubectl scale deployment dispatchhub-worker --replicas=20 -n dispatchhub

# HPA 自动扩缩（如已配置）会自动处理
```

### 日志查看

```bash
# Scheduler 日志
kubectl logs -f deployment/dispatchhub-scheduler -n dispatchhub

# Worker 日志（所有 Pod）
kubectl logs -f -l app.kubernetes.io/component=worker -n dispatchhub

# 过滤特定任务
kubectl logs deployment/dispatchhub-worker -n dispatchhub | jq 'select(.task_id=="xxx")'
```

### 故障排查

| 问题 | 排查命令 |
|------|----------|
| 队列积压 | `curl /api/v1/queues/default/stats` 查看 pending 数 |
| Worker 不工作 | 检查 etcd 注册：`etcdctl get /dispatchhub/workers/ --prefix` |
| Leader 切换频繁 | 检查 etcd 健康：`etcdctl endpoint health` |
| 任务卡在 Running | 检查 inflight：`redis-cli HLEN dispatchhub:queue:default:inflight` |
| 连接池耗尽 | 查看 MySQL: `SHOW PROCESSLIST`; Redis: `INFO clients` |
