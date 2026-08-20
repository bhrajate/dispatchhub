# DispatchHub 文档索引

本目录按用途分组组织。如果你是第一次接触本项目，建议先读 [项目根 README](../README.md)，再按需进入对应子目录。

## 架构与设计 — `architecture/`

| 文档 | 内容 |
|------|------|
| [架构设计](architecture/architecture.md) | 整体架构与设计决策 |
| [核心组件](architecture/components.md) | Scheduler / Worker / 存储层 |
| [数据模型](architecture/data-models.md) | Task / Worker / CronJob 数据结构与状态机 |
| [存储层设计](architecture/storage.md) | MySQL / Redis / etcd 选型与容量规划 |
| [MySQL 设计 — 运维视角](architecture/mysql-design.md) | 索引匹配、写路径性能、容量与归档、连接池、schema 演进遗留 |
| [Leader 选举与脑裂防护](architecture/leader-split-brain.md) | 双主窗口分析、各循环危害评估、为什么选 CAS 而非 fence token |
| [队列选型分析](architecture/queue-selection.md) | 6 种 MQ 方案对比与决策推导 |
| [队列设计](architecture/queue-design.md) | 优先级队列 / 延迟队列 / Lua 脚本详解 |

## 接口与运维 — `reference/`

| 文档 | 内容 |
|------|------|
| [API 参考](reference/api-reference.md) | REST + gRPC 接口文档 |
| [部署指南](reference/deployment.md) | K8s 部署 / Helm Chart / 运维手册 |
| [配置参考](reference/configuration.md) | 全部配置项说明 |

## 修复记录 — `fixes/`

按时间倒序排列，每篇对应一次具体的 bug 修复或功能增强。

| 日期 | 主题 |
|------|------|
| [2026-04-17](fixes/2026-04-17-queue-type-route-validation.md) | Queue-Type 路由校验 |
| [2026-04-17](fixes/2026-04-17-election-defer-deadlock-fix.md) | Leader Election defer 顺序死锁修复 |
| [2026-04-16](fixes/2026-04-16-task-cancellation.md) | 任务取消功能增强 |
| [2026-04-16](fixes/2026-04-16-scheduler-queues-consistency.md) | Scheduler workers/queues 一致性修复 |
| [2026-04-16](fixes/2026-04-16-election-race-fix.md) | Leader Election 竞态条件修复 |
| [2026-04-14](fixes/2026-04-14-optimization-analysis.md) | 全量代码审查与修复方案（后续修复均源自此） |

## 项目规划

| 文档 | 内容 |
|------|------|
| [TODO 清单](TODO.md) | 已完成与待办事项 |
| [2026-04-14 优化分析](fixes/2026-04-14-optimization-analysis.md) | 全量代码审查记录（位于 `fixes/`，TODO 中的修复条目均源自此） |
