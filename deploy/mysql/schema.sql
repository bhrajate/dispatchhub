-- ============================================================
-- DispatchHub MySQL Schema
-- 数据库: dispatchhub
-- 字符集: utf8mb4 (完整 Unicode 支持)
-- 排序规则: utf8mb4_general_ci
-- ============================================================

CREATE DATABASE IF NOT EXISTS `dispatchhub`
    DEFAULT CHARACTER SET utf8mb4
    DEFAULT COLLATE utf8mb4_general_ci;

USE `dispatchhub`;

-- ============================================================
-- 1. tasks - 任务主表
-- 核心表，存储所有任务的状态和元数据。
-- 设计要点:
--   - id 使用 VARCHAR(64) 存储 UUID，兼容分布式 ID 生成
--   - version 乐观锁字段，每次更新 +1，防止并发冲突
--   - payload/labels/result/error 使用 TEXT，支持大文本
--   - 组合索引 idx_queue_state_priority 覆盖调度查询热路径
--   - 分区键预留 created_at，便于后续按时间归档
-- ============================================================

CREATE TABLE IF NOT EXISTS `tasks` (
    -- 标识
    `id`          VARCHAR(64)   NOT NULL COMMENT '任务唯一ID (UUID)',
    `name`        VARCHAR(255)  NOT NULL DEFAULT '' COMMENT '任务可读名称',
    `namespace`   VARCHAR(128)  NOT NULL DEFAULT '' COMMENT '命名空间 (多租户隔离)',
    `group`       VARCHAR(128)  NOT NULL DEFAULT '' COMMENT '逻辑分组 (亲和性调度)',

    -- 载荷
    `type`        VARCHAR(128)  NOT NULL DEFAULT '' COMMENT 'Handler类型 (email.send, report.generate)',
    `payload`     TEXT                   DEFAULT NULL COMMENT '任务载荷 (任意JSON)',
    `labels`      TEXT                   DEFAULT NULL COMMENT 'K8s风格标签 (JSON: {"k":"v"})',

    -- 调度
    `priority`    TINYINT       NOT NULL DEFAULT 5 COMMENT '优先级 (1=Low, 5=Default, 8=High, 10=Critical)',
    `delay`       BIGINT        NOT NULL DEFAULT 0 COMMENT '延迟执行纳秒数',
    `schedule_at` DATETIME(3)            DEFAULT NULL COMMENT '绝对调度时间',
    `cron_expr`   VARCHAR(128)  NOT NULL DEFAULT '' COMMENT 'Cron表达式 (周期任务)',
    `timeout`     BIGINT        NOT NULL DEFAULT 0 COMMENT '执行超时纳秒数',
    `deadline`    DATETIME(3)            DEFAULT NULL COMMENT '绝对截止时间',

    -- 重试策略
    `max_retries`   INT         NOT NULL DEFAULT 3 COMMENT '最大重试次数',
    `retry_count`   INT         NOT NULL DEFAULT 0 COMMENT '已重试次数',
    `retry_backoff` BIGINT      NOT NULL DEFAULT 0 COMMENT '重试退避基础纳秒数',

    -- 状态
    `state`       TINYINT       NOT NULL DEFAULT 0 COMMENT '状态 (0=Pending 1=Scheduled 2=Running 3=Retrying 4=Completed 5=Failed 6=Cancelled 7=Timeout)',
    `result`      TEXT                   DEFAULT NULL COMMENT '执行成功输出',
    `error`       TEXT                   DEFAULT NULL COMMENT '执行失败错误信息',
    `worker_id`   VARCHAR(128)  NOT NULL DEFAULT '' COMMENT '执行Worker ID',
    `queue_name`  VARCHAR(128)  NOT NULL DEFAULT 'default' COMMENT '所属队列',

    -- 元数据
    `created_at`  DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '创建时间',
    `updated_at`  DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3) COMMENT '更新时间',
    `started_at`  DATETIME(3)            DEFAULT NULL COMMENT '开始执行时间',
    `finished_at` DATETIME(3)            DEFAULT NULL COMMENT '完成时间',
    `version`     BIGINT        NOT NULL DEFAULT 1 COMMENT '乐观锁版本号',

    PRIMARY KEY (`id`),

    -- 单列索引: 覆盖常见过滤查询
    KEY `idx_name`       (`name`),
    KEY `idx_namespace`  (`namespace`),
    KEY `idx_group`      (`group`),
    KEY `idx_type`       (`type`),
    KEY `idx_state`      (`state`),
    KEY `idx_priority`   (`priority`),
    KEY `idx_queue_name` (`queue_name`),
    KEY `idx_worker_id`  (`worker_id`),
    KEY `idx_created_at` (`created_at`),

    -- 组合索引: 覆盖调度核心查询路径
    -- 场景: SELECT ... WHERE queue_name=? AND state=? ORDER BY priority DESC, created_at ASC
    KEY `idx_queue_state_priority` (`queue_name`, `state`, `priority`, `created_at`),

    -- 组合索引: 按命名空间+类型查询
    KEY `idx_ns_type_state` (`namespace`, `type`, `state`),

    -- 组合索引: Worker维度查询 (查看Worker正在执行的任务)
    KEY `idx_worker_state` (`worker_id`, `state`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_general_ci
  COMMENT='任务主表';


-- ============================================================
-- 2. task_events - 任务事件表 (审计日志)
-- 记录任务每次状态变更，用于审计追踪和问题排查。
-- 设计要点:
--   - 仅追加写入 (append-only)，不更新不删除
--   - task_id + timestamp 组合索引，支持按任务查询事件流
--   - 高写入量表，建议按时间分区或定期归档
-- ============================================================

CREATE TABLE IF NOT EXISTS `task_events` (
    `id`         VARCHAR(64)   NOT NULL COMMENT '事件唯一ID (UUID)',
    `task_id`    VARCHAR(64)   NOT NULL COMMENT '关联任务ID',
    `type`       VARCHAR(64)   NOT NULL DEFAULT '' COMMENT '事件类型 (created/scheduled/started/completed/failed/retried/cancelled)',
    `old_state`  TINYINT       NOT NULL DEFAULT 0 COMMENT '变更前状态',
    `new_state`  TINYINT       NOT NULL DEFAULT 0 COMMENT '变更后状态',
    `worker_id`  VARCHAR(128)  NOT NULL DEFAULT '' COMMENT '触发事件的Worker',
    `message`    TEXT                   DEFAULT NULL COMMENT '事件附加信息',
    `timestamp`  DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '事件时间',

    PRIMARY KEY (`id`),

    -- 按任务查询事件流 (按时间倒序)
    KEY `idx_task_timestamp` (`task_id`, `timestamp`),

    -- 按时间范围查询 (全局事件流、清理归档)
    KEY `idx_timestamp` (`timestamp`),

    -- 按事件类型统计
    KEY `idx_type` (`type`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_general_ci
  COMMENT='任务事件审计表';


-- ============================================================
-- 3. dead_letters - 死信表
-- 存储最终失败 (用尽重试) 的任务快照，便于人工排查和重新投递。
-- ============================================================

CREATE TABLE IF NOT EXISTS `dead_letters` (
    `id`            VARCHAR(64)   NOT NULL COMMENT '死信ID (UUID)',
    `task_id`       VARCHAR(64)   NOT NULL COMMENT '原始任务ID',
    `queue_name`    VARCHAR(128)  NOT NULL DEFAULT '' COMMENT '来源队列',
    `type`          VARCHAR(128)  NOT NULL DEFAULT '' COMMENT 'Handler类型',
    `payload`       TEXT                   DEFAULT NULL COMMENT '任务载荷快照',
    `error`         TEXT                   DEFAULT NULL COMMENT '最后一次错误信息',
    `retry_count`   INT           NOT NULL DEFAULT 0 COMMENT '累计重试次数',
    `max_retries`   INT           NOT NULL DEFAULT 0 COMMENT '最大重试次数',
    `failed_at`     DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '最终失败时间',
    `redelivered`   TINYINT(1)    NOT NULL DEFAULT 0 COMMENT '是否已重新投递 (0=否 1=是)',
    `redelivered_at` DATETIME(3)           DEFAULT NULL COMMENT '重新投递时间',

    PRIMARY KEY (`id`),
    KEY `idx_task_id`   (`task_id`),
    KEY `idx_queue`     (`queue_name`),
    KEY `idx_failed_at` (`failed_at`),
    KEY `idx_redelivered` (`redelivered`, `failed_at`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_general_ci
  COMMENT='死信表 (最终失败任务)';


-- ============================================================
-- 4. cron_jobs - 定时任务定义表
-- 存储 Cron 周期任务的定义，每次触发时生成一个 Task 实例。
-- ============================================================

CREATE TABLE IF NOT EXISTS `cron_jobs` (
    `id`            VARCHAR(64)   NOT NULL COMMENT '定时任务ID (UUID)',
    `name`          VARCHAR(255)  NOT NULL DEFAULT '' COMMENT '任务名称',
    `namespace`     VARCHAR(128)  NOT NULL DEFAULT '' COMMENT '命名空间',
    `type`          VARCHAR(128)  NOT NULL COMMENT 'Handler类型',
    `payload`       TEXT                   DEFAULT NULL COMMENT '任务载荷模板',
    `labels`        TEXT                   DEFAULT NULL COMMENT '标签',
    `cron_expr`     VARCHAR(128)  NOT NULL COMMENT 'Cron表达式 (5/6字段)',
    `queue_name`    VARCHAR(128)  NOT NULL DEFAULT 'default' COMMENT '目标队列',
    `priority`      TINYINT       NOT NULL DEFAULT 5 COMMENT '优先级',
    `timeout`       BIGINT        NOT NULL DEFAULT 0 COMMENT '执行超时纳秒数',
    `max_retries`   INT           NOT NULL DEFAULT 3 COMMENT '最大重试次数',
    `retry_backoff` BIGINT        NOT NULL DEFAULT 0 COMMENT '重试退避纳秒数',
    `enabled`       TINYINT(1)    NOT NULL DEFAULT 1 COMMENT '是否启用 (0=禁用 1=启用)',
    `last_run_at`   DATETIME(3)            DEFAULT NULL COMMENT '上次执行时间',
    `next_run_at`   DATETIME(3)            DEFAULT NULL COMMENT '下次执行时间',
    `created_at`    DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    `updated_at`    DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),

    PRIMARY KEY (`id`),
    KEY `idx_enabled_next` (`enabled`, `next_run_at`),
    KEY `idx_namespace`    (`namespace`),
    KEY `idx_type`         (`type`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_general_ci
  COMMENT='定时任务定义表';


-- ============================================================
-- 5. scheduler_locks - 调度器分布式锁表 (MySQL方案备选)
-- 当无法使用 etcd 时，可通过 MySQL 实现分布式锁。
-- ============================================================

CREATE TABLE IF NOT EXISTS `scheduler_locks` (
    `lock_name`   VARCHAR(128)  NOT NULL COMMENT '锁名称',
    `holder`      VARCHAR(255)  NOT NULL DEFAULT '' COMMENT '持有者标识',
    `acquired_at` DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3) COMMENT '获取时间',
    `expires_at`  DATETIME(3)   NOT NULL COMMENT '过期时间',
    `version`     BIGINT        NOT NULL DEFAULT 1 COMMENT '版本号',

    PRIMARY KEY (`lock_name`)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_general_ci
  COMMENT='分布式锁表 (etcd备选方案)';
