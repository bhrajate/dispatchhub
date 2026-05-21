package entity

import (
	"encoding/json"
	"time"
)

// ConcurrencyPolicy 控制 cron job 触发重叠执行时的处理策略。
type ConcurrencyPolicy string

const (
	// ConcurrencyAllow 允许并发执行（默认）。
	ConcurrencyAllow ConcurrencyPolicy = "Allow"
	// ConcurrencyForbid 在上一次执行仍在运行时跳过本次触发。
	ConcurrencyForbid ConcurrencyPolicy = "Forbid"
)

// CronJob 定义按 cron 周期触发的周期性任务。
// 每次触发都会根据配置 spec 创建一个新的 Task 实例。
type CronJob struct {
	ID                string            `json:"id" gorm:"primaryKey;size:64"`
	Name              string            `json:"name" gorm:"size:255"`
	Namespace         string            `json:"namespace" gorm:"index;size:128"`
	Type              string            `json:"type" gorm:"index;size:128"`
	Payload           json.RawMessage   `json:"payload" gorm:"type:text"`
	Labels            Labels            `json:"labels" gorm:"type:text"`
	CronExpr          string            `json:"cron_expr" gorm:"size:128"`
	QueueName         string            `json:"queue_name" gorm:"size:128"`
	Priority          TaskPriority      `json:"priority"`
	Timeout           Duration          `json:"timeout"`
	MaxRetries        int               `json:"max_retries"`
	RetryBackoff      Duration          `json:"retry_backoff"`
	ConcurrencyPolicy ConcurrencyPolicy `json:"concurrency_policy" gorm:"size:32;default:'Allow'"`
	Enabled           bool              `json:"enabled" gorm:"default:true"`
	LastRunAt         *time.Time        `json:"last_run_at,omitempty"`
	NextRunAt         *time.Time        `json:"next_run_at,omitempty"`
	CreatedAt         time.Time         `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt         time.Time         `json:"updated_at" gorm:"autoUpdateTime"`
}

// ToTask 根据当前 CronJob 的 spec 创建新的 Task 实例。
func (c *CronJob) ToTask() *Task {
	return &Task{
		Name:         c.Name,
		Namespace:    c.Namespace,
		Type:         c.Type,
		Payload:      c.Payload,
		Labels:       c.Labels,
		QueueName:    c.QueueName,
		Priority:     c.Priority,
		Timeout:      c.Timeout,
		MaxRetries:   c.MaxRetries,
		RetryBackoff: c.RetryBackoff,
	}
}
