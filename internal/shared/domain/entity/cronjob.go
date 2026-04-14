package entity

import (
	"encoding/json"
	"time"
)

// CronJob defines a recurring task that is triggered on a cron schedule.
// Each trigger creates a new Task instance with the configured spec.
type CronJob struct {
	ID           string          `json:"id" gorm:"primaryKey;size:64"`
	Name         string          `json:"name" gorm:"size:255"`
	Namespace    string          `json:"namespace" gorm:"index;size:128"`
	Type         string          `json:"type" gorm:"index;size:128"`
	Payload      json.RawMessage `json:"payload" gorm:"type:text"`
	Labels       Labels          `json:"labels" gorm:"type:text"`
	CronExpr     string          `json:"cron_expr" gorm:"size:128"`
	QueueName    string          `json:"queue_name" gorm:"size:128"`
	Priority     TaskPriority    `json:"priority"`
	Timeout      Duration        `json:"timeout"`
	MaxRetries   int             `json:"max_retries"`
	RetryBackoff Duration        `json:"retry_backoff"`
	Enabled      bool            `json:"enabled" gorm:"default:true"`
	LastRunAt    *time.Time      `json:"last_run_at,omitempty"`
	NextRunAt    *time.Time      `json:"next_run_at,omitempty"`
	CreatedAt    time.Time       `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt    time.Time       `json:"updated_at" gorm:"autoUpdateTime"`
}

// ToTask creates a new Task instance from this CronJob's spec.
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
