package entity

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

// TaskState 表示任务的生命周期状态。
type TaskState int

const (
	TaskStatePending   TaskState = iota // 等待调度
	TaskStateScheduled                  // 已分配给 worker
	TaskStateRunning                    // 正在执行
	TaskStateRetrying                   // 等待重试
	TaskStateCompleted                  // 成功完成
	TaskStateFailed                     // 重试次数已耗尽
	TaskStateCancelled                  // 已被用户取消
	TaskStateTimeout                    // 已超时
)

var taskStateNames = map[TaskState]string{
	TaskStatePending:   "pending",
	TaskStateScheduled: "scheduled",
	TaskStateRunning:   "running",
	TaskStateRetrying:  "retrying",
	TaskStateCompleted: "completed",
	TaskStateFailed:    "failed",
	TaskStateCancelled: "cancelled",
	TaskStateTimeout:   "timeout",
}

func (s TaskState) String() string {
	if name, ok := taskStateNames[s]; ok {
		return name
	}
	return "unknown"
}

// TaskPriority 定义调度优先级。
type TaskPriority int

const (
	PriorityLow      TaskPriority = 1
	PriorityDefault  TaskPriority = 5
	PriorityHigh     TaskPriority = 8
	PriorityCritical TaskPriority = 10
)

// Task 是调度系统中的基本工作单元。
type Task struct {
	// 身份信息
	ID        string `json:"id" gorm:"primaryKey;size:64"`
	Name      string `json:"name" gorm:"index;size:255"`
	Namespace string `json:"namespace" gorm:"index;size:128"`
	Group     string `json:"group" gorm:"index;size:128"`

	// 负载
	Type    string          `json:"type" gorm:"index;size:128"`
	Payload json.RawMessage `json:"payload" gorm:"type:text"`
	Labels  Labels          `json:"labels" gorm:"type:text"`

	// 调度
	Priority   TaskPriority `json:"priority" gorm:"index"`
	Delay      Duration     `json:"delay,omitempty"`
	ScheduleAt *time.Time   `json:"schedule_at,omitempty"`
	Timeout    Duration     `json:"timeout"`

	// 重试策略
	MaxRetries   int      `json:"max_retries"`
	RetryCount   int      `json:"retry_count"`
	RetryBackoff Duration `json:"retry_backoff"`

	// 状态
	State     TaskState `json:"state" gorm:"index"`
	Result    string    `json:"result,omitempty" gorm:"type:text"`
	Error     string    `json:"error,omitempty" gorm:"type:text"`
	WorkerID  string    `json:"worker_id,omitempty" gorm:"index;size:128"`
	QueueName string    `json:"queue_name" gorm:"index;size:128"`

	// 元数据
	CreatedAt  time.Time  `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt  time.Time  `json:"updated_at" gorm:"autoUpdateTime"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	Version    int64      `json:"version" gorm:"default:1"`
}

// IsTerminal 判断任务是否处于终态。
func (t *Task) IsTerminal() bool {
	switch t.State {
	case TaskStateCompleted, TaskStateFailed, TaskStateCancelled, TaskStateTimeout:
		return true
	}
	return false
}

// CanRetry 检查任务是否仍可重试。
func (t *Task) CanRetry() bool {
	return t.RetryCount < t.MaxRetries && !t.IsTerminal()
}

// Labels 是挂载到任务上的键值对集合（k8s 风格）。
type Labels map[string]string

// Value 为 GORM MySQL 序列化实现 driver.Valuer。
func (l Labels) Value() (driver.Value, error) {
	if l == nil {
		return nil, nil
	}
	data, err := json.Marshal(l)
	if err != nil {
		return nil, fmt.Errorf("marshal labels: %w", err)
	}
	return string(data), nil
}

// Scan 为 GORM MySQL 反序列化实现 sql.Scanner。
func (l *Labels) Scan(value any) error {
	if value == nil {
		*l = nil
		return nil
	}
	var bytes []byte
	switch v := value.(type) {
	case string:
		bytes = []byte(v)
	case []byte:
		bytes = v
	default:
		return fmt.Errorf("unsupported labels type: %T", value)
	}
	return json.Unmarshal(bytes, l)
}

// Duration 封装 time.Duration，用于 JSON 和 GORM 序列化。
type Duration struct {
	time.Duration
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v string
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	dur, err := time.ParseDuration(v)
	if err != nil {
		return err
	}
	d.Duration = dur
	return nil
}

// Value 为 GORM MySQL 序列化实现 driver.Valuer（按纳秒存储）。
func (d Duration) Value() (driver.Value, error) {
	return int64(d.Duration), nil
}

// Scan 为 GORM MySQL 反序列化实现 sql.Scanner（按纳秒读取）。
func (d *Duration) Scan(value any) error {
	if value == nil {
		d.Duration = 0
		return nil
	}
	switch v := value.(type) {
	case int64:
		d.Duration = time.Duration(v)
	case float64:
		d.Duration = time.Duration(int64(v))
	case []byte:
		n := int64(0)
		fmt.Sscanf(string(v), "%d", &n)
		d.Duration = time.Duration(n)
	default:
		return fmt.Errorf("unsupported duration type: %T", value)
	}
	return nil
}

// TaskFilter 定义任务查询条件。
type TaskFilter struct {
	Namespace string            `json:"namespace,omitempty"`
	Group     string            `json:"group,omitempty"`
	Type      string            `json:"type,omitempty"`
	State     *TaskState        `json:"state,omitempty"`
	Labels    map[string]string `json:"labels,omitempty"`
	QueueName string            `json:"queue_name,omitempty"`
	WorkerID  string            `json:"worker_id,omitempty"`
	Limit     int               `json:"limit,omitempty"`
	Offset    int               `json:"offset,omitempty"`
}

// TaskResult 是 handler 处理任务后的返回结果。
type TaskResult struct {
	Output string `json:"output,omitempty"`
	Error  error  `json:"-"`
}

// ErrPanic 将 recover 到的 panic 值包装为 error。
func ErrPanic(v any) error {
	return fmt.Errorf("panic: %v", v)
}
