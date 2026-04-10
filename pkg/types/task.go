package types

import (
	"encoding/json"
	"fmt"
	"time"
)

// TaskState represents the lifecycle state of a task.
type TaskState int

const (
	TaskStatePending   TaskState = iota // waiting to be scheduled
	TaskStateScheduled                  // assigned to a worker
	TaskStateRunning                    // actively executing
	TaskStateRetrying                   // waiting for retry
	TaskStateCompleted                  // finished successfully
	TaskStateFailed                     // exhausted all retries
	TaskStateCancelled                  // cancelled by user
	TaskStateTimeout                    // timed out
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

// TaskPriority defines scheduling priority levels.
type TaskPriority int

const (
	PriorityLow      TaskPriority = 1
	PriorityDefault  TaskPriority = 5
	PriorityHigh     TaskPriority = 8
	PriorityCritical TaskPriority = 10
)

// Task is the fundamental unit of work in the scheduling system.
type Task struct {
	// Identity
	ID        string `json:"id" gorm:"primaryKey;size:64"`
	Name      string `json:"name" gorm:"index;size:255"`
	Namespace string `json:"namespace" gorm:"index;size:128"`
	Group     string `json:"group" gorm:"index;size:128"` // logical grouping for affinity

	// Payload
	Type    string          `json:"type" gorm:"index;size:128"`    // handler type (e.g., "email.send", "report.generate")
	Payload json.RawMessage `json:"payload" gorm:"type:text"`      // arbitrary JSON payload
	Labels  Labels          `json:"labels" gorm:"type:text"`       // k8s-style labels for filtering

	// Scheduling
	Priority    TaskPriority `json:"priority" gorm:"index"`
	Delay       Duration     `json:"delay,omitempty"`              // initial delay before first execution
	ScheduleAt  *time.Time   `json:"schedule_at,omitempty"`       // absolute schedule time
	CronExpr    string       `json:"cron_expr,omitempty" gorm:"size:128"` // cron expression for recurring tasks
	Timeout     Duration     `json:"timeout"`                     // per-execution timeout
	Deadline    *time.Time   `json:"deadline,omitempty"`          // absolute deadline

	// Retry policy
	MaxRetries   int      `json:"max_retries"`
	RetryCount   int      `json:"retry_count"`
	RetryBackoff Duration `json:"retry_backoff"` // base backoff duration

	// State
	State     TaskState  `json:"state" gorm:"index"`
	Result    string     `json:"result,omitempty" gorm:"type:text"`
	Error     string     `json:"error,omitempty" gorm:"type:text"`
	WorkerID  string     `json:"worker_id,omitempty" gorm:"index;size:128"`
	QueueName string     `json:"queue_name" gorm:"index;size:128"`

	// Metadata
	CreatedAt  time.Time  `json:"created_at" gorm:"autoCreateTime"`
	UpdatedAt  time.Time  `json:"updated_at" gorm:"autoUpdateTime"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	FinishedAt *time.Time `json:"finished_at,omitempty"`
	Version    int64      `json:"version" gorm:"default:1"` // optimistic lock
}

// IsTerminal returns true if the task is in a terminal state.
func (t *Task) IsTerminal() bool {
	switch t.State {
	case TaskStateCompleted, TaskStateFailed, TaskStateCancelled, TaskStateTimeout:
		return true
	}
	return false
}

// CanRetry checks whether the task is eligible for a retry.
func (t *Task) CanRetry() bool {
	return t.RetryCount < t.MaxRetries && !t.IsTerminal()
}

// TaskEvent represents a state transition or lifecycle event.
type TaskEvent struct {
	ID        string    `json:"id"`
	TaskID    string    `json:"task_id" gorm:"index;size:64"`
	Type      string    `json:"type" gorm:"size:64"`       // created, scheduled, started, completed, failed, retried, cancelled
	OldState  TaskState `json:"old_state"`
	NewState  TaskState `json:"new_state"`
	WorkerID  string    `json:"worker_id,omitempty" gorm:"size:128"`
	Message   string    `json:"message,omitempty" gorm:"type:text"`
	Timestamp time.Time `json:"timestamp"`
}

// Labels is a set of key-value pairs attached to a task (k8s-style).
type Labels map[string]string

func (l Labels) Matches(selector map[string]string) bool {
	for k, v := range selector {
		if l[k] != v {
			return false
		}
	}
	return true
}

// Duration wraps time.Duration for JSON serialization.
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

// TaskFilter defines criteria for querying tasks.
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

// TaskResult is returned after a handler processes a task.
type TaskResult struct {
	Output string `json:"output,omitempty"`
	Error  error  `json:"-"`
}

// ErrPanic wraps a recovered panic value into an error.
func ErrPanic(v interface{}) error {
	return fmt.Errorf("panic: %v", v)
}
