package repository

import (
	"context"
	"time"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
)

// TaskReader provides read-only access to tasks.
type TaskReader interface {
	Get(ctx context.Context, id string) (*entity.Task, error)
	List(ctx context.Context, filter entity.TaskFilter) ([]*entity.Task, int64, error)
}

// TaskWriter provides write access to tasks.
type TaskWriter interface {
	Create(ctx context.Context, task *entity.Task) error
	Update(ctx context.Context, task *entity.Task) error
}

// TaskStore combines read and write access for full CRUD operations.
type TaskStore interface {
	TaskReader
	TaskWriter
}

// TaskCompensator provides queries and updates for background maintenance loops.
type TaskCompensator interface {
	FindStaleByState(ctx context.Context, state entity.TaskState, olderThan time.Duration, limit int) ([]*entity.Task, error)
	// TouchUpdatedAt refreshes updated_at WITHOUT incrementing version.
	TouchUpdatedAt(ctx context.Context, id string) error
	// HasRunningTasks returns true if there are tasks in Running state with the given type and namespace.
	HasRunningTasks(ctx context.Context, taskType, namespace string) (bool, error)
	// DeleteTerminalOlderThan deletes completed/failed/cancelled/timeout tasks older than the threshold.
	DeleteTerminalOlderThan(ctx context.Context, olderThan time.Duration, limit int) (int64, error)
}
