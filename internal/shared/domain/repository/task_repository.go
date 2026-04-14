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

// TaskCompensator provides queries and updates for the compensate loop.
type TaskCompensator interface {
	FindStaleByState(ctx context.Context, state entity.TaskState, olderThan time.Duration, limit int) ([]*entity.Task, error)
	// TouchUpdatedAt refreshes updated_at WITHOUT incrementing version.
	// This prevents version mismatch between Redis (task JSON) and MySQL.
	TouchUpdatedAt(ctx context.Context, id string) error
}
