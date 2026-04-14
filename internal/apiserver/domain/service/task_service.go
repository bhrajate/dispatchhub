package service

import (
	"context"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
)

// TaskService defines the contract for task management operations.
// Both apiserver and scheduler interfaces depend on this abstraction
// rather than on a concrete implementation.
type TaskService interface {
	SubmitTask(ctx context.Context, task *entity.Task) error
	GetTask(ctx context.Context, taskID string) (*entity.Task, error)
	ListTasks(ctx context.Context, filter entity.TaskFilter) ([]*entity.Task, int64, error)
	CancelTask(ctx context.Context, taskID string) error
	QueueStats(ctx context.Context, queue string) (*entity.QueueStats, error)
}
