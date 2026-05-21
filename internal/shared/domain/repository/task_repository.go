package repository

import (
	"context"
	"time"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
)

// TaskReader 提供任务只读访问能力。
type TaskReader interface {
	Get(ctx context.Context, id string) (*entity.Task, error)
	List(ctx context.Context, filter entity.TaskFilter) ([]*entity.Task, int64, error)
}

// TaskWriter 提供任务写访问能力。
type TaskWriter interface {
	Create(ctx context.Context, task *entity.Task) error
	Update(ctx context.Context, task *entity.Task) error
}

// TaskStore 组合完整 CRUD 所需的任务读写访问能力。
type TaskStore interface {
	TaskReader
	TaskWriter
}

// TaskCompensator 为后台维护循环提供查询和更新能力。
type TaskCompensator interface {
	FindStaleByState(ctx context.Context, state entity.TaskState, olderThan time.Duration, limit int) ([]*entity.Task, error)
	// TouchUpdatedAt 刷新 updated_at，但不递增 version。
	TouchUpdatedAt(ctx context.Context, id string) error
	// HasRunningTasks 判断指定 type 和 namespace 下是否存在 Running 状态的任务。
	HasRunningTasks(ctx context.Context, taskType, namespace string) (bool, error)
	// DeleteTerminalOlderThan 删除早于阈值的 completed/failed/cancelled/timeout 终态任务。
	DeleteTerminalOlderThan(ctx context.Context, olderThan time.Duration, limit int) (int64, error)
}
