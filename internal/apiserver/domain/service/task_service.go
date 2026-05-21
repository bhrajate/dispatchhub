package service

import (
	"context"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
)

// TaskService 定义任务和 cron job 管理操作的契约。
type TaskService interface {
	// 任务操作
	SubmitTask(ctx context.Context, task *entity.Task) error
	GetTask(ctx context.Context, taskID string) (*entity.Task, error)
	ListTasks(ctx context.Context, filter entity.TaskFilter) ([]*entity.Task, int64, error)
	CancelTask(ctx context.Context, taskID string) error
	QueueStats(ctx context.Context, queue string) (*entity.QueueStats, error)

	// CronJob 操作
	CreateCronJob(ctx context.Context, job *entity.CronJob) error
	GetCronJob(ctx context.Context, id string) (*entity.CronJob, error)
	ListCronJobs(ctx context.Context, namespace string, limit, offset int) ([]*entity.CronJob, int64, error)
	DeleteCronJob(ctx context.Context, id string) error
}
