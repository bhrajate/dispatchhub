package repository

import (
	"context"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
)

// CronJobReader 提供 cron job 的读访问能力。
type CronJobReader interface {
	GetCronJob(ctx context.Context, id string) (*entity.CronJob, error)
	ListCronJobs(ctx context.Context, namespace string, limit, offset int) ([]*entity.CronJob, int64, error)
	// FindDueCronJobs 返回已启用且 next_run_at <= now 的 cron job。
	FindDueCronJobs(ctx context.Context, limit int) ([]*entity.CronJob, error)
}

// CronJobWriter 提供 cron job 的写访问能力。
type CronJobWriter interface {
	CreateCronJob(ctx context.Context, job *entity.CronJob) error
	UpdateCronJob(ctx context.Context, job *entity.CronJob) error
	DeleteCronJob(ctx context.Context, id string) error
}

// CronJobStore 组合 cron job 的读写访问能力。
type CronJobStore interface {
	CronJobReader
	CronJobWriter
}
