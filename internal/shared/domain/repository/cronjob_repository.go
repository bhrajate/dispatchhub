package repository

import (
	"context"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
)

// CronJobReader provides read access to cron jobs.
type CronJobReader interface {
	GetCronJob(ctx context.Context, id string) (*entity.CronJob, error)
	ListCronJobs(ctx context.Context, namespace string, limit, offset int) ([]*entity.CronJob, int64, error)
	// FindDueCronJobs returns enabled cron jobs whose next_run_at <= now.
	FindDueCronJobs(ctx context.Context, limit int) ([]*entity.CronJob, error)
}

// CronJobWriter provides write access to cron jobs.
type CronJobWriter interface {
	CreateCronJob(ctx context.Context, job *entity.CronJob) error
	UpdateCronJob(ctx context.Context, job *entity.CronJob) error
	DeleteCronJob(ctx context.Context, id string) error
}

// CronJobStore combines read and write access to cron jobs.
type CronJobStore interface {
	CronJobReader
	CronJobWriter
}
