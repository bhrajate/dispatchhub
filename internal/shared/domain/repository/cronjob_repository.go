package repository

import (
	"context"
	"time"

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
	// ClaimCronJob 以 next_run_at 作为 CAS 条件原子地推进调度时间。
	// 仅当数据库中该 job 的 next_run_at 仍等于 expectedNextRunAt 时才更新成功。
	// 用于防御 scheduler 双主期间同一 cron job 被重复触发：先 Claim 成功的实例
	// 才能继续 Create+Enqueue task，Claim 失败的实例直接跳过本次触发。
	// 返回 true 表示 Claim 成功，false 表示已被其他实例抢占。
	ClaimCronJob(ctx context.Context, jobID string, expectedNextRunAt, newLastRunAt, newNextRunAt time.Time) (bool, error)
}

// CronJobStore 组合 cron job 的读写访问能力。
type CronJobStore interface {
	CronJobReader
	CronJobWriter
}
