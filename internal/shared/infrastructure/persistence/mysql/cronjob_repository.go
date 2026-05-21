package mysql

import (
	"context"
	"fmt"
	"time"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
	"github.com/dispatchhub/dispatchhub/internal/shared/domain/repository"
	"gorm.io/gorm"
)

// CronJobRepository 基于 GORM 使用 MySQL 实现 cron job 持久化。
type CronJobRepository struct {
	db *gorm.DB
}

// NewCronJobRepository 创建 cron job 仓储并自动迁移 schema。
func NewCronJobRepository(db *gorm.DB) (*CronJobRepository, error) {
	if err := db.AutoMigrate(&entity.CronJob{}); err != nil {
		return nil, fmt.Errorf("auto migrate cron_jobs: %w", err)
	}
	return &CronJobRepository{db: db}, nil
}

func (r *CronJobRepository) CreateCronJob(ctx context.Context, job *entity.CronJob) error {
	return r.db.WithContext(ctx).Create(job).Error
}

func (r *CronJobRepository) GetCronJob(ctx context.Context, id string) (*entity.CronJob, error) {
	var job entity.CronJob
	err := r.db.WithContext(ctx).Where("id = ?", id).First(&job).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &job, nil
}

func (r *CronJobRepository) UpdateCronJob(ctx context.Context, job *entity.CronJob) error {
	return r.db.WithContext(ctx).Save(job).Error
}

func (r *CronJobRepository) DeleteCronJob(ctx context.Context, id string) error {
	return r.db.WithContext(ctx).Where("id = ?", id).Delete(&entity.CronJob{}).Error
}

func (r *CronJobRepository) ListCronJobs(ctx context.Context, namespace string, limit, offset int) ([]*entity.CronJob, int64, error) {
	query := r.db.WithContext(ctx).Model(&entity.CronJob{})
	if namespace != "" {
		query = query.Where("namespace = ?", namespace)
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	if limit > 0 {
		query = query.Limit(limit)
	} else {
		query = query.Limit(100)
	}
	if offset > 0 {
		query = query.Offset(offset)
	}

	var jobs []*entity.CronJob
	if err := query.Order("created_at DESC").Find(&jobs).Error; err != nil {
		return nil, 0, err
	}
	return jobs, total, nil
}

// FindDueCronJobs 返回已启用且 next_run_at <= now 的 cron job。
func (r *CronJobRepository) FindDueCronJobs(ctx context.Context, limit int) ([]*entity.CronJob, error) {
	var jobs []*entity.CronJob
	err := r.db.WithContext(ctx).
		Where("enabled = ? AND next_run_at IS NOT NULL AND next_run_at <= ?", true, time.Now()).
		Order("next_run_at ASC").
		Limit(limit).
		Find(&jobs).Error
	return jobs, err
}

var (
	_ repository.CronJobReader = (*CronJobRepository)(nil)
	_ repository.CronJobWriter = (*CronJobRepository)(nil)
)
