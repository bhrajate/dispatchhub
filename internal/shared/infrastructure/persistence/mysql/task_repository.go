package mysql

import (
	"context"
	"fmt"
	"time"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
	"github.com/dispatchhub/dispatchhub/internal/shared/domain/repository"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// TaskRepository implements repository.TaskRepository using MySQL via GORM.
type TaskRepository struct {
	db *gorm.DB
}

// NewTaskRepository creates a task repository and auto-migrates the schema.
func NewTaskRepository(db *gorm.DB) (*TaskRepository, error) {
	if err := db.AutoMigrate(&entity.Task{}); err != nil {
		return nil, fmt.Errorf("auto migrate: %w", err)
	}
	return &TaskRepository{db: db}, nil
}

func (s *TaskRepository) Create(ctx context.Context, task *entity.Task) error {
	return s.db.WithContext(ctx).Create(task).Error
}

func (s *TaskRepository) Get(ctx context.Context, id string) (*entity.Task, error) {
	var task entity.Task
	err := s.db.WithContext(ctx).Where("id = ?", id).First(&task).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &task, nil
}

func (s *TaskRepository) Update(ctx context.Context, task *entity.Task) error {
	oldVersion := task.Version
	task.Version++

	result := s.db.WithContext(ctx).
		Model(task).
		Where("id = ? AND version = ?", task.ID, oldVersion).
		Clauses(clause.Locking{Strength: "UPDATE"}).
		Updates(task)

	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("optimistic lock conflict: task %s version %d", task.ID, oldVersion)
	}
	return nil
}

func (s *TaskRepository) List(ctx context.Context, filter entity.TaskFilter) ([]*entity.Task, int64, error) {
	query := s.db.WithContext(ctx).Model(&entity.Task{})

	if filter.Namespace != "" {
		query = query.Where("namespace = ?", filter.Namespace)
	}
	if filter.Group != "" {
		query = query.Where("`group` = ?", filter.Group)
	}
	if filter.Type != "" {
		query = query.Where("type = ?", filter.Type)
	}
	if filter.State != nil {
		query = query.Where("state = ?", *filter.State)
	}
	if filter.QueueName != "" {
		query = query.Where("queue_name = ?", filter.QueueName)
	}
	if filter.WorkerID != "" {
		query = query.Where("worker_id = ?", filter.WorkerID)
	}

	var total int64
	if err := query.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	if filter.Limit > 0 {
		query = query.Limit(filter.Limit)
	} else {
		query = query.Limit(100)
	}
	if filter.Offset > 0 {
		query = query.Offset(filter.Offset)
	}

	var tasks []*entity.Task
	if err := query.Order("priority DESC, created_at ASC").Find(&tasks).Error; err != nil {
		return nil, 0, err
	}

	return tasks, total, nil
}

func (s *TaskRepository) FindStaleByState(ctx context.Context, state entity.TaskState, olderThan time.Duration, limit int) ([]*entity.Task, error) {
	threshold := time.Now().Add(-olderThan)
	var tasks []*entity.Task
	err := s.db.WithContext(ctx).
		Where("state = ? AND updated_at < ?", state, threshold).
		Order("updated_at ASC").
		Limit(limit).
		Find(&tasks).Error
	return tasks, err
}

// Verify MySQL TaskRepository satisfies all repository interfaces.
var (
	_ repository.TaskReader      = (*TaskRepository)(nil)
	_ repository.TaskWriter      = (*TaskRepository)(nil)
	_ repository.TaskCompensator = (*TaskRepository)(nil)
)
