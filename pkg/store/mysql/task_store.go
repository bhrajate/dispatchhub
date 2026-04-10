package mysql

import (
	"context"
	"fmt"

	"github.com/dispatchhub/dispatchhub/pkg/store"
	"github.com/dispatchhub/dispatchhub/pkg/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// TaskStore implements store.TaskStore using MySQL via GORM.
type TaskStore struct {
	db *gorm.DB
}

// NewTaskStore creates a task store and auto-migrates the schema.
func NewTaskStore(db *gorm.DB) (*TaskStore, error) {
	if err := db.AutoMigrate(&types.Task{}, &types.TaskEvent{}); err != nil {
		return nil, fmt.Errorf("auto migrate: %w", err)
	}
	return &TaskStore{db: db}, nil
}

func (s *TaskStore) Create(ctx context.Context, task *types.Task) error {
	return s.db.WithContext(ctx).Create(task).Error
}

func (s *TaskStore) Get(ctx context.Context, id string) (*types.Task, error) {
	var task types.Task
	err := s.db.WithContext(ctx).Where("id = ?", id).First(&task).Error
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &task, nil
}

// Update applies changes using optimistic locking on the Version field.
func (s *TaskStore) Update(ctx context.Context, task *types.Task) error {
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

func (s *TaskStore) Delete(ctx context.Context, id string) error {
	return s.db.WithContext(ctx).Where("id = ?", id).Delete(&types.Task{}).Error
}

func (s *TaskStore) List(ctx context.Context, filter types.TaskFilter) ([]*types.Task, int64, error) {
	query := s.db.WithContext(ctx).Model(&types.Task{})

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
		query = query.Limit(100) // default limit
	}
	if filter.Offset > 0 {
		query = query.Offset(filter.Offset)
	}

	var tasks []*types.Task
	if err := query.Order("priority DESC, created_at ASC").Find(&tasks).Error; err != nil {
		return nil, 0, err
	}

	return tasks, total, nil
}

// BatchUpdateState atomically transitions tasks from one state to another.
func (s *TaskStore) BatchUpdateState(ctx context.Context, ids []string, from, to types.TaskState) (int64, error) {
	result := s.db.WithContext(ctx).
		Model(&types.Task{}).
		Where("id IN ? AND state = ?", ids, from).
		Updates(map[string]interface{}{
			"state":   to,
			"version": gorm.Expr("version + 1"),
		})
	return result.RowsAffected, result.Error
}

// --- TaskEventStore ---

type TaskEventStore struct {
	db *gorm.DB
}

func NewTaskEventStore(db *gorm.DB) *TaskEventStore {
	return &TaskEventStore{db: db}
}

func (s *TaskEventStore) Append(ctx context.Context, event *types.TaskEvent) error {
	return s.db.WithContext(ctx).Create(event).Error
}

func (s *TaskEventStore) ListByTask(ctx context.Context, taskID string, limit int) ([]*types.TaskEvent, error) {
	var events []*types.TaskEvent
	query := s.db.WithContext(ctx).Where("task_id = ?", taskID).Order("timestamp DESC")
	if limit > 0 {
		query = query.Limit(limit)
	}
	return events, query.Find(&events).Error
}

var (
	_ store.TaskStore      = (*TaskStore)(nil)
	_ store.TaskEventStore = (*TaskEventStore)(nil)
)
