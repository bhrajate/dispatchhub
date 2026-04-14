package service

import (
	"context"
	"fmt"
	"time"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
	"github.com/dispatchhub/dispatchhub/internal/shared/domain/repository"
	"github.com/google/uuid"
)

// BeforeSubmitHook is called before task submission. Return non-nil error to reject.
// Used for rate limiting, quota checks, etc. at the infrastructure layer.
type BeforeSubmitHook func(task *entity.Task) error

// AfterSubmitHook is called after a task is successfully submitted.
// Used for logging, metrics, etc. at the infrastructure layer.
type AfterSubmitHook func(task *entity.Task)

// TaskServiceImpl handles task CRUD and queue operations for the API Server.
// Hooks allow infrastructure concerns (rate limiting, logging, metrics)
// to be injected without polluting the domain layer.
type TaskServiceImpl struct {
	broker       repository.QueueBroker
	taskStore    repository.TaskStore
	beforeSubmit BeforeSubmitHook
	afterSubmit  AfterSubmitHook
}

// NewTaskServiceImpl creates a new TaskServiceImpl.
func NewTaskServiceImpl(broker repository.QueueBroker, taskStore repository.TaskStore) *TaskServiceImpl {
	return &TaskServiceImpl{
		broker:    broker,
		taskStore: taskStore,
	}
}

// SetBeforeSubmit registers a hook called before task submission (e.g., rate limiting).
func (s *TaskServiceImpl) SetBeforeSubmit(hook BeforeSubmitHook) {
	s.beforeSubmit = hook
}

// SetAfterSubmit registers a hook called after successful task submission (e.g., logging/metrics).
func (s *TaskServiceImpl) SetAfterSubmit(hook AfterSubmitHook) {
	s.afterSubmit = hook
}

func (s *TaskServiceImpl) SubmitTask(ctx context.Context, task *entity.Task) error {
	if task.ID == "" {
		task.ID = uuid.New().String()
	}
	if task.QueueName == "" {
		task.QueueName = entity.DefaultQueueName
	}
	if task.Priority == 0 {
		task.Priority = entity.PriorityDefault
	}
	if task.MaxRetries == 0 {
		task.MaxRetries = 3
	}
	if task.Timeout.Duration == 0 {
		task.Timeout = entity.Duration{Duration: 5 * time.Minute}
	}
	task.State = entity.TaskStatePending
	now := time.Now()
	task.CreatedAt = now
	task.UpdatedAt = now

	// Pre-submit hook (rate limiting, quota checks)
	if s.beforeSubmit != nil {
		if err := s.beforeSubmit(task); err != nil {
			return err
		}
	}

	if err := s.taskStore.Create(ctx, task); err != nil {
		return fmt.Errorf("persist task: %w", err)
	}

	if task.ScheduleAt != nil || task.Delay.Duration > 0 {
		if err := s.broker.EnqueueDelayed(ctx, task.QueueName, task); err != nil {
			return fmt.Errorf("enqueue delayed: %w", err)
		}
	} else {
		if err := s.broker.Enqueue(ctx, task.QueueName, task); err != nil {
			return fmt.Errorf("enqueue: %w", err)
		}
	}

	// Post-submit hook (logging, metrics)
	if s.afterSubmit != nil {
		s.afterSubmit(task)
	}

	return nil
}

func (s *TaskServiceImpl) CancelTask(ctx context.Context, taskID string) error {
	task, err := s.taskStore.Get(ctx, taskID)
	if err != nil {
		return err
	}
	if task == nil {
		return fmt.Errorf("task %s not found", taskID)
	}
	if task.IsTerminal() {
		return fmt.Errorf("task %s already in terminal state: %s", taskID, task.State)
	}

	task.State = entity.TaskStateCancelled
	now := time.Now()
	task.FinishedAt = &now
	return s.taskStore.Update(ctx, task)
}

func (s *TaskServiceImpl) GetTask(ctx context.Context, taskID string) (*entity.Task, error) {
	return s.taskStore.Get(ctx, taskID)
}

func (s *TaskServiceImpl) ListTasks(ctx context.Context, filter entity.TaskFilter) ([]*entity.Task, int64, error) {
	return s.taskStore.List(ctx, filter)
}

func (s *TaskServiceImpl) QueueStats(ctx context.Context, queue string) (*entity.QueueStats, error) {
	return s.broker.Stats(ctx, queue)
}

var _ TaskService = (*TaskServiceImpl)(nil)
