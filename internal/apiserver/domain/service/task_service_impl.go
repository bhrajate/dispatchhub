package service

import (
	"context"
	"fmt"
	"time"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
	"github.com/dispatchhub/dispatchhub/internal/shared/domain/repository"
	"github.com/dispatchhub/dispatchhub/pkg/log"
	"github.com/google/uuid"
)

// BeforeSubmitHook 在任务提交前调用。返回非 nil error 表示拒绝提交。
// 基础设施层用它执行限流、配额检查等逻辑。
type BeforeSubmitHook func(task *entity.Task) error

// AfterSubmitHook 在任务成功提交后调用。
// 基础设施层用它执行日志、metrics 等逻辑。
type AfterSubmitHook func(task *entity.Task)

// TaskServiceImpl 处理 API Server 的任务 CRUD 和队列操作。
// Hook 允许注入限流、日志、metrics 等基础设施关注点，而不污染领域层。
type TaskServiceImpl struct {
	broker         repository.QueueBroker
	taskStore      repository.TaskStore
	cronStore      repository.CronJobStore
	routeValidator *RouteValidator
	beforeSubmit   BeforeSubmitHook
	afterSubmit    AfterSubmitHook
}

// NewTaskServiceImpl 创建新的 TaskServiceImpl。
func NewTaskServiceImpl(broker repository.QueueBroker, taskStore repository.TaskStore, cronStore repository.CronJobStore) *TaskServiceImpl {
	return &TaskServiceImpl{
		broker:    broker,
		taskStore: taskStore,
		cronStore: cronStore,
	}
}

// SetBeforeSubmit 注册任务提交前调用的 hook（例如限流）。
func (s *TaskServiceImpl) SetBeforeSubmit(hook BeforeSubmitHook) {
	s.beforeSubmit = hook
}

// SetRouteValidator 设置可选的 route validator，用于检查 queue+type 可行性。
func (s *TaskServiceImpl) SetRouteValidator(rv *RouteValidator) {
	s.routeValidator = rv
}

// SetAfterSubmit 注册任务成功提交后调用的 hook（例如日志和 metrics）。
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

	// 提交前 hook（限流、配额检查）
	if s.beforeSubmit != nil {
		if err := s.beforeSubmit(task); err != nil {
			return err
		}
	}

	if s.routeValidator != nil {
		if err := s.routeValidator.Validate(ctx, task.QueueName, task.Type); err != nil {
			return fmt.Errorf("route validation: %w", err)
		}
	}

	if err := s.taskStore.Create(ctx, task); err != nil {
		return fmt.Errorf("persist task: %w", err)
	}

	// 入队到 Redis。即使这里失败，任务仍已持久化到 MySQL，
	// scheduler 的补偿循环会在 30 秒内重新入队。
	// 这里刻意不返回错误，避免客户端重试导致重复任务。
	if task.ScheduleAt != nil || task.Delay.Duration > 0 {
		_ = s.broker.EnqueueDelayed(ctx, task.QueueName, task)
	} else {
		_ = s.broker.Enqueue(ctx, task.QueueName, task)
	}

	// 提交后 hook（日志、metrics）
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
	if err := s.taskStore.Update(ctx, task); err != nil {
		return err
	}

	// 尽力而为：从 Redis 队列移除，避免 worker 继续取到该任务。
	if err := s.broker.Remove(ctx, task.QueueName, taskID); err != nil {
		log.Errorf("remove cancelled task %s from queue: %v", taskID, err)
	}

	// 尽力而为：通知正在运行的 worker 取消该任务的 context。
	if err := s.broker.PublishCancel(ctx, taskID); err != nil {
		log.Errorf("publish cancel signal for task %s: %v", taskID, err)
	}

	return nil
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

// --- CronJob 操作 ---

func (s *TaskServiceImpl) CreateCronJob(ctx context.Context, job *entity.CronJob) error {
	if job.ID == "" {
		job.ID = uuid.New().String()
	}
	if job.QueueName == "" {
		job.QueueName = entity.DefaultQueueName
	}
	if job.Priority == 0 {
		job.Priority = entity.PriorityDefault
	}
	if job.MaxRetries == 0 {
		job.MaxRetries = 3
	}
	// NextRunAt 必须由调用方（HTTP/gRPC handler）解析 cron expr 后设置。
	// 领域层不依赖 cron 解析库。
	return s.cronStore.CreateCronJob(ctx, job)
}

func (s *TaskServiceImpl) GetCronJob(ctx context.Context, id string) (*entity.CronJob, error) {
	return s.cronStore.GetCronJob(ctx, id)
}

func (s *TaskServiceImpl) ListCronJobs(ctx context.Context, namespace string, limit, offset int) ([]*entity.CronJob, int64, error) {
	return s.cronStore.ListCronJobs(ctx, namespace, limit, offset)
}

func (s *TaskServiceImpl) DeleteCronJob(ctx context.Context, id string) error {
	return s.cronStore.DeleteCronJob(ctx, id)
}

var _ TaskService = (*TaskServiceImpl)(nil)
