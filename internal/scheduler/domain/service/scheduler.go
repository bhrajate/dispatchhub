package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
	"github.com/dispatchhub/dispatchhub/internal/shared/domain/repository"
	"github.com/dispatchhub/dispatchhub/pkg/cronutil"
	"github.com/dispatchhub/dispatchhub/pkg/log"
	"github.com/google/uuid"
)

// SchedulerService 是后台调度服务，负责：
// 1. 将 delayed 任务提升到 ready 队列
// 2. 检测 stale worker 并移除
// 3. 监听 worker 拓扑变化
// 4. 补偿 orphaned task（MySQL 有记录但 Redis 入队缺失）
// 5. 触发到期的 cron job
// 6. 发布队列深度 metrics
//
// 它不处理任务提交或查询，那是 API Server 的职责。
// 它只在 Leader 实例上运行（通过 etcd Leader 选举）。

// taskMaintainer 组合 scheduler 执行任务补偿所需的接口。
type taskMaintainer interface {
	repository.TaskCompensator
	repository.TaskWriter
}

// cronMaintainer 组合 scheduler 触发 cron job 所需的接口。
type cronMaintainer interface {
	repository.CronJobReader
	repository.CronJobWriter
}

type SchedulerService struct {
	broker    repository.QueueBroker
	taskMaint taskMaintainer
	cronMaint cronMaintainer
	registry  repository.WorkerRegistry

	mu      sync.RWMutex
	workers map[string]*entity.WorkerInfo
}

// NewSchedulerService 创建新的 SchedulerService。
func NewSchedulerService(
	broker repository.QueueBroker,
	taskMaint taskMaintainer,
	cronMaint cronMaintainer,
	registry repository.WorkerRegistry,
) *SchedulerService {
	return &SchedulerService{
		broker:    broker,
		taskMaint: taskMaint,
		cronMaint: cronMaint,
		registry:  registry,
		workers:   make(map[string]*entity.WorkerInfo),
	}
}

// TriggerDueCronJobs 扫描已启用且 next_run_at 已到期的 cron job，
// 为每个 job 创建 Task、入队，并推进 next_run_at。
// 返回已触发的 cron job 数量。
func (s *SchedulerService) TriggerDueCronJobs(ctx context.Context, limit int) (int, error) {
	jobs, err := s.cronMaint.FindDueCronJobs(ctx, limit)
	if err != nil {
		return 0, fmt.Errorf("find due cron jobs: %w", err)
	}

	triggered := 0
	var lastErr error
	for _, job := range jobs {
		// 先计算下一次运行时间，若 cron expr 无效则跳过该 job
		now := time.Now()
		nextTime, err := cronutil.NextRunTime(job.CronExpr, now)
		if err != nil {
			lastErr = fmt.Errorf("cron job %s: invalid cron expr %q: %w", job.ID, job.CronExpr, err)
			continue
		}

		// FindDueCronJobs 已过滤 next_run_at IS NULL 的行，到这里 NextRunAt 不会为 nil。
		expectedNextRun := *job.NextRunAt

		// 并发策略：若为 Forbid 且上一次执行仍在运行，则跳过本次实际入队，
		// 但仍需用 CAS 推进 next_run_at，避免下一轮被同一实例（或脑裂中的另一实例）反复扫到。
		if job.ConcurrencyPolicy == entity.ConcurrencyForbid {
			running, err := s.taskMaint.HasRunningTasks(ctx, job.Type, job.Namespace)
			if err != nil {
				lastErr = fmt.Errorf("cron job %s: check running tasks: %w", job.ID, err)
				continue
			}
			if running {
				if _, err := s.cronMaint.ClaimCronJob(ctx, job.ID, expectedNextRun, now, nextTime); err != nil {
					lastErr = fmt.Errorf("cron job %s: claim (forbid skip): %w", job.ID, err)
				}
				continue
			}
		}

		// CAS 抢占：先把 next_run_at 推进到新值，成功者才有权 Create+Enqueue。
		// 这是防御 scheduler 双主期间重复触发的关键：MySQL 行锁让两个 scheduler
		// 的 UPDATE 串行执行，第二个的 WHERE 条件不再匹配，RowsAffected=0。
		// 注意：CAS 必须在 Create 之前；放在之后等于先重复入队再防御，无效。
		claimed, err := s.cronMaint.ClaimCronJob(ctx, job.ID, expectedNextRun, now, nextTime)
		if err != nil {
			lastErr = fmt.Errorf("cron job %s: claim: %w", job.ID, err)
			continue
		}
		if !claimed {
			// 已被其他 scheduler 实例抢占（脑裂窗口或多实例误部署），跳过本次触发。
			log.Debugf("cron job %s: claim lost, another scheduler triggered it", job.ID)
			continue
		}

		// CAS 成功后再 Create+Enqueue。即使后续步骤失败，最坏情况也只是漏掉本次触发，
		// 而不会重复触发——这比"双触发"安全得多。
		task := job.ToTask()
		task.ID = uuid.New().String()
		task.State = entity.TaskStatePending
		task.CreatedAt = now
		task.UpdatedAt = now

		if err := s.taskMaint.Create(ctx, task); err != nil {
			lastErr = fmt.Errorf("cron job %s: create task after claim: %w", job.ID, err)
			continue
		}
		if err := s.broker.Enqueue(ctx, task.QueueName, task); err != nil {
			lastErr = fmt.Errorf("cron job %s: enqueue task: %w", job.ID, err)
			// Task 已持久化到 MySQL 但未写入 Redis，补偿循环会修复
			continue
		}

		triggered++
	}

	return triggered, lastErr
}

// CompensateOrphanedTasks 查找卡在 Pending 状态的任务
// （MySQL 有记录但 Redis 入队可能失败），并重新入队。
func (s *SchedulerService) CompensateOrphanedTasks(ctx context.Context, olderThan time.Duration, limit int) (int, error) {
	tasks, err := s.taskMaint.FindStaleByState(ctx, entity.TaskStatePending, olderThan, limit)
	if err != nil {
		return 0, fmt.Errorf("find orphaned tasks: %w", err)
	}

	compensated := 0
	for _, task := range tasks {
		enqueued, err := s.broker.EnqueueIfNotInflight(ctx, task.QueueName, task)
		if err != nil {
			return compensated, fmt.Errorf("re-enqueue task %s: %w", task.ID, err)
		}
		if enqueued {
			// 刷新 updated_at，但不递增 version，因此：
			// 1. 下一轮补偿不会再次捞起该任务（updated_at 是新的）
			// 2. Worker 仍可使用 Redis JSON 中的原始 version 执行 Update
			if err := s.taskMaint.TouchUpdatedAt(ctx, task.ID); err != nil {
				log.Warnf("task %s: touch updated_at failed (may cause re-compensation): %v", task.ID, err)
			}
			compensated++
		}
	}

	return compensated, nil
}

// CleanupTerminalTasks 删除早于阈值的 completed/failed/cancelled/timeout 终态任务。
func (s *SchedulerService) CleanupTerminalTasks(ctx context.Context, olderThan time.Duration, limit int) (int64, error) {
	return s.taskMaint.DeleteTerminalOlderThan(ctx, olderThan, limit)
}

// ReclaimInflightTasks 遍历所有已知队列，把可见性超时的 inflight 任务移回 ready。
// 当 Worker 崩溃 / OOM kill / 网络分区导致任务未及时 Ack/Nack 时，此循环兜底
// 保证任务最终被重新投递（至少一次语义）。返回所有队列的回收总数。
func (s *SchedulerService) ReclaimInflightTasks(ctx context.Context, batchSize int) (int64, error) {
	var total int64
	var lastErr error
	for _, q := range s.Queues() {
		n, err := s.broker.ReclaimInflight(ctx, q, batchSize)
		if err != nil {
			lastErr = fmt.Errorf("queue %s: %w", q, err)
			continue
		}
		total += n
	}
	return total, lastErr
}

// SyncWorkers 从 registry 刷新 worker 列表。
func (s *SchedulerService) SyncWorkers(ctx context.Context) (int, error) {
	workers, err := s.registry.ListWorkers(ctx)
	if err != nil {
		return 0, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.workers = make(map[string]*entity.WorkerInfo, len(workers))
	for _, w := range workers {
		if w.State == entity.WorkerStateOnline {
			s.workers[w.ID] = w
		}
	}

	return len(s.workers), nil
}

// HandleWorkerEvent 处理 worker 拓扑变更事件。
func (s *SchedulerService) HandleWorkerEvent(event repository.WorkerEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch event.Type {
	case repository.WorkerEventJoined:
		if event.Worker != nil {
			s.workers[event.WorkerID] = event.Worker
		}
	case repository.WorkerEventLeft:
		delete(s.workers, event.WorkerID)
	case repository.WorkerEventUpdated:
		if event.Worker != nil {
			s.workers[event.WorkerID] = event.Worker
		}
	}
}

// DetectStaleWorkers 检查错过 heartbeat 的 worker 并移除。
func (s *SchedulerService) DetectStaleWorkers(staleThreshold time.Duration) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	threshold := time.Now().Add(-staleThreshold)
	var staleWorkers []string
	for id, w := range s.workers {
		if w.LastHeartbeat.Before(threshold) {
			staleWorkers = append(staleWorkers, id)
			delete(s.workers, id)
		}
	}

	return staleWorkers
}

// Queues 返回已知队列列表，该列表从 online worker 推导得到。
func (s *SchedulerService) Queues() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	queueSet := make(map[string]struct{})
	for _, w := range s.workers {
		for _, q := range w.Queues {
			queueSet[q] = struct{}{}
		}
	}

	if len(queueSet) == 0 {
		return []string{entity.DefaultQueueName}
	}

	queues := make([]string, 0, len(queueSet))
	for q := range queueSet {
		queues = append(queues, q)
	}
	return queues
}

// Broker 返回 queue broker。
func (s *SchedulerService) Broker() repository.QueueBroker {
	return s.broker
}

// Registry 返回 worker registry。
func (s *SchedulerService) Registry() repository.WorkerRegistry {
	return s.registry
}
