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

// SchedulerService is the background scheduling service responsible for:
// 1. Promoting delayed tasks to the ready queue
// 2. Detecting stale workers and removing them
// 3. Watching worker topology changes
// 4. Compensating orphaned tasks (MySQL has record but Redis missed enqueue)
// 5. Triggering cron jobs when due
// 6. Publishing queue depth metrics
//
// It does NOT handle task submission/query — that is the API Server's job.
// It runs only on the Leader instance (via etcd leader election).

// taskMaintainer combines the interfaces the scheduler needs for task compensation.
type taskMaintainer interface {
	repository.TaskCompensator
	repository.TaskWriter
}

// cronMaintainer combines the interfaces the scheduler needs for cron job triggering.
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

// NewSchedulerService creates a new SchedulerService.
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

// TriggerDueCronJobs scans for enabled cron jobs whose next_run_at has passed,
// creates a Task for each, enqueues it, and advances next_run_at.
// Returns the number of cron jobs triggered.
func (s *SchedulerService) TriggerDueCronJobs(ctx context.Context, limit int) (int, error) {
	jobs, err := s.cronMaint.FindDueCronJobs(ctx, limit)
	if err != nil {
		return 0, fmt.Errorf("find due cron jobs: %w", err)
	}

	triggered := 0
	var lastErr error
	for _, job := range jobs {
		// Compute next run time first — skip this job if cron expr is invalid
		now := time.Now()
		nextTime, err := cronutil.NextRunTime(job.CronExpr, now)
		if err != nil {
			lastErr = fmt.Errorf("cron job %s: invalid cron expr %q: %w", job.ID, job.CronExpr, err)
			continue
		}

		// Concurrency policy: skip if Forbid and previous execution still running
		if job.ConcurrencyPolicy == entity.ConcurrencyForbid {
			running, err := s.taskMaint.HasRunningTasks(ctx, job.Type, job.Namespace)
			if err != nil {
				lastErr = fmt.Errorf("cron job %s: check running tasks: %w", job.ID, err)
				continue
			}
			if running {
				// Skip this trigger but still advance next_run_at
				job.NextRunAt = &nextTime
				_ = s.cronMaint.UpdateCronJob(ctx, job)
				continue
			}
		}

		task := job.ToTask()
		task.ID = uuid.New().String()
		task.State = entity.TaskStatePending
		task.CreatedAt = now
		task.UpdatedAt = now

		if err := s.taskMaint.Create(ctx, task); err != nil {
			lastErr = fmt.Errorf("cron job %s: create task: %w", job.ID, err)
			continue
		}
		if err := s.broker.Enqueue(ctx, task.QueueName, task); err != nil {
			lastErr = fmt.Errorf("cron job %s: enqueue task: %w", job.ID, err)
			// Task persisted in MySQL but not in Redis — compensate loop will fix it
			continue
		}

		// Advance cron schedule
		job.LastRunAt = &now
		job.NextRunAt = &nextTime
		if err := s.cronMaint.UpdateCronJob(ctx, job); err != nil {
			lastErr = fmt.Errorf("cron job %s: update schedule: %w", job.ID, err)
			continue
		}

		triggered++
	}

	return triggered, lastErr
}

// CompensateOrphanedTasks finds tasks stuck in Pending state (MySQL has record
// but Redis enqueue may have failed) and re-enqueues them.
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
			// Refresh updated_at WITHOUT incrementing version, so:
			// 1. Next compensate cycle won't re-pick this task (updated_at is fresh)
			// 2. Worker can still Update with the original version from Redis JSON
			if err := s.taskMaint.TouchUpdatedAt(ctx, task.ID); err != nil {
				log.Warnf("task %s: touch updated_at failed (may cause re-compensation): %v", task.ID, err)
			}
			compensated++
		}
	}

	return compensated, nil
}

// CleanupTerminalTasks deletes completed/failed/cancelled/timeout tasks older than the threshold.
func (s *SchedulerService) CleanupTerminalTasks(ctx context.Context, olderThan time.Duration, limit int) (int64, error) {
	return s.taskMaint.DeleteTerminalOlderThan(ctx, olderThan, limit)
}

// SyncWorkers refreshes the worker list from the registry.
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

// HandleWorkerEvent processes a worker topology change event.
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

// DetectStaleWorkers checks for workers that missed heartbeats and removes them.
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

// Queues returns the list of known queues, derived from online workers.
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

// Broker returns the queue broker.
func (s *SchedulerService) Broker() repository.QueueBroker {
	return s.broker
}

// Registry returns the worker registry.
func (s *SchedulerService) Registry() repository.WorkerRegistry {
	return s.registry
}
