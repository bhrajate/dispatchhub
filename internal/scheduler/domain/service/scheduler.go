package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
	"github.com/dispatchhub/dispatchhub/internal/shared/domain/repository"
)

// SchedulerService is the background scheduling service responsible for:
// 1. Promoting delayed tasks to the ready queue
// 2. Detecting stale workers and removing them
// 3. Watching worker topology changes
// 4. Compensating orphaned tasks (MySQL has record but Redis missed enqueue)
// 5. Publishing queue depth metrics
//
// It does NOT handle task submission/query — that is the API Server's job.
// It runs only on the Leader instance (via etcd leader election).
// taskMaintainer combines the interfaces the scheduler needs for task compensation.
type taskMaintainer interface {
	repository.TaskCompensator
	repository.TaskWriter
}

type SchedulerService struct {
	broker    repository.QueueBroker
	taskMaint taskMaintainer
	registry  repository.WorkerRegistry

	mu      sync.RWMutex
	workers map[string]*entity.WorkerInfo
	queues  []string
}

// NewSchedulerService creates a new SchedulerService.
func NewSchedulerService(
	broker repository.QueueBroker,
	taskMaint taskMaintainer,
	registry repository.WorkerRegistry,
) *SchedulerService {
	return &SchedulerService{
		broker:    broker,
		taskMaint: taskMaint,
		registry:  registry,
		workers:  make(map[string]*entity.WorkerInfo),
		queues:   []string{entity.DefaultQueueName},
	}
}

// CompensateOrphanedTasks finds tasks stuck in Pending state (MySQL has record
// but Redis enqueue may have failed) and re-enqueues them.
// Returns the number of tasks compensated.
func (s *SchedulerService) CompensateOrphanedTasks(ctx context.Context, olderThan time.Duration, limit int) (int, error) {
	tasks, err := s.taskMaint.FindStaleByState(ctx, entity.TaskStatePending, olderThan, limit)
	if err != nil {
		return 0, fmt.Errorf("find orphaned tasks: %w", err)
	}

	compensated := 0
	for _, task := range tasks {
		// Atomically check inflight + enqueue to avoid re-enqueuing tasks
		// that a worker has dequeued but not yet updated in MySQL.
		enqueued, err := s.broker.EnqueueIfNotInflight(ctx, task.QueueName, task)
		if err != nil {
			return compensated, fmt.Errorf("re-enqueue task %s: %w", task.ID, err)
		}
		if enqueued {
			// Update task to refresh updated_at, so FindStaleByState won't
			// pick it up again on the next cycle (it queries updated_at < threshold).
			task.UpdatedAt = time.Now()
			_ = s.taskMaint.Update(ctx, task)
			compensated++
		}
	}

	return compensated, nil
}

// SyncWorkers refreshes the worker list from the registry.
// Returns the number of online workers synced.
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
// Returns the IDs of stale workers that were removed.
func (s *SchedulerService) DetectStaleWorkers(staleThreshold time.Duration) []string {
	s.mu.RLock()
	threshold := time.Now().Add(-staleThreshold)
	var staleWorkers []string
	for id, w := range s.workers {
		if w.LastHeartbeat.Before(threshold) {
			staleWorkers = append(staleWorkers, id)
		}
	}
	s.mu.RUnlock()

	for _, id := range staleWorkers {
		s.mu.Lock()
		delete(s.workers, id)
		s.mu.Unlock()
	}

	return staleWorkers
}

// Queues returns the list of known queues.
func (s *SchedulerService) Queues() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]string, len(s.queues))
	copy(result, s.queues)
	return result
}

// Broker returns the queue broker for use by the application layer.
func (s *SchedulerService) Broker() repository.QueueBroker {
	return s.broker
}

// Registry returns the worker registry for use by the application layer.
func (s *SchedulerService) Registry() repository.WorkerRegistry {
	return s.registry
}
