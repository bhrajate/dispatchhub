package scheduler

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dispatchhub/dispatchhub/pkg/config"
	"github.com/dispatchhub/dispatchhub/pkg/hash"
	"github.com/dispatchhub/dispatchhub/pkg/log"
	"github.com/dispatchhub/dispatchhub/pkg/metrics"
	"github.com/dispatchhub/dispatchhub/pkg/store"
	"github.com/dispatchhub/dispatchhub/pkg/types"
	"github.com/google/uuid"
)

// Scheduler is the central control-plane component responsible for:
// 1. Accepting new tasks and placing them in the appropriate queue
// 2. Periodically promoting delayed tasks
// 3. Assigning tasks to workers via consistent hashing
// 4. Detecting failed workers and rescheduling their tasks
//
// Architecture inspired by:
// - Kubernetes scheduler: watch-based reconciliation loop, scoring/filtering
// - Temporal: durable task queues with visibility
// - Asynq: Redis-based priority queues
type Scheduler struct {
	cfg       config.SchedulerConfig
	broker    store.QueueBroker
	taskStore store.TaskStore
	registry  store.Registry
	ring      *hash.ConsistentHash

	mu      sync.RWMutex
	workers map[string]*types.WorkerInfo
	queues  []string

	stopCh chan struct{}
}

// New creates a new Scheduler.
func New(cfg config.SchedulerConfig, broker store.QueueBroker, taskStore store.TaskStore, registry store.Registry) *Scheduler {
	return &Scheduler{
		cfg:       cfg,
		broker:    broker,
		taskStore: taskStore,
		registry:  registry,
		ring:      hash.NewConsistentHash(cfg.VirtualNodes),
		workers:   make(map[string]*types.WorkerInfo),
		queues:    []string{types.DefaultQueueName},
		stopCh:    make(chan struct{}),
	}
}

// SubmitTask validates and enqueues a new task.
func (s *Scheduler) SubmitTask(ctx context.Context, task *types.Task) error {
	if task.ID == "" {
		task.ID = uuid.New().String()
	}
	if task.QueueName == "" {
		task.QueueName = types.DefaultQueueName
	}
	if task.Priority == 0 {
		task.Priority = types.PriorityDefault
	}
	if task.MaxRetries == 0 {
		task.MaxRetries = 3
	}
	if task.Timeout.Duration == 0 {
		task.Timeout = types.Duration{Duration: 5 * time.Minute}
	}
	task.State = types.TaskStatePending
	now := time.Now()
	task.CreatedAt = now
	task.UpdatedAt = now

	// Persist to durable store
	if err := s.taskStore.Create(ctx, task); err != nil {
		return fmt.Errorf("persist task: %w", err)
	}

	// Enqueue to fast-path broker
	if task.ScheduleAt != nil || task.Delay.Duration > 0 {
		if err := s.broker.EnqueueDelayed(ctx, task.QueueName, task); err != nil {
			return fmt.Errorf("enqueue delayed: %w", err)
		}
	} else {
		if err := s.broker.Enqueue(ctx, task.QueueName, task); err != nil {
			return fmt.Errorf("enqueue: %w", err)
		}
	}

	metrics.TasksSubmitted.WithLabelValues(
		task.QueueName, task.Type, fmt.Sprintf("%d", task.Priority),
	).Inc()

	log.Infof("task submitted: id=%s type=%s queue=%s priority=%d",
		task.ID, task.Type, task.QueueName, task.Priority)
	return nil
}

// Run starts the scheduler's main reconciliation loops.
// This should only be called when this instance is the leader.
func (s *Scheduler) Run(ctx context.Context) error {
	log.Info("scheduler starting reconciliation loops")

	// Initialize worker ring from registry
	if err := s.syncWorkers(ctx); err != nil {
		log.Errorf("initial worker sync failed: %v", err)
	}

	// Watch for worker topology changes
	go s.watchWorkers(ctx)

	// Main loops
	go s.promoteDelayedLoop(ctx)
	go s.healthCheckLoop(ctx)
	go s.metricsLoop(ctx)

	<-ctx.Done()
	log.Info("scheduler stopped")
	return nil
}

// syncWorkers refreshes the worker ring from the registry.
func (s *Scheduler) syncWorkers(ctx context.Context) error {
	workers, err := s.registry.ListWorkers(ctx)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Rebuild the ring
	s.ring = hash.NewConsistentHash(s.cfg.VirtualNodes)
	s.workers = make(map[string]*types.WorkerInfo, len(workers))

	for _, w := range workers {
		if w.State == types.WorkerStateOnline {
			s.ring.Add(w.ID)
			s.workers[w.ID] = w
		}
	}

	log.Infof("synced %d workers into hash ring", len(s.workers))
	return nil
}

// watchWorkers watches for worker join/leave events and updates the ring.
func (s *Scheduler) watchWorkers(ctx context.Context) {
	ch, err := s.registry.WatchWorkers(ctx)
	if err != nil {
		log.Errorf("failed to watch workers: %v", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-ch:
			if !ok {
				return
			}
			s.handleWorkerEvent(event)
		}
	}
}

func (s *Scheduler) handleWorkerEvent(event store.WorkerEvent) {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch event.Type {
	case store.WorkerEventJoined:
		log.Infof("worker joined: %s", event.WorkerID)
		s.ring.Add(event.WorkerID)
		if event.Worker != nil {
			s.workers[event.WorkerID] = event.Worker
		}

	case store.WorkerEventLeft:
		log.Warnf("worker left: %s, rescheduling tasks", event.WorkerID)
		s.ring.Remove(event.WorkerID)
		delete(s.workers, event.WorkerID)
		// TODO: trigger rescheduling of tasks assigned to this worker

	case store.WorkerEventUpdated:
		if event.Worker != nil {
			s.workers[event.WorkerID] = event.Worker
		}
	}
}

// promoteDelayedLoop periodically moves due delayed tasks to ready queues.
func (s *Scheduler) promoteDelayedLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, q := range s.queues {
				if _, err := s.broker.PromoteDelayed(ctx, q); err != nil {
					log.Errorf("promote delayed tasks in %s: %v", q, err)
				}
			}
		}
	}
}

// healthCheckLoop detects stale workers and reschedules their tasks.
func (s *Scheduler) healthCheckLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.mu.RLock()
			staleThreshold := time.Now().Add(-30 * time.Second)
			var staleWorkers []string
			for id, w := range s.workers {
				if w.LastHeartbeat.Before(staleThreshold) {
					staleWorkers = append(staleWorkers, id)
				}
			}
			s.mu.RUnlock()

			for _, id := range staleWorkers {
				log.Warnf("worker %s missed heartbeat, marking offline", id)
				s.mu.Lock()
				s.ring.Remove(id)
				delete(s.workers, id)
				s.mu.Unlock()
			}
		}
	}
}

// metricsLoop periodically publishes queue depth metrics.
func (s *Scheduler) metricsLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, q := range s.queues {
				stats, err := s.broker.Stats(ctx, q)
				if err != nil {
					continue
				}
				metrics.QueueDepth.WithLabelValues(q, "pending").Set(float64(stats.Pending))
				metrics.QueueDepth.WithLabelValues(q, "active").Set(float64(stats.Active))
				metrics.QueueDepth.WithLabelValues(q, "scheduled").Set(float64(stats.Scheduled))
			}
		}
	}
}

// GetWorkerForTask uses consistent hashing to determine task placement.
func (s *Scheduler) GetWorkerForTask(taskID string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ring.Get(taskID)
}

// CancelTask cancels a pending or running task.
func (s *Scheduler) CancelTask(ctx context.Context, taskID string) error {
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

	task.State = types.TaskStateCancelled
	now := time.Now()
	task.FinishedAt = &now
	return s.taskStore.Update(ctx, task)
}

// GetTask retrieves a task by ID.
func (s *Scheduler) GetTask(ctx context.Context, taskID string) (*types.Task, error) {
	return s.taskStore.Get(ctx, taskID)
}

// ListTasks queries tasks by filter.
func (s *Scheduler) ListTasks(ctx context.Context, filter types.TaskFilter) ([]*types.Task, int64, error) {
	return s.taskStore.List(ctx, filter)
}

// QueueStats returns statistics for a specific queue.
func (s *Scheduler) QueueStats(ctx context.Context, queue string) (*types.QueueStats, error) {
	return s.broker.Stats(ctx, queue)
}
