package application

import (
	"context"
	"time"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/repository"

	domainservice "github.com/dispatchhub/dispatchhub/internal/scheduler/domain/service"
	"github.com/dispatchhub/dispatchhub/pkg/log"
	"github.com/dispatchhub/dispatchhub/pkg/metrics"
)

// SchedulerAppConfig holds configuration for the scheduler application service.
type SchedulerAppConfig struct {
	HealthCheckInterval    time.Duration
	StaleWorkerThreshold   time.Duration
	MetricsInterval        time.Duration
	PromoteDelayedInterval time.Duration
	CompensateInterval     time.Duration
	CompensateOlderThan    time.Duration
	CompensateBatchSize    int
	CronCheckInterval      time.Duration
	CronBatchSize          int
	CleanupInterval        time.Duration
	CleanupOlderThan       time.Duration
	CleanupBatchSize       int
}

// DefaultSchedulerAppConfig returns sensible defaults.
func DefaultSchedulerAppConfig() SchedulerAppConfig {
	return SchedulerAppConfig{
		HealthCheckInterval:    10 * time.Second,
		StaleWorkerThreshold:   30 * time.Second,
		MetricsInterval:        5 * time.Second,
		PromoteDelayedInterval: time.Second,
		CompensateInterval:     30 * time.Second,
		CompensateOlderThan:    30 * time.Second,
		CompensateBatchSize:    100,
		CronCheckInterval:      time.Second,
		CronBatchSize:          100,
		CleanupInterval:        time.Hour,
		CleanupOlderThan:       7 * 24 * time.Hour, // 7 days
		CleanupBatchSize:       1000,
	}
}

// SchedulerAppService orchestrates the scheduler's background reconciliation loops.
// It does NOT handle task submission — that is the API Server's responsibility.
type SchedulerAppService struct {
	cfg       SchedulerAppConfig
	domainSvc *domainservice.SchedulerService
}

// NewSchedulerAppService creates a new SchedulerAppService.
func NewSchedulerAppService(cfg SchedulerAppConfig, domainSvc *domainservice.SchedulerService) *SchedulerAppService {
	return &SchedulerAppService{
		cfg:       cfg,
		domainSvc: domainSvc,
	}
}

// Run starts the scheduler's main reconciliation loops.
func (s *SchedulerAppService) Run(ctx context.Context) error {
	log.Info("scheduler starting reconciliation loops")

	n, err := s.domainSvc.SyncWorkers(ctx)
	if err != nil {
		log.Errorf("initial worker sync failed: %v", err)
	} else {
		log.Infof("synced %d workers", n)
	}

	go s.watchWorkers(ctx)
	go s.promoteDelayedLoop(ctx)
	go s.healthCheckLoop(ctx)
	go s.metricsLoop(ctx)
	go s.compensateLoop(ctx)
	go s.cronLoop(ctx)
	go s.cleanupLoop(ctx)

	<-ctx.Done()
	log.Info("scheduler stopped")
	return nil
}

func (s *SchedulerAppService) watchWorkers(ctx context.Context) {
	ch, err := s.domainSvc.Registry().WatchWorkers(ctx)
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
			switch event.Type {
			case repository.WorkerEventJoined:
				log.Infof("worker joined: %s", event.WorkerID)
			case repository.WorkerEventLeft:
				log.Warnf("worker left: %s", event.WorkerID)
			}
			s.domainSvc.HandleWorkerEvent(event)
		}
	}
}

func (s *SchedulerAppService) promoteDelayedLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.PromoteDelayedInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, q := range s.domainSvc.Queues() {
				if _, err := s.domainSvc.Broker().PromoteDelayed(ctx, q, 100); err != nil {
					log.Errorf("promote delayed tasks in %s: %v", q, err)
				}
			}
		}
	}
}

func (s *SchedulerAppService) healthCheckLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.HealthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stale := s.domainSvc.DetectStaleWorkers(s.cfg.StaleWorkerThreshold)
			for _, id := range stale {
				log.Warnf("worker %s missed heartbeat, marked offline", id)
			}
		}
	}
}

func (s *SchedulerAppService) metricsLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.MetricsInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, q := range s.domainSvc.Queues() {
				stats, err := s.domainSvc.Broker().Stats(ctx, q)
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

// compensateLoop periodically scans MySQL for tasks stuck in Pending state
// that may not have been enqueued to Redis (e.g., Redis write failed after MySQL write).
// ZADD is idempotent, so re-enqueuing an already-queued task is a safe no-op.
func (s *SchedulerAppService) compensateLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.CompensateInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := s.domainSvc.CompensateOrphanedTasks(ctx, s.cfg.CompensateOlderThan, s.cfg.CompensateBatchSize)
			if err != nil {
				log.Errorf("compensate orphaned tasks: %v", err)
			} else if n > 0 {
				log.Infof("compensated %d orphaned tasks", n)
			}
		}
	}
}

// cronLoop periodically checks for due cron jobs and triggers them.
func (s *SchedulerAppService) cronLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.CronCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := s.domainSvc.TriggerDueCronJobs(ctx, s.cfg.CronBatchSize)
			if err != nil {
				log.Errorf("trigger cron jobs: %v", err)
			} else if n > 0 {
				log.Infof("triggered %d cron jobs", n)
			}
		}
	}
}

// cleanupLoop periodically deletes old terminal tasks to prevent unbounded table growth.
func (s *SchedulerAppService) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.CleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := s.domainSvc.CleanupTerminalTasks(ctx, s.cfg.CleanupOlderThan, s.cfg.CleanupBatchSize)
			if err != nil {
				log.Errorf("cleanup terminal tasks: %v", err)
			} else if n > 0 {
				log.Infof("cleaned up %d old terminal tasks", n)
			}
		}
	}
}
