package application

import (
	"context"
	"time"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/repository"

	domainservice "github.com/dispatchhub/dispatchhub/internal/scheduler/domain/service"
	"github.com/dispatchhub/dispatchhub/pkg/log"
	"github.com/dispatchhub/dispatchhub/pkg/metrics"
)

// SchedulerAppConfig 保存 scheduler 应用服务配置。
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
	ReclaimInflightInterval time.Duration
	ReclaimInflightBatch    int
}

// DefaultSchedulerAppConfig 返回合理的默认配置。
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
		CleanupOlderThan:       7 * 24 * time.Hour, // 7 天
		CleanupBatchSize:       1000,
		// 回收间隔需小于 DefaultVisibilityTimeout (30s)，确保超时任务被及时检测。
		// 5s 平均回收延迟 ~2.5s，可与 healthCheckLoop 区分（后者只下线 worker）。
		ReclaimInflightInterval: 5 * time.Second,
		ReclaimInflightBatch:    100,
	}
}

// SchedulerAppService 编排 scheduler 的后台 reconciliation 循环。
// 它不处理任务提交，那是 API Server 的职责。
type SchedulerAppService struct {
	cfg       SchedulerAppConfig
	domainSvc *domainservice.SchedulerService
}

// NewSchedulerAppService 创建新的 SchedulerAppService。
func NewSchedulerAppService(cfg SchedulerAppConfig, domainSvc *domainservice.SchedulerService) *SchedulerAppService {
	return &SchedulerAppService{
		cfg:       cfg,
		domainSvc: domainSvc,
	}
}

// Run 启动 scheduler 的主要 reconciliation 循环。
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
	go s.reclaimInflightLoop(ctx)

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

// compensateLoop 定期扫描 MySQL 中卡在 Pending 状态的任务，
// 这些任务可能没有成功入队到 Redis（例如 MySQL 写入后 Redis 写入失败）。
// ZADD 是幂等的，因此重复入队已在队列中的任务是安全的 no-op。
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

// cronLoop 定期检查到期的 cron job 并触发。
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

// reclaimInflightLoop 定期把可见性超时的 inflight 任务移回 ready。
// Worker 取走任务后若进程崩溃 / OOM kill / 网络分区导致未及时 Ack/Nack，
// 任务会卡在 inflight 永不被处理；此循环按 lease zset 的 deadline 兜底回收。
//
// 与 healthCheckLoop 的区别：healthCheckLoop 只把 worker 标记为 offline，
// 但不会触碰已被该 worker 取走的 inflight 任务，回收必须靠这个循环完成。
func (s *SchedulerAppService) reclaimInflightLoop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.ReclaimInflightInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := s.domainSvc.ReclaimInflightTasks(ctx, s.cfg.ReclaimInflightBatch)
			if err != nil {
				log.Errorf("reclaim inflight tasks: %v", err)
			} else if n > 0 {
				log.Warnf("reclaimed %d inflight tasks (visibility timeout)", n)
			}
		}
	}
}

// cleanupLoop 定期删除旧的终态任务，避免表无限增长。
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
