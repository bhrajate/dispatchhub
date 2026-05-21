package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
	"github.com/dispatchhub/dispatchhub/internal/shared/domain/repository"
	"github.com/dispatchhub/dispatchhub/pkg/log"
)

// RouteValidator 检查 queue+type 组合是否至少有一个 online worker 可处理。
// worker 拓扑会被缓存并定期刷新，避免每次提交任务都访问 etcd。
type RouteValidator struct {
	registry     repository.WorkerRegistry
	refreshEvery time.Duration

	mu          sync.RWMutex
	queueTypes  map[string]map[string]struct{} // queue -> task type 集合
	lastRefresh time.Time
}

func NewRouteValidator(registry repository.WorkerRegistry, refreshEvery time.Duration) *RouteValidator {
	return &RouteValidator{
		registry:     registry,
		refreshEvery: refreshEvery,
		queueTypes:   make(map[string]map[string]struct{}),
	}
}

// Validate 在指定 queue 上没有 online worker 可处理 taskType 时返回错误。
// 以下情况返回 nil（允许提交）：
//   - 组合有效
//   - 当前还没有 online worker（冷启动容忍）
//   - 缓存刷新失败（fail-open，避免阻塞提交）
func (v *RouteValidator) Validate(ctx context.Context, queue, taskType string) error {
	v.mu.RLock()
	stale := time.Since(v.lastRefresh) > v.refreshEvery
	v.mu.RUnlock()

	if stale {
		if err := v.refresh(ctx); err != nil {
			log.Errorf("route validator refresh: %v", err)
			return nil // fail-open
		}
	}

	v.mu.RLock()
	defer v.mu.RUnlock()

	if len(v.queueTypes) == 0 {
		return nil // 当前还没有 online worker，允许提交
	}

	types, queueExists := v.queueTypes[queue]
	if !queueExists {
		return fmt.Errorf("no online worker is consuming queue %q", queue)
	}

	if _, typeHandled := types[taskType]; !typeHandled {
		return fmt.Errorf("no worker on queue %q handles task type %q", queue, taskType)
	}

	return nil
}

func (v *RouteValidator) refresh(ctx context.Context) error {
	workers, err := v.registry.ListWorkers(ctx)
	if err != nil {
		return err
	}

	queueTypes := make(map[string]map[string]struct{})
	for _, w := range workers {
		if w.State != entity.WorkerStateOnline {
			continue
		}
		for _, q := range w.Queues {
			if queueTypes[q] == nil {
				queueTypes[q] = make(map[string]struct{})
			}
			for _, t := range w.TaskTypes {
				queueTypes[q][t] = struct{}{}
			}
		}
	}

	v.mu.Lock()
	v.queueTypes = queueTypes
	v.lastRefresh = time.Now()
	v.mu.Unlock()

	return nil
}
