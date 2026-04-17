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

// RouteValidator checks whether a queue+type combination has at least one
// online worker capable of handling it. The worker topology is cached and
// refreshed periodically to avoid hitting etcd on every task submission.
type RouteValidator struct {
	registry     repository.WorkerRegistry
	refreshEvery time.Duration

	mu          sync.RWMutex
	queueTypes  map[string]map[string]struct{} // queue -> set of task types
	lastRefresh time.Time
}

func NewRouteValidator(registry repository.WorkerRegistry, refreshEvery time.Duration) *RouteValidator {
	return &RouteValidator{
		registry:     registry,
		refreshEvery: refreshEvery,
		queueTypes:   make(map[string]map[string]struct{}),
	}
}

// Validate returns an error if no online worker can handle taskType on the
// given queue. Returns nil (allows submission) when:
//   - the combination is valid
//   - no workers are online yet (cold-start tolerance)
//   - the cache refresh fails (fail-open to avoid blocking submissions)
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
		return nil // no workers online yet — allow submission
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
