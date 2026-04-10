package middleware

import (
	"context"
	"time"

	"github.com/dispatchhub/dispatchhub/pkg/log"
	"github.com/dispatchhub/dispatchhub/pkg/types"
	"github.com/dispatchhub/dispatchhub/pkg/worker"
)

// Logging returns middleware that logs task execution details.
func Logging() worker.Middleware {
	return func(next worker.Handler) worker.Handler {
		return worker.HandlerFunc(func(ctx context.Context, task *types.Task) *types.TaskResult {
			start := time.Now()
			logger := log.With(
				"task_id", task.ID,
				"type", task.Type,
				"queue", task.QueueName,
				"retry", task.RetryCount,
			)
			logger.Info("task execution started")
			result := next.Handle(ctx, task)
			duration := time.Since(start)
			if result.Error != nil {
				logger.Errorf("task execution failed in %v: %v", duration, result.Error)
			} else {
				logger.Infof("task execution succeeded in %v", duration)
			}
			return result
		})
	}
}

// Recovery returns middleware that recovers from panics in handlers.
func Recovery() worker.Middleware {
	return func(next worker.Handler) worker.Handler {
		return worker.HandlerFunc(func(ctx context.Context, task *types.Task) (result *types.TaskResult) {
			defer func() {
				if r := recover(); r != nil {
					log.Errorf("handler panic for task %s: %v", task.ID, r)
					result = &types.TaskResult{
						Error: types.ErrPanic(r),
					}
				}
			}()
			return next.Handle(ctx, task)
		})
	}
}

// Timeout returns middleware that enforces a per-task timeout.
func Timeout(defaultTimeout time.Duration) worker.Middleware {
	return func(next worker.Handler) worker.Handler {
		return worker.HandlerFunc(func(ctx context.Context, task *types.Task) *types.TaskResult {
			timeout := task.Timeout.Duration
			if timeout == 0 {
				timeout = defaultTimeout
			}
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			done := make(chan *types.TaskResult, 1)
			go func() {
				done <- next.Handle(ctx, task)
			}()

			select {
			case result := <-done:
				return result
			case <-ctx.Done():
				return &types.TaskResult{
					Error: ctx.Err(),
				}
			}
		})
	}
}
