package middleware

import (
	"context"
	"time"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
	"github.com/dispatchhub/dispatchhub/internal/worker/application/service"
	"github.com/dispatchhub/dispatchhub/pkg/log"
)

// Logging returns middleware that logs task execution details.
func Logging() service.Middleware {
	return func(next service.Handler) service.Handler {
		return service.HandlerFunc(func(ctx context.Context, task *entity.Task) *entity.TaskResult {
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
func Recovery() service.Middleware {
	return func(next service.Handler) service.Handler {
		return service.HandlerFunc(func(ctx context.Context, task *entity.Task) (result *entity.TaskResult) {
			defer func() {
				if r := recover(); r != nil {
					log.Errorf("handler panic for task %s: %v", task.ID, r)
					result = &entity.TaskResult{
						Error: entity.ErrPanic(r),
					}
				}
			}()
			return next.Handle(ctx, task)
		})
	}
}

// Timeout returns middleware that enforces a per-task timeout.
// When the timeout fires, ctx is cancelled and the middleware returns immediately.
// The handler goroutine continues until it checks ctx.Done() or finishes naturally.
// The buffered channel (cap=1) ensures the goroutine won't block on send after
// the parent returns, so there is no goroutine leak — just delayed cleanup.
// NOTE: Handlers SHOULD check ctx.Done() for prompt cancellation.
func Timeout(defaultTimeout time.Duration) service.Middleware {
	return func(next service.Handler) service.Handler {
		return service.HandlerFunc(func(ctx context.Context, task *entity.Task) *entity.TaskResult {
			timeout := task.Timeout.Duration
			if timeout == 0 {
				timeout = defaultTimeout
			}
			ctx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()

			done := make(chan *entity.TaskResult, 1)
			go func() {
				done <- next.Handle(ctx, task)
			}()

			select {
			case result := <-done:
				return result
			case <-ctx.Done():
				return &entity.TaskResult{
					Error: ctx.Err(),
				}
			}
		})
	}
}
