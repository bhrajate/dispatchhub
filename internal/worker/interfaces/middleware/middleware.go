package middleware

import (
	"context"
	"time"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
	"github.com/dispatchhub/dispatchhub/internal/worker/application/service"
	"github.com/dispatchhub/dispatchhub/pkg/log"
)

// Logging 返回记录任务执行细节的 middleware。
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

// Recovery 返回可从 handler panic 中恢复的 middleware。
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

// Timeout 返回强制执行单任务 timeout 的 middleware。
// timeout 触发时，ctx 会被取消，middleware 会立即返回。
// handler goroutine 会继续运行，直到检查 ctx.Done() 或自然结束。
// 带缓冲的 channel（cap=1）确保父流程返回后 goroutine 不会卡在发送上，
// 因此不会发生 goroutine 泄漏，只是延迟清理。
// 注意：Handler 应检查 ctx.Done()，以便及时取消。
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
