package repository

import (
	"context"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
)

// QueueBroker 定义任务队列 fast-path 接口。
type QueueBroker interface {
	Enqueue(ctx context.Context, queue string, task *entity.Task) error
	EnqueueDelayed(ctx context.Context, queue string, task *entity.Task) error
	Dequeue(ctx context.Context, queues []string) (*entity.Task, error)
	Ack(ctx context.Context, queue string, taskID string) error
	Nack(ctx context.Context, queue string, task *entity.Task) error
	PromoteDelayed(ctx context.Context, queue string, batchSize int) (int64, error)
	Len(ctx context.Context, queue string) (int64, error)
	Stats(ctx context.Context, queue string) (*entity.QueueStats, error)
	// EnqueueIfNotInflight 原子检查 task ID 是否在 inflight 集合中；
	// 若不在，则入 ready 队列。返回 true 表示已入队，false 表示已跳过。
	// 补偿循环用它避免重复入队正在处理的任务。
	EnqueueIfNotInflight(ctx context.Context, queue string, task *entity.Task) (bool, error)
	// Remove 从所有队列阶段（ready、delayed、inflight）移除任务。
	// 任务取消时使用，避免 worker 再取到该任务。
	Remove(ctx context.Context, queue string, taskID string) error
	// PublishCancel 为指定 task ID 发布取消信号。
	PublishCancel(ctx context.Context, taskID string) error
	// SubscribeCancel 订阅任务取消信号。
	// 返回已取消 task ID 的 channel 和清理函数。
	SubscribeCancel(ctx context.Context) (<-chan string, func(), error)
}
