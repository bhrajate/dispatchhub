package repository

import (
	"context"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
)

// QueueBroker defines the fast-path task queue interface.
type QueueBroker interface {
	Enqueue(ctx context.Context, queue string, task *entity.Task) error
	EnqueueDelayed(ctx context.Context, queue string, task *entity.Task) error
	Dequeue(ctx context.Context, queues []string) (*entity.Task, error)
	Ack(ctx context.Context, queue string, taskID string) error
	Nack(ctx context.Context, queue string, task *entity.Task) error
	PromoteDelayed(ctx context.Context, queue string, batchSize int) (int64, error)
	Len(ctx context.Context, queue string) (int64, error)
	Stats(ctx context.Context, queue string) (*entity.QueueStats, error)
	// EnqueueIfNotInflight atomically checks if the task ID is in the inflight set;
	// if not, enqueues it to the ready queue. Returns true if enqueued, false if skipped.
	// Used by the compensate loop to avoid re-enqueuing tasks being processed.
	EnqueueIfNotInflight(ctx context.Context, queue string, task *entity.Task) (bool, error)
	// Remove removes a task from all queue stages (ready, delayed, inflight).
	// Used when a task is cancelled to prevent workers from picking it up.
	Remove(ctx context.Context, queue string, taskID string) error
	// PublishCancel publishes a cancel signal for the given task ID.
	PublishCancel(ctx context.Context, taskID string) error
	// SubscribeCancel subscribes to task cancel signals.
	// Returns a channel of cancelled task IDs and a cleanup function.
	SubscribeCancel(ctx context.Context) (<-chan string, func(), error)
}
