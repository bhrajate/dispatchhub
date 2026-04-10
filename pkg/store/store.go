package store

import (
	"context"

	"github.com/dispatchhub/dispatchhub/pkg/types"
)

// TaskStore defines the persistence interface for task state.
// Backed by MySQL for durable storage.
type TaskStore interface {
	// Create persists a new task.
	Create(ctx context.Context, task *types.Task) error
	// Get retrieves a task by ID.
	Get(ctx context.Context, id string) (*types.Task, error)
	// Update updates a task with optimistic locking (using Version field).
	Update(ctx context.Context, task *types.Task) error
	// Delete removes a task.
	Delete(ctx context.Context, id string) error
	// List queries tasks matching the filter.
	List(ctx context.Context, filter types.TaskFilter) ([]*types.Task, int64, error)
	// BatchUpdateState atomically transitions multiple tasks.
	BatchUpdateState(ctx context.Context, ids []string, from, to types.TaskState) (int64, error)
}

// TaskEventStore persists task lifecycle events for auditing.
type TaskEventStore interface {
	Append(ctx context.Context, event *types.TaskEvent) error
	ListByTask(ctx context.Context, taskID string, limit int) ([]*types.TaskEvent, error)
}

// QueueBroker defines the fast-path task queue interface.
// Backed by Redis for high-throughput enqueue/dequeue.
type QueueBroker interface {
	// Enqueue adds a task to the named queue.
	Enqueue(ctx context.Context, queue string, task *types.Task) error
	// EnqueueDelayed adds a task that becomes available after the given delay.
	EnqueueDelayed(ctx context.Context, queue string, task *types.Task) error
	// Dequeue atomically pops the highest-priority task from any of the given queues.
	Dequeue(ctx context.Context, queues []string) (*types.Task, error)
	// Ack acknowledges successful processing; removes from in-flight set.
	Ack(ctx context.Context, queue string, taskID string) error
	// Nack returns a task to the queue for retry.
	Nack(ctx context.Context, queue string, task *types.Task) error
	// PromoteDelayed moves due delayed tasks into the ready queue (called periodically).
	PromoteDelayed(ctx context.Context, queue string) (int64, error)
	// Len returns the number of ready tasks in a queue.
	Len(ctx context.Context, queue string) (int64, error)
	// Stats returns queue statistics.
	Stats(ctx context.Context, queue string) (*types.QueueStats, error)
}

// Registry manages worker registration and discovery.
// Backed by etcd for distributed coordination.
type Registry interface {
	// Register registers a worker with a lease-based TTL.
	Register(ctx context.Context, worker *types.WorkerInfo) error
	// Deregister removes a worker.
	Deregister(ctx context.Context, workerID string) error
	// Heartbeat refreshes the worker's registration TTL.
	Heartbeat(ctx context.Context, heartbeat *types.Heartbeat) error
	// GetWorker retrieves a worker by ID.
	GetWorker(ctx context.Context, workerID string) (*types.WorkerInfo, error)
	// ListWorkers returns all registered workers.
	ListWorkers(ctx context.Context) ([]*types.WorkerInfo, error)
	// WatchWorkers watches for worker changes (join/leave).
	WatchWorkers(ctx context.Context) (<-chan WorkerEvent, error)
}

// WorkerEventType represents the type of worker event.
type WorkerEventType int

const (
	WorkerEventJoined WorkerEventType = iota
	WorkerEventLeft
	WorkerEventUpdated
)

// WorkerEvent is emitted when a worker's registration changes.
type WorkerEvent struct {
	Type     WorkerEventType
	WorkerID string
	Worker   *types.WorkerInfo // nil for Left events
}
