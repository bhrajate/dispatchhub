package repository

import (
	"context"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
)

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
	Worker   *entity.WorkerInfo
}

// WorkerRegistry manages worker registration and discovery.
type WorkerRegistry interface {
	Register(ctx context.Context, worker *entity.WorkerInfo) error
	Deregister(ctx context.Context, workerID string) error
	Heartbeat(ctx context.Context, heartbeat *entity.Heartbeat) error
	GetWorker(ctx context.Context, workerID string) (*entity.WorkerInfo, error)
	ListWorkers(ctx context.Context) ([]*entity.WorkerInfo, error)
	WatchWorkers(ctx context.Context) (<-chan WorkerEvent, error)
}
