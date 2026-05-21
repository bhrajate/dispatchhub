package repository

import (
	"context"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
)

// WorkerEventType 表示 worker 事件类型。
type WorkerEventType int

const (
	WorkerEventJoined WorkerEventType = iota
	WorkerEventLeft
	WorkerEventUpdated
)

// WorkerEvent 在 worker 注册信息变更时发出。
type WorkerEvent struct {
	Type     WorkerEventType
	WorkerID string
	Worker   *entity.WorkerInfo
}

// WorkerRegistry 管理 worker 注册与发现。
type WorkerRegistry interface {
	Register(ctx context.Context, worker *entity.WorkerInfo) error
	Deregister(ctx context.Context, workerID string) error
	Heartbeat(ctx context.Context, worker *entity.WorkerInfo) error
	GetWorker(ctx context.Context, workerID string) (*entity.WorkerInfo, error)
	ListWorkers(ctx context.Context) ([]*entity.WorkerInfo, error)
	WatchWorkers(ctx context.Context) (<-chan WorkerEvent, error)
}
