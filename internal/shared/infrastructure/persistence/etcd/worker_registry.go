package etcd

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
	"github.com/dispatchhub/dispatchhub/internal/shared/domain/repository"
	"github.com/dispatchhub/dispatchhub/pkg/log"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	workerPrefix = "/dispatchhub/workers/"
	leaseTTL     = 15
)

// WorkerRegistry implements repository.WorkerRegistry using etcd.
type WorkerRegistry struct {
	client *clientv3.Client
	leases map[string]clientv3.LeaseID
}

func NewWorkerRegistry(client *clientv3.Client) *WorkerRegistry {
	return &WorkerRegistry{
		client: client,
		leases: make(map[string]clientv3.LeaseID),
	}
}

func workerKey(id string) string { return workerPrefix + id }

func (r *WorkerRegistry) Register(ctx context.Context, worker *entity.WorkerInfo) error {
	grant, err := r.client.Grant(ctx, leaseTTL)
	if err != nil {
		return fmt.Errorf("grant lease: %w", err)
	}

	data, err := json.Marshal(worker)
	if err != nil {
		return fmt.Errorf("marshal worker: %w", err)
	}

	_, err = r.client.Put(ctx, workerKey(worker.ID), string(data), clientv3.WithLease(grant.ID))
	if err != nil {
		return fmt.Errorf("put worker: %w", err)
	}

	r.leases[worker.ID] = grant.ID

	ch, err := r.client.KeepAlive(ctx, grant.ID)
	if err != nil {
		return fmt.Errorf("keepalive: %w", err)
	}

	go func() {
		for {
			select {
			case _, ok := <-ch:
				if !ok {
					log.Warnf("lease keepalive channel closed for worker %s", worker.ID)
					return
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	log.Infof("worker registered: %s", worker.ID)
	return nil
}

func (r *WorkerRegistry) Deregister(ctx context.Context, workerID string) error {
	if leaseID, ok := r.leases[workerID]; ok {
		_, _ = r.client.Revoke(ctx, leaseID)
		delete(r.leases, workerID)
	}
	_, err := r.client.Delete(ctx, workerKey(workerID))
	return err
}

func (r *WorkerRegistry) Heartbeat(ctx context.Context, hb *entity.Heartbeat) error {
	worker, err := r.GetWorker(ctx, hb.WorkerID)
	if err != nil {
		return err
	}
	if worker == nil {
		return fmt.Errorf("worker %s not found", hb.WorkerID)
	}

	worker.State = hb.State
	worker.ActiveTasks = hb.ActiveTasks
	worker.CPUUsage = hb.CPUUsage
	worker.MemUsage = hb.MemUsage
	worker.LastHeartbeat = hb.Timestamp

	data, err := json.Marshal(worker)
	if err != nil {
		return err
	}

	leaseID, ok := r.leases[hb.WorkerID]
	if !ok {
		return fmt.Errorf("no lease for worker %s", hb.WorkerID)
	}

	_, err = r.client.Put(ctx, workerKey(hb.WorkerID), string(data), clientv3.WithLease(leaseID))
	return err
}

func (r *WorkerRegistry) GetWorker(ctx context.Context, workerID string) (*entity.WorkerInfo, error) {
	resp, err := r.client.Get(ctx, workerKey(workerID))
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	var worker entity.WorkerInfo
	if err := json.Unmarshal(resp.Kvs[0].Value, &worker); err != nil {
		return nil, err
	}
	return &worker, nil
}

func (r *WorkerRegistry) ListWorkers(ctx context.Context) ([]*entity.WorkerInfo, error) {
	resp, err := r.client.Get(ctx, workerPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	workers := make([]*entity.WorkerInfo, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var w entity.WorkerInfo
		if err := json.Unmarshal(kv.Value, &w); err != nil {
			log.Warnf("skip malformed worker data: %v", err)
			continue
		}
		workers = append(workers, &w)
	}
	return workers, nil
}

func (r *WorkerRegistry) WatchWorkers(ctx context.Context) (<-chan repository.WorkerEvent, error) {
	ch := make(chan repository.WorkerEvent, 64)

	watchCh := r.client.Watch(ctx, workerPrefix, clientv3.WithPrefix(), clientv3.WithPrevKV())

	go func() {
		defer close(ch)
		for {
			select {
			case <-ctx.Done():
				return
			case resp, ok := <-watchCh:
				if !ok {
					return
				}
				for _, ev := range resp.Events {
					event := repository.WorkerEvent{}
					key := string(ev.Kv.Key)
					if len(key) > len(workerPrefix) {
						event.WorkerID = key[len(workerPrefix):]
					}

					switch ev.Type {
					case clientv3.EventTypePut:
						var w entity.WorkerInfo
						if err := json.Unmarshal(ev.Kv.Value, &w); err != nil {
							continue
						}
						event.Worker = &w
						if ev.IsCreate() {
							event.Type = repository.WorkerEventJoined
						} else {
							event.Type = repository.WorkerEventUpdated
						}
					case clientv3.EventTypeDelete:
						event.Type = repository.WorkerEventLeft
						if ev.PrevKv != nil {
							var w entity.WorkerInfo
							if err := json.Unmarshal(ev.PrevKv.Value, &w); err == nil {
								event.Worker = &w
							}
						}
					}

					select {
					case ch <- event:
					default:
						log.Warn("worker event channel full, dropping event")
					}
				}
			}
		}
	}()

	return ch, nil
}

var _ repository.WorkerRegistry = (*WorkerRegistry)(nil)
