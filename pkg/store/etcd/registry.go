package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dispatchhub/dispatchhub/pkg/log"
	"github.com/dispatchhub/dispatchhub/pkg/store"
	"github.com/dispatchhub/dispatchhub/pkg/types"
	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	workerPrefix = "/dispatchhub/workers/"
	leaseTTL     = 15 // seconds
)

// Registry implements store.Registry using etcd for service discovery.
// Workers register with ephemeral keys backed by etcd leases,
// providing automatic deregistration on failure — similar to
// Kubernetes endpoints and node registration.
type Registry struct {
	client *clientv3.Client
	leases map[string]clientv3.LeaseID
}

// NewRegistry creates a new etcd-backed worker registry.
func NewRegistry(client *clientv3.Client) *Registry {
	return &Registry{
		client: client,
		leases: make(map[string]clientv3.LeaseID),
	}
}

func workerKey(id string) string {
	return workerPrefix + id
}

// Register registers a worker with a TTL-based lease.
func (r *Registry) Register(ctx context.Context, worker *types.WorkerInfo) error {
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

	// Start keepalive in background
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

// Deregister removes a worker and revokes its lease.
func (r *Registry) Deregister(ctx context.Context, workerID string) error {
	if leaseID, ok := r.leases[workerID]; ok {
		_, _ = r.client.Revoke(ctx, leaseID)
		delete(r.leases, workerID)
	}
	_, err := r.client.Delete(ctx, workerKey(workerID))
	return err
}

// Heartbeat updates worker info, refreshing the etcd key.
func (r *Registry) Heartbeat(ctx context.Context, hb *types.Heartbeat) error {
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

// GetWorker retrieves a single worker by ID.
func (r *Registry) GetWorker(ctx context.Context, workerID string) (*types.WorkerInfo, error) {
	resp, err := r.client.Get(ctx, workerKey(workerID))
	if err != nil {
		return nil, err
	}
	if len(resp.Kvs) == 0 {
		return nil, nil
	}
	var worker types.WorkerInfo
	if err := json.Unmarshal(resp.Kvs[0].Value, &worker); err != nil {
		return nil, err
	}
	return &worker, nil
}

// ListWorkers returns all registered workers.
func (r *Registry) ListWorkers(ctx context.Context) ([]*types.WorkerInfo, error) {
	resp, err := r.client.Get(ctx, workerPrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	workers := make([]*types.WorkerInfo, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var w types.WorkerInfo
		if err := json.Unmarshal(kv.Value, &w); err != nil {
			log.Warnf("skip malformed worker data: %v", err)
			continue
		}
		workers = append(workers, &w)
	}
	return workers, nil
}

// WatchWorkers watches for worker join/leave events.
func (r *Registry) WatchWorkers(ctx context.Context) (<-chan store.WorkerEvent, error) {
	ch := make(chan store.WorkerEvent, 64)

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
					event := store.WorkerEvent{}
					// Extract worker ID from key
					key := string(ev.Kv.Key)
					if len(key) > len(workerPrefix) {
						event.WorkerID = key[len(workerPrefix):]
					}

					switch ev.Type {
					case clientv3.EventTypePut:
						var w types.WorkerInfo
						if err := json.Unmarshal(ev.Kv.Value, &w); err != nil {
							continue
						}
						event.Worker = &w
						if ev.IsCreate() {
							event.Type = store.WorkerEventJoined
						} else {
							event.Type = store.WorkerEventUpdated
						}
					case clientv3.EventTypeDelete:
						event.Type = store.WorkerEventLeft
						if ev.PrevKv != nil {
							var w types.WorkerInfo
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

func (r *Registry) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for id, leaseID := range r.leases {
		_, _ = r.client.Revoke(ctx, leaseID)
		delete(r.leases, id)
	}
	return nil
}

var _ store.Registry = (*Registry)(nil)
