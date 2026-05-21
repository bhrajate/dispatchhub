package election

import (
	"context"
	"sync"
	"time"

	"github.com/dispatchhub/dispatchhub/pkg/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// LeaderElector 使用 etcd 执行 Leader 选举。
type LeaderElector struct {
	client *clientv3.Client
	prefix string
	id     string
	ttl    int

	mu       sync.RWMutex
	isLeader bool

	onStartedLeading func(ctx context.Context)
	onStoppedLeading func()
	onNewLeader      func(identity string)
}

// Config 保存 Leader 选举参数。
type Config struct {
	Client           *clientv3.Client
	ElectionPrefix   string
	ID               string
	TTL              int
	OnStartedLeading func(ctx context.Context)
	OnStoppedLeading func()
	OnNewLeader      func(identity string)
}

// New 创建新的 LeaderElector。
func New(cfg Config) *LeaderElector {
	if cfg.TTL <= 0 {
		cfg.TTL = 15
	}
	return &LeaderElector{
		client:           cfg.Client,
		prefix:           cfg.ElectionPrefix,
		id:               cfg.ID,
		ttl:              cfg.TTL,
		onStartedLeading: cfg.OnStartedLeading,
		onStoppedLeading: cfg.OnStoppedLeading,
		onNewLeader:      cfg.OnNewLeader,
	}
}

// Run 启动 Leader 选举循环。
func (le *LeaderElector) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if err := le.campaign(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Errorf("leader election campaign failed: %v, retrying in 3s", err)
			time.Sleep(3 * time.Second)
		}
	}
}

func (le *LeaderElector) campaign(ctx context.Context) error {
	session, err := concurrency.NewSession(le.client, concurrency.WithTTL(le.ttl))
	if err != nil {
		return err
	}
	defer session.Close()

	election := concurrency.NewElection(session, le.prefix)

	// 使用局部 context，确保 observe goroutine 在本函数返回前退出，
	// 避免 goroutine 泄漏和 campaign 重试之间的数据竞争。
	observeCtx, observeCancel := context.WithCancel(ctx)

	var observeWg sync.WaitGroup
	observeWg.Go(func() {
		le.observe(observeCtx, election)
	})
	defer observeWg.Wait()
	defer observeCancel()

	log.Infof("campaigning for leader: %s", le.id)
	if err := election.Campaign(ctx, le.id); err != nil {
		return err
	}

	le.mu.Lock()
	le.isLeader = true
	le.mu.Unlock()

	log.Infof("elected as leader: %s", le.id)

	leaderCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		select {
		case <-session.Done():
			cancel()
		case <-ctx.Done():
		}
	}()

	if le.onStartedLeading != nil {
		le.onStartedLeading(leaderCtx)
	}

	<-leaderCtx.Done()

	le.mu.Lock()
	le.isLeader = false
	le.mu.Unlock()

	if le.onStoppedLeading != nil {
		le.onStoppedLeading()
	}

	ctxResign, cancelResign := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelResign()
	_ = election.Resign(ctxResign)

	return nil
}

func (le *LeaderElector) observe(ctx context.Context, election *concurrency.Election) {
	ch := election.Observe(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case resp, ok := <-ch:
			if !ok {
				return
			}
			if len(resp.Kvs) > 0 {
				leader := string(resp.Kvs[0].Value)
				if le.onNewLeader != nil {
					le.onNewLeader(leader)
				}
			}
		}
	}
}

// IsLeader 返回当前实例是否持有 Leader 锁。
func (le *LeaderElector) IsLeader() bool {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.isLeader
}
