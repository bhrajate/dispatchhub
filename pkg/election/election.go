package election

import (
	"context"
	"sync"
	"time"

	"github.com/dispatchhub/dispatchhub/pkg/log"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// LeaderElector performs leader election using etcd, similar to
// Kubernetes' client-go leaderelection package.
type LeaderElector struct {
	client     *clientv3.Client
	session    *concurrency.Session
	election   *concurrency.Election
	prefix     string
	id         string
	ttl        int // lease TTL in seconds

	mu       sync.RWMutex
	isLeader bool

	onStartedLeading func(ctx context.Context)
	onStoppedLeading func()
	onNewLeader      func(identity string)
}

// Config holds leader election parameters.
type Config struct {
	Client           *clientv3.Client
	ElectionPrefix   string // etcd key prefix, e.g., "/dispatchhub/scheduler/leader"
	ID               string // unique identity of this candidate
	TTL              int    // lease TTL in seconds (default 15)
	OnStartedLeading func(ctx context.Context)
	OnStoppedLeading func()
	OnNewLeader      func(identity string)
}

// New creates a new LeaderElector.
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

// Run starts the leader election loop. It blocks until ctx is cancelled.
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

	le.session = session
	election := concurrency.NewElection(session, le.prefix)
	le.election = election

	// Start a goroutine to observe leader changes
	go le.observe(ctx)

	log.Infof("campaigning for leader: %s", le.id)
	if err := election.Campaign(ctx, le.id); err != nil {
		return err
	}

	// We are the leader now
	le.mu.Lock()
	le.isLeader = true
	le.mu.Unlock()

	log.Infof("elected as leader: %s", le.id)

	// Create a context that's cancelled when the session expires
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

	// Wait until we lose leadership or context is cancelled
	<-leaderCtx.Done()

	le.mu.Lock()
	le.isLeader = false
	le.mu.Unlock()

	if le.onStoppedLeading != nil {
		le.onStoppedLeading()
	}

	// Resign leadership cleanly
	ctxResign, cancelResign := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelResign()
	_ = election.Resign(ctxResign)

	return nil
}

func (le *LeaderElector) observe(ctx context.Context) {
	ch := le.election.Observe(ctx)
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

// IsLeader returns whether this instance currently holds the leader lock.
func (le *LeaderElector) IsLeader() bool {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.isLeader
}

// Leader returns the current leader's identity.
func (le *LeaderElector) Leader(ctx context.Context) (string, error) {
	if le.election == nil {
		return "", nil
	}
	resp, err := le.election.Leader(ctx)
	if err != nil {
		return "", err
	}
	if len(resp.Kvs) == 0 {
		return "", nil
	}
	return string(resp.Kvs[0].Value), nil
}
