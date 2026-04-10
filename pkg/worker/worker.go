package worker

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dispatchhub/dispatchhub/pkg/config"
	"github.com/dispatchhub/dispatchhub/pkg/log"
	"github.com/dispatchhub/dispatchhub/pkg/metrics"
	"github.com/dispatchhub/dispatchhub/pkg/store"
	"github.com/dispatchhub/dispatchhub/pkg/types"
	"github.com/google/uuid"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
)

// Handler processes a specific task type.
type Handler interface {
	Handle(ctx context.Context, task *types.Task) *types.TaskResult
}

// HandlerFunc is an adapter for using ordinary functions as Handlers.
type HandlerFunc func(ctx context.Context, task *types.Task) *types.TaskResult

func (f HandlerFunc) Handle(ctx context.Context, task *types.Task) *types.TaskResult {
	return f(ctx, task)
}

// Middleware wraps a Handler to add cross-cutting concerns.
type Middleware func(Handler) Handler

// Worker is the data-plane component that:
// 1. Fetches tasks from Redis queues
// 2. Executes handlers in a bounded goroutine pool
// 3. Reports heartbeats to the registry
// 4. Supports graceful shutdown with in-flight task draining
//
// Backpressure: when all goroutine slots are occupied, the worker stops
// dequeuing — applying natural backpressure without rejecting tasks.
type Worker struct {
	cfg      config.WorkerConfig
	broker   store.QueueBroker
	registry store.Registry
	taskStore store.TaskStore

	mu       sync.RWMutex
	handlers map[string]Handler
	mw       []Middleware

	info     *types.WorkerInfo
	active   int64 // atomic counter of active tasks
	sem      chan struct{}
	wg       sync.WaitGroup
	stopOnce sync.Once
	stopCh   chan struct{}
}

// New creates a new Worker.
func New(cfg config.WorkerConfig, broker store.QueueBroker, registry store.Registry, taskStore store.TaskStore) *Worker {
	if cfg.ID == "" {
		hostname, _ := os.Hostname()
		cfg.ID = fmt.Sprintf("%s-%s", hostname, uuid.New().String()[:8])
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = runtime.NumCPU() * 10
	}

	return &Worker{
		cfg:       cfg,
		broker:    broker,
		registry:  registry,
		taskStore: taskStore,
		handlers:  make(map[string]Handler),
		sem:       make(chan struct{}, cfg.Concurrency),
		stopCh:    make(chan struct{}),
	}
}

// Register registers a handler for a task type.
func (w *Worker) Register(taskType string, handler Handler) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.handlers[taskType] = handler
}

// RegisterFunc is a convenience method for registering handler functions.
func (w *Worker) RegisterFunc(taskType string, fn func(ctx context.Context, task *types.Task) *types.TaskResult) {
	w.Register(taskType, HandlerFunc(fn))
}

// Use adds middleware that wraps all handlers.
func (w *Worker) Use(mw ...Middleware) {
	w.mw = append(w.mw, mw...)
}

// Run starts the worker: registers with the cluster, begins fetching tasks,
// and sends periodic heartbeats.
func (w *Worker) Run(ctx context.Context) error {
	// Build worker info
	hostname, _ := os.Hostname()
	w.info = &types.WorkerInfo{
		ID:          w.cfg.ID,
		Hostname:    hostname,
		Queues:      w.cfg.Queues,
		Concurrency: w.cfg.Concurrency,
		State:       types.WorkerStateOnline,
		StartedAt:   time.Now(),
		Version:     "v0.1.0",
	}

	// Register with the cluster
	if err := w.registry.Register(ctx, w.info); err != nil {
		return fmt.Errorf("register worker: %w", err)
	}
	defer w.registry.Deregister(context.Background(), w.cfg.ID)

	metrics.ActiveWorkers.Inc()
	defer metrics.ActiveWorkers.Dec()

	log.Infof("worker started: id=%s queues=%v concurrency=%d",
		w.cfg.ID, w.cfg.Queues, w.cfg.Concurrency)

	// Start heartbeat
	go w.heartbeatLoop(ctx)

	// Start the fetch loop
	w.fetchLoop(ctx)

	// Wait for in-flight tasks to finish (graceful shutdown)
	log.Info("waiting for in-flight tasks to complete...")
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		log.Info("all in-flight tasks completed")
	case <-time.After(w.cfg.ShutdownTimeout):
		log.Warn("shutdown timeout reached, some tasks may not have completed")
	}

	return nil
}

// fetchLoop continuously dequeues and processes tasks.
func (w *Worker) fetchLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case w.sem <- struct{}{}: // backpressure: block when pool is full
		}

		task, err := w.broker.Dequeue(ctx, w.cfg.Queues)
		if err != nil {
			<-w.sem
			if ctx.Err() != nil {
				return
			}
			log.Errorf("dequeue error: %v", err)
			time.Sleep(time.Second)
			continue
		}

		if task == nil {
			<-w.sem
			// No tasks available, back off briefly
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}

		w.wg.Add(1)
		go func() {
			defer func() {
				<-w.sem
				w.wg.Done()
			}()
			w.processTask(ctx, task)
		}()
	}
}

// processTask executes a single task with timeout and error handling.
func (w *Worker) processTask(ctx context.Context, task *types.Task) {
	active := atomic.AddInt64(&w.active, 1)
	defer atomic.AddInt64(&w.active, -1)

	metrics.ActiveTasks.WithLabelValues(task.QueueName).Inc()
	defer metrics.ActiveTasks.WithLabelValues(task.QueueName).Dec()

	logger := log.With("task_id", task.ID, "type", task.Type, "queue", task.QueueName)
	logger.Infof("processing task (active=%d/%d)", active, w.cfg.Concurrency)

	start := time.Now()

	// Find handler
	w.mu.RLock()
	handler, ok := w.handlers[task.Type]
	w.mu.RUnlock()

	if !ok {
		logger.Errorf("no handler registered for type: %s", task.Type)
		w.handleFailure(ctx, task, fmt.Errorf("no handler for type: %s", task.Type))
		return
	}

	// Apply middleware chain
	for i := len(w.mw) - 1; i >= 0; i-- {
		handler = w.mw[i](handler)
	}

	// Update state to running
	task.State = types.TaskStateRunning
	task.WorkerID = w.cfg.ID
	now := time.Now()
	task.StartedAt = &now
	_ = w.taskStore.Update(ctx, task)

	// Execute with timeout
	timeout := task.Timeout.Duration
	if timeout == 0 {
		timeout = w.cfg.TaskTimeout
	}
	taskCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Run handler with panic recovery
	result := w.safeHandle(taskCtx, handler, task)

	duration := time.Since(start)
	metrics.TaskDuration.WithLabelValues(task.QueueName, task.Type).Observe(duration.Seconds())

	if result.Error != nil {
		logger.Errorf("task failed after %v: %v", duration, result.Error)
		w.handleFailure(ctx, task, result.Error)
		metrics.TasksProcessed.WithLabelValues(task.QueueName, task.Type, "failed").Inc()
	} else {
		logger.Infof("task completed in %v", duration)
		w.handleSuccess(ctx, task, result)
		metrics.TasksProcessed.WithLabelValues(task.QueueName, task.Type, "completed").Inc()
	}
}

func (w *Worker) safeHandle(ctx context.Context, handler Handler, task *types.Task) (result *types.TaskResult) {
	defer func() {
		if r := recover(); r != nil {
			result = &types.TaskResult{
				Error: fmt.Errorf("panic: %v", r),
			}
		}
	}()
	return handler.Handle(ctx, task)
}

func (w *Worker) handleSuccess(ctx context.Context, task *types.Task, result *types.TaskResult) {
	task.State = types.TaskStateCompleted
	task.Result = result.Output
	now := time.Now()
	task.FinishedAt = &now
	_ = w.taskStore.Update(ctx, task)
	_ = w.broker.Ack(ctx, task.QueueName, task.ID)
}

func (w *Worker) handleFailure(ctx context.Context, task *types.Task, err error) {
	task.Error = err.Error()
	task.RetryCount++

	if task.CanRetry() {
		task.State = types.TaskStateRetrying
		_ = w.taskStore.Update(ctx, task)
		_ = w.broker.Nack(ctx, task.QueueName, task)
		log.Infof("task %s scheduled for retry %d/%d", task.ID, task.RetryCount, task.MaxRetries)
	} else {
		task.State = types.TaskStateFailed
		now := time.Now()
		task.FinishedAt = &now
		_ = w.taskStore.Update(ctx, task)
		_ = w.broker.Ack(ctx, task.QueueName, task.ID) // remove from inflight
		log.Warnf("task %s exhausted retries, marked as failed", task.ID)
	}
}

// heartbeatLoop sends periodic heartbeats to the registry.
func (w *Worker) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cpuPct, memPct := w.systemStats()
			hb := &types.Heartbeat{
				WorkerID:    w.cfg.ID,
				State:       types.WorkerStateOnline,
				ActiveTasks: int(atomic.LoadInt64(&w.active)),
				CPUUsage:    cpuPct,
				MemUsage:    memPct,
				Timestamp:   time.Now(),
			}
			if err := w.registry.Heartbeat(ctx, hb); err != nil {
				log.Errorf("heartbeat failed: %v", err)
			}
			metrics.WorkerHeartbeats.WithLabelValues(w.cfg.ID).Inc()
		}
	}
}

func (w *Worker) systemStats() (cpuPct, memPct float64) {
	if pcts, err := cpu.Percent(0, false); err == nil && len(pcts) > 0 {
		cpuPct = pcts[0]
	}
	if v, err := mem.VirtualMemory(); err == nil {
		memPct = v.UsedPercent
	}
	return
}
