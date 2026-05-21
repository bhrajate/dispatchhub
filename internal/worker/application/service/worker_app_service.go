package service

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
	"github.com/dispatchhub/dispatchhub/internal/shared/domain/repository"
	"github.com/dispatchhub/dispatchhub/internal/shared/infrastructure/version"
	"github.com/dispatchhub/dispatchhub/pkg/log"
	"github.com/dispatchhub/dispatchhub/pkg/metrics"
	"github.com/google/uuid"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/mem"
)

// Handler 处理指定的任务类型。
type Handler interface {
	Handle(ctx context.Context, task *entity.Task) *entity.TaskResult
}

// HandlerFunc 是将普通函数作为 Handler 使用的适配器。
type HandlerFunc func(ctx context.Context, task *entity.Task) *entity.TaskResult

func (f HandlerFunc) Handle(ctx context.Context, task *entity.Task) *entity.TaskResult {
	return f(ctx, task)
}

// Middleware 包装 Handler，用于添加横切关注点。
type Middleware func(Handler) Handler

// WorkerConfig 保存 worker 配置。
type WorkerConfig struct {
	ID                string
	Queues            []string
	Concurrency       int
	HeartbeatInterval time.Duration
	ShutdownTimeout   time.Duration
	TaskTimeout       time.Duration
}

// WorkerAppService 是 data-plane 组件，负责拉取任务、
// 执行 handler 并上报 heartbeat。
type WorkerAppService struct {
	cfg       WorkerConfig
	broker    repository.QueueBroker
	registry  repository.WorkerRegistry
	taskStore repository.TaskStore

	mu       sync.RWMutex
	handlers map[string]Handler
	mw       []Middleware

	cancelsMu sync.RWMutex
	cancels   map[string]context.CancelFunc

	info   *entity.WorkerInfo
	active int64
	sem    chan struct{}
	wg     sync.WaitGroup
}

// NewWorkerAppService 创建新的 WorkerAppService。
func NewWorkerAppService(
	cfg WorkerConfig,
	broker repository.QueueBroker,
	registry repository.WorkerRegistry,
	taskStore repository.TaskStore,
) *WorkerAppService {
	if cfg.ID == "" {
		hostname, _ := os.Hostname()
		cfg.ID = fmt.Sprintf("%s-%s", hostname, uuid.New().String()[:8])
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = runtime.NumCPU() * 10
	}

	return &WorkerAppService{
		cfg:       cfg,
		broker:    broker,
		registry:  registry,
		taskStore: taskStore,
		handlers:  make(map[string]Handler),
		cancels:   make(map[string]context.CancelFunc),
		sem:       make(chan struct{}, cfg.Concurrency),
	}
}

// Register 为任务类型注册 handler。
func (w *WorkerAppService) Register(taskType string, handler Handler) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.handlers[taskType] = handler
}

// RegisterFunc 是注册 handler 函数的便捷方法。
func (w *WorkerAppService) RegisterFunc(taskType string, fn func(ctx context.Context, task *entity.Task) *entity.TaskResult) {
	w.Register(taskType, HandlerFunc(fn))
}

// Use 添加包装所有 handler 的 middleware。
func (w *WorkerAppService) Use(mw ...Middleware) {
	w.mw = append(w.mw, mw...)
}

// Run 启动 worker。
func (w *WorkerAppService) Run(ctx context.Context) error {
	hostname, _ := os.Hostname()
	now := time.Now()

	w.mu.RLock()
	taskTypes := make([]string, 0, len(w.handlers))
	for t := range w.handlers {
		taskTypes = append(taskTypes, t)
	}
	w.mu.RUnlock()

	w.info = &entity.WorkerInfo{
		ID:            w.cfg.ID,
		Hostname:      hostname,
		Queues:        w.cfg.Queues,
		TaskTypes:     taskTypes,
		Concurrency:   w.cfg.Concurrency,
		State:         entity.WorkerStateOnline,
		StartedAt:     now,
		LastHeartbeat: now,
		Version:       version.Version,
	}

	if err := w.registry.Register(ctx, w.info); err != nil {
		return fmt.Errorf("register worker: %w", err)
	}
	defer w.registry.Deregister(context.Background(), w.cfg.ID)

	metrics.ActiveWorkers.Inc()
	defer metrics.ActiveWorkers.Dec()

	log.Infof("worker started: id=%s queues=%v concurrency=%d",
		w.cfg.ID, w.cfg.Queues, w.cfg.Concurrency)

	go w.heartbeatLoop(ctx)

	cancelCh, cancelCleanup, err := w.broker.SubscribeCancel(ctx)
	if err != nil {
		log.Errorf("failed to subscribe to cancel channel: %v", err)
	} else {
		defer cancelCleanup()
		go w.cancelListenLoop(ctx, cancelCh)
	}

	w.fetchLoop(ctx)

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

func (w *WorkerAppService) fetchLoop(ctx context.Context) {
	const minBackoff = 100 * time.Millisecond
	const maxBackoff = 2 * time.Second
	backoff := minBackoff

	for {
		select {
		case <-ctx.Done():
			return
		case w.sem <- struct{}{}:
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
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			// 空队列时使用指数 backoff
			if backoff < maxBackoff {
				backoff *= 2
			}
			continue
		}

		// 已获取任务，重置 backoff
		backoff = minBackoff

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

func (w *WorkerAppService) processTask(ctx context.Context, task *entity.Task) {
	active := atomic.AddInt64(&w.active, 1)
	defer atomic.AddInt64(&w.active, -1)

	metrics.ActiveTasks.WithLabelValues(task.QueueName).Inc()
	defer metrics.ActiveTasks.WithLabelValues(task.QueueName).Dec()

	logger := log.With("task_id", task.ID, "type", task.Type, "queue", task.QueueName)
	logger.Infof("processing task (active=%d/%d)", active, w.cfg.Concurrency)

	// 从 MySQL 检查最新状态，若已取消或完成则跳过
	if latest, err := w.taskStore.Get(ctx, task.ID); err == nil && latest != nil && latest.IsTerminal() {
		logger.Infof("task already in terminal state %s, skipping", latest.State)
		_ = w.broker.Ack(ctx, task.QueueName, task.ID)
		return
	}

	// 为该任务创建可取消的 context，使其可被 API server 的取消信号终止。
	taskCtx, taskCancel := context.WithCancel(ctx)
	defer taskCancel()
	defer w.untrackCancel(task.ID)
	w.trackCancel(task.ID, taskCancel)

	// 注册 cancel 后再次检查状态，关闭取消信号早于 trackCancel 到达的竞争窗口。
	if latest, err := w.taskStore.Get(ctx, task.ID); err == nil && latest != nil && latest.IsTerminal() {
		logger.Infof("task cancelled during setup, skipping")
		_ = w.broker.Ack(ctx, task.QueueName, task.ID)
		return
	}

	start := time.Now()

	w.mu.RLock()
	handler, ok := w.handlers[task.Type]
	w.mu.RUnlock()

	if !ok {
		logger.Errorf("no handler registered for type: %s", task.Type)
		w.handleFailure(ctx, task, fmt.Errorf("no handler for type: %s", task.Type))
		return
	}

	for i := len(w.mw) - 1; i >= 0; i-- {
		handler = w.mw[i](handler)
	}

	task.State = entity.TaskStateRunning
	task.WorkerID = w.cfg.ID
	now := time.Now()
	task.StartedAt = &now
	if err := w.taskStore.Update(ctx, task); err != nil {
		logger.Errorf("update task state to running: %v", err)
	}

	// Timeout 由 middleware 链中的 Timeout middleware 处理。
	// 这里不要再添加 context.WithTimeout，避免双重 timeout。
	// 使用 taskCtx，确保取消信号能传递给 handler。
	result := w.safeHandle(taskCtx, handler, task)

	duration := time.Since(start)
	metrics.TaskDuration.WithLabelValues(task.QueueName, task.Type).Observe(duration.Seconds())

	// 检查这是否为显式取消，而不是 timeout
	if result.Error != nil && taskCtx.Err() == context.Canceled {
		if latest, err := w.taskStore.Get(ctx, task.ID); err == nil && latest != nil && latest.State == entity.TaskStateCancelled {
			logger.Infof("task cancelled after %v", duration)
			_ = w.broker.Ack(ctx, task.QueueName, task.ID)
			metrics.TasksProcessed.WithLabelValues(task.QueueName, task.Type, "cancelled").Inc()
			return
		}
	}

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

func (w *WorkerAppService) safeHandle(ctx context.Context, handler Handler, task *entity.Task) (result *entity.TaskResult) {
	defer func() {
		if r := recover(); r != nil {
			result = &entity.TaskResult{
				Error: fmt.Errorf("panic: %v", r),
			}
		}
	}()
	return handler.Handle(ctx, task)
}

func (w *WorkerAppService) handleSuccess(ctx context.Context, task *entity.Task, result *entity.TaskResult) {
	task.State = entity.TaskStateCompleted
	task.Result = result.Output
	now := time.Now()
	task.FinishedAt = &now
	if err := w.taskStore.Update(ctx, task); err != nil {
		log.Errorf("task %s: update completed state: %v", task.ID, err)
	}
	if err := w.broker.Ack(ctx, task.QueueName, task.ID); err != nil {
		log.Errorf("task %s: ack: %v", task.ID, err)
	}
}

func (w *WorkerAppService) handleFailure(ctx context.Context, task *entity.Task, taskErr error) {
	task.Error = taskErr.Error()
	task.RetryCount++

	if task.CanRetry() {
		task.State = entity.TaskStateRetrying
		if err := w.taskStore.Update(ctx, task); err != nil {
			log.Errorf("task %s: update retrying state: %v (skip nack to avoid wrong retry count in queue)", task.ID, err)
			return
		}
		if err := w.broker.Nack(ctx, task.QueueName, task); err != nil {
			log.Errorf("task %s: nack: %v", task.ID, err)
		}
		log.Infof("task %s scheduled for retry %d/%d", task.ID, task.RetryCount, task.MaxRetries)
	} else {
		task.State = entity.TaskStateFailed
		now := time.Now()
		task.FinishedAt = &now
		if err := w.taskStore.Update(ctx, task); err != nil {
			log.Errorf("task %s: update failed state: %v", task.ID, err)
		}
		if err := w.broker.Ack(ctx, task.QueueName, task.ID); err != nil {
			log.Errorf("task %s: ack after failure: %v", task.ID, err)
		}
		log.Warnf("task %s exhausted retries, marked as failed", task.ID)
	}
}

func (w *WorkerAppService) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cpuPct, memPct := systemStats()
			w.info.State = entity.WorkerStateOnline
			w.info.ActiveTasks = int(atomic.LoadInt64(&w.active))
			w.info.CPUUsage = cpuPct
			w.info.MemUsage = memPct
			w.info.LastHeartbeat = time.Now()
			if err := w.registry.Heartbeat(ctx, w.info); err != nil {
				log.Errorf("heartbeat failed: %v", err)
			}
			metrics.WorkerHeartbeats.WithLabelValues(w.cfg.ID).Inc()
		}
	}
}

func (w *WorkerAppService) trackCancel(taskID string, cancel context.CancelFunc) {
	w.cancelsMu.Lock()
	w.cancels[taskID] = cancel
	w.cancelsMu.Unlock()
}

func (w *WorkerAppService) untrackCancel(taskID string) {
	w.cancelsMu.Lock()
	delete(w.cancels, taskID)
	w.cancelsMu.Unlock()
}

func (w *WorkerAppService) cancelRunningTask(taskID string) bool {
	w.cancelsMu.RLock()
	cancel, ok := w.cancels[taskID]
	w.cancelsMu.RUnlock()
	if ok {
		cancel()
	}
	return ok
}

func (w *WorkerAppService) cancelListenLoop(ctx context.Context, ch <-chan string) {
	for {
		select {
		case <-ctx.Done():
			return
		case taskID, ok := <-ch:
			if !ok {
				return
			}
			if w.cancelRunningTask(taskID) {
				log.Infof("received cancel signal for task %s, context cancelled", taskID)
			}
		}
	}
}

func systemStats() (cpuPct, memPct float64) {
	if pcts, err := cpu.Percent(0, false); err == nil && len(pcts) > 0 {
		cpuPct = pcts[0]
	}
	if v, err := mem.VirtualMemory(); err == nil {
		memPct = v.UsedPercent
	}
	return
}
