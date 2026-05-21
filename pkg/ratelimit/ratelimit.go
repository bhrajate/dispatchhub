package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrRateLimited 是队列超出限流阈值时返回的哨兵错误。
// 可用 fmt.Errorf("...: %w", ErrRateLimited) 附加上下文；
// 传输层可通过 errors.Is 将其映射为 429 / ResourceExhausted。
var ErrRateLimited = errors.New("rate limit exceeded")

// QueueLimitExceededError 记录触发限流的队列，同时仍满足 errors.Is(err, ErrRateLimited)。
type QueueLimitExceededError struct {
	Queue string
}

func (e *QueueLimitExceededError) Error() string {
	return fmt.Sprintf("queue %q rate limit exceeded", e.Queue)
}

func (e *QueueLimitExceededError) Unwrap() error { return ErrRateLimited }

// Limiter 实现具有 token bucket 语义的滑动窗口限流器。
type Limiter struct {
	mu       sync.Mutex
	rate     float64 // 每秒 token 数
	burst    int     // 最大 token 数
	tokens   float64
	lastTime time.Time
}

// NewLimiter 创建一个每秒允许 `rate` 次操作并带 burst 容量的限流器。
func NewLimiter(rate float64, burst int) *Limiter {
	return &Limiter{
		rate:     rate,
		burst:    burst,
		tokens:   float64(burst),
		lastTime: time.Now(),
	}
}

// Allow 检查当前是否允许一次操作（非阻塞）。
func (l *Limiter) Allow() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refill()
	if l.tokens >= 1.0 {
		l.tokens--
		return true
	}
	return false
}

// Wait 会阻塞到操作被允许或 context 过期。
func (l *Limiter) Wait(ctx context.Context) error {
	for {
		if l.Allow() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Millisecond * 10):
		}
	}
}

func (l *Limiter) refill() {
	now := time.Now()
	elapsed := now.Sub(l.lastTime).Seconds()
	l.tokens += elapsed * l.rate
	if l.tokens > float64(l.burst) {
		l.tokens = float64(l.burst)
	}
	l.lastTime = now
}

// MultiQueueLimiter 管理各队列自己的限流器。
type MultiQueueLimiter struct {
	mu           sync.RWMutex
	limiters     map[string]*Limiter
	defaultRate  float64
	defaultBurst int
}

// NewMultiQueueLimiter 创建多队列限流管理器。
func NewMultiQueueLimiter(defaultRate float64, defaultBurst int) *MultiQueueLimiter {
	return &MultiQueueLimiter{
		limiters:     make(map[string]*Limiter),
		defaultRate:  defaultRate,
		defaultBurst: defaultBurst,
	}
}

// SetRate 配置指定队列的限流速率。
func (m *MultiQueueLimiter) SetRate(queue string, rate float64, burst int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.limiters[queue] = NewLimiter(rate, burst)
}

// Allow 检查指定队列上的操作是否被允许。
func (m *MultiQueueLimiter) Allow(queue string) bool {
	m.mu.RLock()
	lim, ok := m.limiters[queue]
	m.mu.RUnlock()

	if !ok {
		m.mu.Lock()
		lim, ok = m.limiters[queue]
		if !ok {
			lim = NewLimiter(m.defaultRate, m.defaultBurst)
			m.limiters[queue] = lim
		}
		m.mu.Unlock()
	}
	return lim.Allow()
}
