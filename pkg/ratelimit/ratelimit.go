package ratelimit

import (
	"context"
	"sync"
	"time"
)

// Limiter implements a sliding-window rate limiter with token bucket semantics.
type Limiter struct {
	mu       sync.Mutex
	rate     float64       // tokens per second
	burst    int           // max tokens
	tokens   float64
	lastTime time.Time
}

// NewLimiter creates a rate limiter that allows `rate` operations per second
// with a burst capacity.
func NewLimiter(rate float64, burst int) *Limiter {
	return &Limiter{
		rate:     rate,
		burst:    burst,
		tokens:   float64(burst),
		lastTime: time.Now(),
	}
}

// Allow checks if one operation is allowed right now (non-blocking).
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

// Wait blocks until an operation is allowed or the context expires.
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

// MultiQueueLimiter manages per-queue rate limiters.
type MultiQueueLimiter struct {
	mu       sync.RWMutex
	limiters map[string]*Limiter
	defaultRate  float64
	defaultBurst int
}

// NewMultiQueueLimiter creates a limiter manager for multiple queues.
func NewMultiQueueLimiter(defaultRate float64, defaultBurst int) *MultiQueueLimiter {
	return &MultiQueueLimiter{
		limiters:     make(map[string]*Limiter),
		defaultRate:  defaultRate,
		defaultBurst: defaultBurst,
	}
}

// SetRate configures the rate for a specific queue.
func (m *MultiQueueLimiter) SetRate(queue string, rate float64, burst int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.limiters[queue] = NewLimiter(rate, burst)
}

// Allow checks if an operation on the given queue is allowed.
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
