package retry

import (
	"context"
	"math"
	"math/rand"
	"time"
)

// Strategy defines a retry backoff strategy.
type Strategy interface {
	// NextBackoff returns the duration to wait before the nth retry.
	NextBackoff(attempt int) time.Duration
}

// ExponentialBackoff implements exponential backoff with jitter.
type ExponentialBackoff struct {
	Base       time.Duration
	MaxBackoff time.Duration
	Multiplier float64 // default 2.0
}

func (e *ExponentialBackoff) NextBackoff(attempt int) time.Duration {
	if e.Multiplier == 0 {
		e.Multiplier = 2.0
	}
	backoff := float64(e.Base) * math.Pow(e.Multiplier, float64(attempt))
	if time.Duration(backoff) > e.MaxBackoff {
		backoff = float64(e.MaxBackoff)
	}
	// Add 0-25% jitter
	jitter := backoff * 0.25 * rand.Float64()
	return time.Duration(backoff + jitter)
}

// DefaultStrategy returns the default retry strategy.
func DefaultStrategy() Strategy {
	return &ExponentialBackoff{
		Base:       time.Second,
		MaxBackoff: 5 * time.Minute,
		Multiplier: 2.0,
	}
}

// Do executes fn with retries using the given strategy.
func Do(ctx context.Context, maxAttempts int, strategy Strategy, fn func() error) error {
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if err := fn(); err != nil {
			lastErr = err
			if attempt < maxAttempts-1 {
				backoff := strategy.NextBackoff(attempt)
				select {
				case <-time.After(backoff):
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			continue
		}
		return nil
	}
	return lastErr
}
