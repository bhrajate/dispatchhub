package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dispatchhub/dispatchhub/pkg/log"
	"github.com/dispatchhub/dispatchhub/pkg/store"
	"github.com/dispatchhub/dispatchhub/pkg/types"
	"github.com/redis/go-redis/v9"
)

const (
	keyPrefix     = "dispatchhub:"
	readyKey      = keyPrefix + "queue:%s:ready"     // sorted set: score=priority, member=taskJSON
	delayedKey    = keyPrefix + "queue:%s:delayed"    // sorted set: score=executeAt, member=taskJSON
	inflightKey   = keyPrefix + "queue:%s:inflight"   // hash: taskID -> taskJSON
	statsKey      = keyPrefix + "queue:%s:stats"      // hash: field -> count
)

// QueueBroker implements store.QueueBroker using Redis sorted sets.
// Design: priority queue via ZSET with negative priority as score (higher priority = lower score = dequeued first).
// Delayed tasks use a separate ZSET with Unix timestamp as score.
type QueueBroker struct {
	client redis.UniversalClient
}

// NewQueueBroker creates a new Redis-backed queue broker.
func NewQueueBroker(client redis.UniversalClient) *QueueBroker {
	return &QueueBroker{client: client}
}

func readyKeyFor(queue string) string    { return fmt.Sprintf(readyKey, queue) }
func delayedKeyFor(queue string) string  { return fmt.Sprintf(delayedKey, queue) }
func inflightKeyFor(queue string) string { return fmt.Sprintf(inflightKey, queue) }
func statsKeyFor(queue string) string    { return fmt.Sprintf(statsKey, queue) }

// ErrQueueFull is returned when the queue has reached its maximum capacity.
var ErrQueueFull = fmt.Errorf("queue is full")

// enqueueWithCapScript atomically checks queue length and enqueues only if under the limit.
// KEYS[1] = ready key, KEYS[2] = stats key
// ARGV[1] = score, ARGV[2] = task data, ARGV[3] = max size (0 = unlimited)
var enqueueWithCapScript = redis.NewScript(`
local ready_key = KEYS[1]
local stats_key = KEYS[2]
local score = tonumber(ARGV[1])
local data = ARGV[2]
local max_size = tonumber(ARGV[3])
if max_size > 0 then
    local current = redis.call('ZCARD', ready_key)
    if current >= max_size then
        return -1
    end
end
redis.call('ZADD', ready_key, score, data)
redis.call('HINCRBY', stats_key, 'enqueued', 1)
return 0
`)

// Enqueue adds a task to the ready queue using priority as score.
// If maxSize > 0, it atomically checks the queue length before enqueuing
// and returns ErrQueueFull if the capacity limit is reached.
func (q *QueueBroker) Enqueue(ctx context.Context, queue string, task *types.Task) error {
	return q.EnqueueWithCap(ctx, queue, task, 0)
}

// EnqueueWithCap adds a task to the ready queue with an optional capacity limit.
// maxSize=0 means unlimited.
func (q *QueueBroker) EnqueueWithCap(ctx context.Context, queue string, task *types.Task, maxSize int64) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal task: %w", err)
	}
	score := float64(-task.Priority)

	result, err := enqueueWithCapScript.Run(ctx, q.client,
		[]string{readyKeyFor(queue), statsKeyFor(queue)},
		score, string(data), maxSize,
	).Int64()
	if err != nil {
		return fmt.Errorf("enqueue script: %w", err)
	}
	if result == -1 {
		return ErrQueueFull
	}
	return nil
}

// EnqueueDelayed adds a task to the delayed set.
func (q *QueueBroker) EnqueueDelayed(ctx context.Context, queue string, task *types.Task) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("marshal task: %w", err)
	}

	var executeAt time.Time
	if task.ScheduleAt != nil {
		executeAt = *task.ScheduleAt
	} else {
		executeAt = time.Now().Add(task.Delay.Duration)
	}

	return q.client.ZAdd(ctx, delayedKeyFor(queue), redis.Z{
		Score:  float64(executeAt.UnixMilli()),
		Member: string(data),
	}).Err()
}

// dequeueScript atomically pops the lowest-score member from a ZSET
// and moves it to the inflight hash.
var dequeueScript = redis.NewScript(`
local queues = KEYS
for i, queue_key in ipairs(queues) do
    local result = redis.call('ZPOPMIN', queue_key, 1)
    if #result > 0 then
        local data = result[1]
        local task = cjson.decode(data)
        local inflight_key = string.gsub(queue_key, ':ready', ':inflight')
        redis.call('HSET', inflight_key, task.id, data)
        return data
    end
end
return nil
`)

// Dequeue atomically pops the highest-priority task from the given queues.
func (q *QueueBroker) Dequeue(ctx context.Context, queues []string) (*types.Task, error) {
	keys := make([]string, len(queues))
	for i, queue := range queues {
		keys[i] = readyKeyFor(queue)
	}

	result, err := dequeueScript.Run(ctx, q.client, keys).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, nil
		}
		return nil, fmt.Errorf("dequeue script: %w", err)
	}

	data, ok := result.(string)
	if !ok {
		return nil, nil
	}

	var task types.Task
	if err := json.Unmarshal([]byte(data), &task); err != nil {
		return nil, fmt.Errorf("unmarshal task: %w", err)
	}
	return &task, nil
}

// Ack acknowledges a task was successfully processed.
func (q *QueueBroker) Ack(ctx context.Context, queue string, taskID string) error {
	pipe := q.client.Pipeline()
	pipe.HDel(ctx, inflightKeyFor(queue), taskID)
	pipe.HIncrBy(ctx, statsKeyFor(queue), "completed", 1)
	_, err := pipe.Exec(ctx)
	return err
}

// Nack returns a failed task to the ready queue for retry.
func (q *QueueBroker) Nack(ctx context.Context, queue string, task *types.Task) error {
	pipe := q.client.Pipeline()
	pipe.HDel(ctx, inflightKeyFor(queue), task.ID)

	if task.CanRetry() && task.RetryBackoff.Duration > 0 {
		// Re-enqueue as delayed with backoff
		data, err := json.Marshal(task)
		if err != nil {
			return err
		}
		executeAt := time.Now().Add(task.RetryBackoff.Duration)
		pipe.ZAdd(ctx, delayedKeyFor(queue), redis.Z{
			Score:  float64(executeAt.UnixMilli()),
			Member: string(data),
		})
	} else if task.CanRetry() {
		// Immediate retry
		data, err := json.Marshal(task)
		if err != nil {
			return err
		}
		pipe.ZAdd(ctx, readyKeyFor(queue), redis.Z{
			Score:  float64(-task.Priority),
			Member: string(data),
		})
	} else {
		pipe.HIncrBy(ctx, statsKeyFor(queue), "failed", 1)
	}

	_, err := pipe.Exec(ctx)
	return err
}

// promoteScript atomically moves due delayed tasks into the ready queue.
var promoteScript = redis.NewScript(`
local delayed_key = KEYS[1]
local ready_key = KEYS[2]
local now = tonumber(ARGV[1])
local tasks = redis.call('ZRANGEBYSCORE', delayed_key, '-inf', now, 'LIMIT', 0, 100)
if #tasks == 0 then
    return 0
end
for _, data in ipairs(tasks) do
    local task = cjson.decode(data)
    local score = -(task.priority or 5)
    redis.call('ZADD', ready_key, score, data)
end
redis.call('ZREMRANGEBYSCORE', delayed_key, '-inf', now)
return #tasks
`)

// PromoteDelayed moves due delayed tasks into the ready queue.
func (q *QueueBroker) PromoteDelayed(ctx context.Context, queue string) (int64, error) {
	now := time.Now().UnixMilli()
	result, err := promoteScript.Run(ctx, q.client,
		[]string{delayedKeyFor(queue), readyKeyFor(queue)},
		now,
	).Int64()
	if err != nil && err != redis.Nil {
		return 0, err
	}
	if result > 0 {
		log.Debugf("promoted %d delayed tasks in queue %s", result, queue)
	}
	return result, nil
}

// Len returns the count of ready tasks in a queue.
func (q *QueueBroker) Len(ctx context.Context, queue string) (int64, error) {
	return q.client.ZCard(ctx, readyKeyFor(queue)).Result()
}

// Stats returns queue statistics.
func (q *QueueBroker) Stats(ctx context.Context, queue string) (*types.QueueStats, error) {
	pipe := q.client.Pipeline()
	readyCmd := pipe.ZCard(ctx, readyKeyFor(queue))
	delayedCmd := pipe.ZCard(ctx, delayedKeyFor(queue))
	inflightCmd := pipe.HLen(ctx, inflightKeyFor(queue))
	completedCmd := pipe.HGet(ctx, statsKeyFor(queue), "completed")
	failedCmd := pipe.HGet(ctx, statsKeyFor(queue), "failed")

	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}

	stats := &types.QueueStats{
		Name:      queue,
		Pending:   readyCmd.Val(),
		Scheduled: delayedCmd.Val(),
		Active:    inflightCmd.Val(),
	}

	if v, err := completedCmd.Int64(); err == nil {
		stats.Completed = v
	}
	if v, err := failedCmd.Int64(); err == nil {
		stats.Failed = v
	}

	return stats, nil
}

// Verify interface compliance.
var _ store.QueueBroker = (*QueueBroker)(nil)
