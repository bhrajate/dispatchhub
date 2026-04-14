package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
	"github.com/dispatchhub/dispatchhub/internal/shared/domain/repository"
	"github.com/dispatchhub/dispatchhub/pkg/log"
	"github.com/redis/go-redis/v9"
)

const (
	keyPrefix   = "dispatchhub:"
	readyKey    = keyPrefix + "queue:%s:ready"
	delayedKey  = keyPrefix + "queue:%s:delayed"
	inflightKey = keyPrefix + "queue:%s:inflight"
	statsKey    = keyPrefix + "queue:%s:stats"
)

// QueueBroker implements repository.QueueBroker using Redis sorted sets.
type QueueBroker struct {
	client redis.UniversalClient
}

func NewQueueBroker(client redis.UniversalClient) *QueueBroker {
	return &QueueBroker{client: client}
}

func readyKeyFor(queue string) string    { return fmt.Sprintf(readyKey, queue) }
func delayedKeyFor(queue string) string  { return fmt.Sprintf(delayedKey, queue) }
func inflightKeyFor(queue string) string { return fmt.Sprintf(inflightKey, queue) }
func statsKeyFor(queue string) string    { return fmt.Sprintf(statsKey, queue) }

var ErrQueueFull = fmt.Errorf("queue is full")

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

func (q *QueueBroker) Enqueue(ctx context.Context, queue string, task *entity.Task) error {
	return q.EnqueueWithCap(ctx, queue, task, 0)
}

func (q *QueueBroker) EnqueueWithCap(ctx context.Context, queue string, task *entity.Task, maxSize int64) error {
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

func (q *QueueBroker) EnqueueDelayed(ctx context.Context, queue string, task *entity.Task) error {
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

func (q *QueueBroker) Dequeue(ctx context.Context, queues []string) (*entity.Task, error) {
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

	var task entity.Task
	if err := json.Unmarshal([]byte(data), &task); err != nil {
		return nil, fmt.Errorf("unmarshal task: %w", err)
	}
	return &task, nil
}

func (q *QueueBroker) Ack(ctx context.Context, queue string, taskID string) error {
	pipe := q.client.Pipeline()
	pipe.HDel(ctx, inflightKeyFor(queue), taskID)
	pipe.HIncrBy(ctx, statsKeyFor(queue), "completed", 1)
	_, err := pipe.Exec(ctx)
	return err
}

func (q *QueueBroker) Nack(ctx context.Context, queue string, task *entity.Task) error {
	pipe := q.client.Pipeline()
	pipe.HDel(ctx, inflightKeyFor(queue), task.ID)

	if task.CanRetry() && task.RetryBackoff.Duration > 0 {
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
    redis.call('ZREM', delayed_key, data)
end
return #tasks
`)

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

func (q *QueueBroker) Len(ctx context.Context, queue string) (int64, error) {
	return q.client.ZCard(ctx, readyKeyFor(queue)).Result()
}

func (q *QueueBroker) Stats(ctx context.Context, queue string) (*entity.QueueStats, error) {
	pipe := q.client.Pipeline()
	readyCmd := pipe.ZCard(ctx, readyKeyFor(queue))
	delayedCmd := pipe.ZCard(ctx, delayedKeyFor(queue))
	inflightCmd := pipe.HLen(ctx, inflightKeyFor(queue))
	completedCmd := pipe.HGet(ctx, statsKeyFor(queue), "completed")
	failedCmd := pipe.HGet(ctx, statsKeyFor(queue), "failed")

	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}

	stats := &entity.QueueStats{
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

// enqueueIfNotInflightScript atomically checks if a task ID exists in the
// inflight hash. If it does, the task is being processed — skip. If not,
// ZADD it to the ready queue. This prevents the compensate loop from
// re-enqueuing tasks that a worker has already dequeued but not yet
// updated in MySQL.
//
// KEYS[1] = inflight key, KEYS[2] = ready key
// ARGV[1] = task ID, ARGV[2] = score, ARGV[3] = task JSON
// Returns: 1 = enqueued, 0 = skipped (in inflight)
var enqueueIfNotInflightScript = redis.NewScript(`
local inflight_key = KEYS[1]
local ready_key = KEYS[2]
local task_id = ARGV[1]
local score = tonumber(ARGV[2])
local data = ARGV[3]
if redis.call('HEXISTS', inflight_key, task_id) == 1 then
    return 0
end
redis.call('ZADD', ready_key, score, data)
return 1
`)

func (q *QueueBroker) EnqueueIfNotInflight(ctx context.Context, queue string, task *entity.Task) (bool, error) {
	data, err := json.Marshal(task)
	if err != nil {
		return false, fmt.Errorf("marshal task: %w", err)
	}
	score := float64(-task.Priority)

	result, err := enqueueIfNotInflightScript.Run(ctx, q.client,
		[]string{inflightKeyFor(queue), readyKeyFor(queue)},
		task.ID, score, string(data),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("enqueue-if-not-inflight script: %w", err)
	}
	return result == 1, nil
}

var _ repository.QueueBroker = (*QueueBroker)(nil)
