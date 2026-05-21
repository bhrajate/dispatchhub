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

// QueueBroker 使用 Redis sorted set 实现 repository.QueueBroker。
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
local batch = tonumber(ARGV[2])
local tasks = redis.call('ZRANGEBYSCORE', delayed_key, '-inf', now, 'LIMIT', 0, batch)
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

func (q *QueueBroker) PromoteDelayed(ctx context.Context, queue string, batchSize int) (int64, error) {
	now := time.Now().UnixMilli()
	result, err := promoteScript.Run(ctx, q.client,
		[]string{delayedKeyFor(queue), readyKeyFor(queue)},
		now, batchSize,
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

// enqueueIfNotInflightScript 原子检查 task ID 是否存在于 inflight hash。
// 若存在，说明任务正在处理，跳过；若不存在，则通过 ZADD 写入 ready 队列。
// 这可以避免补偿循环重复入队已被 worker 取走但尚未更新到 MySQL 的任务。
//
// KEYS[1] = inflight key，KEYS[2] = ready key
// ARGV[1] = task ID，ARGV[2] = score，ARGV[3] = task JSON
// 返回：1 = 已入队，0 = 已跳过（在 inflight 中）
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

const cancelChannel = keyPrefix + "task:cancel"

// removeScript 原子地从所有队列阶段移除任务。
// KEYS[1] = ready key，KEYS[2] = delayed key，KEYS[3] = inflight key
// ARGV[1] = 用于匹配 JSON member 的 task ID
var removeScript = redis.NewScript(`
local removed = 0
-- 从 inflight hash 中移除（taskID -> JSON）
if redis.call('HDEL', KEYS[3], ARGV[1]) == 1 then
    removed = removed + 1
end
-- 从 ready sorted set 中移除（扫描 JSON member 中匹配的 task ID）
local cursor = "0"
repeat
    local result = redis.call('ZSCAN', KEYS[1], cursor, 'MATCH', '*"id":"' .. ARGV[1] .. '"*', 'COUNT', 100)
    cursor = result[1]
    local members = result[2]
    for i = 1, #members, 2 do
        redis.call('ZREM', KEYS[1], members[i])
        removed = removed + 1
    end
until cursor == "0"
-- 从 delayed sorted set 中移除
cursor = "0"
repeat
    local result = redis.call('ZSCAN', KEYS[2], cursor, 'MATCH', '*"id":"' .. ARGV[1] .. '"*', 'COUNT', 100)
    cursor = result[1]
    local members = result[2]
    for i = 1, #members, 2 do
        redis.call('ZREM', KEYS[2], members[i])
        removed = removed + 1
    end
until cursor == "0"
return removed
`)

func (q *QueueBroker) Remove(ctx context.Context, queue string, taskID string) error {
	_, err := removeScript.Run(ctx, q.client,
		[]string{readyKeyFor(queue), delayedKeyFor(queue), inflightKeyFor(queue)},
		taskID,
	).Int64()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("remove task %s from queue %s: %w", taskID, queue, err)
	}
	return nil
}

func (q *QueueBroker) PublishCancel(ctx context.Context, taskID string) error {
	return q.client.Publish(ctx, cancelChannel, taskID).Err()
}

func (q *QueueBroker) SubscribeCancel(ctx context.Context) (<-chan string, func(), error) {
	pubsub := q.client.Subscribe(ctx, cancelChannel)
	if _, err := pubsub.Receive(ctx); err != nil {
		_ = pubsub.Close()
		return nil, nil, fmt.Errorf("subscribe cancel channel: %w", err)
	}

	ch := make(chan string, 64)
	go func() {
		defer close(ch)
		for msg := range pubsub.Channel() {
			select {
			case ch <- msg.Payload:
			case <-ctx.Done():
				return
			}
		}
	}()

	cleanup := func() { _ = pubsub.Close() }
	return ch, cleanup, nil
}

var _ repository.QueueBroker = (*QueueBroker)(nil)
