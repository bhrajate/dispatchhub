package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
	"github.com/dispatchhub/dispatchhub/internal/shared/domain/repository"
	"github.com/dispatchhub/dispatchhub/pkg/log"
	"github.com/redis/go-redis/v9"
)

const (
	keyPrefix     = "dispatchhub:"
	readyKey      = keyPrefix + "queue:%s:ready"
	delayedKey    = keyPrefix + "queue:%s:delayed"
	inflightKey   = keyPrefix + "queue:%s:inflight"
	leaseKey      = keyPrefix + "queue:%s:lease"
	statsKey      = keyPrefix + "queue:%s:stats"
)

// DefaultVisibilityTimeout 是 inflight 任务在没有 Ack/Nack 情况下的最长可见性超时。
// Worker 取走任务后若进程崩溃 / OOM kill / 网络断开，超过此时长仍未 ack，
// scheduler 的回收循环会把它 HDEL inflight + ZADD ready 重新投递。
//
// 这是一个 floor：当 task.Timeout 显式配置且大于该值时，lease 会按
// task.Timeout + LeaseBuffer 拉长，避免长任务被错误回收。
const DefaultVisibilityTimeout = 30 * time.Second

// LeaseBuffer 是 lease deadline 在 task.Timeout 之上额外预留的安全边界。
// 用于覆盖 worker 端 handler 退出后到 Ack/Nack 实际写入 Redis 之间的尾部
// 延迟（pipeline flush、网络抖动），避免 handler 刚好在 timeout 边界完成
// 却被回收循环抢先重投。
const LeaseBuffer = 10 * time.Second

// computeVisibilityTimeout 给定单个任务，返回它的 lease 时长。
// 规则：max(DefaultVisibilityTimeout, task.Timeout + LeaseBuffer)。
// task.Timeout=0 表示未声明，按默认走；非零时确保 lease 严格大于 handler
// 超时，给 worker 主动 Nack 留出窗口。
func computeVisibilityTimeout(taskTimeout time.Duration) time.Duration {
	if taskTimeout <= 0 {
		return DefaultVisibilityTimeout
	}
	v := taskTimeout + LeaseBuffer
	if v < DefaultVisibilityTimeout {
		return DefaultVisibilityTimeout
	}
	return v
}

// MaxRetryBackoff 是单次重试退避的上限，避免指数增长把任务推到无限远的将来。
// 5 分钟是经验值：业务能容忍的最长"再来一次"间隔，且足够让下游故障恢复。
const MaxRetryBackoff = 5 * time.Minute

// computeRetryBackoff 按 RetryCount 计算下一次重试的实际等待时长。
//
// 公式：base * 2^(retryCount-1) + jitter，cap 到 MaxRetryBackoff。
//   - retryCount=1（首次失败后）→ base
//   - retryCount=2 → 2 * base
//   - retryCount=3 → 4 * base
//   - ...
//
// jitter ∈ [0, base/4)，避免大量同时失败的任务在同一时刻被一起唤醒造成
// "惊群"打爆下游。jitter 用 base 的 25% 而不是当前 backoff 的 25%，是为了
// 在退避被 cap 后仍然保留足够的去同步空间。
//
// 调用方传入的 retryCount 应该是"即将进行的这次重试是第几次"（已经 ++ 过）。
func computeRetryBackoff(base time.Duration, retryCount int) time.Duration {
	if base <= 0 || retryCount <= 0 {
		return 0
	}
	// 指数计算用 int64 防止 time.Duration 直接左移溢出。
	// 2^30 = 10 亿，足以让任何合理 base 都触顶 MaxRetryBackoff。
	shift := min(retryCount-1, 30)
	backoff := time.Duration(int64(base) << shift)
	if backoff > MaxRetryBackoff || backoff < 0 { // <0 防御 int64 溢出
		backoff = MaxRetryBackoff
	}
	jitter := time.Duration(rand.Int64N(int64(base)/4 + 1))
	return backoff + jitter
}

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
func leaseKeyFor(queue string) string    { return fmt.Sprintf(leaseKey, queue) }
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

// dequeueScript 原子完成"取出最高优先级任务 + 登记 inflight + 登记 lease"。
// inflight Hash 用于补偿循环幂等校验（HEXISTS）和 Stats 实时读取（HLEN）；
// lease Sorted Set 用于可见性超时回收循环（ZRANGEBYSCORE 找过期任务）。
// 两者同时维护：lease zset 的成员是 task ID，score 是 deadline 毫秒时间戳。
//
// ARGV[1] = 当前时间戳（毫秒），ARGV[2] = 可见性超时（毫秒）
var dequeueScript = redis.NewScript(`
local now = tonumber(ARGV[1])
local visibility_ms = tonumber(ARGV[2])
local deadline = now + visibility_ms
local queues = KEYS
for i, queue_key in ipairs(queues) do
    local result = redis.call('ZPOPMIN', queue_key, 1)
    if #result > 0 then
        local data = result[1]
        local task = cjson.decode(data)
        local inflight_key = string.gsub(queue_key, ':ready', ':inflight')
        local lease_key = string.gsub(queue_key, ':ready', ':lease')
        redis.call('HSET', inflight_key, task.id, data)
        redis.call('ZADD', lease_key, deadline, task.id)
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

	now := time.Now().UnixMilli()
	defaultVisibilityMs := DefaultVisibilityTimeout.Milliseconds()
	result, err := dequeueScript.Run(ctx, q.client, keys, now, defaultVisibilityMs).Result()
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

	// task.Timeout 序列化为 "30m" 形式的字符串，Lua 脚本里无法解析；因此先在
	// 脚本里以默认值落盘 lease（崩溃也有 lower bound 保护），再在 Go 侧根据
	// task.Timeout 拉长 deadline。仅当任务声明的 timeout 超过默认值时才覆盖，
	// 避免大量短任务多打一次 Redis 调用。
	visibility := computeVisibilityTimeout(task.Timeout.Duration)
	if visibility > DefaultVisibilityTimeout {
		deadline := now + visibility.Milliseconds()
		if err := q.client.ZAdd(ctx, leaseKeyFor(task.QueueName), redis.Z{
			Score:  float64(deadline),
			Member: task.ID,
		}).Err(); err != nil {
			return nil, fmt.Errorf("extend lease for %s: %w", task.ID, err)
		}
	}

	return &task, nil
}

func (q *QueueBroker) Ack(ctx context.Context, queue string, taskID string) error {
	pipe := q.client.Pipeline()
	pipe.HDel(ctx, inflightKeyFor(queue), taskID)
	pipe.ZRem(ctx, leaseKeyFor(queue), taskID)
	pipe.HIncrBy(ctx, statsKeyFor(queue), "completed", 1)
	_, err := pipe.Exec(ctx)
	return err
}

func (q *QueueBroker) Nack(ctx context.Context, queue string, task *entity.Task) error {
	pipe := q.client.Pipeline()
	pipe.HDel(ctx, inflightKeyFor(queue), task.ID)
	pipe.ZRem(ctx, leaseKeyFor(queue), task.ID)

	if task.CanRetry() && task.RetryBackoff.Duration > 0 {
		data, err := json.Marshal(task)
		if err != nil {
			return err
		}
		// 指数退避 + jitter：base * 2^(RetryCount-1)，cap 到 MaxRetryBackoff。
		// task.RetryCount 在 worker handleFailure 里已 ++，传入这里就是"即将
		// 进行的这次重试是第几次"，符合 computeRetryBackoff 的输入语义。
		backoff := computeRetryBackoff(task.RetryBackoff.Duration, task.RetryCount)
		executeAt := time.Now().Add(backoff)
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
	// HLEN inflight 即可表示 active；lease zset 仅用于回收循环，不参与 Stats。
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
// KEYS[1] = ready key，KEYS[2] = delayed key，KEYS[3] = inflight key，KEYS[4] = lease key
// ARGV[1] = 用于匹配 JSON member 的 task ID
var removeScript = redis.NewScript(`
local removed = 0
-- 从 inflight hash 中移除（taskID -> JSON）
if redis.call('HDEL', KEYS[3], ARGV[1]) == 1 then
    removed = removed + 1
end
-- 从 lease zset 中移除（taskID -> deadline）
redis.call('ZREM', KEYS[4], ARGV[1])
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
		[]string{readyKeyFor(queue), delayedKeyFor(queue), inflightKeyFor(queue), leaseKeyFor(queue)},
		taskID,
	).Int64()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("remove task %s from queue %s: %w", taskID, queue, err)
	}
	return nil
}

// reclaimInflightScript 扫描 lease zset 中 deadline 已过的 task ID，
// 把对应任务从 inflight 移回 ready，让其重新可被消费。
//
// 处理过程对每条到期任务原子执行：
//  1. ZREM lease zset
//  2. HGET inflight 拿到原 task JSON
//  3. HDEL inflight
//  4. ZADD ready（按原 priority 计算 score）
//
// 限制 batch 数量避免 Lua 阻塞 Redis 主循环（DefaultReclaimBatch=100）。
//
// KEYS[1] = lease key，KEYS[2] = inflight key，KEYS[3] = ready key
// ARGV[1] = 当前时间戳（毫秒），ARGV[2] = batch 上限
// 返回：实际回收的任务数
var reclaimInflightScript = redis.NewScript(`
local lease_key = KEYS[1]
local inflight_key = KEYS[2]
local ready_key = KEYS[3]
local now = tonumber(ARGV[1])
local batch = tonumber(ARGV[2])
local expired = redis.call('ZRANGEBYSCORE', lease_key, '-inf', now, 'LIMIT', 0, batch)
if #expired == 0 then
    return 0
end
local reclaimed = 0
for _, task_id in ipairs(expired) do
    local data = redis.call('HGET', inflight_key, task_id)
    redis.call('ZREM', lease_key, task_id)
    if data then
        redis.call('HDEL', inflight_key, task_id)
        local task = cjson.decode(data)
        local score = -(task.priority or 5)
        redis.call('ZADD', ready_key, score, data)
        reclaimed = reclaimed + 1
    end
end
return reclaimed
`)

// DefaultReclaimBatch 是单次回收循环处理的最大任务数。
const DefaultReclaimBatch = 100

// ReclaimInflight 扫描指定队列的 lease zset，把可见性超时的任务从 inflight
// 移回 ready，返回实际回收的数量。Worker 进程崩溃 / OOM kill / 网络分区导致
// 任务未及时 Ack/Nack 时，scheduler 周期调用此方法兜底，避免任务永久卡住。
//
// 注意：这是"至少一次"语义的代价 —— 若 Worker 实际仍在执行只是慢了，回收后
// 可能产生重复执行。Handler 自身需要幂等。
func (q *QueueBroker) ReclaimInflight(ctx context.Context, queue string, batchSize int) (int64, error) {
	if batchSize <= 0 {
		batchSize = DefaultReclaimBatch
	}
	now := time.Now().UnixMilli()
	result, err := reclaimInflightScript.Run(ctx, q.client,
		[]string{leaseKeyFor(queue), inflightKeyFor(queue), readyKeyFor(queue)},
		now, batchSize,
	).Int64()
	if err != nil && err != redis.Nil {
		return 0, fmt.Errorf("reclaim inflight in queue %s: %w", queue, err)
	}
	if result > 0 {
		log.Warnf("reclaimed %d inflight tasks in queue %s (visibility timeout exceeded)", result, queue)
	}
	return result, nil
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
