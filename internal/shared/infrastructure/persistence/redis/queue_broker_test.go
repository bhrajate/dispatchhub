package redis

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/dispatchhub/dispatchhub/internal/shared/domain/entity"
	"github.com/redis/go-redis/v9"
)

func newTestBroker(t *testing.T) (*QueueBroker, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis run: %v", err)
	}
	t.Cleanup(mr.Close)

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	return NewQueueBroker(client), mr
}

func makeTask(id, queue string, priority entity.TaskPriority) *entity.Task {
	return &entity.Task{
		ID:        id,
		QueueName: queue,
		Priority:  priority,
		State:     entity.TaskStatePending,
		Type:      "test.handler",
	}
}

// TestDequeueWritesLeaseEntry 验证 Dequeue 同时写入 inflight Hash 和 lease Sorted Set，
// 这是回收循环能够发现超时任务的前提。
func TestDequeueWritesLeaseEntry(t *testing.T) {
	broker, mr := newTestBroker(t)
	ctx := context.Background()

	task := makeTask("t1", "default", entity.PriorityDefault)
	if err := broker.Enqueue(ctx, "default", task); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	got, err := broker.Dequeue(ctx, []string{"default"})
	if err != nil {
		t.Fatalf("dequeue: %v", err)
	}
	if got == nil || got.ID != "t1" {
		t.Fatalf("expected task t1, got %+v", got)
	}

	if !mr.Exists(inflightKeyFor("default")) {
		t.Fatal("inflight hash should contain task after dequeue")
	}
	if !mr.Exists(leaseKeyFor("default")) {
		t.Fatal("lease zset should contain task after dequeue")
	}

	score, err := mr.ZScore(leaseKeyFor("default"), "t1")
	if err != nil {
		t.Fatalf("lease zscore: %v", err)
	}
	now := time.Now().UnixMilli()
	deadline := int64(score)
	expectedMin := now + DefaultVisibilityTimeout.Milliseconds() - 5_000
	expectedMax := now + DefaultVisibilityTimeout.Milliseconds() + 5_000
	if deadline < expectedMin || deadline > expectedMax {
		t.Fatalf("lease deadline %d not in [%d, %d]", deadline, expectedMin, expectedMax)
	}
}

// TestDequeueExtendsLeaseForLongTimeout 验证当 task.Timeout 大于
// DefaultVisibilityTimeout 时，lease deadline 按 timeout + LeaseBuffer 拉长。
// 这是长任务（如批处理 / 生成调用）不被回收循环误杀的关键路径。
func TestDequeueExtendsLeaseForLongTimeout(t *testing.T) {
	broker, mr := newTestBroker(t)
	ctx := context.Background()

	timeout := 10 * time.Minute
	task := makeTask("t1", "default", entity.PriorityDefault)
	task.Timeout = entity.Duration{Duration: timeout}
	if err := broker.Enqueue(ctx, "default", task); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	now := time.Now().UnixMilli()
	if _, err := broker.Dequeue(ctx, []string{"default"}); err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	score, err := mr.ZScore(leaseKeyFor("default"), "t1")
	if err != nil {
		t.Fatalf("lease zscore: %v", err)
	}
	expected := now + (timeout + LeaseBuffer).Milliseconds()
	deadline := int64(score)
	// ±5s 余地：覆盖测试机时钟抖动。
	if deadline < expected-5_000 || deadline > expected+5_000 {
		t.Fatalf("lease deadline %d not near expected %d (timeout+buffer)", deadline, expected)
	}
}

// TestDequeueUsesDefaultVisibilityForShortTimeout 验证 task.Timeout 小于
// DefaultVisibilityTimeout 时，lease 仍取默认 floor —— 避免短 timeout 任务
// 因 lease 比 handler 还短而出现重复执行。
func TestDequeueUsesDefaultVisibilityForShortTimeout(t *testing.T) {
	broker, mr := newTestBroker(t)
	ctx := context.Background()

	task := makeTask("t1", "default", entity.PriorityDefault)
	task.Timeout = entity.Duration{Duration: 5 * time.Second} // < DefaultVisibilityTimeout
	_ = broker.Enqueue(ctx, "default", task)

	now := time.Now().UnixMilli()
	_, _ = broker.Dequeue(ctx, []string{"default"})

	score, err := mr.ZScore(leaseKeyFor("default"), "t1")
	if err != nil {
		t.Fatalf("lease zscore: %v", err)
	}
	deadline := int64(score)
	expected := now + DefaultVisibilityTimeout.Milliseconds()
	if deadline < expected-5_000 || deadline > expected+5_000 {
		t.Fatalf("expected default-floored deadline ~%d, got %d", expected, deadline)
	}
}

// TestAckRemovesLeaseEntry 验证 Ack 同时清理 inflight 与 lease，
// 否则已完成任务的 lease 条目会被回收循环误判为超时重投。
func TestAckRemovesLeaseEntry(t *testing.T) {
	broker, mr := newTestBroker(t)
	ctx := context.Background()

	task := makeTask("t1", "default", entity.PriorityDefault)
	_ = broker.Enqueue(ctx, "default", task)
	_, _ = broker.Dequeue(ctx, []string{"default"})

	if err := broker.Ack(ctx, "default", "t1"); err != nil {
		t.Fatalf("ack: %v", err)
	}

	if mr.Exists(leaseKeyFor("default")) {
		t.Fatal("lease zset should be cleared after ack")
	}
}

// TestReclaimInflightMovesExpiredTasks 是核心回收路径测试：
// 模拟 Worker 取走任务后崩溃（不 Ack 也不 Nack），验证回收循环把
// 超过 deadline 的任务从 inflight 移回 ready。
func TestReclaimInflightMovesExpiredTasks(t *testing.T) {
	broker, mr := newTestBroker(t)
	ctx := context.Background()

	task := makeTask("t1", "default", entity.PriorityHigh)
	_ = broker.Enqueue(ctx, "default", task)
	if _, err := broker.Dequeue(ctx, []string{"default"}); err != nil {
		t.Fatalf("dequeue: %v", err)
	}

	// 把 lease deadline 改成"过去"，模拟 Worker 已超时未 ack。
	mr.ZAdd(leaseKeyFor("default"), float64(time.Now().Add(-time.Second).UnixMilli()), "t1")

	n, err := broker.ReclaimInflight(ctx, "default", 100)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 reclaimed, got %d", n)
	}

	if mr.Exists(inflightKeyFor("default")) {
		t.Fatal("inflight hash should be empty after reclaim")
	}
	if mr.Exists(leaseKeyFor("default")) {
		t.Fatal("lease zset should be empty after reclaim")
	}

	// 验证回到 ready 后能再次出队。
	got, err := broker.Dequeue(ctx, []string{"default"})
	if err != nil {
		t.Fatalf("re-dequeue: %v", err)
	}
	if got == nil || got.ID != "t1" {
		t.Fatalf("expected reclaimed task to be re-dequeueable, got %+v", got)
	}
	if got.Priority != entity.PriorityHigh {
		t.Fatalf("priority lost during reclaim: got %d", got.Priority)
	}
}

// TestReclaimInflightSkipsActiveTasks 验证未到 deadline 的 inflight 任务
// 不会被错误回收（否则正常执行的任务会被重复投递）。
func TestReclaimInflightSkipsActiveTasks(t *testing.T) {
	broker, mr := newTestBroker(t)
	ctx := context.Background()

	task := makeTask("t1", "default", entity.PriorityDefault)
	_ = broker.Enqueue(ctx, "default", task)
	_, _ = broker.Dequeue(ctx, []string{"default"})

	// lease deadline 默认 30s 后，不应被回收。
	n, err := broker.ReclaimInflight(ctx, "default", 100)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if n != 0 {
		t.Fatalf("expected 0 reclaimed for active task, got %d", n)
	}

	if !mr.Exists(inflightKeyFor("default")) {
		t.Fatal("inflight should still contain active task")
	}
}

// TestReclaimInflightOrphanedLease 验证 lease zset 中存在但 inflight Hash 中
// 缺失的孤儿条目会被清理，不会反复触发回收（防御补偿循环 + Remove 竞态）。
func TestReclaimInflightOrphanedLease(t *testing.T) {
	broker, mr := newTestBroker(t)
	ctx := context.Background()

	// 直接构造孤儿 lease：lease 有但 inflight 没有（模拟 Remove 之后的残留）。
	mr.ZAdd(leaseKeyFor("default"), float64(time.Now().Add(-time.Second).UnixMilli()), "ghost")

	n, err := broker.ReclaimInflight(ctx, "default", 100)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	// 没有 inflight 数据可恢复，但孤儿 lease 应被 ZREM 清理。
	if n != 0 {
		t.Fatalf("expected 0 reclaimed for orphan lease, got %d", n)
	}
	if mr.Exists(leaseKeyFor("default")) {
		t.Fatal("orphan lease entry should be cleaned up")
	}
}

// TestReclaimInflightBatchLimit 验证 batch 上限被 Lua 脚本尊重，避免单次脚本
// 阻塞 Redis 主循环过久。
func TestReclaimInflightBatchLimit(t *testing.T) {
	broker, mr := newTestBroker(t)
	ctx := context.Background()

	// 入队 10 条 + 全部出队 + 把 lease 改成已过期。
	for i := range 10 {
		id := "t" + string(rune('0'+i))
		task := makeTask(id, "default", entity.PriorityDefault)
		_ = broker.Enqueue(ctx, "default", task)
	}
	for range 10 {
		if _, err := broker.Dequeue(ctx, []string{"default"}); err != nil {
			t.Fatalf("dequeue: %v", err)
		}
	}
	past := float64(time.Now().Add(-time.Second).UnixMilli())
	for i := range 10 {
		id := "t" + string(rune('0'+i))
		mr.ZAdd(leaseKeyFor("default"), past, id)
	}

	n, err := broker.ReclaimInflight(ctx, "default", 3)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	if n != 3 {
		t.Fatalf("expected batch=3, got %d reclaimed", n)
	}

	// 第二轮再回收 3 条。
	n2, _ := broker.ReclaimInflight(ctx, "default", 3)
	if n2 != 3 {
		t.Fatalf("expected second batch=3, got %d", n2)
	}
}

// TestRemoveAlsoCleansLease 验证 Remove 同时清理 lease zset，
// 否则取消的任务会在超时后被回收循环重投。
func TestRemoveAlsoCleansLease(t *testing.T) {
	broker, mr := newTestBroker(t)
	ctx := context.Background()

	task := makeTask("t1", "default", entity.PriorityDefault)
	_ = broker.Enqueue(ctx, "default", task)
	_, _ = broker.Dequeue(ctx, []string{"default"})

	if err := broker.Remove(ctx, "default", "t1"); err != nil {
		t.Fatalf("remove: %v", err)
	}

	if mr.Exists(leaseKeyFor("default")) {
		t.Fatal("lease zset should be cleared after remove")
	}
}


// TestComputeRetryBackoffExponential 验证指数公式 base * 2^(retryCount-1) 正确，
// jitter 范围 [0, base/4)。
func TestComputeRetryBackoffExponential(t *testing.T) {
	base := time.Second
	jitterMax := base / 4

	cases := []struct {
		retryCount int
		min        time.Duration
		max        time.Duration
	}{
		{1, base, base + jitterMax},     // 1s + jitter
		{2, 2 * base, 2*base + jitterMax}, // 2s + jitter
		{3, 4 * base, 4*base + jitterMax}, // 4s + jitter
		{4, 8 * base, 8*base + jitterMax}, // 8s + jitter
	}
	for _, c := range cases {
		got := computeRetryBackoff(base, c.retryCount)
		if got < c.min || got >= c.max+1 {
			t.Errorf("retry %d: got %v, want in [%v, %v)", c.retryCount, got, c.min, c.max+1)
		}
	}
}

// TestComputeRetryBackoffCap 验证退避达到 MaxRetryBackoff 后不再增长。
func TestComputeRetryBackoffCap(t *testing.T) {
	base := time.Minute
	jitterMax := base / 4
	// 1m << 30 远超 MaxRetryBackoff=5m，必然被 cap。
	got := computeRetryBackoff(base, 30)
	if got < MaxRetryBackoff || got >= MaxRetryBackoff+jitterMax+1 {
		t.Errorf("expected capped backoff, got %v (cap=%v)", got, MaxRetryBackoff)
	}
}

// TestComputeRetryBackoffEdgeCases 验证边界输入返回 0，避免 worker
// 在 RetryCount=0 / base=0 时计算出意外值。
func TestComputeRetryBackoffEdgeCases(t *testing.T) {
	if got := computeRetryBackoff(0, 5); got != 0 {
		t.Errorf("base=0 should return 0, got %v", got)
	}
	if got := computeRetryBackoff(time.Second, 0); got != 0 {
		t.Errorf("retryCount=0 should return 0, got %v", got)
	}
	if got := computeRetryBackoff(time.Second, -1); got != 0 {
		t.Errorf("negative retryCount should return 0, got %v", got)
	}
}

// TestNackBackoffGrowsWithRetryCount 验证 Nack 实际写入的 delayed score
// 随 RetryCount 单调指数增长，确认 worker 重试链路接到了新公式。
func TestNackBackoffGrowsWithRetryCount(t *testing.T) {
	broker, mr := newTestBroker(t)
	ctx := context.Background()

	// 用 1s base 让指数差距明显（jitter 仅 0~250ms）。
	scoreAt := func(retryCount int) float64 {
		mr.FlushAll()
		task := makeTask("t1", "default", entity.PriorityDefault)
		task.MaxRetries = 10
		task.RetryCount = retryCount
		task.RetryBackoff = entity.Duration{Duration: time.Second}

		_ = broker.Enqueue(ctx, "default", task)
		_, _ = broker.Dequeue(ctx, []string{"default"})

		nackStart := time.Now().UnixMilli()
		if err := broker.Nack(ctx, "default", task); err != nil {
			t.Fatalf("nack: %v", err)
		}

		members, err := mr.ZMembers(delayedKeyFor("default"))
		if err != nil {
			t.Fatalf("zmembers: %v", err)
		}
		if len(members) != 1 {
			t.Fatalf("expected 1 delayed member, got %d", len(members))
		}
		score, _ := mr.ZScore(delayedKeyFor("default"), members[0])
		// 返回 deadline 相对 nack 时间的偏移，便于断言。
		return score - float64(nackStart)
	}

	d1 := scoreAt(1) // ~1000ms + jitter
	d2 := scoreAt(2) // ~2000ms + jitter
	d3 := scoreAt(3) // ~4000ms + jitter

	if d1 < 1000 || d1 > 1300 {
		t.Errorf("retry 1 backoff %vms not in [1000, 1300]", d1)
	}
	if d2 < 2000 || d2 > 2300 {
		t.Errorf("retry 2 backoff %vms not in [2000, 2300]", d2)
	}
	if d3 < 4000 || d3 > 4300 {
		t.Errorf("retry 3 backoff %vms not in [4000, 4300]", d3)
	}
	// 单调性：jitter 不可能让 d2 < d1（base/4=250ms < 1000ms 差距）。
	if d2 <= d1 || d3 <= d2 {
		t.Errorf("backoff not monotonic: d1=%v d2=%v d3=%v", d1, d2, d3)
	}
}


