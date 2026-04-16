# Leader Election 竞态条件修复

> 日期：2026-04-16

## 一、问题描述

**文件：** `internal/scheduler/infrastructure/election/election.go`

`LeaderElector` 的 `campaign()` 方法存在三个并发问题：

### 1.1 数据竞态：`le.session` 和 `le.election` 无锁读写

`session` 和 `election` 被存储为结构体字段，在 `campaign()` 中无锁写入，在 `observe()` 中无锁读取：

```go
// campaign() — 无锁写
le.session = session
le.election = election

go le.observe(ctx)  // 启动 goroutine

// observe() — 无锁读
ch := le.election.Observe(ctx)
```

结构体中已有 `mu sync.RWMutex`，但只保护 `isLeader`，未保护 `session` 和 `election`。

### 1.2 observe goroutine 泄漏/重叠

每次调用 `campaign()` 都会启动一个新的 `observe` goroutine。如果 `Campaign()` 失败，`campaign()` 返回后 `Run()` 会重试，但上一轮的 observe goroutine 可能仍在运行：

```
时间线:
  campaign() 迭代1 → go observe1(ctx) → Campaign 失败 → session1.Close() → return
  campaign() 迭代2 → go observe2(ctx) → Campaign 失败 → session2.Close() → return
  campaign() 迭代3 → go observe3(ctx) → ...

  observe1 和 observe2 可能尚未退出，多个 observe goroutine 并发运行
```

`session.Close()` 最终会导致 Observe channel 关闭，但在传播完成之前，多个 observe goroutine 可能同时向 `onNewLeader` 回调报告来自不同 session 的过期 leader 信息。

### 1.3 observe 读到错误的 election 对象

当 `Run()` 因错误重试时：

```
迭代1: le.election = election1 → go observe1() → Campaign() 失败 → return
迭代2: le.election = election2 → ...
                                    ↑
                  observe1 可能此时才执行 le.election.Observe(ctx)
                  读到的是 election2，而非预期的 election1
```

### 1.4 影响评估

严格意义上的脑裂（两个节点同时认为自己是 leader）**不会发生**，因为 etcd election 本身提供强一致保证，且 `isLeader` 的读写有锁保护。但上述竞态会导致：

- Go race detector 报错
- `onNewLeader` 回调收到过期的 leader 信息
- 反复失败重试时 observe goroutine 堆积

## 二、修复方案

### 2.1 移除共享字段，改为局部变量

将 `session` 和 `election` 从结构体字段中移除，改为 `campaign()` 的局部变量，从根本上消除跨 goroutine 的共享状态：

```go
// 修改前
type LeaderElector struct {
    client   *clientv3.Client
    session  *concurrency.Session   // ← 删除
    election *concurrency.Election  // ← 删除
    // ...
}

// 修改后
type LeaderElector struct {
    client *clientv3.Client
    // session 和 election 下沉为 campaign() 局部变量
    // ...
}
```

### 2.2 observe 通过参数接收 election

修改 `observe` 方法签名，直接接收当前迭代的 `election` 对象，不再从结构体字段读取：

```go
// 修改前
func (le *LeaderElector) observe(ctx context.Context) {
    ch := le.election.Observe(ctx)  // 无锁读共享字段
    // ...
}

// 修改后
func (le *LeaderElector) observe(ctx context.Context, election *concurrency.Election) {
    ch := election.Observe(ctx)  // 使用参数，无竞态
    // ...
}
```

### 2.3 scoped context + WaitGroup 防止 goroutine 泄漏

为 observe 创建独立的 context，并通过 WaitGroup 保证 observe goroutine 在 `campaign()` 返回前完全退出：

```go
func (le *LeaderElector) campaign(ctx context.Context) error {
    session, err := concurrency.NewSession(le.client, concurrency.WithTTL(le.ttl))
    if err != nil {
        return err
    }
    defer session.Close()

    election := concurrency.NewElection(session, le.prefix)

    // scoped context：campaign 退出时通知 observe 停止
    observeCtx, observeCancel := context.WithCancel(ctx)
    defer observeCancel()

    // WaitGroup：保证 observe 完全退出后 campaign 才返回
    var observeWg sync.WaitGroup
    observeWg.Add(1)
    go func() {
        defer observeWg.Done()
        le.observe(observeCtx, election)
    }()
    defer observeWg.Wait()

    // ... 后续逻辑不变 ...
}
```

**defer 执行顺序**（LIFO）保证正确的清理时序：

```
1. observeWg.Wait()    — 等待 observe goroutine 退出
2. observeCancel()     — 取消 observe 的 context（触发退出）
3. session.Close()     — 关闭 etcd session
```

实际上 `observeCancel()` 先执行，observe 收到取消信号退出后，`observeWg.Wait()` 才返回，最后 `session.Close()` 清理底层资源。

## 三、修复效果

| 问题 | 修复前 | 修复后 |
|------|-------|-------|
| `le.session`/`le.election` 无锁读写 | race detector 会报错 | 字段已移除，不存在共享状态 |
| observe goroutine 跨迭代重叠 | 多个 observe 并发运行 | WaitGroup 保证每轮 observe 在 campaign 返回前退出 |
| observe 读到错误的 election | 可能读到下一次迭代的 election | 通过参数传递，每个 observe 绑定自己的 election |
| onNewLeader 收到过期信息 | 旧 observe 可能报告旧 session 的数据 | 旧 observe 已退出，不会产生过期回调 |
