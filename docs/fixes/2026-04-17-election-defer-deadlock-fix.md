# Leader Election defer 顺序死锁修复

> 日期：2026-04-17

## 一、问题描述

**文件：** `internal/scheduler/infrastructure/election/election.go`

### 1.1 defer LIFO 顺序导致潜在死锁

`campaign()` 方法中 `observeCancel()` 和 `observeWg.Wait()` 的 defer 注册顺序有误。Go 的 defer 按 LIFO（后进先出）执行，修复前的注册顺序为：

```go
observeCtx, observeCancel := context.WithCancel(ctx)
defer observeCancel()       // 注册第1 → 执行第2

var observeWg sync.WaitGroup
observeWg.Add(1)
go func() {
    defer observeWg.Done()
    le.observe(observeCtx, election)
}()
defer observeWg.Wait()      // 注册第2 → 执行第1（先于 cancel！）
```

实际 defer 执行顺序：

```
1. observeWg.Wait()    ← 等待 observe goroutine 退出
2. observeCancel()     ← 取消 observe 的 context
```

**死锁触发场景：** 当 etcd session 过期（而非父 context 取消）导致 `campaign()` 返回时：

1. session 过期 → `leaderCtx` 被取消 → `campaign()` 开始执行 defer 链
2. 先执行 `observeWg.Wait()`，等待 observe goroutine 退出
3. 但 `observeCtx` 派生自父 ctx，父 ctx 仍然有效，observe goroutine 阻塞在 `election.Observe(observeCtx)` 的 channel 读取上
4. `observeCancel()` 排在 Wait 之后，永远不会被执行
5. **死锁** — `Wait()` 永远不返回

### 1.2 WaitGroup 用法可简化

Go 1.25 引入了 `sync.WaitGroup.Go()` 方法，可替代手动的 `Add(1)` + `go func() { defer Done() }()` 模式，代码更简洁且不易出错。

## 二、修复方案

### 2.1 交换 defer 注册顺序

将 `defer observeCancel()` 移到 `defer observeWg.Wait()` **之后**注册，利用 LIFO 保证 cancel 先于 wait 执行：

```go
observeCtx, observeCancel := context.WithCancel(ctx)

var observeWg sync.WaitGroup
observeWg.Go(func() {
    le.observe(observeCtx, election)
})
defer observeWg.Wait()      // 注册第1 → 执行第2
defer observeCancel()        // 注册第2 → 执行第1（先取消！）
```

修复后 defer 执行顺序：

```
1. observeCancel()     ← 先取消 context，通知 observe goroutine 退出
2. observeWg.Wait()    ← 再等待 observe goroutine 完成退出
3. session.Close()     ← 最后关闭 etcd session
```

### 2.2 使用 WaitGroup.Go 简化 goroutine 创建

```go
// 修改前
var observeWg sync.WaitGroup
observeWg.Add(1)
go func() {
    defer observeWg.Done()
    le.observe(observeCtx, election)
}()

// 修改后
var observeWg sync.WaitGroup
observeWg.Go(func() {
    le.observe(observeCtx, election)
})
```

## 三、修复效果

| 问题 | 修复前 | 修复后 |
|------|--------|--------|
| defer 执行顺序 | Wait 先于 Cancel，可能死锁 | Cancel 先于 Wait，保证有序退出 |
| session 过期场景 | observe goroutine 无法退出，函数永久阻塞 | context 先取消，observe 正常退出 |
| WaitGroup 用法 | 手动 Add/Done 模式 | 使用 Go 1.25 的 WaitGroup.Go 简化 |

## 四、与 2026-04-16 修复的关系

本次修复是 [2026-04-16-election-race-fix](2026-04-16-election-race-fix.md) 的后续。上次修复引入了 scoped context + WaitGroup 机制来防止 goroutine 泄漏，但 defer 的注册顺序有误，在特定场景下会导致死锁。上次文档第 135-143 行对 defer 顺序的描述实际上是修复后的期望行为，而非代码的实际行为。
