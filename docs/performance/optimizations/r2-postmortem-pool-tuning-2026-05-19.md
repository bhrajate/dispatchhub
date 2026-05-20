# Postmortem：2026-05-19 r2 — MySQL 连接池调优

> **TL;DR**：r1 后 300 RPS 仍 FAIL（errors 10.9%）。**没有抓 profile，因为 errors 不是 CPU 问题**——直接 `tail apiserver.log` 1 分钟就分类清楚了：100% 是 `driver: bad connection`。配合 `ss -tan` 看到 `MaxOpenConns=50` 但 `MaxIdleConns=10` 导致连接 churn + TIME_WAIT 堆积。一行配置 `max_idle_conns: 50` + `conn_max_idle_time: 5m` 让 300 RPS 阶梯首次 PASS。
>
> **教训**：[playbook §5](../../playbooks/bottleneck-investigation.md) 推荐"先 profile 再优化"是默认动作；但如果**错误已经显式落到日志里**，先看日志比 profile 高效得多——profile 看的是 CPU time 分布，对"连接池行为异常"基本看不见。
>
> 配套：
> - [../../playbooks/bottleneck-investigation.md](../../playbooks/bottleneck-investigation.md)
> - [../reports/r1-i5-13500h-2026-05-19.md](../reports/r1-i5-13500h-2026-05-19.md) —— 上一轮（瓶颈尚未解）
> - [../reports/r2-i5-13500h-2026-05-19.md](../reports/r2-i5-13500h-2026-05-19.md) —— 本轮结果

## 时间线

| 时间 | 动作 |
|---|---|
| 18:47 | r1 之后回看 [roadmap §F2](../optimization-roadmap.md)：300 RPS errors 来源未知，下一刀必须先定位 |
| 18:48 | 启动 apiserver（带 pprof tag 备用），跑 300 RPS @ 60s |
| 18:50 | log 解析：1964 / 1964 = 100% `driver: bad connection` |
| 18:52 | `ss -tan` 看到 50 ESTAB + 45 TIME-WAIT，初步判断连接池 churn |
| 18:54 | 试跑 warmup（50 RPS @ 30s）→ 300 RPS：non2xx 1964 → 1796，p99 1062 → 419ms。**确认部分是冷启动**，但根因仍未消除 |
| 18:55 | 看 factory.go + perf config：`MaxOpenConns=50, MaxIdleConns=10`。明确这是经典的"max ≠ idle"反模式 |
| 18:56 | 改 `max_idle_conns: 50`、新增 `conn_max_idle_time: 5m`、新增 config 字段 + factory.go 调用 |
| 18:58 | 重 build + 单独 300 RPS 验证：non2xx **1964 → 0**，p99 1062 → 177ms |
| 18:59 | 跑完整 trim 验证不破坏其他阶梯 |
| 19:19 | trim 完成：300 RPS 阶梯 verdict FAIL → PASS（HTTP+gRPC 都过）|

总耗时 **约 30 分钟**（不含 17 分钟 trim 等待）。

## 现象

r1 后 trim 数据：

```
http_submit_task@300:  actual=298, errors=15.5%, p99=1.7s, FAIL
http_submit_task@600:  actual=434, errors=22%,   p99=17s,  FAIL
```

actual_rps **跟得上目标**（达标率 99%），所以瓶颈不是 CPU 处理速度——**是 errors**。
但 errors 是什么？r1 报告里写了"未知"，没继续追。

## 调查过程

### 第一步：看日志（不是 profile）

apiserver 用 zap JSON 日志写 stdout。300 RPS 跑 60s 后：

```bash
grep '"level":"error"' /tmp/f2/apiserver.log \
  | grep -oE '"msg":"[^"]*"' | sort | uniq -c | sort -rn | head
```

输出：

```
1964 "msg":"submit task: persist task: driver: bad connection"
  15 "msg":"submit task: persist task: context canceled"
```

100% 是 `driver: bad connection`。这是 Go `database/sql` 的固定字符串，含义：**client 池子里的连接拿出来准备用时已经不可用了**。

### 第二步：看时序

```bash
grep '"msg":"submit task: persist task: driver: bad connection"' \
  /tmp/f2/apiserver.log | grep -oE '"ts":"[^"]+"' | head -1
# 18:49:32  ← 第一个 error
grep ... | tail -1
# 18:49:55  ← 最后一个 error
```

错误**集中在压测前 23 秒**，后段消失。这与"突然全速注入 → 连接池冷启动 → churn"模式吻合。

### 第三步：看 OS 层

```bash
ss -tan | grep ':33307' | awk '{print $1}' | sort | uniq -c
# 50 ESTAB
# 45 TIME-WAIT
```

50 ESTAB 是 `MaxOpenConns` 上限；45 TIME-WAIT 表示**短时间内 ~45 个连接被关掉**。
loopback 上 `tcp_tw_reuse=2`（WSL2 默认）禁用 TIME_WAIT reuse，这些连接释放得很慢。

### 第四步：定位代码

```bash
grep 'max_idle\|MaxIdleConns' test/perf/configs/apiserver.yaml
# max_idle_conns: 10
```

`MaxIdleConns=10`、`MaxOpenConns=50`。这是经典反模式：

- 流量高峰时连接数会拉到 50
- 短暂回落到 10 以下时，超过 10 的 idle 连接会被 `database/sql` 关闭
- 流量再上来又重新 dial
- 整个过程中：
  - server 端可能比 client 早一步关连接 → client 拿到"幽灵"连接 → `bad connection`
  - 关闭后留下大量 TIME_WAIT（loopback 不能 reuse）

### 第五步：验证假设（warmup 实验）

跑 30s @ 50 RPS 把池子撑到 idle 充满，**不重启** apiserver 接着跑 300 RPS：

```
non2xx:    1964 → 1796   (-9%, 部分缓解)
p99:       1062 → 419ms  (-60%)
```

**warmup 缓解但没根除**——确认根因有两个 layer：
1. 冷启动时连接池要从 0 撑到 50，过程中失败
2. 稳态下 idle conn 仍在 churn（不到 10 时 server-side 主动关）

### 第六步：修复

`max_idle_conns: 10 → 50`（与 max 同），新增 `conn_max_idle_time: 5m`：

```diff
mysql:
  max_open_conns: 50
- max_idle_conns: 10
+ max_idle_conns: 50
  conn_max_lifetime: 1h
+ conn_max_idle_time: 5m
```

`max_idle == max_open` 让连接池**永远保留所有连接**，不再 churn。
`conn_max_idle_time: 5m` 是防御性设置——比 MySQL `wait_timeout=28800s` 短很多，让 client 主动关 stale 连接而不是被 server 端突然断。

### 第七步：单 cell 验证

直接 300 RPS 60s 不 warmup：

```
non2xx:    1964 → 0      ✓
p99:       1062 → 177ms  (-83%)
p95:        660 → 76ms   (-88%)
```

100% errors 消失。然后跑完整 trim 确认其他阶梯没破。

## 这一轮没用 profile，为什么？

r1 时用 CPU profile 找到 GORM 默认事务是对的——因为问题是"CPU 满载、单核跑不动"。r2 不一样：

| r1 问题 | r2 问题 |
|---|---|
| actual_rps 跟不上目标（287 / 600） | actual_rps 跟得上（298 / 300）|
| CPU 100% 单核 | CPU 90%（仍接近但不饱和）|
| errors 22% | errors 10% |
| **算力问题** | **错误率问题** |

CPU profile 看的是"在 CPU 上消耗的时间"。**连接池 churn 不消耗 CPU，是 network IO 抖动**。强行去抓 cpu profile 看到的还是 INSERT / Redis / log 那些，得不出 connection 层结论。

**判断准则**：

- actual 不达标、CPU 满载 → profile
- actual 达标、errors 高 → 看日志、看 OS 层（`ss`、`netstat`、`vmstat`）
- 两者都有 → profile + log 一起

这条经验值得回填到 [playbook](../../playbooks/bottleneck-investigation.md) phase 4。

## 错误假设登记表

本轮只有 1 个被推翻的次要假设：

| 假设 | 来源 | 真相 |
|---|---|---|
| "warmup 后没 errors 就证明只是冷启动" | warmup 实验 errors 确实降了 60% | warmup 只解决 layer 1（冷启动），稳态 layer 2（idle conn churn）仍存在；最终修复只能靠 max_idle_conns 调整 |

## 还没解决的事

1. **600 RPS 阶梯仍 FAIL**：但失败模式变了——errors=0，达标率 71%。下一刀需要重新抓 CPU profile（r1 那份过期了），看是不是 INSERT 本身或 Redis 写成了新瓶颈。
2. **goroutine 数翻倍（27 → 65）**：50 个 idle 连接各带 keepalive goroutine。
   这是预期行为，但如果将来加大 MaxOpenConns 到几百，要看 goroutine 是否成为新成本。
3. **`tcp_tw_reuse=2` 是 WSL2 / 容器特定行为**。生产 Linux host 通常 `=1`， loopback 上 TIME_WAIT 影响小。这意味着**生产环境连接池修复的相对收益可能小于 dev 环境**——dev 上从 1962 → 0，生产上可能本来就只有 200 → 0。
   重要但不影响"是否要修"的结论。

## 工程性产物

- `internal/shared/infrastructure/config/MySQLConfig` 新增 `ConnMaxIdleTime` 字段
- `factory.go::NewMySQLDB` 调用 `SetConnMaxIdleTime`
- `test/perf/configs/apiserver.yaml` + `config/apiserver.yaml` 同步更新

## 复用建议

playbook 的 [Phase 3 决策树](../../playbooks/bottleneck-investigation.md) 已经覆盖了"actual 不达标 / CPU 满载 → profile"路径，但没有覆盖 "actual 达标但 errors 高"的情况。下次更新 playbook 时，应该把"errors 高 → 先看日志分类、再看 OS 层 ss/netstat"作为独立路径加进去。
