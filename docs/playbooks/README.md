# Playbooks

可复用的工程经验沉淀。每份 playbook 是一类**反复出现的问题**的标准操作流程：
工具、判断准则、避坑清单，**与具体事件无关**。

写作约定：
- **场景中性**：不假设特定 endpoint / 特定 commit / 特定时间点。
- **量化优先**：每一步要回答"我能拿什么数据回答这个问题"，而不是"这看起来像什么"。
- **附真实案例**：每份 playbook 链一个验证过它的 postmortem，证明流程不是空想。
- **避免与代码同步腐化**：playbook 引用的命令、工具、目录路径在改动时一并更新。

事件级"故事"（postmortem / RCA / baseline 报告）放各自主题目录的 `reports/` 或同级，
不要放进 playbooks。两类文档相互交叉引用：playbook 抽象 + postmortem 具体。

## 目录

| 文件 | 适用场景 | 真实案例 |
|---|---|---|
| [bottleneck-investigation.md](./bottleneck-investigation.md) | HTTP / gRPC 服务吞吐、延迟、单核 CPU 饱和等性能瓶颈定位 | [2026-05-19 写路径](../performance/optimizations/r1-postmortem-2026-05-19-write-path.md) |

## 何时新增一份 playbook

- 同一类问题第二次出现，且第一次的解决路径**与具体业务无关**——把流程抽出来。
- 现有 playbook 的某个 phase 多次被人误用——把那个 phase 拆成独立 playbook。

不要为"还没真发生过的问题"提前写 playbook。空想的流程在第一次实战时几乎一定不对。
