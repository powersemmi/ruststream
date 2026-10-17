---
description: "安装 RustStream 的智能体技能，让编码智能体按照框架推荐的方式编写服务、测试和 Broker crate。"
---

# 智能体技能

仓库附带了智能体技能：编码智能体在任务需要时加载的指南，让它写出的代码遵循框架推荐的路径。
在 Claude Code 中用两条命令安装：

```text
/plugin marketplace add powersemmi/ruststream
/plugin install ruststream@ruststream
```

插件提供五个技能。
智能体根据描述选择技能，所以关于 RustStream 服务的请求即使不点名，也会加载对应的技能。

| 技能 | 智能体何时使用 |
|---|---|
| `ruststream` | 编写服务：处理器、应用、回复、发布、重试、状态、编解码器 |
| `ruststream-testing` | 用 `TestApp` 测试服务 |
| `ruststream-without-macros` | 不启用 `macros` feature 构建服务 |
| `ruststream-broker` | 编写 Broker crate |
| `ruststream-core` | 为核心仓库贡献代码 |

## 其他智能体

每个技能是一个目录，包含 `SKILL.md` 和它的参考页面，位于仓库的
[`.agents/skills/`](https://github.com/powersemmi/ruststream/tree/main/.agents/skills)。
Cursor 和 Codex 从所在项目的 `.agents/skills/` 读取技能。
要让智能体在你的服务中使用这些技能，把需要的目录复制到你项目的这个目录，或复制到你的智能体读取的技能目录。

## 版本对应

技能描述的是它所在修订版本的框架。
请从与服务所依赖的 `ruststream` 相同的发布线安装技能。
技能是指南而不是检查：结果仍由编译器和测试来判定。
