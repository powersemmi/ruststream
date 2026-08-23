# 安装

RustStream 以单个 crate `ruststream` 发布，其接口全部由可加的 cargo feature 控制。把它加进你的
`Cargo.toml`：

```toml
[dependencies]
ruststream = { version = "0.6", features = ["macros", "memory", "json"] }
serde = { version = "1", features = ["derive"] }
```

`serde` 是你的服务的直接依赖，因为你的消息类型要 derive `Deserialize` / `Serialize`。

!!! note "Edition 与 MSRV"
    RustStream 面向 **edition 2024**，最低支持的 Rust 版本是 **1.85**（原生的
    `async fn in trait`）。请在 `Cargo.toml` 里设置 `edition = "2024"`。CI 会在这个下限版本和当前
    stable 上构建并测试本 crate，并在 beta 上构建，因此任何不低于 1.85 的下限都能用。
    当 Broker crate 底层的客户端需要更新的工具链时，它们可能要求比核心更高的版本；请查看该 Broker
    crate 自己的 `rust-version`。

## Features

核心的各个 trait、`RustStream` 应用对象、`Router`、中间件和分发始终会被编译。除此之外的一切都是可加、
需要显式启用的 feature。

| feature | 引入依赖 | 提供什么 |
|---|---|---|
| `json` *(默认)* | serde_json | `JsonCodec` |
| `msgpack` | rmp-serde | `MsgpackCodec` |
| `cbor` | ciborium | `CborCodec` |
| `memory` | - | `MemoryBroker`，作为参考实现的内存 Broker |
| `macros` | ruststream-macros | `#[subscriber]`、`#[ruststream::app]`、`#[derive(Message)]` |
| `asyncapi` | schemars, serde_norway | AsyncAPI 生成与 HTML 查看器 |
| `metrics` | prometheus | Prometheus 中间件与导出器 |
| `logging` | tracing-subscriber | `ruststream::logging`，带颜色的控制台日志订阅者（[日志](../guides/logging.md)） |
| `conformance` | - | 面向 Broker 作者的 conformance 校验套件 |
| `cli` | clap, anyhow | `ruststream` 二进制程序 |

各个编解码器 feature 互不冲突，需要几个就开几个（参见[编解码器](../guides/codecs.md)）。如果要去掉随
crate 附带的 JSON 编解码器（比如某个只需要 trait 接口和运行时的 Broker crate），就关掉默认 feature：

```toml
[dependencies]
ruststream = { version = "0.6", default-features = false }
```

## CLI

可选的 `ruststream` 二进制程序随 crate 一起发布，位于 `cli` 这个 cargo feature 之后，它用框架的子命令
（`run`、`asyncapi gen`）来驱动 `cargo`；安装方式和各个命令见 [CLI 指南](../guides/cli.md)。生成新项目
的骨架并不需要它，那件事由 `cargo generate` 基于模板完成，参见[快速上手](quickstart.md)。

## 具体的 Broker

`memory` Broker 用于本地开发和测试。生产环境请依赖某个 Broker crate，它会从 `ruststream` 中重新导出自己
需要的东西。每个 Broker 都独立地做版本管理和发布，因此确切的依赖写法（包括当前版本，以及用于处理器测试
的 `testing` feature）连同它的 `Config` 和各项能力，都写在它自己的文档里。

可用的 Broker 列在 [Broker](../brokers/index.md) 一节，从那里的链接可以进到每个 Broker 的文档查看安装
方式。想自己写一个，参见 [Broker 作者](../broker-authors/index.md)。
