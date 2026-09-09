# 安装

RustStream 以单个 crate `ruststream` 发布，它的接口全部由可加的 cargo feature 控制。把它加进你的
`Cargo.toml`：

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "memory", "json"] }
serde = { version = "1", features = ["derive"] }
```

`serde` 是你服务的直接依赖，因为消息类型要 derive `Deserialize` / `Serialize`。

!!! note "Edition 与 MSRV"
    RustStream 面向 **edition 2024**，最低支持的 Rust 版本是 **1.88**。在你的 `Cargo.toml` 里写上
    `edition = "2024"`。
    如果 Broker crate 的客户端库需要更新的工具链，它的下限就会高于核心。确切的下限见该 Broker crate
    自己的 `rust-version` 字段。

## Features

核心的各个 trait、`RustStream` 应用对象、`Router`、中间件和面向订阅者的消息分发始终参与编译。
其余的一切都是可加的 feature，你按需启用。

| feature | 引入依赖 | 提供什么 |
|---|---|---|
| `json` *（默认）* | serde_json | `JsonCodec` |
| `msgpack` | rmp-serde | `MsgpackCodec` |
| `cbor` | ciborium | `CborCodec` |
| `memory` | - | `MemoryBroker`，作为参考实现的内存 Broker |
| `macros` | ruststream-macros | `#[subscriber]`、`#[ruststream::app]`，以及各个 derive（`Outgoing`、`OutSlot`、`OutMessages`、`Deserialized`、`Serialized`、`FromRef`、`MessageInfo`） |
| `asyncapi` | schemars, serde_norway | AsyncAPI 生成与 HTML 查看器 |
| `metrics` | prometheus | Prometheus 中间件与导出器 |
| `logging` | tracing-subscriber | `ruststream::logging`，彩色的控制台日志记录器（[日志](../guides/logging.md)） |
| `otel` | opentelemetry, opentelemetry-otlp | 通过 OTLP 导出链路与指标，并按 W3C 规范传递 trace-context（[OpenTelemetry](../guides/opentelemetry.md)） |
| `testing` | inventory | `TestApp` 与断言构建器（[测试](../guides/testing.md)） |
| `conformance` | inventory | 面向 Broker 作者的 conformance 校验套件 |
| `cli` | clap, anyhow | `ruststream` 二进制程序 |

一个服务里可以同时启用多个编解码器（参见[编解码器](../guides/codecs.md)）。要去掉内置的 JSON
编解码器（例如在只需要 trait 和运行时的 Broker crate 里），关掉默认 feature：

```toml
[dependencies]
ruststream = { version = "0.7", default-features = false }
```

## CLI

`ruststream` 二进制程序随 crate 一起发布，由 cargo feature `cli` 控制。它用框架的子命令（`run`、
`asyncapi gen`）驱动 `cargo`；安装方式和各个命令见 [CLI 指南](../guides/cli.md)。新项目的骨架由
`cargo generate` 按模板生成，参见[快速上手](quickstart.md)。

## 具体的 Broker

`memory` Broker 内置在 crate 里，不需要外部服务。要连接进程之外的 Broker，就依赖它的 crate：
该 crate 会从 `ruststream` 重新导出自己需要的东西。

每个 Broker 独立管理版本和发布，因此确切的依赖写法要看它自己的文档。那里给出当前版本，以及用于
处理器测试的 `testing` feature。同一份文档还描述了它的连接选项和各项能力。

可用的 Broker 列在 [Broker](../brokers/index.md) 一节，从那里可以进到每个 Broker 的文档。
想自己写一个 Broker，参见 [Broker 作者](../broker-authors/index.md)。
