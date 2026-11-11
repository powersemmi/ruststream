<h1 align="center">RustStream</h1>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream/blob/main/README.md">English</a> | <a href="https://github.com/powersemmi/ruststream/blob/main/docs/readme/README.ru.md">Русский</a> | <b>中文</b>
</p>

<p align="center">
  <i>Rust 的异步消息框架：与 Broker 无关的 trait、带路由器的运行时、编解码器、AsyncAPI 生成、基于 Prometheus 和 OpenTelemetry 的可观测性，以及面向 Broker 作者的 conformance 校验套件。</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://coveralls.io/github/powersemmi/ruststream?branch=main"><img src="https://coveralls.io/repos/github/powersemmi/ruststream/badge.svg?branch=main" alt="Coverage"></a>
  <a href="https://crates.io/crates/ruststream"><img src="https://img.shields.io/crates/v/ruststream.svg" alt="crates.io"></a>
  <a href="https://crates.io/crates/ruststream"><img src="https://img.shields.io/crates/dr/ruststream" alt="Recent downloads"></a>
  <a href="https://docs.rs/ruststream"><img src="https://img.shields.io/docsrs/ruststream" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.95-blue.svg" alt="MSRV 1.95">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <img src="https://img.shields.io/badge/unsafe-none-success.svg" alt="100% safe Rust">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
  <a href="https://context7.com/powersemmi/ruststream"><img src="https://img.shields.io/badge/Context7-Ask_AI-6200ee" alt="Ask AI"></a>
  <a href="https://www.greptile.com/?utm_source=oss_badge&amp;utm_medium=readme&amp;utm_campaign=greptile_for_open_source"><img src="https://www.greptile.com/badge.svg" alt="Greptile: The War on Bugs"></a>
</p>

<p align="center">
  <b><a href="https://powersemmi.github.io/ruststream/latest/zh/">文档</a></b>
</p>

---

RustStream 通过一小组泛型 trait，把你的服务接入消息 Broker。在此之上，它提供路由器、中间件、编解码器和配套工具。核心不依赖任何 Broker：每个 Broker 都是独立的 crate，并遵守同一份契约。核心完全使用安全的 Rust 编写。

## 特性

- **核心与 Broker 无关。** Broker 是独立的 crate，由 conformance 校验套件检查。
- **误用无法通过编译。** 重复 ack、不按顺序调用生命周期方法，以及缺少必要参数的发布，都会产生编译错误，错误信息会指出修正方法。
- **三种运行时。** `tokio`（默认）；`compio`，每个 CPU 核心一个运行时，内核支持时使用 `io_uring`；`embassy`，面向设备，有堆或无堆均可。三者的处理器相同，由应用选择运行器。
- **可插拔的编解码器：** JSON、MessagePack 和 CBOR 由 cargo feature 启用；无堆的设备使用 postcard 和基于 `serde-json-core` 的 JSON；也可以不用编解码器，直接传递原始字节。
- **零样板的可执行程序。** `#[subscriber]` 和 `#[ruststream::app]` 宏，以及一个能运行服务并打印其 AsyncAPI 文档的 CLI。
- **可观测性：** AsyncAPI 3.1、Prometheus 指标、健康探针和 OpenTelemetry。
- **无需 Broker 的测试。** 测试工具在进程内运行服务自己的应用。
- **发布错误会带回消息。** 凡是 Broker 的客户端没有接管载荷的地方，发布遭到拒绝时都会把未发送的消息交还给调用方，调用方可以把它发往别处。
- **能力 trait**：用于批、事务、请求-响应、分区和重新定位；Broker 只实现它所支持的能力。

## 安装

```toml
[dependencies]
ruststream = { version = "0.8", features = ["macros", "memory", "json", "logging"] }
serde = { version = "1", features = ["derive"] }
```

CLI 随 crate 一起提供，由 `cli` feature 启用：

```bash
cargo install ruststream --features cli
```

## 智能体技能

仓库附带了一组技能，教编码智能体按推荐的方式编写 RustStream 服务、测试和 Broker crate。在 Claude Code 中安装：

```text
/plugin marketplace add powersemmi/ruststream
/plugin install ruststream@ruststream
```

Cursor 和 Codex 从 [`.agents/skills/`](https://github.com/powersemmi/ruststream/tree/main/.agents/skills) 读取这些技能；[智能体技能](https://powersemmi.github.io/ruststream/latest/zh/guides/getting-started/agent-skills/)页面列出了全部技能。

## 编写服务

```rust
use ruststream::diagnostics::TracingLogger;
use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .logger(TracingLogger)
        .with_broker(MemoryBroker::new(), |b| b.include(handle))
}
```

`#[ruststream::app]` 会生成 `main`，所以不需要运行时样板代码。`.logger(TracingLogger)` 把框架的日志交给 `tracing`，`logging` feature 再把它打印到终端。处理器只需要 `Deserialize`；其余的 derive 供下面的测试使用。

## 运行

```bash
ruststream run                 # start the service (or: cargo run -- run)
ruststream asyncapi gen        # print the AsyncAPI document
```

服务启动时会打印几行日志，然后一直运行，直到你按下 <kbd>Ctrl</kbd>+<kbd>C</kbd>。下面省略了每行开头的时间戳：

```text
INFO ruststream::lifecycle starting service service=orders version=0.1.0 brokers=1 subscribers=1
INFO ruststream::lifecycle broker connected broker="ruststream_memory::broker::MemoryBroker"
INFO ruststream::dispatch subscriber started subscriber=orders input="orders::Order"
INFO ruststream::lifecycle service running subscribers=1
```

内存 Broker 在进程内部传递消息：服务自己向 `orders` 发布订单时，处理器会打印 `got order`；下面的测试就发布了这样一个订单。

`asyncapi gen` 需要启用 `ruststream` 的 `asyncapi` feature。

如果想从模板开始，用 `cargo generate` 创建项目：

```bash
cargo generate --git https://github.com/powersemmi/ruststream templates/memory --name my-service
```

`templates/compio-memory` 创建运行在 compio 运行时上的服务，`templates/embassy-memory` 创建 embassy 上、无堆的 Cortex-M 固件。每个模板都附带测试，在进程内运行它的应用。有几个 Broker crate 提供了自己的模板，并在各自的文档中说明。[快速上手](https://powersemmi.github.io/ruststream/latest/zh/guides/getting-started/quickstart/)逐步演示第一次运行。

## 测试

`TestApp` 在进程内运行服务自己的应用，不需要外部 Broker。它由 `testing` feature 提供，服务为自己的测试启用该 feature：

```toml
[dev-dependencies]
ruststream = { version = "0.8", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

测试通过 Broker 发布一个订单，并检查处理器如何结算它：

```rust
#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, app};

    #[tokio::test]
    async fn an_order_is_acked() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        // Inject an order; the publish drives the handler to a standstill before returning.
        tb.broker::<MemoryBroker>()
            .message(&Order { id: 42 })
            .to("orders")
            .publish()
            .await?;

        // The handler ran once, decoded the order, and acked.
        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 42 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
```

[测试](https://powersemmi.github.io/ruststream/latest/zh/components/application/testing/)页面介绍测试工具的其余部分。

## Broker

| Broker | Crate |
|---|---|
| NATS | [`ruststream-nats`](https://github.com/powersemmi/ruststream-nats) |
| Redis / Valkey | [`ruststream-fred`](https://github.com/powersemmi/ruststream-fred) |
| RabbitMQ (AMQP 0.9.1) | [`ruststream-lapin`](https://github.com/powersemmi/ruststream-lapin) |
| Apache Kafka | [`ruststream-rdkafka`](https://github.com/powersemmi/ruststream-rdkafka) |
| AMQP 1.0 | [`ruststream-amqp`](https://github.com/powersemmi/ruststream-amqp) |
| Google Cloud Pub/Sub | [`ruststream-gcp-pubsub`](https://github.com/powersemmi/ruststream-gcp-pubsub) |
| Amazon SQS / SNS | [`ruststream-sqs-sns`](https://github.com/powersemmi/ruststream-sqs-sns) |
| Apache Pulsar | [`ruststream-pulsar`](https://github.com/powersemmi/ruststream-pulsar) |
| MQTT 5 | [`ruststream-rumqttc`](https://github.com/powersemmi/ruststream-rumqttc) |
| embassy 和 compio 上的 MQTT 5，另有面向无堆设备的变体 | [`ruststream-rust-mqtt`](https://github.com/powersemmi/ruststream-rust-mqtt) |
| ZeroMQ | [`ruststream-zeromq`](https://github.com/powersemmi/ruststream-zeromq) |
| 文件与 stdio | [`ruststream-sea-file`](https://github.com/powersemmi/ruststream-sea-file) |
| Amazon Kinesis | [`ruststream-kinesis`](https://github.com/powersemmi/ruststream-kinesis) |
| SQL 数据库（Postgres、MySQL / MariaDB、SQLite） | [`ruststream-sqlx`](https://github.com/powersemmi/ruststream-sqlx) |

各个 Broker 支持什么，见 [Broker](https://powersemmi.github.io/ruststream/latest/zh/brokers/) 页面。要编写 Broker，从 [`Broker` trait](https://docs.rs/ruststream/latest/ruststream/trait.Broker.html) 开始。

## 文档

- 站点：<https://powersemmi.github.io/ruststream/latest/zh/>
- API 参考与主题概览：<https://docs.rs/ruststream>

## 最低支持的 Rust 版本

MSRV 为 **1.95**，edition 2024。策略是滚动式的，与 tower 相同：MSRV 只会升到至少发布了六个月的 Rust 版本，并且只在次版本发布中升级。如果 Broker crate 的客户端库需要更新的工具链，该 crate 可以要求更高的版本。

## 参与贡献

参见 [CONTRIBUTING.md](https://github.com/powersemmi/ruststream/blob/main/CONTRIBUTING.md)。

## 许可证

采用 [Apache-2.0](https://github.com/powersemmi/ruststream/blob/main/LICENSE) 许可证。

<sub>灵感来自 [FastStream](https://github.com/ag2ai/faststream)。</sub>
