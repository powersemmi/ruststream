# RustStream

**RustStream** 让 Rust 服务订阅事件流并向其发布消息，而不必把服务绑定在某一个消息 Broker 上。核心
是一组 trait 和一个路由器运行时；随之一起提供的还有编解码器、AsyncAPI 生成、Prometheus 指标，以及
面向 Broker 作者的 conformance 校验套件。

框架的形态由两条架构承诺决定：

1. **为第三方 Broker 提供真正的接口。** 核心只包含 trait 和类型，不依赖任何 Broker。每个 Broker
   都是独立的 crate，其契约由 `conformance` 校验套件检查。
2. **Broker 专有的配置留在 Broker crate 里。** 核心不携带任何 Broker 专有的配置或默认值。每个
   Broker crate 各自拥有自己的 `Config`，因此上游的一次变更只会波及一个 Broker crate，而不是整个
   框架。

=== "宏"

    ```rust
    --8<-- "examples/quickstart.rs"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/quickstart.rs"
    ```

`#[ruststream::app]` 会生成 `main`，因此 `cargo run -- run` 即可启动服务，`cargo run -- asyncapi
gen` 即可打印它的 AsyncAPI 文档，不需要任何运行时样板代码。

## 设计原则

- **完全异步，基于 tokio。** 公开接口中没有任何阻塞式 API。
- **核心是泛型的，契约里没有 `dyn`。** 使用关联类型和原生的 `async fn in trait`；服务需要类型擦除
  时，擦除住在运行时里，而不在契约里。
- **订阅者是 `Stream`，而不是回调。** 背压天然可用；回调式的开发体验由运行时在其之上构建。
- **ack 会消费 `self`。** 你不可能 ack 两次，这一点由编译器保证。
- **可选功能通过能力 trait 表达**（`BatchSubscriber`、`TransactionalPublisher`、`RequestReply`、
  `Partitioned`、`Seekable`），绝不塞进必需的接口。

## 接下来读什么

<div class="grid cards" markdown>

- :material-download: **[安装](getting-started/installation.md)** - 各项 feature 与 crate 配置。
- :material-rocket-launch: **[快速上手](getting-started/quickstart.md)** - 用 `cargo generate` 生成服务骨架。
- :material-school: **[教程](getting-started/tutorial.md)** - 一步步构建一个服务。
- :material-test-tube: **[测试](guides/testing.md)** - 在进程内测试处理器，无需启动服务器。
- :material-web: **[HTTP 框架](guides/http.md)** - 与 axum 并行运行，配合事务性 outbox。
- :material-transit-connection-variant: **[Broker](brokers/index.md)** - 内存 Broker、NATS 和 Redis。
- :material-server-network: **[Broker 作者](broker-authors/index.md)** - 实现契约并通过 conformance 校验。

</div>

## 本仓库的范围

本站点介绍的是 `ruststream`，即纯 Rust 的核心（不含 PyO3，也不含任何具体 Broker）。具体的 Broker
（NATS、Kafka、RabbitMQ、Redis、MQTT）各自位于独立的 crate 中，并从 crates.io 引入 `ruststream`。
Python 绑定位于 [`ruststream-py`](https://github.com/powersemmi/ruststream-py) 仓库。

Rust API 参考文档发布在 [docs.rs](https://docs.rs/ruststream)，另见
[API 参考](reference.md)。
