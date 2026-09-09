# RustStream

**RustStream** 让 Rust 服务订阅事件流，并向事件流发布消息。服务不会因此绑定到某一个消息 Broker。
核心是一组 trait 和一个带路由器的运行时。
随核心一起提供的还有编解码器、AsyncAPI 生成、Prometheus 指标，以及面向 Broker 作者的 `conformance` 校验套件。

两条架构承诺决定了框架的形态：

1. **为第三方 Broker 提供真正的接口。** 核心只包含 trait 和类型，不依赖任何 Broker。
   每个 Broker 都是独立的 crate。`conformance` 校验套件检查 Broker 是否遵守契约。
2. **Broker 专有的配置和默认值留在 Broker crate 中。** 每个 Broker crate 都有自己的 `Config`。
   因此上游的一次变更只波及一个 Broker crate，框架本身不受影响。

=== "宏"

    ```rust
    --8<-- "examples/quickstart.rs"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/quickstart.rs"
    ```

`#[ruststream::app]` 会生成 `main`，其中包含运行时的全部样板代码。
因此 `cargo run -- run` 启动服务，`cargo run -- asyncapi gen` 打印该服务的 AsyncAPI 文档。

## 设计原则

- **完全异步，基于 tokio。** 公开 API 中没有阻塞调用。
- **核心是泛型的，契约里没有 `dyn`。** 契约建立在关联类型和原生的 `async fn in trait` 之上。
  服务需要类型擦除时，运行时负责完成。
- **订阅者是 `Stream`，不是回调。** `Stream` 本身提供背压。运行时在其之上构建回调式的写法。
- **ack 会消费 `self`。** 第二次 ack 是编译错误。
- **能力 trait 提供可选功能。** 必需接口之外还有 `BatchSubscriber`、
  `TransactionalPublisher`、`RequestReply`、`Partitioned` 和 `Seekable`。

## 接下来读什么

<div class="grid cards" markdown>

- :material-download: **[安装](getting-started/installation.md)** - 各项 feature 和 crate 的引入方式。
- :material-rocket-launch: **[快速上手](getting-started/quickstart.md)** - 用 `cargo generate` 生成服务骨架。
- :material-school: **[教程](getting-started/tutorial.md)** - 一步步构建一个服务。
- :material-test-tube: **[测试](guides/testing.md)** - 在进程内测试处理器，不需要服务器。
- :material-web: **[HTTP 框架](guides/http.md)** - 与 axum 并行运行，配合事务性 outbox。
- :material-transit-connection-variant: **[Broker](brokers/index.md)** - 内存 Broker 和各个 Broker crate。
- :material-server-network: **[Broker 作者](broker-authors/index.md)** - 实现契约，并通过 `conformance` 校验。

</div>

## 本仓库的范围

本站点介绍 `ruststream`，也就是与 Broker 无关的核心 crate。
具体的 Broker（NATS、Kafka、RabbitMQ、Redis、MQTT 等）各自是独立的 crate。
这些 crate 从 crates.io 引入 `ruststream`。

Rust API 参考文档发布在 [docs.rs](https://docs.rs/ruststream)，参见 [API 参考](reference.md)。
