# 教程：构建你的第一个服务

本教程从零开始构建一个订单服务，并逐块讲解。服务运行在内存 Broker 上，不需要额外启动任何外部
服务。换成真正的 Broker 只是一行改动，第 7 步会讲到。

## 1. 创建 crate

```bash
cargo new orders-service
cd orders-service
```

```toml title="Cargo.toml"
[package]
name = "orders-service"
version = "0.1.0"
edition = "2024"

[dependencies]
ruststream = { version = "0.7", features = ["macros", "memory", "json", "asyncapi"] }
serde = { version = "1", features = ["derive"] }
```

## 2. 定义消息和处理器

处理器是一个 `async fn`，第一个参数是解码后的载荷。`#[subscriber]` 宏把它变成可挂载的定义，名字
取自函数名。

=== "宏"

    ```rust title="src/orders.rs"
    --8<-- "examples/tutorial/orders.rs:order"
    ```

=== "手写"

    ```rust title="src/orders.rs"
    --8<-- "examples/manual/tutorial/orders.rs:order"
    ```

处理器返回 [`HandlerOutcome`](../guides/subscribers.md#acking)：要么是 `ack`，要么是 `nack`。
`nack` 丢弃消息，或者把它重新入队。处理器也可以返回 `()` 或 `Result<(), E>`，其中 `Ok` 表示
ack，`Err` 表示丢弃。

`JsonSchema` derive 把载荷的 schema 写进第 6 步的 AsyncAPI 文档。文档里这条消息的描述取自类型的
文档注释。这不需要额外的依赖：`asyncapi` feature 已经重导出了 `schemars`。

## 3. 接入应用

=== "宏"

    ```rust title="src/main.rs"
    --8<-- "examples/tutorial/first_app.rs:app"
    ```

=== "手写"

    ```rust title="src/main.rs"
    --8<-- "examples/manual/tutorial/first_app.rs:app"
    ```

!!! tip "编解码器的默认值"
    `include` 用默认编解码器解码，因此不需要编解码器参数。默认编解码器由 `json`、`cbor`、
    `msgpack` 中第一个启用的 feature 选出。要让该 Broker 下的所有处理器换用另一个编解码器，
    可以用 `with_broker_codec(broker, codec, |b| ...)` 设定一次。
    完整的选取规则参见[编解码器](../guides/codecs.md)。

运行它：

```bash
cargo run -- run
```

## 4. 回复消息

要发布一条回复，就返回回复值，并在订阅者上写 `publish`。目的地由回复类型上的 `Outgoing` derive
声明：

=== "宏"

    ```rust title="src/orders.rs"
    --8<-- "examples/tutorial/orders.rs:confirm"
    ```

=== "手写"

    ```rust title="src/orders.rs"
    --8<-- "examples/manual/tutorial/orders.rs:confirm"
    ```

用同一个 `include` 把 `confirm` 挂在 `handle` 旁边。回复由 Broker 的默认发布策略发出，并用默认
编解码器编码。

=== "宏"

    ```rust title="src/main.rs"
    --8<-- "examples/tutorial/reply_app.rs:reply"
    ```

=== "手写"

    ```rust title="src/main.rs"
    --8<-- "examples/manual/tutorial/reply_app.rs:reply"
    ```

在处理器内部发布以及其余的发布方式，参见[发布与回复](../guides/publishing.md)。

## 5. 用路由器组织代码

处理器多起来以后，把它们放进各自的模块，再汇总到 [`Router`](../guides/routing.md) 里：

=== "宏"

    ```rust title="src/routes.rs"
    --8<-- "examples/tutorial/routes.rs:routes"
    ```

=== "手写"

    ```rust title="src/routes.rs"
    --8<-- "examples/manual/tutorial/routes.rs:routes"
    ```

带回复的处理器用链式调用挂到路由器上：`.out_reply(..)` 指定回复的发布策略，`.build()` 提交这次
注册。不写 `.out_reply(..)` 时，`.build()` 采用 Broker 的默认发布策略，也就是第 4 步里 `include`
用的那一个。路由器的其余用法参见[路由](../guides/routing.md)。

=== "宏"

    ```rust title="src/main.rs"
    --8<-- "examples/tutorial/main.rs:main"
    ```

=== "手写"

    ```rust title="src/main.rs"
    --8<-- "examples/manual/tutorial/main.rs:main"
    ```

## 6. 查看 AsyncAPI 文档

```bash
cargo run -- asyncapi gen
```

每个订阅者都会给文档添加一个通道和一个 `receive` 操作。`handle` 和 `confirm` 共用 `orders` 这个
通道，但各自有一个操作：两者的订阅是分开的。回复在 `confirmations` 上添加一个 `send` 操作。

载荷的 schema 放在文档的 `components.messages` 下。输出参数（`-o`、`--yaml`）和文档本身参见
[AsyncAPI](../guides/asyncapi.md)。

## 7. 换成真正的 Broker

上面写的一切都不绑定在内存 Broker 上。Broker 在 `with_broker` 处选定，更换只是一行改动。把对应的
Broker crate 加进依赖，在那里构造它，例如用 `NatsBroker::new("nats://localhost:4222")` 代替
`MemoryBroker::new()`。处理器、路由器和编解码器保持不变。可用的 Broker 和每一种的替换写法，参见
[Broker](../brokers/index.md#switching-brokers)。

!!! info "完整的服务是一个可编译的示例"
    本页的每一段代码都来自仓库里的
    [`examples/tutorial`](https://github.com/powersemmi/ruststream/tree/main/examples/tutorial)，
    CI 每次改动都会构建它。`first_app.rs` 和 `reply_app.rs` 是第 3 步和第 4 步结束时的服务，
    `main.rs` 是最终版本。你也可以用
    `cargo run --example tutorial --features macros,memory,json,asyncapi -- run` 自己运行一遍。

## 下一步

- [中间件](../guides/middleware.md)：围绕处理器的横切逻辑。
- [生命周期](../guides/lifespan.md)：共享状态与启动/关闭钩子。
- [测试](../guides/testing.md)：在进程内测试你刚写好的处理器。
- [指标](../guides/metrics.md)：Prometheus 计数器与直方图。
