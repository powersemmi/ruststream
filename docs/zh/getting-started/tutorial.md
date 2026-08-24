# 教程：构建你的第一个服务

本教程从零开始构建一个订单服务，并逐块讲解。它使用内存 Broker，因此不需要另外运行任何外部依赖；
换成真正的 Broker 只是一行改动，本文最后会讲到。

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
ruststream = { version = "0.6", features = ["macros", "memory", "json", "asyncapi"] }
serde = { version = "1", features = ["derive"] }
```

## 2. 定义消息和处理器

处理器是一个 `async fn`，它的第一个参数就是解码后的载荷。`#[subscriber]` 宏把它变成一个可挂载的
定义，名字与函数同名。

```rust title="src/orders.rs"
--8<-- "examples/tutorial/orders.rs:order"
```

处理器返回一个 [`HandlerResult`](../guides/subscribers.md#acking)：要么是 `Ack`，要么是一个丢弃或
重新入队该消息的 `nack`。返回 `()` 或 `Result<(), E>` 同样可行，它们会转换成一个结果（`Ok` 表示
ack，`Err` 表示丢弃）。

把载荷的 schema 放进第 6 步那份 AsyncAPI 文档的，正是 `JsonSchema` derive；类型的文档注释则成为该
消息的描述。这不需要额外的依赖，因为 `asyncapi` 特性已经重导出了 `schemars`。

## 3. 接入应用

```rust title="src/main.rs"
--8<-- "examples/tutorial/first_app.rs:app"
```

宏把 `handle` 变成一个与函数同名的值，所以你直接导入它并原样传入即可。

!!! tip "编解码器的默认值"
    `include` 用默认编解码器解码：启用了 `json` 就用 `json`，否则用 `cbor`，再否则用 `msgpack`，
    因此它不需要编解码器参数。想在所有地方都换成另一个，用
    `with_broker_codec(broker, codec, |b| ...)` 设置一次即可。完整的选取规则参见
    [编解码器](../guides/codecs.md)。

运行它：

```bash
cargo run -- run
```

## 4. 回复消息

要发布一条回复，就返回回复值，并用 `publish(..)` 指明目的地：

```rust title="src/orders.rs"
--8<-- "examples/tutorial/orders.rs:confirm"
```

把它挂在 `handle` 旁边，用同一个普通的 `include` 即可：回复会经由 Broker 的默认发布策略发出，编码
用的是默认编解码器。

```rust title="src/main.rs"
--8<-- "examples/tutorial/reply_app.rs:reply"
```

完整的图景（包括在处理器内部发布）参见[发布与回复](../guides/publishing.md)。

## 5. 用路由器组织代码

随着处理器变多，把它们放进各自的模块，再汇总进一个 [`Router`](../guides/routing.md)：

```rust title="src/routes.rs"
--8<-- "examples/tutorial/routes.rs:routes"
```

路由器本身不持有 Broker，因此注册无法像作用域的构建器那样在丢弃时自动提交，它要以一个显式的终结调用
收尾。`.publisher(..)` 指定回复的接线方式，发布策略仍然是纯粹的声明，所以路由器依旧不需要 Broker；
`.mount()` 则采用 Broker 自带的默认发布策略，也就是把第 4 步自动拿到的那一份显式写出来。路由器的
其余接口参见[路由](../guides/routing.md)。

```rust title="src/main.rs"
--8<-- "examples/tutorial/main.rs:main"
```

## 6. 查看 AsyncAPI 文档

```bash
cargo run -- asyncapi gen
```

每个订阅者都会变成一个 channel 和一个 `receive` 操作。`handle` 和 `confirm` 共用 `orders` 这个
channel，却各自拿到一个操作，因为它们开的是两条独立的订阅；回复则在 `confirmations` 上添加一个
`send` 操作。两个载荷类型都 derive 了 `schemars::JsonSchema`，所以文档在 `components.messages` 下
带上了它们的 schema，每一个的描述就是类型的文档注释。输出相关的参数（`-o`、`--yaml`）以及文档本身，参见
[AsyncAPI](../guides/asyncapi.md)。

## 7. 换成真正的 Broker

以上内容没有一处绑死在内存 Broker 上。Broker 是在 `with_broker` 处选定的，所以更换只是一行改动：
把对应的 Broker crate 加为依赖，并在那里构造它（例如把 `MemoryBroker::new()` 换成
`NatsBroker::new("nats://localhost:4222")`），处理器、路由器和编解码器都不用动。可用的 Broker，以及
每一种 Broker 的对照写法，参见 [Broker](../brokers/index.md#switching-brokers)。

!!! info "完整的服务是一个会参与编译的示例"
    本页的每一段代码都嵌自仓库中的
    [`examples/tutorial`](https://github.com/powersemmi/ruststream/tree/main/examples/tutorial)，
    CI 在每次改动时都会构建它：`first_app.rs` 和 `reply_app.rs` 分别是第 3 步和第 4 步结束时的服务，
    `main.rs` 则是最终版本。也可以用
    `cargo run --example tutorial --features macros,memory,json,asyncapi -- run` 自己跑一遍。

## 下一步

- [中间件](../guides/middleware.md)：围绕处理器的横切逻辑。
- [Lifespan](../guides/lifespan.md)：共享状态与启动/关闭钩子。
- [测试](../guides/testing.md)：在进程内测试你刚写好的处理器。
- [指标](../guides/metrics.md)：Prometheus 计数器与直方图。
