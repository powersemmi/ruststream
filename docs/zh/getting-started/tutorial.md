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
use ruststream::runtime::HandlerResult;
use ruststream::subscriber;
use serde::{Deserialize, Serialize};

--8<-- "examples/tutorial/orders.rs:order"
```

处理器返回一个 [`HandlerResult`](../guides/subscribers.md#acking)：要么是 `Ack`，要么是一个丢弃或
重新入队该消息的 `nack`。返回 `()` 或 `Result<(), E>` 同样可行，它们会被转换成一个结果（`Ok` 表示
ack，`Err` 表示丢弃）。

## 3. 接入应用

```rust title="src/main.rs"
mod orders;

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, RustStream};

use crate::orders::handle;

--8<-- "examples/quickstart.rs:app"
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

用普通的 `include` 挂载它即可；回复会经由 Broker 的默认发布策略、以默认编解码器发出（想为回复指定
另一个编解码器或加上变换，就再链式调用 `.publisher(..)` 并传入一个 `TypedPublisher` 栈）：

<!-- inline-rust: minimal mount fragment isolating the reply wiring; the full compiled program is examples/tutorial/main.rs:main, pulled in below -->
```rust
// inside with_broker(...), with `confirm` imported from the orders module
b.include(confirm);
```

完整的图景（包括在处理器内部发布）参见[发布与回复](../guides/publishing.md)。

## 5. 用路由器组织代码

随着处理器变多，把它们放进各自的模块，再汇总进一个 [`Router`](../guides/routing.md)：

```rust title="src/routes.rs"
--8<-- "examples/tutorial/routes.rs:routes"
```

```rust title="src/main.rs"
--8<-- "examples/tutorial/main.rs:main"
```

## 6. 查看 AsyncAPI 文档

```bash
cargo run -- asyncapi gen
```

每个订阅者都会变成一个 channel 和一个 `receive` 操作；派生了 `schemars::JsonSchema` 的载荷类型还会
贡献出 schema。输出相关的参数（`-o`、`--yaml`）以及文档本身，参见
[AsyncAPI](../guides/asyncapi.md)。

## 7. 换成真正的 Broker

以上内容没有一处绑死在内存 Broker 上。Broker 是在 `with_broker` 处选定的，所以更换只是一行改动：
把对应的 Broker crate 加为依赖，并在那里构造它（例如把 `MemoryBroker::new()` 换成
`NatsBroker::new("nats://localhost:4222")`），处理器、路由器和编解码器都不用动。可用的 Broker，以及
每一种 Broker 的对照写法，参见 [Broker](../brokers/index.md#switching-brokers)。

!!! info "完整的服务是一个会被编译的示例"
    本页的每一段代码都嵌自仓库中的
    [`examples/tutorial`](https://github.com/powersemmi/ruststream/tree/main/examples/tutorial)，
    CI 在每次改动时都会构建它。你也可以用
    `cargo run --example tutorial --features macros,memory,json -- run` 自己跑一遍。

## 下一步

- [中间件](../guides/middleware.md)：围绕处理器的横切逻辑。
- [Lifespan](../guides/lifespan.md)：共享状态与启动/关闭钩子。
- [测试](../guides/testing.md)：在进程内测试你刚写好的处理器。
- [指标](../guides/metrics.md)：Prometheus 计数器与直方图。
