---
title: 切换到联网的 Broker
description: "更换构建商店所用的 Broker，商店就迁移到 NATS；订阅者和路由器的设置都保持不变。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 切换到联网的 Broker

更换构建商店所用的 Broker，商店就迁移到 NATS；订阅者和路由器的设置都保持不变。

Broker 只在一个地方选定：传给 `shop` 的值。
Broker crate 提供 Broker 类型和一个 prelude，其中的名字与内存 Broker 的 prelude 相同，`Publish` 也在其中。
因此路由器在每个 Broker 上读起来都一样，变化的只有它指明的类型。
订阅者的主体不从 Broker 导入任何东西，所以完全不需要改动。
每个 Broker 都有自己的 [crate](../../brokers/index.md)，本章使用 [`ruststream-nats`](https://docs.rs/ruststream-nats)。

## 依赖

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "cbor", "asyncapi", "logging"] }
ruststream-nats = { version = "0.7", features = ["asyncapi"] }
```

不再需要 `memory` feature。
两个 crate 的 `testing` feature 放进 `[dev-dependencies]`。

## 替换

入口在 `NatsBroker` 上构建商店。与内存 Broker 一样，它只在服务启动时才连接。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

{/* inline-rust: the shop on NATS needs the ruststream-nats crate, which this repository does not depend on; this code was built and run against a NATS 2 server for this page */}
```rust title="main.rs"
use ruststream_nats::prelude::*;

use crate::state::Shop;

#[ruststream::app]
fn app() -> impl App<State = Shop> {
    service::shop(NatsBroker::new("nats://localhost:4222"))
}
```

</TabItem>
<TabItem value="manual" label="手写">

{/* inline-rust: the shop on NATS needs the ruststream-nats crate, which this repository does not depend on; this code was built and run against a NATS 2 server for this page */}
```rust title="main.rs"
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream_nats::prelude::*;

use crate::state::Shop;

fn app() -> impl App<State = Shop> {
    service::shop(NatsBroker::new("nats://localhost:4222"))
}

fn main() -> ExitCode {
    run_main(app)
}
```

</TabItem>
</Tabs>

其他文件有两处改动。
每个文件导入 `ruststream_nats::prelude::*`，代替 `ruststream::memory::prelude::*`。
每个路由器返回 `RouterDef<NatsBroker, Shop>`，`shop` 接收类型为 `NatsBroker` 的 Broker。
编解码器、槽位、尝试上限和死信主题保持不变。
`http.rs` 以同样的方式在 `NatsBroker::new(..).bindable()` 上绑定它的发布者令牌。

## 测试

`NatsBroker` 有进程内模式，所以 `TestApp` 不需要服务器就能运行商店，与在内存 Broker 上一样。
测试从 `service.rs` 的 `test_broker` 取得 Broker，现在它构建的是 `NatsBroker`：

{/* inline-rust: the tests' broker helper on NATS needs the ruststream-nats crate, which this repository does not depend on; this code was built and its tests run in process for this page */}
```rust title="service.rs"
#[cfg(test)]
pub(crate) fn test_broker() -> NatsBroker {
    NatsBroker::new("nats://localhost:4222")
}
```

在进程内模式下，这个地址只会被解析，不会真正连接。
测试按类型请求 Broker：`tb.broker::<NatsBroker>()`。
除此之外测试没有任何改动，包括第 8 章的延迟和 AsyncAPI 文档。

## NATS 带来的变化

```bash
docker run --rm -p 4222:4222 nats:2-alpine
cargo run -- run
```

在 Core NATS 上，主题名就是 subject。
Core NATS 不保存消息：在没有订阅者监听时发布的消息就此消失，投递也没有确认。
`retry` 或 `retry_after` 把一份副本发布回该 subject，尝试上限统计这些副本。
如果主题的消息必须在重启后保留，就改为读取 JetStream 流：在属性中使用 [`JetStreamSubject`](https://docs.rs/ruststream-nats/latest/ruststream_nats/struct.JetStreamSubject.html)，并配一个持久消费者。
该 crate 的 [`nats_jetstream`](https://github.com/powersemmi/ruststream-nats/blob/main/crates/ruststream-nats/examples/nats_jetstream.rs) 示例展示了这样的用法，[`nats_retry`](https://github.com/powersemmi/ruststream-nats/blob/main/crates/ruststream-nats/examples/nats_retry.rs) 展示了每种订阅如何重试。
