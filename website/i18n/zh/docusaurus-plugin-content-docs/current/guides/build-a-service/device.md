---
title: 基于本地内存 Broker 的设备服务
description: "一块温度计板在没有堆的 embassy 运行时上运行 RustStream 服务，并在主机上用一个分配器测试它：框架一旦发生任何分配，测试就会失败。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 基于本地内存 Broker 的设备服务

一块温度计板在没有堆的 embassy 运行时上运行 RustStream 服务，并在主机上用一个分配器测试它：框架一旦发生任何分配，测试就会失败。

前面各章的商店运行在 `MemoryBroker` 和 tokio 运行时上。
固件有自己的一对组合：[嵌入式运行时](../../components/application/embedded-runtime.md)和[本地内存 Broker](../../brokers/local-memory.md)，后者是同一块板子各部分之间的队列。
订阅者的写法不变，挂载也是同样的链式形式。
本章构建一块小板子：传感器发布读数，板子把读数换算成摄氏度，读数过高时发出警报。

## 依赖

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.8", default-features = false, features = ["embassy", "memory", "macros", "postcard"] }
serde = { version = "1", default-features = false, features = ["derive"] }
```

`default-features = false` 去掉 `tokio` 和 `json`，`embassy` 引入面向固件的运行器。
`postcard` 是编解码器：它向切片编码、从切片解码，所以板子不需要堆来存放缓冲区。
这份列表里没有任何内容会启用框架的 `alloc` feature，因此每个大小本该由堆决定的部分都采用固定形态。

## 总线

板子把总线声明为 `static`，应用函数收到的是基于它构建的 Broker。
每个订阅容纳四条消息，处理器手中的那条也计入。
总线所依赖的临界区由二进制 crate 提供 `critical-section` 实现。
在没有比较并交换指令的内核上（例如 Cortex-M0+），固件还要为 `portable-atomic` 开启 `critical-section` 特性。

```rust reference="crates/ruststream/tests/doc_device.rs#bus"
```

## 处理器与回复

读数和报告是普通的 `serde` 类型，它们自己声明要发往哪里。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device.rs#messages"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device_manual.rs#messages"
```

</TabItem>
</Tabs>

处理器接收一条读数，返回它的报告。
回复发往报告类型所声明的目的地，所以处理器自己不发布任何东西。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device.rs#reading-handler"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device_manual.rs#reading-handler"
```

</TabItem>
</Tabs>

应用把处理器挂载到板子的总线上。
`out_reply` 通过总线发布报告，`codec` 用 postcard 编码它，`encode_capacity` 为每个工作者指定编码回复用的字节数，默认 256。
放不下的回复会以 `CodecFailure::BufferFull` 被拒绝，绝不会被截断：这次投递会被确认，失败则交给处理器的 `.error` 延续。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device.rs#reply-mount"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device_manual.rs#reply-mount"
```

</TabItem>
</Tabs>

## 发布到另一个主题

第二个处理器读取报告，温度超过五十度时发出警报。
警报经由 `Out` 槽位发出，与[商店的通知](./replies.md)相同，并用一个字节携带传感器编号。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device.rs#alarm"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device_manual.rs#alarm"
```

</TabItem>
</Tabs>

挂载加入这个处理器，并指明槽位背后的发布者。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device.rs#alarm-mount"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device_manual.rs#alarm-mount"
```

</TabItem>
</Tabs>

在设备上，`#[ruststream::app(runtime = embassy)]` 把这个函数交给板子的执行器。
设备方面的其余能力见[嵌入式运行时](../../components/application/embedded-runtime.md)页面：按板子时钟的重试、延续和批。

## 在主机上测试

测试在主机上运行，使用 `std`，通过嵌入式 [`TestApp`](../../components/application/testing/embedded-app.md) 驱动。
它原样启动板子运行的那个应用，总线本身就是进程内模式，不需要替身 Broker。
测试框架自己的记录（每个订阅收到了什么、每个槽位发布了什么）放在主机的堆上。
它之下的框架保持设备的形态：测试构建不启用 `alloc`，应用以固定形态被轮询。

测试可以证明服务不做任何分配。
测试二进制把 `assert_no_alloc::AllocDisabler` 安装为全局分配器，应用被轮询期间框架的任何分配都会使测试失败。
测试框架自己的记录由分配器放行。
这项保护在调试构建中生效，而 `cargo test` 构建的正是调试版本。

```toml title="Cargo.toml"
[dev-dependencies]
ruststream = { version = "0.8", default-features = false, features = ["testing"] }
assert_no_alloc = "1.1"
futures = "0.3"
```

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="tests/device.rs" reference="crates/ruststream/tests/doc_device.rs#allocator"
```

```rust title="tests/device.rs" reference="crates/ruststream/tests/doc_device.rs#device-test"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="tests/device.rs" reference="crates/ruststream/tests/doc_device_manual.rs#allocator"
```

```rust title="tests/device.rs" reference="crates/ruststream/tests/doc_device_manual.rs#device-test"
```

</TabItem>
</Tabs>

测试发布一条 1200 毫伏的读数，处理器把它换算为 70 度。
它检查报告已到达 `reports`，并且传感器 2 的警报经由它的槽位发出。
测试中的发布会等到它引发的一切都结算之后才返回，所以检查可以紧随其后。
板子的时钟只在测试调用 `advance` 时前进，与[商店测试](./testing.md)中暂停的时钟一样。

## 让测试保持设备的形态

Cargo 对一次构建中的每个 crate 只构建一次，使用这次构建里所有 crate 请求的 feature 的并集。
如果某个开发依赖，或测试构建中的任何其他 crate，启用了框架的 `alloc` feature，被测服务也会随之启用它。
这时测试检查的是带堆的框架形态，分配器对设备什么也没证明。

常见原因是依赖行保留了默认 feature，例如 `ruststream = { version = "0.8", features = ["testing"] }`：它引入 `json` 和 `tokio`，而这两者都隐含 `alloc`。
`alloc`、`tokio`、`compio`、`tracing`、`json`、`msgpack`、`cbor` 和 `conformance` 同样如此。
因此服务中每一行 `ruststream`（包括开发依赖）都带 `default-features = false`，测试构建中也没有其他 crate 请求这些 feature。

核心仓库把自己的无 `alloc` 构建单独放在 [`checks/noalloc`](https://github.com/powersemmi/ruststream/tree/main/checks/noalloc)，因为 crate 自身单元测试所需的开发依赖会启用 `alloc`。
它的 `just test-no-alloc` 配方用 `cargo tree -e features` 列出每个构建的 feature 树，一旦其中出现 `alloc` 就失败。
同样的检查也适用于服务：

```bash
cargo tree -e features,normal,build,dev -i ruststream-core | grep 'feature "alloc"'
```

只要服务保持设备的形态，这条命令就不输出任何内容。
