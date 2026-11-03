---
title: 运行时
description: "服务只写一次，既可以在 tokio 上运行，也可以在 embassy 上运行：处理器相同，由应用选择运行器。"
---

# 运行时

服务只写一次，处理器和挂载链都相同。它在两种运行器之一上运行：tokio 上的多线程运行器，或用于设备的 embassy 上的单任务运行器。
门面 crate 为每种运行器提供一个 feature：`tokio` 和 `embassy`。两者可以叠加，同一次构建可以同时启用。

```toml
# 服务器：tokio 在默认 feature 中。
ruststream = "0.8"

# 有堆、没有操作系统的设备：只用 embassy，构建中不会出现 tokio。
ruststream = { version = "0.8", default-features = false, features = ["embassy", "macros", "json"] }

# 没有堆的设备：构建在启动时和处理每条消息时都不分配内存。
ruststream = { version = "0.8", default-features = false, features = ["embassy", "memory", "macros", "postcard"] }

# 运行在 tokio 上的 broker crate，它关闭默认 feature。
ruststream = { version = "0.8", default-features = false, features = ["tokio"] }
```

## 由应用选择运行器

`#[ruststream::app]` 构建 `RustStream`，即 tokio 运行器的应用。
`#[ruststream::app(runtime = embassy)]` 构建 `EmbeddedApp`，即 embassy 运行器的应用，也就是一个 embassy 任务。
其他任何运行时名称都是编译错误。
处理器本身不带运行时：`#[subscriber]` 在两种运行器下展开成相同的代码，运行器在挂载处自己声明对处理器的要求。

## 各运行器对处理器的要求

tokio 运行器在每个 `.await` 处可能把处理器移到另一个线程，所以处理器函数体的 future 必须是 `Send`。
在 `.await` 之间持有 `Rc` 的函数体会在挂载它的 `include` 处报错，错误信息会指出这个处理器。
embassy 运行器在同一个任务里轮询所有处理器，对 future 没有要求，所以同一个函数体可以在那里挂载。
确认之后运行的后续任务（`and_after` 和上下文钩子）遵循同样的规则：启用 tokio 时它的 future 必须是 `Send`，只启用 embassy 时则没有这个要求，因此设备上的后续任务可以通过本地总线发布。

## 两种运行器共用一套 trait

一个 broker 只实现一个 `Publisher`、一个 `Settle` 和一组能力 trait，两种运行器都使用它们。
这些 trait 的 future 和父 trait 在启用 `tokio` 时要求 `Send`（以及 `Sync`），只启用 `embassy` 时没有这些要求，所以设备上的 broker 与服务器上的 broker 实现的是同样的 trait。
broker 的选项步骤以 `PublishSink<Options = ..>` 为界，在任一运行器上都生效。
同时启用两个 feature 的构建采用更严格的约束：持有 `Rc` 的 broker 类型无法与 tokio 一起编译，系列自带的本地内存总线在这种情况下改用线程安全的指针共享状态。

## 各运行器提供的能力

tokio 运行器提供 `workers(n)`、`threads(n)`、信号处理、指标、OpenTelemetry 和 CLI。
embassy 运行器提供固定数量的 worker（`.pool::<N>()` 和 `.keyed::<N>()`）、容量在挂载处声明的重试存储（`.retry_capacity::<CAP>()`），以及通过 `Control` 和 `Health` 进行的关闭与健康状态。
某个运行器没有提供的条目，在另一个运行器上使用就是编译错误。

当延迟重试发现 embassy 的存储已满时，副本无处等待。
这与其他发布失败一样，有它自己的错误“没有空闲记录”：处理函数的 `.error` 续体会收到通知；没有它时，失败会写入日志，副本丢失。
无论哪种情况，这条投递都已确认，所以处理函数不会因为存储已满而再次运行。

## 没有堆的 embassy 运行器

embassy 运行器有两种构建形态，一次构建只含其中一种。
启用 `alloc` feature 时，它把头部、载荷、后续任务和失败信息放在堆上。
不启用时，同一个运行器把它们放在固定大小的存储里，启动时和处理每条消息时都不分配内存，所以没有堆的设备也能运行这个框架。
`tokio`、`json`、`cbor` 和 `msgpack` 会打开 `alloc`，依赖图中任何请求它的 crate 也会：feature 只会叠加，打开了 `alloc` 的依赖图整体都是带堆的形态。

两种形态下服务的写法相同：`#[subscriber]`、`#[ruststream::app(runtime = embassy)]`、`.then`、`.error`、`Out` 槽位和类型化头部。
不同的是那些大小原本由堆决定的部分：

| 部分 | 有 `alloc` | 无 `alloc` |
|---|---|---|
| 消息的头部 | `HeaderMap` | `FixedHeaders`：8 个条目和 256 字节，装满后 `insert` 返回错误 |
| 编解码器 | `Codec`：`json`、`cbor`、`msgpack` | `SliceCodec`：`postcard` 和 `json-core`，编码到切片中 |
| 后续任务（`and_after`、`.then`、`.error`） | 放在堆上 | 放在 128 字节的空间里；更大的捕获无法编译 |
| `.error` 读取的失败信息 | 按值传入 | 从订阅的固定缓冲区借用 |
| `ctx.after(..)` | 登记 | 返回 `Result`，因为一次投递只有四个登记名额 |
| 批 | 结果的 `Vec` | 最多 16 条，在挂载处用 `.batch::<N>()` 指定 |
| 本地总线 | `LocalMemoryBroker::new()` | 由开发板声明为 `static` 的 `LocalMemoryBus` |
| 应用选定的日志器 | `TracingLogger`、`LogLogger` 或 `DefmtLogger` | `LogLogger` 或 `DefmtLogger`，取决于启用了哪个 feature |

形态由构建选定，所以处理器不会按它分支。
带堆形态的类型在固定形态中无法解析，超出的容量是点名该容量的编译错误。
组件页面[嵌入式运行时](../components/application/embedded-runtime.md)展示了一个固定形态下的完整设备服务，以及挂载处要指定的每项容量。

## 测试

`TestApp` 运行 tokio 应用，`testing::embedded::TestApp` 在主机上用 embassy-time 的模拟时钟运行嵌入式应用。
启用 `testing` 时，两者同时位于门面中。
启用 `testing` 时，嵌入式应用注册的每个 broker 都实现进程内模式 `InProcess`。
嵌入式的 `TestApp` 跟随构建的形态：没有 `alloc` 时它驱动固定形态，测试程序可以装上一个对框架的任何内存分配都报错的分配器。
