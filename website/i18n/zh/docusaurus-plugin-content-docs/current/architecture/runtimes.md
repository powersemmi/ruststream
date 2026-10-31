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

## 各运行器提供的能力

tokio 运行器提供 `workers(n)`、`threads(n)`、信号处理、指标、OpenTelemetry 和 CLI。
embassy 运行器提供固定数量的 worker（`.pool::<N>()` 和 `.keyed::<N>()`）、容量在挂载处声明的重试存储（`.retry_capacity::<CAP>()`），以及通过 `Control` 和 `Health` 进行的关闭与健康状态。
某个运行器没有提供的条目，在另一个运行器上使用就是编译错误。

## 测试

`TestApp` 运行 tokio 应用，`testing::embedded::TestApp` 在主机上用 embassy-time 的模拟时钟运行嵌入式应用。
启用 `testing` 时，两者同时位于门面中。
启用 `testing` 时，嵌入式应用注册的每个 broker 都实现进程内模式 `InProcess`。
