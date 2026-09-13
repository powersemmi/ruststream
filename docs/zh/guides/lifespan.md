# 生命周期与共享状态

大多数服务都需要几样资源：数据库连接池、HTTP 客户端、解析好的配置。它们在启动时创建一次，
之后由每个处理器共享。RustStream 为它们提供一个带类型的共享状态值，并在运行循环前后的固定时点
执行生命周期钩子。

## 共享状态

应用状态是 `on_startup` 钩子返回的单个带类型的值。任何处理器或中间件都通过 `ctx.state()` 借用它。

完整的状态用法在[上下文与状态](context.md#application-level-typed-state)一节：编译期检查的挂载
规则、`State<T>` 注入，以及存放单次投递数据的上下文。本页只讲产出状态和释放状态的钩子。

## 生命周期钩子 { #lifecycle-hooks }

需要 `async` 的工作都放进钩子里：连上连接池，或者干净地关掉它。四个钩子把运行循环夹在中间：

```text
on_startup(prev) -> S            # Broker 连接之前；构建异步资源，产出状态
  -> Broker 完成连接，订阅打开
after_startup(Arc<S>)            # 处理器已在工作；发布第一条消息、上报就绪
  ... 运行中 ...
  -> 触发关闭（收到信号，或 run_until 的 future 完成）
on_shutdown(Arc<S>)              # Broker 仍处于连接状态
  -> Broker 关闭，处理中的处理器排空
after_shutdown(Arc<S>)           # 最终清理
```

- **`on_startup`** 以**值**的方式接收上一个状态（首次调用时是 `()`），并返回新状态。因此它的
  future 可以跨 `await` 持有资源：连上连接池，组装状态结构体，再把它返回。之后的钩子拿到的是共享
  的 `Arc<S>`。`on_startup` 只能写在第一个 `with_broker` 之前：处理器按它产出的状态类型注册，
  反过来的顺序无法通过编译。其余生命周期钩子要注册在它之后：更早注册的钩子会捕获错误的状态类型，
  这时 `on_startup` 会 panic。
- **`after_startup`** 在订阅打开、处理器开始工作之后执行。要发布第一条消息，建议用作用域级的写法
  `b.after_startup(policy, hook)`：时点相同，而钩子拿到的发布者由策略在已连接的 Broker 上实例化。
  应用级的钩子留给就绪上报和与 Broker 无关的工作，[测试指南](testing.md)就这样用它。应用自己要
  消费的初始消息也在这里发布：在此之前发布的消息没有任何订阅者能收到。
- **`on_shutdown`** 在关闭开始时执行，此时各个 Broker 仍处于连接状态。
- **`after_shutdown`** 在 Broker 关闭之后执行，做最后的异步清理。

启动钩子返回错误，服务就中止启动。关闭钩子只把错误记进日志，因此关闭流程总能走完。同一类的钩子
按注册顺序执行。

## 传入数据库连接

常见的做法：对外服务之前打开连接池，把它交给每个处理器，退出时关掉。下面的 `Database` 是任意异步
资源的替身。`sqlx::PgPool` 或者一个 HTTP 客户端都能这样接进来，区别只在各自的 `connect` /
`close` 调用：

=== "宏"

    ```rust
    --8<-- "examples/lifespan.rs:hooks"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/lifespan.rs:hooks"
    ```

钩子的错误类型由返回的 `Result` 推导，只要求它实现 `std::error::Error + Send + Sync`。资源本身是
`Send + Sync`，因此并发运行的处理器都通过 `ctx.state()` 借用同一个实例，不必为每条消息重新建立
连接：

=== "宏"

    ```rust
    --8<-- "examples/lifespan.rs:handler"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/lifespan.rs:handler"
    ```

可运行的完整程序见
[`examples/lifespan.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/lifespan.rs)。

## 与另一个服务器并行运行

`run` 独占整个进程：它安装信号处理程序，并且只在服务停止之后返回。

如果服务要和另一个前台服务器（通常是 HTTP 框架）共用进程，就用 `start` 启动消息这一侧。`start`
走同样的启动流程，在订阅打开之后完成，因此启动的错误会在宿主开始接收流量之前返回。`start` 不安装
信号处理程序：用什么停止服务，由宿主决定。生命周期的其余部分由返回的 `RunningApp` 句柄推进：

```rust
--8<-- "tests/app_start.rs:handle"
```

- `stopping()` 返回一个持有所有权的 future：服务在 fail-fast 模式下因故障自行停止时，它就完成。
  可以把它接到宿主的优雅关闭上（axum 的 `with_graceful_shutdown`），消息这一侧一停，进程也就
  不再对外服务。
- `shutdown()` 是显式的优雅关闭：先执行 `on_shutdown` 钩子，再等待正在执行的处理器和结算后的
  后续任务（不超过[关闭超时](#shutdown-timeout)），然后按注册的相反顺序关闭各个 Broker，最后
  执行 `after_shutdown` 钩子。fail-fast 的故障原因由 `shutdown()` 以错误的形式返回。

句柄带 `#[must_use]`：不调用 `shutdown` 就丢弃它，服务会脱离管理，也不做优雅关闭。`run` 和
`run_until` 建立在同一条 start/shutdown 路径上，因此三种写法共用一套启动与关闭流程。

## 关闭超时 { #shutdown-timeout }

触发关闭之后，`run` 默认无限期等待正在执行的处理器结束。可以用 `shutdown_timeout` 给这段等待
设上界，就像上面的例子那样。超时之后仍在运行的处理器会被中止：

<!-- inline-rust: isolates the shutdown_timeout call; the full chain is compiled in lifespan.rs:hooks, shown earlier on this page -->
```rust
use std::time::Duration;

RustStream::new(info)
    .shutdown_timeout(Duration::from_secs(10))
    .with_broker(broker, |b| { b.include(handle); });
```
