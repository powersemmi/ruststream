# 生命周期与共享状态

大多数服务都需要一些在启动时创建一次、之后由每个处理器共享的资源：数据库连接池、HTTP 客户端、
解析好的配置。RustStream 为此提供了一个带类型的共享状态值，外加一组在运行循环前后固定时点执行的
生命周期钩子。

## 共享状态

应用状态是由 `on_startup` 钩子产出的单个带类型的值：钩子返回的值就成为状态，并就此确定应用的状态
类型。任何处理器或中间件都通过 `ctx.state()` 借用它。关于状态的完整说明，包括编译期的挂载规则、
`State<T>` 注入，以及承载单次投递数据的上下文，参见[上下文与状态](context.md#application-level-typed-state)；
本页只讲产出状态和销毁状态的钩子。

## 生命周期钩子 { #lifecycle-hooks }

任何需要 `async` 的工作（把连接池连上、干净地关掉它）都放进钩子里。运行循环由四个钩子夹住：

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

- **`on_startup`** 以**值**的方式接收上一个状态（首次调用时是 `()`）并返回新状态，因此它的 future
  可以跨 await 持有资源：连接一个连接池、构造状态结构体、把它返回。返回的类型即成为应用的状态类型。
  `on_startup` 失败会中止启动。之后的钩子拿到的状态是共享的 `Arc<S>`。`on_startup` 只能出现在第一个
  `with_broker` 之前：处理器是针对它产出的状态类型注册的，所以反过来的顺序无法通过编译。其余生命
  周期钩子要注册在它之后（更早注册的钩子会捕获到错误的状态类型；若已存在这样的钩子，`on_startup`
  会 panic）。
- **`after_startup`** 在订阅打开、处理器开始工作之后执行一次。若只是要发布一条初始消息，优先用作用域
  级的写法 `b.after_startup(policy, hook)`：它在同一个时点执行，但钩子拿到的是已经配对好的、可直接
  使用的发布者，因此不必把任何东西从接线闭包里透出来。应用级的钩子则保留给就绪上报和与 Broker 无关
  的工作（[测试指南](testing.md)就把它当作“处理器已就绪”的关卡）。两者中任何一个失败都会中止启动。
  对于应用自己要消费的种子消息，这里也是投递上正确的时点：在订阅打开之前发布，根本没有订阅者能收到。
- **`on_shutdown`** 在关闭开始时执行，此时 Broker 仍处于连接状态。
- **`after_shutdown`** 在 Broker 已经关闭之后执行，用于最后的异步清理。

启动钩子出错会中止服务；关闭钩子出错只记录日志，因此关闭流程总能走完。同一类的钩子按注册顺序执行。

## 传入数据库连接

常见场景：在开始提供服务之前打开连接池，把它共享给每个处理器，退出时再关掉。下面的 `Database` 只是
任意异步资源的替身，`sqlx::PgPool` 或者一个 HTTP 客户端都能以同样的方式接进来，区别仅在于各自的
`connect` / `close` 调用：

```rust
--8<-- "examples/lifespan.rs:hooks"
```

钩子的错误类型由返回的 `Result` 推导得出，只要求它实现 `std::error::Error + Send + Sync`。该资源是
`Send + Sync` 的，因此每个并发执行的处理器都通过 `ctx.state()` 借用同一个共享实例，无需为每条消息
单独建立连接：

```rust
--8<-- "examples/lifespan.rs:handler"
```

可直接运行的完整程序见
[`examples/lifespan.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/lifespan.rs)。

## 与另一个服务器并行运行

`run` 独占整个进程：它会安装信号处理器，并且只在服务停止之后才返回。如果一个服务要与另一个前台
服务器（通常是某个 HTTP 框架）共用进程，就改用 `start` 把消息这一侧拉起来。`start` 执行同样的启动
流程，并在订阅打开后即完成，因此启动失败会在宿主开始接收流量之前就暴露出来。它不安装任何信号
处理器：什么时候停止服务由宿主决定。返回的 `RunningApp` 句柄负责推进生命周期的其余部分：

```rust
--8<-- "tests/app_start.rs:handle"
```

- `stopping()` 返回一个持有所有权的 future，当服务因 fail-fast 失败而自行拆除时它会完成；把它接到
  宿主的优雅关闭上（axum 的 `with_graceful_shutdown`），这样消息侧一旦挂掉，进程也就不再对外提供
  服务。
- `shutdown()` 是显式的优雅拆除：先执行 `on_shutdown` 钩子，再排空处理中的处理器与结算之后的后续
  动作（受[关闭超时](#shutdown-timeout)约束），然后按注册的相反顺序关闭各个 Broker，最后执行
  `after_shutdown` 钩子。fail-fast 的原因会在这里以错误的形式浮现。

该句柄带 `#[must_use]`：不调用 `shutdown` 就把它丢弃，会让服务脱离管理，不做任何优雅拆除。`run`
和 `run_until` 都建立在同一条 start/shutdown 路径之上，因此这三种写法共用同一套启动与关闭流程。

## 关闭超时 { #shutdown-timeout }

默认情况下，触发关闭之后 `run` 会无限期等待处理中的处理器结束。用 `shutdown_timeout` 给这段等待
设上界，就像上面的例子那样；超时后仍在运行的处理器会被中止：

<!-- inline-rust: isolates the shutdown_timeout call; the full chain is compiled in lifespan.rs:hooks, shown earlier on this page -->
```rust
use std::time::Duration;

RustStream::new(info)
    .shutdown_timeout(Duration::from_secs(10))
    .with_broker(broker, |b| b.include(handle));
```
