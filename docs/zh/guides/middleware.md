# 中间件

中间件为处理器包裹横切逻辑：链路追踪、指标、鉴权、重试。RustStream 有两个中间件作用域，它们建立在
同一套 `Layer` 机制之上，只是作用在分发路径的不同位置。

## 中间件的作用域 { #middleware-scopes }

两个作用域可以组合：应用级的栈在外层，路由器自己的栈嵌在它里面。

**应用作用域。** 用 `RustStream::layer` 给整个应用加一层，调用位置在 `with_broker` 之前。该层会包裹
在它之后注册的每一个处理器，既包括直接注册在 Broker 作用域上的处理器，也包括路由器通过
`include_router` 带进来的处理器。顺序由编译期强制保证：第一次调用 `with_broker` 会把构建器推进到
另一个阶段，在那里 `layer`（以及 `publish_layer`、`on_startup`）已经不复存在，因此一个包裹不到已注册
处理器的层是编译错误，而不是悄无声息地什么都不做：

```rust
--8<-- "examples/middleware_app_scope.rs:app_scope"
```

**路由器作用域。** 用 `Router::layer` 给某个路由器配置它自己的中间件，它会在挂载该路由器时包裹
上面的每一个处理器（参见[路由](routing.md#router-middleware)）。直接挂载在 Broker 作用域上的处理器
不在其中：

```rust
--8<-- "examples/middleware_router_scope.rs:router_scope"
```

这两个程序分别是
[`middleware_app_scope.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/middleware_app_scope.rs)
和
[`middleware_router_scope.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/middleware_router_scope.rs)；
其中 `LogLayer` 是下一节手写的层，内置的 `layers::TracingLayer` 挂载方式完全相同。

最先添加的层位于最外层。两个栈都是静态的：没有任何运行时分发开销，而类型会随着你每次调用 `layer`
不断增长。

!!! note "要作用到路由器里的处理器，需要 `BlanketLayer`"
    路由器隐藏了其处理器的具体类型，因此想要包裹它们的层（`include_router` 处的应用栈，或者
    `Router::layer`）必须实现 `BlanketLayer`，也就是一个能包裹任意处理器的泛型方法。内置的各个层
    都实现了它；对自定义的层来说，在它的 `Layer` 实现旁边多写几行即可（参见上面示例中的
    `LogLayer`）。

## 编写一个层

一个层把某个处理器变换成另一个处理器。实现 `Layer<H>` 即可：

```rust
use ruststream::runtime::{Context, Handler, HandlerResult, Layer};

--8<-- "examples/middleware.rs:layer_impl"
```

`Identity` 是什么都不做的层（全局栈的默认值），`Stack<Inner, Outer>` 则把两个层组合起来。这里的
`ctx` 就是处理器收到的同一个按投递创建的 [`Context`](context.md)，因此一个层可以在处理器读取之前，先
丰富[消息头工作副本](context.md#the-headers-working-copy)。

## 单个处理器的中间件

如果只想包裹某一个处理器，而不是整个应用，就用 `HandlerExt::with`：

<!-- inline-rust: HandlerExt::with API-shape fragment with placeholder handler and layer; the LogLayer impl it composes is compiled in middleware.rs:layer_impl, shown above -->
```rust
use ruststream::runtime::HandlerExt;

let handler = base_handler.with(LogLayer);
```

只有部分处理器需要某个层时，这才是合适的工具。它可以和全局栈组合使用。

## 为什么中间件默认是静态的

上面这些层都在编译期解析完毕：`with` / `layer` 会构建出一个具体的嵌套处理器类型
（`Logged<Typed<..>>`），而 `Handler::handle` 返回的 `impl Future` 类型也是已知的。编译器会把整条链
单态化成一个状态机，并跨层边界内联，因此静态的层不增加任何分发开销，也不产生任何分配，它是一个
零成本抽象。

动态（`dyn`）的链条则要放弃这些。`Handler::handle` 是 `async fn in trait`，它的 future 是一个匿名的
`impl Future`，而返回 `impl Trait` 的 trait 不是对象安全的。要把中间件放到 `dyn` 背后，就必须把
future 装箱（`Pin<Box<dyn Future>>`）：每条消息、每一层都要做一次堆分配，而且跨过 `dyn` 边界之后，
调用再也无法内联或特化。`dyn` + `async` 优化不掉，这份代价会落到每一个处理器头上，而链条几乎总是在
编译期就已经确定，所以默认才是静态的。

## 动态中间件

当链条要到运行时才能决定（层由配置开关控制，或者藏在 `dyn` 背后）时，只针对这些处理器显式启用
动态栈：`DynStack`、`DynMiddleware` 和 `Next`。`DynMiddleware` 采用 around/next 形式的签名：它先
检查输入和上下文，然后要么调用 `next.run(..)` 继续往下走，要么用自己的结果短路返回。因为它是
对象安全的，所以它显式地返回一个装箱的 future：

```rust
use std::future::Future;
use std::pin::Pin;

use ruststream::runtime::{Context, DynMiddleware, HandlerResult, Next};

--8<-- "examples/middleware.rs:dyn_middleware"
```

动态的只有那份*列表*。在运行时把它构建出来，冻结成一个 `DynStack`，得到的就是一个普通的静态
`Layer`，可以像手写的层那样用 `layer` 组合进应用栈。分发链的其余部分仍然是静态的；装箱的代价只在
该栈内部支付：

```rust
use std::sync::Arc;

use ruststream::memory::MemoryMessage;
use ruststream::runtime::DynStack;

--8<-- "examples/middleware.rs:dyn_stack"
```

完整的程序（其中链条由一个环境变量开关控制）见
[`examples/middleware.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/middleware.rs)。

`DynStack<I>` 对它所包裹的输入是泛型的。在应用栈里，它包裹的是整个负责解码的处理器，因此它构建在
Broker 的原始消息类型之上（上面的 `DynStack<MemoryMessage>`），并且运行在解码之前；像 `Audit` 这样
对 `I` 泛型的中间件在两个层次上都能工作。若想让它作用在解码后的值上，就构建一个 `DynStack<Order>`，
再用 `with` 把它套到内层的类型化处理器上（手工注册的写法）。同一个 `DynStack` 里的中间件按列表顺序
执行，最外层的先跑。每个动态层每次调用都要付出一个装箱 future 的代价，而静态层的代价是零，所以把
静态链条当作默认选择，只在运行时组合确实值回票价的地方才动用 `DynStack`。

## 发布侧的中间件

上面讲的中间件跑在消费路径上（进来的消息）。发布路径有它自己的管线，参见
[发布与回复](publishing.md#the-publish-pipeline)。

## 内置的层 { #built-in-layers }

- `layers::TracingLayer` 每条消息发出一个 tracing 事件（到达时 DEBUG，ack 时 INFO，nack 时 WARN）。
  要在控制台上渲染这些事件，需要启用 `logging` feature，参见[日志](logging.md)。
- `metrics` feature 提供了一个层，用于记录 Prometheus 计数器和一个耗时直方图，参见
  [指标](metrics.md)。
