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

=== "宏"

    ```rust
    --8<-- "examples/middleware_app_scope.rs:app_scope"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/middleware_app_scope.rs:app_scope"
    ```

**路由器作用域。** 用 `Router::layer` 给某个路由器配置它自己的中间件，它会在挂载该路由器时包裹
上面的每一个处理器（参见[路由](routing.md#router-middleware)）。直接挂载在 Broker 作用域上的处理器
不在其中：

=== "宏"

    ```rust
    --8<-- "examples/middleware_router_scope.rs:router_scope"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/middleware_router_scope.rs:router_scope"
    ```

这两个程序分别是
[`middleware_app_scope.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/middleware_app_scope.rs)
和
[`middleware_router_scope.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/middleware_router_scope.rs)；
其中 `LogLayer` 是下一节手写的层，内置的 `layers::TracingLayer` 挂载方式完全相同。

最先添加的层位于最外层。两个栈都是静态的：没有任何运行时分发开销，而类型会随着你每次调用 `layer`
不断增长。

!!! note "要作用到路由器里的处理器，需要 `BlanketLayer`"
    包裹路由器里处理器的层（`include_router` 处的应用栈，或者 `Router::layer`）必须实现
    `BlanketLayer`，也就是一个能包裹任意处理器的泛型方法。内置的各个层都实现了它；对自定义的层
    来说，在它的 `Layer` 实现旁边多写几行即可（参见上面示例中的 `LogLayer`）。

## 编写一个层

一个层把某个处理器变换成另一个处理器。实现 `Layer<H>` 即可：

```rust
use ruststream::runtime::{Context, Handler, HandlerOutcome, Layer};

--8<-- "examples/middleware.rs:layer_impl"
```

`Identity` 是什么都不做的层（全局栈的默认值），`Stack<Inner, Outer>` 则把两个层组合起来。这里的
`ctx` 就是处理器收到的同一个按投递创建的 [`Context`](context.md)，因此一个层可以在处理器读取之前，先
丰富[消息头工作副本](context.md#the-headers-working-copy)。

## 单次注册的中间件

如果只想包裹某一次注册，而不是整个应用：路由器上 `include` 之后的 `.layer(..)` 就跟着那次注册走 -
和链上其他步骤跟着它前面那个位置走完全一样。

<!-- inline-rust: the call shape; the LogLayer impl it composes is compiled in middleware.rs:layer_impl, shown above -->
```rust
let router = Router::<MemoryBroker>::new().include(handle).layer(LogLayer);
```

只有部分处理器需要某个层时，这才是合适的工具；而且这是唯一能放下非 `BlanketLayer` 的层的位置：这里
注册的处理器类型还是具体的，所以普通的 `Layer<H>` 就够了。它位于解码步骤之外，因此能看到原始投递，
并且可以和应用栈、路由器栈组合使用。

## 一个层的代价

静态的层在热路径上不花任何代价。动态的层每条消息都要付出代价，所以只在链条要到运行时才组装出来时
才用它们。

## 动态中间件

当链条要到运行时才能决定（层由配置开关控制，或者藏在 `dyn` 背后）时，只针对这些处理器显式启用
动态栈：`DynStack`、`DynMiddleware` 和 `Next`。`DynMiddleware` 采用 around/next 形式的签名：它先
检查输入和上下文，然后要么调用 `next.run(..)` 继续往下走，要么用自己的结果短路返回。它把自己的
返回类型显式写出来：

```rust
use std::future::Future;
use std::pin::Pin;

use ruststream::runtime::{Context, DynMiddleware, HandlerOutcome, Next};

--8<-- "examples/middleware.rs:dyn_middleware"
```

动态的只有那份*列表*。在运行时把它构建出来，冻结成一个 `DynStack`，得到的就是一个普通的静态
`Layer` - 只不过它绑定在单一输入类型上，所以要用 `.layer(..)` 跟在一次注册后面，而不是放进只接受
blanket 层的应用栈。分发链的其余部分仍然是静态的；只有该栈自身付出代价：

=== "宏"

    ```rust
    use std::sync::Arc;

    use ruststream::memory::MemoryMessage;
    use ruststream::runtime::DynStack;

    --8<-- "examples/middleware.rs:dyn_stack"
    ```

=== "手写"

    ```rust
    use std::sync::Arc;

    use ruststream::memory::{MemoryBroker, MemoryMessage};
    use ruststream::prelude::*;
    use ruststream::runtime::{DynMiddleware, DynStack};

    --8<-- "examples/manual/middleware.rs:dyn_stack"
    ```

完整的程序（其中链条由一个环境变量开关控制）见
[`examples/middleware.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/middleware.rs)。

`DynStack<I>` 对它所包裹的输入是泛型的。跟在一次注册后面时，它包裹的是整个负责解码的处理器，因此它
构建在 Broker 的原始消息类型之上（上面的 `DynStack<MemoryMessage>`），并且运行在解码之前；像 `Audit`
这样对 `I` 泛型的中间件在两个层次上都能工作。同一个 `DynStack` 里的中间件按列表顺序执行，最外层的
先跑。把静态链条当作默认选择，只在运行时组合确实值回票价的地方才动用 `DynStack`。

## 发布侧的中间件 { #publish-side-middleware }

上面讲的中间件跑在消费路径上（进来的消息）。发布路径有它自己的管线，参见
[发布与回复](publishing.md#the-publish-pipeline)。

## 内置的层 { #built-in-layers }

- `layers::TracingLayer` 每条消息发出一个 tracing 事件（到达时 DEBUG，ack 时 INFO，nack 时 WARN）。
  要在控制台上渲染这些事件，需要启用 `logging` feature，参见[日志](logging.md)。
- `metrics` feature 提供了一个层，用于记录 Prometheus 计数器和一个耗时直方图，参见
  [指标](metrics.md)。
