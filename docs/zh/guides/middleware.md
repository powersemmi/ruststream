# 中间件

中间件为处理器包裹横切逻辑：链路追踪、指标、鉴权、重试。RustStream 有两个中间件作用域。它们都建立在
`Layer` trait 之上，作用在分发路径的不同位置。

## 中间件的作用域 { #middleware-scopes }

两个作用域嵌套在一起：外层是应用的栈，内层是路由器自己的栈。

**应用作用域。** `RustStream::layer` 给整个应用加一层，调用位置在 `with_broker` 之前。这一层包裹它
之后注册的每一个处理器：既有 Broker 作用域上的处理器，也有 `include_router` 挂载进来的路由器里的
处理器。顺序在编译期检查：第一次 `with_broker` 把构建器推进到没有 `layer`、`publish_layer` 和
`on_startup` 的阶段。包裹不到已注册处理器的层是编译错误，而不是悄无声息地什么都不做：

=== "宏"

    ```rust
    --8<-- "examples/middleware_app_scope.rs:app_scope"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/middleware_app_scope.rs:app_scope"
    ```

**路由器作用域。** `Router::layer` 让路由器拥有自己的栈。挂载该路由器时，栈包裹路由器上的每一个
处理器（参见[路由](routing.md#router-middleware)）。直接挂载在 Broker 作用域上的处理器不在其中：

=== "宏"

    ```rust
    --8<-- "examples/middleware_router_scope.rs:router_scope"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/middleware_router_scope.rs:router_scope"
    ```

两个完整的程序是
[`middleware_app_scope.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/middleware_app_scope.rs)
和
[`middleware_router_scope.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/middleware_router_scope.rs)。
`LogLayer` 是下一节手写的层。内置的 `layers::TracingLayer` 挂载方式相同。

最先添加的层在最外层。两个栈都是静态的：运行时分发不花代价，栈的类型随每次 `layer` 调用增长。

!!! note "要作用到路由器里的处理器，需要 `BlanketLayer`"
    包裹路由器里处理器的层（`include_router` 处的应用栈，或者 `Router::layer`）必须实现
    `BlanketLayer`：一个能包裹任意处理器的泛型方法。内置的层都实现了它。自定义的层只需在它的
    `Layer` 实现旁边多写几行（参见上面示例中的 `LogLayer`）。

## 编写一个层

层把一个处理器变换成另一个处理器。实现 `Layer<H>`：

```rust
use ruststream::runtime::{Context, Handler, HandlerOutcome, Layer};

--8<-- "examples/middleware.rs:layer_impl"
```

`Identity` 是什么都不做的层，也是全局栈的默认值。`Stack<Inner, Outer>` 把两个层连接起来。`ctx` 是按
投递创建的 [`Context`](context.md)，处理器收到的就是同一个。因此层可以在处理器读取之前，往
[消息头工作副本](context.md#the-headers-working-copy)里写入值。

## 单次注册的中间件

层可以只加在一次注册上，而不是整个应用。路由器上 `include` 之后的 `.layer(..)` 属于这次注册，就像
链条上其他步骤都属于写在它们前面的位置。

<!-- inline-rust: the call shape; the LogLayer impl it composes is compiled in middleware.rs:layer_impl, shown above -->
```rust
let router = Router::<MemoryBroker>::new().include(handle).layer(LogLayer);
```

只有部分处理器需要某个层时，一次注册就是合适的做法。这里也是没有实现 `BlanketLayer` 的层唯一能放的
位置：此处的处理器类型还是具体的，普通的 `Layer<H>` 就够了。该层在解码步骤之外，看到的是 Broker 的
原始消息。它可以和应用栈、路由器栈组合使用。

## 一个层的代价

静态的层在热路径上不花代价。动态的层每条消息都有开销，因此它们用在链条到运行时才组装的场合。

## 动态中间件

链条的组成有时到运行时才确定：层由配置开关控制，或者藏在 `dyn` 背后。这类处理器用动态栈：
`DynStack`、`DynMiddleware` 和 `Next`。`DynMiddleware` 拿到输入和上下文，然后要么调用
`next.run(..)` 继续链条，要么用自己的结果中断链条。返回类型由你显式写出：

```rust
use std::future::Future;
use std::pin::Pin;

use ruststream::runtime::{Context, DynMiddleware, HandlerOutcome, Next};

--8<-- "examples/middleware.rs:dyn_middleware"
```

动态的只有那份*列表*。在运行时把它组装好交给 `DynStack`，结果是一个普通的静态 `Layer`，它绑定在
单一输入类型上。因此它通过 `.layer(..)` 加在一次注册上，而不是放进只接受 `BlanketLayer` 的应用栈。
分发链的其余部分仍然是静态的，开销只出现在该栈本身：

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

完整的程序见
[`examples/middleware.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/middleware.rs)，
其中的链条由一个环境变量切换。

`DynStack<I>` 对它包裹的输入是泛型的。加在一次注册上时，它包裹整个解码处理器，因此建立在 Broker 的
原始消息类型之上（上面是 `DynStack<MemoryMessage>`），运行在解码之前。对 `I` 泛型的中间件（例如
`Audit`）适用于任意输入类型。同一个 `DynStack` 里的中间件按列表顺序执行，最外层的先跑。

## 发布侧的中间件 { #publish-side-middleware }

上面的中间件都跑在消费路径上，处理进来的消息。发布路径有自己的管线，参见
[发布与回复](publishing.md#the-publish-pipeline)。

## 内置的层 { #built-in-layers }

- `layers::TracingLayer` 为每条消息发出一个 tracing 事件：到达时 DEBUG，ack 时 INFO，nack 时 WARN。
  要在控制台上看到这些事件，启用 `logging` feature，参见[日志](logging.md)。
- `metrics` feature 提供的层记录 Prometheus 计数器和一个耗时直方图，参见[指标](metrics.md)。
