# 上下文与状态

处理器除了载荷之外拿到的一切，都通过两个生命周期不同的对象传入：

| 层级 | 类型 | 存活范围 | 承载内容 |
|---|---|---|---|
| 应用 | 状态类型 `S` | 整个服务 | 共享资源：连接池、客户端、配置 |
| 投递 | `Context<'_, C, S>` | 一条消息 | 通道名、一份消息头的工作副本、Broker 在这次投递上的类型化上下文 `C`（用键读取），以及类型化的共享状态 `S` |

状态在启动时创建一次，类型由你自己选定。`Context` 每次投递都重新构造，并以 `&mut` 的形式沿中间件链
传给处理器。中间件和处理器操作的是同一个对象，因此处理器看得到中间件写进去的一切。

## 应用层：类型化状态 { #application-level-typed-state }

应用的共享状态是一个类型化的值 `S`：你自己定义的结构体，或者在服务不需要状态时用 `()`。它由
`on_startup` 钩子创建，钩子返回值的类型就是应用的状态类型。

=== "宏"

    ```rust
    --8<-- "examples/context.rs:app"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/context.rs:app"
    ```

状态类型在编译期检查。读取状态的处理器把状态类型写成 `Context` 的最后一个类型参数
（`Context<'_, C, S>`），这样的处理器只挂载到状态类型相同的应用上。不写出状态类型的处理器对状态是
泛型的，可以挂载到任何应用上。

带 `publish(..)` 的处理器规则相同，另有一处特别之处。完全不读状态的处理器省略整个 `Context` 参数，
照样能挂载到带状态的应用上。声明了 `Context` 却不写状态类型的处理器，把状态固定为 `()`；要把这样的
处理器挂载到带状态的应用上，就显式写出应用的状态类型。

处理器通过 `ctx.state()` 读取状态，该方法返回 `&S`。状态处于共享所有权之下，因此所有处理器操作的都
是同一个值。运行期间必须改变的值，用内部可变性来保存：`AtomicU64`，或者互斥量保护的映射。

只属于一条消息而不属于整个服务的数据，交给[按投递的上下文](#per-delivery-context)。启动钩子见
[生命周期](lifespan.md)。

```rust
--8<-- "examples/context.rs:state"
```

## 注入依赖：提取器参数 { #injecting-dependencies-extractor-parameters }

依赖始终可以通过 `ctx.state().field` 取到，处理器也可以把它作为参数取进来。

**提取器**是处理器的一个参数，它的类型实现了 `FromContext`。提取器排在消息参数和可选的
`&mut Context` 参数之后。在函数体执行之前，运行时就从这次投递中解析出提取器的值。返回拒绝的提取器
用自己给出的结果结算这次投递，函数体不执行。

要注入状态中的某一部分，在状态类型上派生 `FromRef`，并在处理器里声明 `State<T>` 参数。`State<T>`
对任意字段类型都能解析（`T: FromRef<S>`），包括来自其他 crate 的类型：某个 Broker 的发布者、一个
客户端连接池：

=== "宏"

    ```rust
    --8<-- "examples/from_context.rs:state"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/from_context.rs:state"
    ```

处理器把 `State<FieldType>` 作为参数取进来：

=== "宏"

    ```rust
    --8<-- "examples/from_context.rs:handler"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/from_context.rs:handler"
    ```

状态的两个字段不能是同一个类型：注入按类型进行。不参与注入的字段，或者类型和另一个字段相同的字段，
你可以用 `#[from_ref(skip)]` 排除。

不只是读状态的提取器，你自己用 `FromContext` 实现：比如拒绝投递的访问检查，或者在一次请求范围内解析
出来的依赖。这样的提取器拿到 `&mut Context`，可以读取消息头、Broker 字段，或者中间件留下的临时值。
要结算这次投递，它返回 `Rejection`。

## 投递层：`Context`

`#[subscriber]` 处理器把上下文声明成载荷之后的第二个参数；只需要消息本身的处理器省略这个参数。类型
由宏自己填入，因此只要 `Context` 只出现在处理器签名里，就不必导入它：

=== "宏"

    ```rust
    --8<-- "examples/context.rs:handler"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/context.rs:handler"
    ```

上下文对外暴露的内容：

| 方法 | 返回 | 用途 |
|---|---|---|
| `name()` | `&str` | 消息到达时所在的通道 / subject |
| `headers()` | `&HeaderMap` | 消息头的工作副本 |
| `headers_mut()` | `&mut HeaderMap` | 同一份副本，供中间件写入 |
| `state()` | `&S` | 类型化的共享应用状态 |
| `context(KEY)` | `KEY::Value` | 按编译期键读取的 [Broker 字段](#per-delivery-context) |
| `set(KEY, v)` | `()` | 写入按投递的[临时值](#per-delivery-context)（供中间件使用） |
| `after(outcome).then(fut)` | `()` | 按结算结果过滤的[结算后钩子](#post-settle-hooks) |
| `after_ack(fut)` / `after_settle(fut)` | `()` | 结算后钩子的语法糖（ack 之后 / 任何结算之后） |

## 按投递的上下文 { #per-delivery-context }

除了共享的应用状态，上下文还保存 Broker 在这次投递上的类型化上下文：投递自身的元数据，比如流 id、
偏移量、投递句柄。

处理器按**编译期键**读取它们。键是 Broker 导出的选择器，`ctx.context(KEY)` 直接从上下文里取出字段的
值，不经过字节形式的消息头。这样的读取在投递路径上不花任何代价。该订阅的 Broker 没有的键是编译错误。

```rust
--8<-- "examples/context_field.rs:field"
```

没有按投递字段的 Broker，上下文类型是默认的 `()`：既不写出上下文类型，也不接收
[`Ctx` 提取器](#context-fields-as-parameters)的处理器，看到的就是 `Context<'_>`。

中间件可以把类型化的临时值交给链上更靠后的处理器：一个关联 id，或者某一层认证出来的用户。通过可写
的键（`FieldMut`），该层调用 `ctx.set(KEY, value)`，处理器再用 `ctx.context(KEY)` 读回来。下一次投递
看不到这些值。

[批量处理器](subscribers.md#batch-subscribers)每个批次拿到一个上下文，里面只有 Broker 在*订阅*这一级
的字段：定位句柄、流的名字。单次投递的数据不在其中，因为一个批次横跨多次投递，位置和消息头改为存在
批次的元素上。

批量函数体把这个类型写成自己的上下文类型（内存 Broker 上是
`ctx: &mut Context<'_, MemoryBatchContext>`），并用 `ctx.context(..)` 读取字段。投递上下文和批量上下文
是不同的类型，所以批量函数体写投递上下文编译不过。没有订阅级字段的 Broker，批量上下文的类型是 `()`。

## 把上下文字段当作参数 { #context-fields-as-parameters }

字段也可以直接作为处理器的参数传入，就像 `State<T>` 注入状态的一部分：`Ctx<K>` 提取器绑定的是键 `K`
读到的值。这时不需要 `&mut Context` 参数，`#[subscriber]` 宏从签名里第一个 `Ctx` 键推导出这条订阅的
上下文类型。

```rust
--8<-- "examples/ctx_extractor.rs:key"
```

=== "宏"

    ```rust
    --8<-- "examples/ctx_extractor.rs:handler"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/ctx_extractor.rs:handler"
    ```

这种写法有三点特别之处：

- 提取器给出的是拥有所有权的值：它在处理器函数体运行之前绑定，因此无法从上下文里借用。产出借用值的
  键（比如以 `&str` 形式给出的名字），在声明了 `ctx` 参数时仍然可以用 `ctx.context(KEY)` 读取。
- 签名里同时还有 `&mut Context<'_, C>` 参数时，每个 `Ctx` 键都必须读取同一个 `C`。
- 这个推导是语法层面的：宏识别字面上的 `Ctx<K>` 形状，也就是任何以 `Ctx` 结尾、带一个类型参数的
  路径。在类型别名背后，宏看不到这个形状，上下文类型于是回退为 `()`。

## 消息头的工作副本 { #the-headers-working-copy }

每次投递都会把收到的消息头复制到上下文里，`ctx.headers()` 返回的就是这份副本，而不是 Broker 消息本身
的消息头。这份副本是整条分发链的草稿：链上更靠前的中间件可以用 `headers_mut()` 往里写值，处理器读到
写入后的结果：

```rust
--8<-- "examples/context.rs:enrich"
```

全局挂载的层在每个处理器之前运行，因此上面的 `handle` 总能找到 `x-request-id`：

=== "宏"

    ```rust
    --8<-- "examples/context.rs:app"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/context.rs:app"
    ```

这份副本有两条边界：

- 修改只留在这次投递之内：Broker 的消息和其他订阅者的投递都看不到。
- 出站消息不继承这份副本：回复和手动发布都从空的消息头开始。出站消息的元数据在
  [发布管线](publishing.md#the-publish-pipeline)里设置，用 `PublishTransform` 或 `PublishLayer`。

## 在处理器中发布

除了 `publish(..)` 这种回复形式，处理器还可以通过 `Out` 槽位发布。发布者不放进状态，而是作为处理器
参数取进来：`Out(out): Out<impl Publisher>` 把 `out` 绑定成函数体内一个活的发布者。

你在挂载处理器的地方指定策略。具体的发布者类型由策略推断出来，策略也在已连接的 Broker 上实例化
发布者。

从这个槽位出去的消息，走的是与回复相同的[发布管线](publishing.md#the-publish-pipeline)：先是这个
槽位自己的 `.out(marker, policy).transform(..)` 步骤，再是应用全局的 `publish_layer` 链。完整写法和
代码片段见
[在处理器内部发布](publishing.md#publishing-from-inside-a-handler)。

## 结算后钩子 { #post-settle-hooks }

在上下文上，你可以注册一个在消息**结算之后**才运行的副作用：一条不关键的通知、一段耗时的后续工作、
一次缓存预热。这样的副作用不左右 ack 的决定，也不影响重新投递。

=== "宏"

    ```rust
    --8<-- "examples/context.rs:handler"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/context.rs:handler"
    ```

上面的处理器以 `ctx.after_ack(..)` 结尾。这段后续任务只在 Broker 对消息 ack 之后运行，并且运行在投递
路径之外，因此不会拖慢 ack，也不会拖慢下一次投递。

三种写法，可以叠加：

- `ctx.after(outcome).then(fut)`：只有消息按 `outcome` 结算时才运行，结果**按种类**匹配。种类有四
  种：`ack()`、`drop()`（nack，不重新入队）、`retry()`（nack，重新入队）和 `retry_after()`（无论延迟
  多久都算匹配）。挂在 `drop()` 上的钩子不会在 `retry()` 结算时运行，反过来也一样。
- `ctx.after_ack(fut)`：`ctx.after(HandlerOutcome::ack()).then(fut)` 的语法糖。
- `ctx.after_settle(fut)`：消息结算之后运行，结果是什么都一样。

后续任务也可以挂在返回值上：任何结算结果都能用 `.and_after(fut)` 带上一个后续任务，批量处理器正是
这样为每个元素给出后续任务的。这种写法见[结算后的后续任务](subscribers.md#post-settle-continuations)；
下面讲的语义对两种写法都适用。

多次注册会累加，所有匹配的钩子都会运行，运行在投递路径之外一个受跟踪的任务集合里。

钩子的语义是**至多一次**：任何钩子运行之前消息就已经结算，因此钩子里的 panic，或者随进程崩溃一起
丢失的钩子，都不会引起重新投递。切勿把一旦丢失就必须重新投递消息的工作放进钩子；用合适的结果结算这
次投递，交给 Broker 重试。

优雅关闭会在 `shutdown_timeout` 之内等待未完成的钩子，中止的关闭可能丢掉它们。

在批量路径上，一个 `Context` 对应一个*批次*，因此钩子在整个批次结算之后运行。批次的结算结果是逐元素
的，按结果过滤在这里没有定义：只有 `after_settle` 钩子会运行，批次上的 `after(..)` 和 `after_ack`
不生效。

## 中间件中的上下文

任何一种中间件形式拿到的 `&mut Context`，都和处理器看到的是同一个：

- 静态层通过 `Handler::handle(&self, msg, ctx)` 拿到它，就像上面的例子。
- 动态中间件通过 `DynMiddleware::handle(&self, input, ctx, next)` 拿到它：先读取或补充上下文，再调用
  `next.run(input, ctx)`。

中间件本身的各种形式见[中间件](middleware.md)。本页对应的完整程序是
[`examples/context.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/context.rs)。
