# 上下文与状态

处理器除了载荷之外能够拿到的一切，都通过两个生命周期不同的对象传入：

| 层级 | 类型 | 存活范围 | 承载内容 |
|---|---|---|---|
| 应用 | 状态类型 `S` | 整个服务 | 共享资源：连接池、客户端、配置 |
| 投递 | `Context<'_, C, S>` | 一条消息 | 通道名、一份消息头的工作副本、Broker 按投递提供的类型化上下文 `C`（按 key 读取），以及类型化的共享状态 `S` |

状态在启动时生成一次，是一个由你自己选定的、单一的类型化值。`Context` 则每次投递都重新构造，并以
`&mut` 的形式穿过中间件链一路传到处理器，因此中间件和处理器观察到（并且可以丰富）的是同一份按消息
的视图。

## 应用层：类型化状态 { #application-level-typed-state }

共享的应用状态是一个类型化的值 `S`（你自己定义的 struct；服务不需要状态时就是 `()`）。它由
`on_startup` 钩子产出：钩子返回的值就是状态，应用的状态类型也就此确定：

```rust
--8<-- "examples/context.rs:app"
```

编译期就会检查状态类型：读取状态的 `#[subscriber]` 处理器把它写成 `Context` 的第三个泛型参数
（`Context<'_, C, S>`），运行时只允许这样的处理器挂载到状态类型与之匹配的应用上。没有写出状态类型
的处理器对状态是泛型的，因此可以挂到任何应用上。`publish(..)` 形式的处理器遵循同样的规则，但有一处
不同：完全不关心状态的处理器可以整个省略 `Context` 参数，这样它照样能挂到带状态的应用上；而声明了
`Context` 却没有写出状态类型的处理器，会把状态钉死成 `()`，所以要把这样的处理器挂到带状态的应用上，
就必须显式写出应用的状态类型。

处理器用 `ctx.state()` 借用状态，拿到的是 `&S`，也就是类型化的状态本身：没有查表，没有 `Option`，
也没有向下转型。服务跑起来之后，状态放在 `Arc` 后面共享，因此处理器拿到的是廉价的共享引用而不是
副本；如果某个共享值必须在运行时改变，内部可变性（`AtomicU64`、由互斥量保护的 map）才是对应的工具。
如果数据只属于一条消息而不是整个服务，改用[按投递的上下文](#per-delivery-context)。启动钩子的
契约见 [Lifespan](lifespan.md)。

```rust
--8<-- "examples/context.rs:state"
```

## 注入依赖：提取器参数 { #injecting-dependencies-extractor-parameters }

通过 `ctx.state().field` 去够依赖当然一直可用，但处理器也可以直接把依赖作为参数收下。在消息参数
（以及可选的 `&mut Context`）之后，凡是类型实现了 `FromContext` 的处理器参数，都是一个**提取器**：
运行时会在函数体运行之前从这次投递中解析出它；一旦解析失败，消息就按拒绝值携带的 `HandlerResult`
结算，函数体根本不会运行。

要注入状态中的某一部分，在状态类型上 derive `FromRef`，然后在处理器里接收 `State<T>` 即可，不必手写
提取器实现。`State<T>` 对任意字段类型都能解析（`T: FromRef<S>`），包括来自其他 crate 的类型（某个
Broker 的发布者、一个客户端连接池），而这些类型在孤儿规则下是无法靠逐字段手写实现覆盖的：

```rust
--8<-- "examples/from_context.rs:state"
```

处理器直接接收 `State<FieldType>`，不必再通过 `ctx.state()` 绕一道：

```rust
--8<-- "examples/from_context.rs:handler"
```

如果某个字段不该参与注入，或者它的类型已由另一个字段占用，就用 `#[from_ref(skip)]` 退出注入；
两个字段不得共用同一个类型，因为按类型注入会产生歧义。若要写一个不只是读状态的自定义提取器，比如
一个会拒绝消息的鉴权守卫、或者一个按请求解析的解析器，就直接实现 `FromContext`：它借用
`&mut Context`，因此可以读取消息头、Broker 字段、或者某个中间件留下的临时值，并通过返回 `Rejection`
来结算这次投递。

## 投递层：`Context`

`#[subscriber]` 处理器通过在载荷之后声明第二个参数来显式启用它；处理器只需要消息本身时就省略该
参数。类型由宏自己解析，因此只要 `Context` 仅出现在处理器签名里，就不需要 import：

```rust
--8<-- "examples/context.rs:handler"
```

上下文对外暴露的内容：

| 方法 | 返回 | 用途 |
|---|---|---|
| `name()` | `&str` | 消息到达时所在的通道 / subject |
| `headers()` | `&Headers` | 消息头的工作副本 |
| `headers_mut()` | `&mut Headers` | 同一份副本，供中间件写入 |
| `state()` | `&S` | 类型化的共享应用状态，直接借用 |
| `context(KEY)` | `KEY::Value` | 按编译期 key 读取的 [Broker 字段](#per-delivery-context) |
| `set(KEY, v)` | `()` | 写入按投递的[临时值](#per-delivery-context)（供中间件使用） |
| `after(outcome).then(fut)` | `()` | 按结算结果过滤的[结算后钩子](#post-settle-hooks) |
| `after_ack(fut)` / `after_settle(fut)` | `()` | 结算后钩子的语法糖（ack 之后 / 任何结算之后） |

闭包形式的处理器（手写的 `typed(codec, |msg, ctx| ...)`）总是把上下文作为第二个参数接收。

## 按投递的上下文 { #per-delivery-context }

除了共享的应用状态之外，上下文还携带 Broker 按投递提供的类型化上下文，它通过**编译期 key** 读取，
没有哈希、没有装箱、也没有向下转型。key 是 Broker 导出的零大小选择器；`ctx.context(KEY)` 会把它
解析成对上下文的一次直接字段读取，于是处理器可以读到原生的投递元数据（一个流 id、一个偏移量、一个
投递句柄），而不必让 Broker 先把它序列化进只能装字节的消息头。key 只为真正带有对应字段的上下文类型
实现 `Field`，因此用了不适用的 key 是一个编译错误，而不是运行时读不到值。

```rust
--8<-- "examples/context_field.rs:field"
```

上下文类型由 `BuildContext` 从消息构造，运行时每次投递调用一次；没有按投递字段的 Broker 使用默认的
`()`（因此，如果一个 `#[subscriber]` 处理器既没有写出上下文类型，也没有接收
[`Ctx` 提取器](#context-fields-as-parameters)，它看到的就是 `Context<'_>`）。中间件也可以把一个
类型化的临时值带给下游的处理器：借助可写的 key（`FieldMut`），某一层可以 `ctx.set(KEY, value)`，
处理器再用 `ctx.context(KEY)` 把它取回来。这样的值可以是一个关联 id，也可以是某一层解析出来的
已认证用户，全程不必序列化进消息头。上下文每次投递都重新构造，所以一次投递的值绝不会泄漏到下一次。

## 把上下文字段当作参数 { #context-fields-as-parameters }

字段也可以像 `State<T>` 注入状态组件那样，直接作为处理器的参数到达：`Ctx<K>` 提取器绑定的就是 key
`K` 读到的值。该 key 实现的是 `ContextField`，一个 `Field` 风格的 trait，只是它还额外写出了自己
读取的上下文类型，并产出一个拥有所有权的值。于是处理器完全不需要 `&mut Context` 参数：
`#[subscriber]` 宏会从签名里第一个 `Ctx` key 推出该订阅的上下文类型。

```rust
--8<-- "examples/ctx_extractor.rs:key"
```

```rust
--8<-- "examples/ctx_extractor.rs:handler"
```

有三点需要知道：

- 值是拥有所有权的（`ContextField::Value` 是 `'static`）：提取器的值在处理器函数体运行之前就已绑定，
  因此从上下文里借用不是一个选项。如果 key 产出的是借用值（比如以 `&str` 形式给出的名字），
  只要声明了 ctx 参数，仍然可以通过 `ctx.context(KEY)` 读取。
- 如果签名里同时还有 `&mut Context<'_, C>` 参数，那么每个 `Ctx` key 都必须读取同一个 `C`；这一点由
  编译器通过提取器的 trait 约束来保证。
- 该推导是语法层面的：宏识别的是字面上的 `Ctx<K>` 形状（任何以 `Ctx` 结尾、带一个类型参数的路径）。
  类型别名会把它藏起来，此时上下文类型退回到 `()`。

## 消息头的工作副本 { #the-headers-working-copy }

`ctx.headers()` 拿到的并不是 Broker 消息本身：每次投递都会把收到的消息头克隆成一份工作副本，放在
上下文里。这让它成了分发链上的一块草稿板：链上更靠前的中间件可以用 `headers_mut()` 往上面写值，处理器
读到的就是丰富之后的结果：

```rust
--8<-- "examples/context.rs:enrich"
```

全局挂载之后，这一层会在每个处理器之前运行，因此上面的 `handle` 总能找到 `x-request-id`：

```rust
--8<-- "examples/context.rs:app"
```

有两条边界要记住：

- 修改只停留在这次投递之内：Broker 消息本身以及其他订阅者的投递都不受影响。
- 发出去的消息不会继承这份副本。回复和手动发布都从全新的消息头开始；要给出站消息附加元数据，改在
  [发布管线](publishing.md#the-publish-pipeline)里做（用 `PublishTransform` 或 `PublishLayer`）。

## 在处理器中发布

要在处理器内部发布消息（`publish(..)` 这种回复形式之外的场景），不要把发布者塞进状态里，而是用 `Out`
把它作为处理器参数收下：`Out(out): Out<impl Publisher>` 这种写法会把 `out` 绑定成函数体内一个可用的
发布者。发布策略在挂载处理器的地方附加，具体的发布者类型由它推断，运行时会在 Broker 连接之后再把
两者配对，因此处理器永远不会拿到一个“尚未连接”的发布者，状态里也不会混进绑定连接的值。完整写法及其
代码片段见[在处理器内部发布](publishing.md#publishing-from-inside-a-handler)。

## 结算后钩子 { #post-settle-hooks }

有时处理器需要某个副作用在消息**结算之后**才触发，比如一条不关键的通知、一段耗时的后续工作、一次
缓存预热，同时又不希望它左右 ack 的决定，也不希望它影响重新投递。这类副作用注册在上下文上：

```rust
--8<-- "examples/context.rs:handler"
```

上面的处理器以 `ctx.after_ack(..)` 结尾：这段后续任务只有在 Broker 对消息完成 ack 之后才会运行，并且
运行在投递路径之外，因此它绝不会拖慢 ack，也绝不会拖慢下一次投递。

三种写法，彼此叠加：

- `ctx.after(outcome).then(fut)`，只有消息按 `outcome` 结算时才运行，匹配是**按种类**进行的。四种
  种类彼此不同：`Ack`、`drop()`（nack，不重新入队）、`retry()`（nack，重新入队）以及 `retry_after()`
  （无论延迟多久都算匹配）。drop 和 retry 是两套不同的机制，因此挂在 `drop()` 上的钩子不会在
  `retry()` 结算时触发，反之亦然。
- `ctx.after_ack(fut)`，是 `ctx.after(HandlerResult::Ack).then(fut)` 的语法糖。
- `ctx.after_settle(fut)`，只要消息结算就运行，无论结果如何。

处理器也可以通过返回值挂上后续任务：任何结算结果都能用 `.and_after(fut)` 转成一个 `Settle`，批量
处理器正是这样为每个元素分别挂上后续任务的。这种写法见[结算后的后续任务](subscribers.md#post-settle-continuations)；
下面讲的语义对两种写法都适用。

多次注册会累加，所有匹配的钩子都会运行，运行在投递路径之外一个受跟踪的任务集合里。语义是**至多一次**：
任何钩子运行之前消息就已经结算完毕，所以钩子里发生 panic、或者钩子随进程崩溃一起丢失，都绝不会引发
重新投递。切勿把“一旦丢失就必须重新投递消息”的工作放进钩子；改用结算结果来表达，交给 Broker
去重试。优雅关闭会把正在执行的钩子排空（受 `shutdown_timeout` 限制）；关闭一旦中止，则可能直接丢弃它们。

在批量路径上，一个 `Context` 对应的是一**批**消息，因此钩子在整批结算之后才运行。由于一批消息的结算
结果是逐元素的，按结果过滤在这里没有明确含义：只有 `after_settle` 钩子会触发（在批量路径上，运行时会
忽略带过滤的 `after(..)` / `after_ack` 形式）。

## 中间件中的上下文

每一种中间件形式拿到的，都是处理器随后会看到的那同一个 `&mut Context`，前面那种“丰富消息头”的写法
正因如此才成立：

- 静态层的 `Handler::handle(&self, msg, ctx)`，就像上面的例子那样。
- 动态的 `DynMiddleware::handle(&self, input, ctx, next)`，先检查或丰富，再 `next.run(input, ctx)`。

中间件本身的各种形式见[中间件](middleware.md)。本页对应的完整程序是
[`examples/context.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/context.rs)。
