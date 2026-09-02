# 订阅者

一个订阅者把一个处理器绑定到一条订阅上。`#[subscriber]` 宏是声明订阅者最省事的方式；本指南讲的是
处理器的契约、宏的几种写法和处理器的挂载方式。把处理器按模块分组见[路由](routing.md)，载荷
如何解码见[编解码器](codecs.md)。

## 处理器契约

处理器是一个 `async fn`，它的第一个参数是解码后载荷的引用：

=== "宏"

    ```rust
    use ruststream::runtime::HandlerOutcome;
    use ruststream::subscriber;

    --8<-- "examples/subscribers.rs:contract"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/subscribers.rs:contract"
    ```

宏会把该函数变成一个与它同名的值（这里是 `handle`），该值实现了挂载契约。把它传给 `include`。

### 接受上下文

声明可选的第二个参数 `&mut Context`，就能读取消息头、订阅名和共享状态，也可以在处理器内部发布
消息：

=== "宏"

    ```rust
    --8<-- "examples/subscribers.rs:context"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/subscribers.rs:context"
    ```

宏会自己解析上下文的类型，因此只要 `Context` 仅出现在 `#[subscriber]` 的签名里，就不需要导入该
名字。上下文的完整能力（消息头的工作副本、状态访问、Broker 的每次投递字段）见
[上下文与状态](context.md)。

### 提取器参数

在消息和可选的 `&mut Context` 之后，再出现的任何参数都是**提取器**：运行时会在函数体执行之前从
这次投递中解析出它，而提取失败会直接结算这次投递，函数体不会执行。可以出现四种：

- `State<T>`，应用状态中的一个字段（在状态类型上派生 `FromRef`）。
- `Ctx<K>`，Broker 的某个每次投递字段，按它的键读取。
- `Headers<T>`，把这次投递的消息头解析成一个类型化的契约；违反契约时按
  `on_failure(decode = ..)` 策略结算（见[类型化消息头](headers.md)）。
- 任何实现了 `FromContext` 的类型，也就是自定义提取器（鉴权守卫、请求作用域的解析器）。

具体机制见[注入依赖](context.md#injecting-dependencies-extractor-parameters)和
[把上下文字段作为参数](context.md#context-fields-as-parameters)。

还有一种参数形态不是提取器，而是**注入**：`Out(out): Out<impl Publisher>` 接收一个活的发布者。
运行时依据挂载点附加的策略（`b.include(handler).publisher(..)`，或者按具名槽位使用
`.out(marker, ..)`）配对出该发布者；具体的发布者类型永远不会出现在签名里。可选的第三个位置声明该
处理器会发布的消息集合，即 `Out<impl Publisher, Marker, (A, B)>`，用于启用字典驱动的类型化发布路径
（[类型化消息头](headers.md)）。参见
[在处理器内部发布](publishing.md#publishing-from-inside-a-handler)。

### 确认（ack） { #acking }

返回类型可以是任何能转换成 [`HandlerOutcome`] 的东西（结算的单位：给 Broker 的状态，外加一个可选
的结算后续任务）：

| 返回值 | 效果 |
|---|---|
| `HandlerOutcome::ack()` | 确认，即 ack；Broker 移除这条消息 |
| `HandlerOutcome::retry()` | nack 并重新入队（稍后重新投递） |
| `HandlerOutcome::retry_after(delay)` | nack，并要求重新投递不早于 `delay` |
| `HandlerOutcome::drop()` | nack 且不重新入队（丢弃或进死信） |
| `()` | 始终 ack |
| `Result<(), E>` | `Ok` 时 ack，`Err` 时 drop |
| `Result<HandlerOutcome, E>` | `Ok` 时用内层的结果，`Err` 时 drop |
| `HandlerOutcome::ack().and_after(..)`（任意结果） | 按该结果结算，然后运行后续任务 |

在消息本身上，ack 会消费 `self`，因此类型系统杜绝了两次 ack。

### 结算后的后续任务 { #post-settle-continuations }

`HandlerOutcome::ack().and_after(fut)` 会给返回的结果附上一个后续任务：一条不关键的通知、一件慢的
收尾工作、一次缓存预热。任何结果都可以这么用（`drop().and_after(..)` 是合法的；中性的读法是
“结算之后”）：

=== "宏"

    ```rust
    --8<-- "examples/post_settle.rs:single"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/post_settle.rs:single"
    ```

后续任务遵循统一的结算后语义（至多一次；只有在 ack 或 nack 结算之后才运行；优雅关闭时会排空），
参见[结算后钩子](context.md#post-settle-hooks)。

在一批消息中，每个元素各自结算，因此后续任务是逐元素携带的，这是按消息的上下文钩子给不了的能力：

=== "宏"

    ```rust
    --8<-- "examples/post_settle.rs:batch"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/post_settle.rs:batch"
    ```

批量*发布*（带 `publish(..)` 的批量处理器）是在一个事务里全有或全无地结算，因此逐元素的
`and_after` 在那里无法组合；它只适用于普通的批量形态和单条形态。

### 延迟重新投递

`retry_after` 针对的是“还没就绪”的情况（依赖的东西还没到、上游被限流），这时立刻重新投递只会空转
而没有任何进展：

=== "宏"

    ```rust
    --8<-- "examples/retry.rs:retry_after"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/retry.rs:retry_after"
    ```

在底层，运行时会这样兑现延迟：

- 原生支持延迟重新投递的 Broker（内存 Broker 用定时器重新投递；NATS JetStream 的 Broker 可以带
  延迟发 `NAK`）直接把它交给传输层处理。
- 在不原生支持的 Broker 上，运行时会安排一次**延后重新发布**：等 `delay` 过去之后，把这条消息重新
  发布到它自己的来源 subject，然后丢弃原件。重新发布的副本会带上框架的重试计数消息头
  （[`RETRY_COUNT_HEADER`](https://docs.rs/ruststream/latest/ruststream/runtime/constant.RETRY_COUNT_HEADER.html)），
  并且已经加一；处理器可以读它来给重新投递次数封顶。

  这条路径按作用域显式启用：
  [`BrokerScope::retry_via(publisher)`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.BrokerScope.html#method.retry_via)
  （该发布者必须指向同一个 Broker）。没有发布者时，运行时会丢弃延迟，消息立即重新入队。在延迟窗口
  内，延后重新发布是**至多一次**的：如果进程在定时器触发之前退出，副本就丢了。

`batch_retry_after` 这种写法可以和[选择性的批量结果](#selective-acknowledgement)组合：一个
`Vec<HandlerOutcome>` 携带逐元素的延迟，于是尚未就绪的条目各自退避，而不拖住这一批里的其余消息：

=== "宏"

    ```rust
    --8<-- "examples/retry.rs:batch_retry_after"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/retry.rs:batch_retry_after"
    ```

## 选择订阅的来源

属性宏总是把订阅的*种类*定死 - subject、JetStream consumer、Redis stream、pub/sub 频道和 list 是
不同的类型。可以省略的是填进该种类里的*值*，它由挂载点补上。一共有四种写法，从最短的开始：

| 写法 | 种类 | 值 |
|---|---|---|
| `#[subscriber]` | 按名字的来源 | 来自挂载点 |
| `#[subscriber(RedisStream)]` | 在这里指定 | 来自挂载点 |
| `#[subscriber("orders")]` | 按名字的来源 | 在这里定死 |
| `#[subscriber(RedisStream::new("orders").group("w"))]` | 在这里指定 | 在这里定死 |

### 按名字

`#[subscriber("orders")]` 按名字订阅。它适用于任何实现了 `Subscribe` 能力的 Broker，而本家族里的
每个 Broker crate 都实现了它：名字映射到该 Broker 视为默认的订阅种类，而该种类在名字
之外还需要的配置，在 Broker 上一次性设定。

`#[subscriber]` 用的是同一个来源，只是把值留空：名字可能是服务在装配自身时才知道的，可能是由分片
号拼出来的 subject，也可能是从配置里读出来的主题：

=== "宏"

    ```rust
    --8<-- "examples/subscribers.rs:deferred_name"
    ```

    ```rust
    --8<-- "examples/subscribers.rs:name_mount"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/subscribers.rs:deferred_name"
    ```

    ```rust
    --8<-- "examples/manual/subscribers.rs:name_mount"
    ```

如果某个种类确实不只靠一个名字就能存在（Pulsar 的来源同时需要一个主题*和*一个订阅名），它就不实现
`FromName`，这种写法对它也编译不过。这类种类要完整写出来。

### Broker 专有的描述符 { #broker-specific-descriptors }

当一条订阅需要 Broker 专有的选项时（消费者组、durable 名称、投递策略），Broker crate 会提供一个
描述符类型。直接在属性宏里使用它的构造函数：

<!-- inline-rust: 示意性的描述符写法；OrdersStream 只是某个 Broker crate 的 SubscriptionSource 类型的替身，那类类型住在别的 crate 里，本仓库没有可编译的落脚点（真实的 NATS 写法在下面引入） -->
```rust
#[subscriber(OrdersStream::new("orders", "workers"))]
async fn handle(order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}
```

宏会从构造函数调用里读出描述符类型，于是编译器会拿该描述符去核对它挂载到的 Broker。描述符
可以是任何实现了 `SubscriptionSource<B>` 的类型；见
[Broker 作者](../broker-authors/index.md#subscription-sources)。

来源也可以是在该构造函数之上的一串构建器调用，这样流式的选项就能就地写完。例如，某个提供了选项
构建器的 Broker，允许处理器直接在属性宏里点名具体的 stream 和 consumer：

<!-- inline-rust: 示意性的构建器链来源；具体的选项类型住在某个 Broker crate 里，本仓库没有可编译的落脚点 -->
```rust
#[subscriber(StreamOptions::new("orders").durable("audit"))]
async fn handle(order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}
```

宏会沿着这条链一路追到最底下的 `Type::new(..)`，以确定来源的类型，因此链上的每个方法都必须返回
`Self`。宏会拒绝自由函数，因为它看不见这些函数的类型。

这样构建出来的来源会在每次挂载时重新构建一遍，因此 Broker 的描述符类型是 `Clone` 的。同一个定义
可以挂到两个 Broker 上。

## 挂载点的设置

名字、worker 策略、失败策略和起始位置都是值，因此每一项都可以写在属性宏里、写在挂载点，或者两边
各写一部分。属性宏展开出来的，正是你自己会写的那些调用：

=== "宏"

    ```rust
    --8<-- "examples/subscribers.rs:builder_settings"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/subscribers.rs:builder_settings"
    ```

属性宏里已经给出的设置会固定在定义的类型中，对应的构建器方法也就不再适用，因此没有优先级规则
需要记：

<!-- inline-rust: 两行必须编译失败的示例；能编译的示例文件放不下这种代码（钉住的诊断信息在 tests/ui 里） -->
```rust
#[subscriber("orders", workers(4))]
async fn handle(order: &Order) -> HandlerOutcome { HandlerOutcome::ack() }

b.include(handle.name("other"));    // does not compile: the name is already given
b.include(handle.on_failure(..));   // fine: the attribute said nothing about failures
```

这些方法来自 `SubscriberSettings` trait，每个生成出来的定义都实现了它；要用到它们，就导入
该 trait（或者导入
[prelude](https://docs.rs/ruststream/latest/ruststream/prelude/index.html)）。

Broker 专有的设置也以同样的方式出现，用的是该 Broker 自己的词汇。核心无从知道一条订阅有 JetStream
的 stream 或者 durable consumer 名，因此它只暴露一个钩子，即对正在构建的来源做一次变换，再由
Broker crate 在其上叠加自己的 trait，并绑定到自己的来源类型；见
[Broker 作者](../broker-authors/index.md#subscription-sources)。链上的顺序由每一步做的事情决定：
名字排在最前，因为它构造出来源；接着 Broker 的设置对它做变换；下文的缓冲最后把它包起来。

## 挂载处理器

在 `with_broker` 内部，用 `include` 挂载一个定义：

<!-- inline-rust: 最小的 include 挂载片段，info 与 broker 都是占位；完整可编译的程序是 examples/subscribers.rs（本页通过其他锚点引入了它的 app） -->
```rust
RustStream::new(info).with_broker(broker, |b| {
    b.include(handle);
});
```

`include` 解码载荷时用的编解码器，取自你设定过的最具体的那一层：按处理器、按作用域，或者由 feature
选出的默认值。参见[解码用的编解码器从哪来](codecs.md#where-the-decode-codec-comes-from)。

要按模块给处理器分组并一次性全部挂载，就把它们收进一个路由器（`Router`）；见[路由](routing.md)。

## 批量订阅者

接收切片的处理器消费的是整批消息：Broker 每投递一批，它就运行一次，对应一次数据库往返、一次批量
API 调用。该形态从签名里读出，属性宏里什么都不用写。

=== "宏"

    ```rust
    --8<-- "examples/subscribers.rs:batch"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/subscribers.rs:batch"
    ```

和其他写法一样，用 `include` 挂载即可，批量形态由定义自己携带：

=== "宏"

    ```rust
    --8<-- "examples/subscribers.rs:batch_mount"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/subscribers.rs:batch_mount"
    ```

签名说的是处理器想一次拿到多条消息；它们是否真的成批到达，则是 Broker 的性质，因此这件事在挂载
定义的地方才敲定。这条订阅的订阅者必须实现 `BatchSubscriber` 能力：客户端原生成批的 Broker
（Kafka 的 poll、JetStream 的 pull consumer）直接提供它，批量大小则在它们的订阅选项里；内存
Broker 同样原生成批。如果订阅本身不成批，编译器就会要求给出框架自带的缓冲，并由挂载点提供，它按
大小、或者按首次投递之后的一个截止时间来封批：

=== "宏"

    ```rust
    --8<-- "examples/subscribers.rs:batch_buffered"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/subscribers.rs:batch_buffered"
    ```

批要么来自 Broker（由 Broker 自己的设置配置），要么来自这层包装；这项设置以适配器命名，正是为了把
两者分开。这层包装改变了订阅的类型，所以它排在最后 - 绑定在未包装类型上的 Broker 设置，过了这一步
就不再适用。

它的语义和单条消息的处理器有几处不同：

- 运行时会逐个 nack 解码失败的元素（按解码失败策略处理），它们根本不会到达处理器；其余的作为一个
  切片一起送达。
- 返回值结算的是整批。单个 `HandlerOutcome`（或 `()` / `Result<_, E>`）会对**每一条**消息一视同仁
  地结算：`ack()` 把它们全部 ack，`retry()` 把它们全部重新入队。
- 在 `&[T]` 这种写法里拿不到逐条消息的消息头，上下文一开始的消息头是空的。
- 应用全局的中间件和路由器上的中间件包裹的是按消息的处理器，对批量注册不生效。

### 选择性确认 { #selective-acknowledgement }

常见的情形是部分就绪：这一批里有些消息已经处理完，另一些还没就绪，应重新投递，同时不去重试那些
已经成功的。返回 `Vec<HandlerOutcome>`，切片中的第 `i` 个元素就按第 `i` 个结果结算：

=== "宏"

    ```rust
    --8<-- "examples/subscribers.rs:batch_selective"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/subscribers.rs:batch_selective"
    ```

Broker 侧的语义和逐条消息的 `nack(requeue = true)` 完全一致：支持逐条重新投递的 Broker 原生支持
选择性重试；基于位置的 Broker 则和它处理单条消息的 nack 时一样降级（这一点由该 Broker 的 crate
自行说明）。返回一个长度和这批消息对不上的向量，是处理器里的 bug：运行时会重试没有对上的剩余部分，
并把这次不匹配记入日志。

## 定位（seek） { #seeking }

传输层是可重放日志的 Broker（Kafka、Redis stream、内存 Broker 的发布日志）实现了 `Seekable`
能力：一条活着的订阅可以移到另一个位置，比如修好处理器的 bug 之后重放一段流、从某个已知的点重新
处理，或者向前跳过一段毒消息，而不必丢弃这条订阅。没有可重放日志的 Broker 不实现它，下面这种
挂载对它们来说编译不过，而不是到运行时才失败。

处理器通过一个 `Seek` 参数给自己的订阅重新定位。订阅一打开，运行时就从它身上铸出 seeker，
因此处理器手里握着的始终是一个活的句柄；挂载点不需要附加任何东西：

=== "宏"

    ```rust
    --8<-- "examples/seek.rs:handler"
    ```

    ```rust
    --8<-- "examples/seek.rs:mount"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/seek.rs:handler"
    ```

    ```rust
    --8<-- "examples/manual/seek.rs:mount"
    ```

该子句在每次启动时都强制使用给定位置；没有它时，订阅就在 Broker 的默认位置打开。有条件的默认值
（只在 Broker 没有为该组存下游标时才生效，比如 Kafka 的 offset reset、JetStream 的 deliver
policy）留在 Broker 自己的订阅描述符上，那里能原生表达它。

一次 seek 的影响范围因 Broker 而异：给一个 consumer 实例重新定位（Kafka）只移动该实例，而移动共享
的组游标（Redis stream）会移动整个组；此外，重新定位会让 Broker 为这条订阅保存的 ack 记账失效。
这两点都由 Broker crate 自行说明。Broker 作者用
[`capabilities::seeking` conformance 校验套件](../broker-authors/conformance.md#capability-suites)
来证明自己满足该契约。

## 原始字节订阅者

当载荷根本不是一个序列化后的值时（一个二进制帧、一种你自己解析的外部线上格式），把消息参数写成
`&[u8]` 就能把编解码器彻底移出这条路径：处理器收到的是每次投递的字节，和 Broker 交过来时一模一样。

=== "宏"

    ```rust
    --8<-- "tests/raw_subscriber.rs:raw"
    ```

=== "手写"

    ```rust
    --8<-- "tests/manual_raw_subscriber.rs:raw"
    ```

一批载荷就是同一件事在批量形态下的样子：`&[Payload<'_>]` 是去掉了解码步骤的类型化批量，其中的载荷在
调用期间从这批消息自身借用。不发生任何拷贝，结算规则沿用批量路径的那一套。

=== "宏"

    ```rust
    --8<-- "examples/subscribers.rs:raw_batch"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/subscribers.rs:raw_batch"
    ```

在这两种形态上写 `on_failure(decode = ..)` 策略都是编译错误：这里没有会失败的解码步骤，除非处理器
声明了 `Headers` 契约，那种情况这条策略确实管得着。提取器、`&mut Context`、`workers(..)`、
`on_failure(panic = ..)`，以及注入的 `Out` / `Seek` 参数，在单次投递的形态上照常可用（批量载荷
形态目前还不接受 `Out` / `Seek`）；原始字节订阅者也和其他任何定义一样，用同一个 `include` 挂载，
而作用域上即便设了编解码器，对它也单纯不生效。原始字节订阅者也是在一个编解码器 feature 都没启用时
唯一可用的订阅者写法。如果某种自定义的序列化格式仍然希望用*类型化*的处理器来写，就实现
[`Codec`](codecs.md)，继续走类型化路径。

原始字节订阅者也能以同样的形式回复：`publish_raw("dest")` 子句会把返回的字节（`-> Vec<u8>`，或者
`-> Result<Vec<u8>, HandlerOutcome>`，后者提供与类型化回复写法相同的显式 ack 控制）原样发布到回复
名字上，走的是挂载点附加的裸发布者（`b.include(relay).publisher(policy)`，不写该调用时就用
Broker 的默认发布策略）；两端都不经过编解码器，而回复发布失败会让这次投递 nack 并重新入队：

=== "宏"

    ```rust
    --8<-- "tests/raw_subscriber.rs:raw_reply"
    ```

=== "手写"

    ```rust
    --8<-- "tests/manual_raw_subscriber.rs:raw_reply"
    ```

`publish_raw` 并不绑定原始字节输入：用在类型化的处理器上时，它只让*回复*是原始字节的，输入仍然用
作用域的编解码器解码（解码失败策略也照旧），而返回的字节则不经编码直接发出。这就是网关式的形态：
消费结构化的消息，发出由处理器自己产生的线上格式：

=== "宏"

    ```rust
    --8<-- "tests/raw_subscriber.rs:raw_reply_typed"
    ```

=== "手写"

    ```rust
    --8<-- "tests/manual_raw_subscriber.rs:raw_reply_typed"
    ```

宏会拒绝写在 `&[u8]` 处理器上的、会编码的 `publish(..)` 子句（原始字节处理器的回复就是字节，
错误信息会点明 `publish_raw` 才是该用的写法）；在同一个处理器上同时使用两种回复子句，宏也一样拒绝。

## Worker 池

每个订阅者的分发循环都是顺序的：一次投递处理并结算之后，才会拉取下一次，因此一个慢的处理器会
拖住整条订阅。`workers(n)` 子句让该订阅者最多并发处理 `n` 次投递，每一次都在多线程运行时上的
独立任务里执行：

=== "宏"

    ```rust
    --8<-- "examples/subscribers.rs:workers"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/subscribers.rs:workers"
    ```

背压依然成立：当有 `n` 次投递正在处理中时，运行时不会轮询流，这和 JetStream `max_ack_pending`
这类 Broker 侧的限制配合得很好。**全局的处理顺序按设计放弃**，只要顺序还有意义，要么保持
顺序执行，要么使用按键分道：

=== "宏"

    ```rust
    --8<-- "examples/subscribers.rs:workers_by_key"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/subscribers.rs:workers_by_key"
    ```

`workers(n, by_key)` 会跑 `n` 条顺序执行的道。一次投递会进入它的分区键哈希到的那条道，因此共享同
一个键的消息绝不会重叠执行，也不会乱序，这相当于在进程内复刻了 Kafka 的分区语义。键取自 Broker
消息的 `partition_key()`（消息实现了 `Partitioned` 能力的 Broker 会提供它；内存 Broker 读的是
`partition-key` 消息头）。没有键的消息在各条道之间轮转。`by_key` 只适用于单条消息的订阅者；
批量写法用的是普通的 `workers(n)`，池里放的是一个个批。

关闭时，订阅者不再拉取新的投递，正在处理中的 worker 在应用的 `shutdown_timeout` 之内排空。

## 组合规则

订阅者的这些能力可以互相组合；下面是每个交叉点上的规则，每一条都由一个集成测试钉住。

| 组合 | 规则 |
|---|---|
| `workers(n)` × 批量处理器 | 池里最多有 `n` **批**同时在处理。`by_key` 不适用于批量写法：分道排的是同一个键下单条消息的顺序，宏会在编译期拒绝这种组合。 |
| `retry()` / `retry_after` × `workers(n)` | 重试的投递会重新进入该池，并像其他投递一样走完。 |
| `retry()` / `retry_after` × `workers(n, by_key)` | 重试能走完，但跨越一次重试之后，同一个键内部的顺序**不**保证：重新入队的消息会从队尾重新进入流。如果某个键的消息即使遇到失败也必须保持顺序，处理器就必须自己消化这次失败，而不是 nack。 |
| `.transactional()` × `workers(n)` | 每批一个事务，和顺序循环时完全一样。并发的批跑的是并发且互相独立的事务；每个事务各自保持原子性（每批先提交再 ack）。 |
| `Buffered` × `workers(n)` | 批仍然只按 `max_size` / `max_wait` 封口；池只限制同时处理多少个已封好的批，永远不影响批的边界。 |
| `publish(..)` × `workers(n)` | 回复是并发产生的，因此跨投递的回复顺序没有保证。回复发布失败只会重试它自己那一次投递。 |
| 中间件 × 批量处理器 | 应用全局的层和路由器上的层包裹的是按消息的处理器，对批量注册不生效（按消息的层没法包裹一个整批的处理器）。 |

## 用宏还是手写

`#[subscriber]` 是泛型 API 之上的语法糖。宏生成的是一个类型化的处理器和它的元数据；同样的注册你也
可以手写出来：一个具名类型，用 `impl Handle` 承载函数体，再用 `subscriber(source, body)` 把它绑定
到来源，最后用 `.build()` 封口。下面两种写法注册的是同一个处理器。

=== "用宏"

    ```rust
    use ruststream::subscriber;

    --8<-- "examples/subscribers.rs:contract"

    // inside with_broker(...):
    b.include(handle);
    ```

=== "手写"

    ```rust
    use ruststream::prelude::*;

    // inside with_broker(...):
    --8<-- "examples/subscribers.rs:manual"
    ```

当处理器需要宏表达不了的状态时（带字段的结构体处理器），或者要设定非默认的
[解码失败策略](codecs.md#decode-failures)时，就动用手写这一形态。其余情况下，宏要维护的东西更少。

## 发布者

产出回复的处理器就是一个发布者。见[发布与回复](publishing.md)。
