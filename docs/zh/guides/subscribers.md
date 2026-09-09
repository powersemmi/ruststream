# 订阅者

订阅者把处理器绑定到一条订阅上。用 `#[subscriber]` 宏声明它。处理器如何按模块分组见
[路由](routing.md)，载荷如何解码见[编解码器](codecs.md)。

## 处理器契约

处理器是一个 `async fn`，第一个参数是解码后载荷的引用：

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

宏会把这个函数变成同名的订阅者定义（这里是 `handle`），它实现了挂载契约。把这个定义传给
`include`。

### 接受上下文

声明可选的第二个参数 `&mut Context`，就能读取消息头、订阅名和共享状态，也能直接在处理器里发布
消息：

=== "宏"

    ```rust
    --8<-- "examples/subscribers.rs:context"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/subscribers.rs:context"
    ```

上下文的类型由宏自己推断，因此只要 `Context` 只出现在 `#[subscriber]` 的签名里，就不必导入这个
名字。其余部分（消息头的工作副本、状态访问和 Broker 的每次投递字段）见
[上下文与状态](context.md)。

### 提取器参数

在消息和可选的 `&mut Context` 之后，其余参数都是**提取器**。运行时在函数体执行之前从这次投递里
取得它。取不到时，这次投递就此结算，函数体不执行。提取器有四种：

- `State<T>`，应用状态的一个字段（在状态类型上派生 `FromRef`）。
- `Ctx<K>`，Broker 的某个每次投递字段，按它的键读取。
- `Headers<T>`，把这次投递的消息头解析成类型化的契约；违反契约时，该投递按
  `on_failure(decode = ..)` 策略结算（见[类型化消息头](headers.md)）。
- 任何实现了 `FromContext` 的类型，也就是你自己的提取器（权限检查和单次请求范围内的依赖查找）。

具体机制见[注入依赖](context.md#injecting-dependencies-extractor-parameters)和
[把上下文字段作为参数](context.md#context-fields-as-parameters)。

还有一种参数形态不是提取器，而是**注入**。`Out(out): Out<impl Publisher>` 接收一个活的发布者，它由
`include` 处给出的策略构造（`b.include(handler).out(marker, policy).build()`）。具体的发布者
类型不会出现在签名里。

可选的第三个位置 `Out<impl Publisher, Marker, (A, B)>` 声明这个处理器发布的消息集合，并启用按
字典的类型化发布路径（[类型化消息头](headers.md)）。参见
[在处理器内部发布](publishing.md#publishing-from-inside-a-handler)。

### 确认（ack） { #acking }

返回值可以是任何能转换成
[`HandlerOutcome`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.HandlerOutcome.html)
的类型（结算的单位：给 Broker 的状态，加上一个可选的结算后续任务）：

| 返回值 | 效果 |
|---|---|
| `HandlerOutcome::ack()` | 确认这条消息；Broker 把它移除 |
| `HandlerOutcome::retry()` | nack 并重新入队（稍后重新投递） |
| `HandlerOutcome::retry_after(delay)` | nack，并要求重新投递不早于 `delay` |
| `HandlerOutcome::drop()` | nack 且不重新入队（丢弃或进死信） |
| `()` | 始终 ack |
| `Result<(), E>` | `Ok` 时 ack，`Err` 时 drop |
| `Result<HandlerOutcome, E>` | `Ok` 时用内层的结果，`Err` 时 drop |
| `HandlerOutcome::ack().and_after(..)`（任意结果） | 按该结果结算，然后运行后续任务 |

消息上的 `ack` 会消费 `self`，因此类型系统不允许 ack 两次。

### 结算后的后续任务 { #post-settle-continuations }

`HandlerOutcome::ack().and_after(fut)` 给返回的结果附上一个后续任务：一条不关键的通知、一件慢的
收尾工作和一次缓存预热。任何结果都可以这么用，`drop().and_after(..)` 也一样：

=== "宏"

    ```rust
    --8<-- "examples/post_settle.rs:single"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/post_settle.rs:single"
    ```

后续任务遵循统一的结算后规则：至多一次、只在 ack 或 nack 结算之后运行、优雅关闭时排空。参见
[结算后钩子](context.md#post-settle-hooks)。

在一个批次里，每个元素各自结算，因此后续任务也逐元素给出：

=== "宏"

    ```rust
    --8<-- "examples/post_settle.rs:batch"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/post_settle.rs:batch"
    ```

批量*发布*（带 `publish(..)` 的批量处理器）在一个事务里整批结算，因此逐元素的 `and_after` 和它
不能组合。

### 延迟重新投递

`retry_after` 针对的是“还没就绪”的情况：依赖还没出现，外部服务在限制请求频率。这时立刻重新投递
同样什么也做不成：

=== "宏"

    ```rust
    --8<-- "examples/retry.rs:retry_after"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/retry.rs:retry_after"
    ```

运行时这样兑现延迟：

- 原生支持延迟重新投递的 Broker 直接拿到这个延迟。内存 Broker 就是这样，它用定时器重新投递；
  NATS JetStream 上的 Broker 可以带延迟发 `NAK`。
- 不原生支持的 Broker 得到的是**延后重新发布**：运行时等 `delay` 过去，把消息重新发布到它原来的
  来源，再丢弃原件。新副本里框架的重试计数消息头
  （[`RETRY_COUNT_HEADER`](https://docs.rs/ruststream/latest/ruststream/runtime/constant.RETRY_COUNT_HEADER.html)）
  加了一，处理器可以按它给重新投递次数封顶。

  在一个作用域里，可以调用
  [`BrokerScope::retry_via(publisher)`](https://docs.rs/ruststream/latest/ruststream/runtime/struct.BrokerScope.html#method.retry_via) 启用它，
  该发布者必须指向同一个 Broker。没有发布者时，运行时丢弃延迟，消息立即重新入队。在延迟的这段
  时间里，延后重新发布是**至多一次**的：如果进程在定时器触发之前退出，副本就丢了。

  完全不能结算的传输（MQTT 的 QoS 0、ZeroMQ 和 Redis pub/sub）走同一条路径：没有原件可丢，重新
  投递就只剩这份延后的副本。另一种情形是 Broker 拒绝了结算：这条消息仍归 Broker，由它自己重新
  投递，因此运行时返回错误，不再叠加一份副本。

`batch_retry_after` 这种写法可以和[选择性的批量结果](#selective-acknowledgement)组合：
`Vec<HandlerOutcome>` 逐元素给出延迟，未就绪的条目各自等待，不拖住这个批次里的其余消息：

=== "宏"

    ```rust
    --8<-- "examples/retry.rs:batch_retry_after"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/retry.rs:batch_retry_after"
    ```

## 选择订阅的来源

属性宏总是定死订阅的*种类*：subject、JetStream consumer、Redis stream、pub/sub 频道和 list 是
不同的类型。可以省略的是*值*，它由挂载点给出。写法有四种，从最短的开始：

| 写法 | 种类 | 值 |
|---|---|---|
| `#[subscriber]` | 按名字的来源 | 来自挂载点 |
| `#[subscriber(RedisStream)]` | 在这里指定 | 来自挂载点 |
| `#[subscriber("orders")]` | 按名字的来源 | 在这里定死 |
| `#[subscriber(RedisStream::new("orders").group("w"))]` | 在这里指定 | 在这里定死 |

### 按名字

`#[subscriber("orders")]` 按名字订阅。这种写法适用于实现了 `Subscribe` 能力的任何 Broker，也就是
本家族的所有 Broker crate。Broker 把名字映射到它自己认定的主要订阅种类。该种类在名字之外还需要
的配置，你在 Broker 上一次性设定。

`#[subscriber]` 用同一个来源，只是不给值：名字可能在服务启动时才知道，可能是由分片号拼出来的
subject，也可能是从配置里读出来的主题：

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

有些种类只有名字还不够：Pulsar 的来源同时接受主题*和*订阅名。这样的种类不实现 `FromName`，因此
从挂载点取名字的那些写法对它编译不过。这类种类要完整写出来。

### Broker 专有的描述符 { #broker-specific-descriptors }

订阅需要 Broker 专有的选项时（消费者组、durable 名称和投递策略），Broker crate 提供一个描述符
类型。直接在属性宏里调用它的构造函数：

<!-- inline-rust: 示意性的描述符写法；OrdersStream 只是某个 Broker crate 的 SubscriptionSource 类型的替身，那类类型住在别的 crate 里，本仓库没有可编译的落脚点（真实的 NATS 写法在下面引入） -->
```rust
#[subscriber(OrdersStream::new("orders", "workers"))]
async fn handle(order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}
```

描述符的类型由宏从构造函数调用里读出，因此编译器会拿它和挂载到的 Broker 核对。描述符是任何实现了
`SubscriptionSource<B>` 的类型；见
[Broker 作者](../broker-authors/index.md#subscription-sources)。

来源也可以是这个构造函数之上的一串设置调用：

<!-- inline-rust: 示意性的构建器链来源；具体的选项类型住在某个 Broker crate 里，本仓库没有可编译的落脚点 -->
```rust
#[subscriber(StreamOptions::new("orders").durable("audit"))]
async fn handle(order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}
```

为了确定来源的类型，宏沿这条链找到最底下的 `Type::new(..)`，因此链上的每个方法都必须返回
`Self`。宏拒绝自由函数：它看不见这些函数的类型。

这样的来源在每次挂载时重新构建，因此 Broker 的描述符类型实现 `Clone`。同一个定义可以挂到两个
Broker 上。

## 挂载点的设置

名字、worker 策略、失败策略和起始位置都是值。每一项都可以写在属性宏里、写在挂载点，或者两边各
写一部分。属性宏展开出来的正是你自己会写的那些调用：

=== "宏"

    ```rust
    --8<-- "examples/subscribers.rs:builder_settings"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/subscribers.rs:builder_settings"
    ```

属性宏里给出的设置固定在定义的类型里，挂载点无法再写一次，因此没有优先级规则要记：

<!-- inline-rust: 两行必须编译失败的示例；能编译的示例文件放不下这种代码（钉住的诊断信息在 tests/ui 里） -->
```rust
#[subscriber("orders", workers(4))]
async fn handle(order: &Order) -> HandlerOutcome { HandlerOutcome::ack() }

b.include(handle.name("other"));    // does not compile: the name is already given
b.include(handle.on_failure(..));   // fine: the attribute said nothing about failures
```

这些方法声明在 `SubscriberSettings` trait 里，每个生成出来的定义都实现了它。导入该 trait 或
[prelude](https://docs.rs/ruststream/latest/ruststream/prelude/index.html)，就能用到它们。

Broker 专有的设置用同样的方式给出，用的是该 Broker 自己的词汇：JetStream 的 stream、consumer 的
durable 名称。核心不认识这套词汇，因此只给出一个扩展点，即对正在构建的来源做一次变换。
Broker crate 在其上叠加自己的 trait，绑定到自己的来源类型；见
[Broker 作者](../broker-authors/index.md#subscription-sources)。

链上的顺序由每一步做的事情决定：名字排在最前，因为它构造出来源；Broker 的设置对它做变换；下文的
缓冲最后把它包起来。

## 挂载处理器

在 `with_broker` 内部，用 `include` 挂载定义：

<!-- inline-rust: 最小的 include 挂载片段，info 与 broker 都是占位；完整可编译的程序是 examples/subscribers.rs（本页通过其他锚点引入了它的 app） -->
```rust
RustStream::new(info).with_broker(broker, |b| {
    b.include(handle);
});
```

`include` 解码载荷用的编解码器取自你设定过的最具体的一层：按处理器，或者按作用域。一层都没设定
时，生效的是 feature 选出的默认编解码器。参见
[解码用的编解码器从哪来](codecs.md#where-the-decode-codec-comes-from)。

要按模块给处理器分组并一次性挂载，把它们收进一个路由器（`Router`）；见[路由](routing.md)。

## 批量订阅者 { #batch-subscribers }

接收切片的处理器拿到整个批次：Broker 每投递一个批次，处理器就运行一次。一次数据库往返，一次批量
API 调用。批量形态由宏从签名里读出，属性里不用声明。

=== "宏"

    ```rust
    --8<-- "examples/subscribers.rs:batch"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/subscribers.rs:batch"
    ```

像其他形态一样，用 `include` 挂载。批量形态已经写在定义里，挂载点补上一个数字，也就是批次的大小：

=== "宏"

    ```rust
    --8<-- "examples/subscribers.rs:batch_mount"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/subscribers.rs:batch_mount"
    ```

`batch(n)` 是框架唯一的批次参数，而且是必填的：批量处理器不写它就编译不过。运行时把这个大小传给
Broker，Broker 照它攒批次：`XREADGROUP COUNT`、JetStream 的 pull 批次和 Kafka 的 poll 上限。函数
体看到的正是 Broker 投递过来的那个批次，绝不是它的一个切片。Broker 手上只有更少的消息时，批次就短
于 `n`。

攒出一个批次还要别的条件（阻塞超时、消费者组和预取窗口），它们属于 Broker 自己的词汇。这些设置排
在大小之后，写在 Broker 的订阅来源上：

<!-- inline-rust: 这一步属于 Broker crate，而本仓库不能依赖它 -->
```rust
// 在 Redis Broker 上：大小是核心的词，`.block(..)` 是 Broker 的词
b.include(reconcile.name("orders").batch(nonzero!(6)).block(Duration::from_secs(5)));
```

任何批量形态都用同一种写法给出大小，处理器回复或者通过 `Out` 槽发布时也一样。单条消息的处理器没有
批次，因此 `batch(n)` 写在它身上编译不过。这类处理器一次取多少条投递，由 `workers(n)` 决定。

任何 Broker 都提供批次。客户端原生按批拉取的 Broker 直接实现 `BatchSubscriber` 能力：Kafka 的
poll、JetStream 的 pull 消费者、Redis 的 `XREADGROUP` 和内存 Broker。传输一次只投递一条消息的
Broker，在自己的 crate 里用核心的 `Buffered` 适配器在客户端攒批次。从挂载点看不出走的是哪一条路。
Broker 怎么做到这一点，见 Broker 作者指南的
[批次](../broker-authors/index.md#batches-batchsubscriber)一节。

和单条消息的处理器相比，语义有几处不同：

- 解码失败的元素按解码失败策略单独 nack，不会到达处理器。其余元素作为一个切片一起送达。
- 返回值结算整个批次。单个 `HandlerOutcome`（或 `()` / `Result<_, E>`）把**每一条**消息结算成同
  一个结果：`ack()` 全部 ack，`retry()` 全部重新入队。
- `&[T]` 这种形态拿不到逐条消息的消息头，上下文里的消息头是空的。
- 上下文每批一个，其中 Broker 的字段是*整条订阅*上的那些。批量函数体写出 Broker 的批量上下文类型
  （内存 Broker 是 `ctx: &mut Context<'_, MemoryBatchContext>`），用 `ctx.context(..)` 读它的键。
  没有订阅级字段的 Broker 把批次留在 `()` 这个默认值上。
- 这个上下文里没有单次投递的数据：一个批次横跨多次投递，位置和消息头因此保存在元素自身中，从
  `&[Message<H, T>]` 这样的批次里逐个元素读取。投递上下文和批量上下文是不同的类型，所以批量函数
  体索要投递上下文时编译不过。
- 应用全局的中间件和路由器上的中间件包裹的是单条消息的处理器，对批量注册不生效。

### 选择性确认 { #selective-acknowledgement }

部分就绪是常见的情形：一个批次里有些消息已经处理完，另一些还没就绪。需要重新投递的只有没就绪的那
些。返回 `Vec<HandlerOutcome>`，切片的第 `i` 个元素就按第 `i` 个结果结算：

=== "宏"

    ```rust
    --8<-- "examples/subscribers.rs:batch_selective"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/subscribers.rs:batch_selective"
    ```

Broker 侧的语义和单条消息的 `nack(requeue = true)` 相同。能逐条重新投递的 Broker 原生支持选择性
重试，基于位置的 Broker 则和单条消息 nack 时一样降级，这一点由该 Broker 的 crate 说明。长度和批
次对不上的向量是处理器里的 bug：运行时重新投递没有对上的剩余部分，并把这次不匹配记入日志。

## 定位（seek） { #seeking }

修好处理器的 bug 之后重放一段流、从某个已知的点重新处理、向前跳过一段毒消息：每一种情形都要在流里
指定一个位置。位置要么是订阅打开的地方，要么是一条运行中的订阅不中断就转到的地方。

建立在可重放日志之上的 Broker（Kafka、Redis 流和内存 Broker 的发布日志）实现 `Seekable` 能力，拥
有自己的位置类型，并在投递上下文里给出定位键。在没有可重放日志的 Broker 上，下面的挂载在编译期
就通不过，而不是留到运行时。

### 在选定的位置打开订阅

一条新订阅在哪里打开由 Broker 决定：普通消费者从末尾开始，持久消费者从存下的游标开始。在那之前发
布的消息，服务看不到。审计日志需要完整的历史，而监控恰恰不该去处理积压。

订阅在哪里打开取决于挂载点，而不是处理器，所以位置也写在那里：属性宏里的 `start_at(<position>)`
子句，或者设置链上的 `.start_at(..)`：

=== "宏"

    ```rust
    --8<-- "examples/seek.rs:start_at"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/seek.rs:start_at"
    ```

    ```rust
    --8<-- "examples/manual/seek.rs:start_at_mount"
    ```

位置是 Broker 自己的位置类型的值，因此可以写出的正是这个 Broker 能表达的东西。内存日志提供
`MemoryPosition::start()` 表示全部历史，`MemoryPosition::end()` 表示从下一次发布开始，
`MemoryPosition::sequence(n)` 表示某一条日志记录。

该子句在每次启动时设定位置。没有它，订阅就在 Broker 的默认位置打开。有条件的默认值，也就是只在
Broker 没有为该组存下游标时才生效的位置（Kafka 的 offset reset、JetStream 的 deliver policy），
写在 Broker 自己的订阅描述符上：那里能原生表达它。

### 在处理器里重新定位

处理器通过 Broker 的上下文键给自己的订阅重新定位。投递上下文里有位置和一个活的定位句柄，句柄由
Broker 在订阅打开时创建一次。处理器按键读取它们：宏路径用 `Ctx` 提取器，手写路径在 Broker 的上下
文类型上调用 `ctx.context(..)`。`include` 处不需要附加任何东西：

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

批量函数体重新定位自己的订阅是同样的做法，只是高了一层。定位句柄属于整条订阅，因此放在 Broker 的
批量上下文里。定位的目标，也就是发布者请求消费者从哪里继续的那个位置，保存在这个批次自己的元素里。

一次定位的影响范围因 Broker 而异：给一个消费者实例重新定位（Kafka）只移动该实例，移动共享的组游标
（Redis 流）则移动整个组。重新定位还会让 Broker 为这条订阅保存的 ack 记账失效。这两点都由 Broker
的 crate 说明。Broker 作者用
[`capabilities::seeking` conformance 套件](../broker-authors/conformance.md#capability-suites)
证明满足这个契约。

## 原始字节订阅者 { #raw-subscribers }

有时载荷不是序列化后的值，而是一个二进制帧，或者一种由你自己解析的外部传输格式。载荷的类型把
编解码器从处理路径上去掉：

```text
解码：broker -> bytes -> codec -> &Order     -> handler
原始：broker -> bytes ->          &Frame<'_> -> handler
```

路径的助记法写在 trait 的名字里：`Deserialize`/`Serialize` 表示工作由框架的编解码器来做，
`Deserialized`/`Serialized` 表示类型自己已经做完。

`Deserialized` 类型就是一个具名的 `&[u8]`：只有一个字段，不发生拷贝。声明的全部就是在包着
`&'a [u8]` 的 newtype 上写 `#[derive(Deserialized)]`，而 `&Frame<'_>` 这个参数把处理器放到原始路
径上。字节和 Broker 交过来时一模一样，直接借用它的缓冲区。

=== "宏"

    ```rust
    --8<-- "tests/raw_subscriber.rs:raw"
    ```

=== "手写"

    ```rust
    --8<-- "tests/manual_raw_subscriber.rs:raw"
    ```

参数直接写成 `&[u8]` 编译不过：载荷总是以服务自己的具名类型到达，编译错误会点明该加哪个 derive。
“手写”标签页展示的正是 derive 写出的那一对 impl：从字节构造，以及把类型指向原始路径的声明。

形态规则不随路径改变：`&T` 是一条消息，`&[T]` 是一个批次。一个批次的帧就是 `&[Frame<'_>]`，同一个
derive 也写出批量的声明，批量函数体不需要第二个 impl。批次的元素在调用期间借用它自己的消息，所以
这里同样不发生拷贝。结算规则和批量路径上的一样。

=== "宏"

    ```rust
    --8<-- "examples/subscribers.rs:raw_batch"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/subscribers.rs:raw_batch"
    ```

会校验内容的构造（flatbuffers 的根、capnp 的 reader 和一次长度检查）从 `from_payload` 返回 `Err`。
这次投递随后由 `on_failure(decode = ..)` 策略结算，和编解码器解码失败、类型化的 `Headers` 违约用
的是同一条策略。

其余部分照常组合。提取器、`&mut Context`、`workers(..)`、`on_failure(panic = ..)` 和注入的 `Out`
参数在单次投递的形态上原样可用，一个批次的帧则不接受 `Out` 参数。这样的订阅者用的也是同一个
`include`，和其他定义没有区别。

作用域的编解码器对它不生效：这条路径不调用编解码器。因此一个编解码器 feature 都不启用时，原始形态
是唯一还能工作的订阅者形态。想给自己的序列化格式写*类型化*的处理器，就实现
[`Codec`](codecs.md)，留在类型化路径上。

这条路径上的处理器用同一个 `publish("dest")` 子句回复，和其他任何回复形态一样。传输方式由回复的
*类型*按同一套助记法选出：带 `serde::Serialize` 的回复由回复的编解码器编码，带
`#[derive(Serialized)]` 的回复自己产出字节，发出去的正是处理器返回的那些字节。回复可以直接返回，
也可以写成 `Result<Export, HandlerOutcome>`，后者和编码形态一样给出显式的 ack 控制。

发布者由 `include` 处指定的策略构造，两种传输方式指定策略的写法相同：
`b.include(relay).out(Reply, Publish)`。不写 `.out(..)` 时，发布者由 Broker 的默认发布策略构造。

之后两条链分开：编码的回复接受 `.codec(..)`、`.transform(..)` 和 `.transactional()`，而
`Serialized` 的字节原样发出，这条路径上没有这些步骤。回复发布失败会让这次投递 nack 并重新入队，和
编码路径上一样：

=== "宏"

    ```rust
    --8<-- "tests/raw_subscriber.rs:raw_reply"
    ```

=== "手写"

    ```rust
    --8<-- "tests/manual_raw_subscriber.rs:raw_reply"
    ```

两侧互不约束：输入的类型选出解码，回复的类型选出编码，两者自由组合。可解码的输入配上 `Serialized`
回复就是网关形态：服务接收结构化的消息，发出处理器自己拼出的传输格式。此时输入仍由作用域的
编解码器解码，解码失败策略也照旧：

=== "宏"

    ```rust
    --8<-- "tests/raw_subscriber.rs:raw_reply_typed"
    ```

=== "手写"

    ```rust
    --8<-- "tests/manual_raw_subscriber.rs:raw_reply_typed"
    ```

反过来的组合读法相同：`Serialize` 回复由回复的编解码器编码，而 `Frame<'_>` 输入不碰编解码器。

有两种情形落在这条规则之外。`Vec<u8>` 回复不算原始字节：它是普通的 `Serialize` 值，发出去时经过编
码，必须原样发走的载荷仍然需要那个 newtype。批次的回复一律经由回复的编解码器发布，`Serialized`
这种传输方式适用于单条回复。

## Worker 池

订阅者的分发循环是顺序的：一次投递处理并结算之后，订阅者才取下一次。因此一个慢的处理器会拖住整条
订阅。有了 `workers(n)` 子句，订阅者最多同时处理 `n` 次投递，每一次都在多线程运行时上的独立任务
里：

=== "宏"

    ```rust
    --8<-- "examples/subscribers.rs:workers"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/subscribers.rs:workers"
    ```

背压依然成立：有 `n` 次投递在处理中时，运行时不再轮询流。这和 JetStream 的 `max_ack_pending` 这类
Broker 侧的限制相符。**全局的处理顺序就此丢失，这是设计如此**。投递顺序重要时，保持顺序执行，或
者按键把处理分成多个分区：

=== "宏"

    ```rust
    --8<-- "examples/subscribers.rs:workers_by_key"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/subscribers.rs:workers_by_key"
    ```

`workers(n, by_key)` 把处理拆成 `n` 个顺序执行的分区。一次投递进入它的分区键哈希到的那个分区，因
此同一个键的消息不会同时处理，也不会乱序。这是 Kafka 分区语义的进程内版本。

键取自 Broker 消息的 `partition_key()`：消息实现 `Partitioned` 能力的 Broker 提供它，内存 Broker
读的是 `partition-key` 消息头。没有键的消息在各个分区之间轮流分配。`by_key` 适用于单条消息的订阅
者，批量形态取的是普通的 `workers(n)` 池，池里放的是一个个批次。

关闭时，订阅者不再拉取新的投递，正在处理中的 worker 在应用的 `shutdown_timeout` 之内做完。

## 组合规则

订阅者的这些能力互相组合。下面是每个交叉点上的规则，每一条都有对应的集成测试。

| 组合 | 规则 |
|---|---|
| `workers(n)` × 批量处理器 | 池里同时最多有 `n` **批**在处理。`by_key` 不适用于批量形态：分区排的是同一个键下单条消息的顺序，宏在编译期拒绝这种组合。 |
| `retry()` / `retry_after` × `workers(n)` | 重新投递的消息重新进入该池，和其他投递一样结算。 |
| `retry()` / `retry_after` × `workers(n, by_key)` | 重试照常完成，但跨过一次重试之后，同一个键内部的顺序**不**保证：重新入队的消息从队尾重新进入流。某个键的消息即使遇到失败也必须保持顺序时，处理器要自己应对这次失败，而不是 nack。 |
| `.transactional()` × `workers(n)` | 每批一个事务，和顺序循环时完全一样。并发的批次运行并发且互相独立的事务，每个事务各自保持原子性（每批先提交再 ack）。 |
| 批次大小 × `workers(n)` | 批次仍然按 `batch(n)` 封口，Broker 在客户端攒批次时则按它的截止时间封口。池只限制同时处理多少个已封好的批次，绝不影响批次的边界。 |
| `publish(..)` × `workers(n)` | 回复是并发产生的，因此跨投递的回复顺序没有保证。回复发布失败只重试它自己那一次投递。 |
| 中间件 × 批量处理器 | 应用全局的层和路由器上的层包裹的是单条消息的处理器，对批量注册不生效（单条消息的层无法包裹整批的处理器）。 |

## 用宏还是手写

`#[subscriber]` 是泛型 API 之上的语法糖。宏生成一个类型化的处理器和它的元数据。同样的注册也可以手
写：把函数体放进具名类型的 `impl Handle`，用 `subscriber(source, body)` 把类型绑定到来源，再用
`.build()` 收尾。下面两种形态注册的是同一个处理器。

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

手写的函数体返回 `Result`。`Ok` 里放处理器产出的东西：一个回复，或者什么都没有。`Err` 里放结算，
因此 `Ok(())` 表示 ack，`Err(HandlerOutcome::retry())` 表示重新入队。批量函数体用
`Err(Vec<HandlerOutcome>)` 逐个元素结算。

在 `subscriber(..)` 和 `.build()` 之间，这条链接受和属性子句相同的设置：`.name`、`.workers`、
`.on_failure` 和 `.batch`。文档也在这里控制：启用 `asyncapi` feature 时，一条注册默认进入文档，
`.describe(..)` 设置它的描述，`.undocumented()` 把它排除在外（参见
[AsyncAPI](asyncapi.md#payload-schemas)）。

处理器需要宏表达不了的状态时（带字段的结构体处理器），或者 `macros` feature 没有启用时，才需要手
写形态。其余情况下，属性宏的维护成本更低。

## 发布者

产出回复的处理器就是一个发布者。见[发布与回复](publishing.md)。
