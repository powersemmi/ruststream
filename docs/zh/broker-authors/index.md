# 编写一个 Broker

Broker 是一个实现了核心 trait 的独立 crate。它以关闭默认 feature 的方式依赖 `ruststream`，因此只引入
trait 接口和运行时，既不会带上自带的 JSON 编解码器，也不会带上任何别的 Broker：

```toml
[dependencies]
ruststream = { version = "0.7", default-features = false }
```

本页就是这份契约。实现必需的 trait，暴露你自己的 `Config`，为你的 Broker 支持的功能补上能力 trait，
然后用 [conformance 校验套件](conformance.md)证明结果。想看一份基于真实客户端的完整实现，参见
[NATS 完整示例](example-nats.md)。

## 必需的 trait

### `Broker` 与 `ConnectedBroker`

Broker 只负责生命周期，而生命周期是一串消费 `self` 的状态转移：每个状态都是不同的类型，因此顺序错乱的
调用无法通过编译。Broker 本身既不携带订阅者类型，也不携带发布者类型，所以同一个应用里可以混用不同种类
的 Broker。

<!-- inline-rust: simplified contract sketch of the real RPITIT traits in src/broker.rs (which carry Send bounds and rustdoc); a compiled copy would just duplicate the source with more noise -->
```rust
pub trait Broker: Send + Sync + Sized {
    type Error: std::error::Error + Send + Sync + 'static;
    type Connected: ConnectedBroker;
    async fn connect(self) -> Result<Self::Connected, Self::Error>;
}

pub trait ConnectedBroker: Send + Sync + Sized + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    type Closed: Send;
    async fn shutdown(self) -> Result<Self::Closed, Self::Error>;
}
```

`shutdown` 不得阻塞，也不得 panic；所有可失败的拆解工作都在这里做完，并返回一个 `Result`。`Closed`
见证值没有任何发布或订阅接口；把拆解过程的诊断信息（flush 结果、丢弃计数）当作普通数据放进去，或者
直接用 `()`。

构造过程是**同步且不做 I/O 的**：`new(addrs)` 只记录配置，所有网络工作都发生在 `connect` 里（由运行时
在启动时调用一次），而已连接形态直接持有活的客户端，它自身的操作永远不必检查“也许已连接”的状态。
Broker 还可以额外保留一个由 `connect` 填充的共享单元（或者像内存 Broker 那样，保留一份可共享的进程内
状态），这样在应用还在组装、`connect` 尚未运行时就能先把发布者发出去；该单元服务的是那些提前拿到的
句柄，而不是已连接形态。[NATS 示例](example-nats.md)展示的就是基于单元的变体。
[conformance 校验套件](conformance.md)会端到端地证明这道阶梯。

在一个已经关闭的 Broker 上，根本没有发布或订阅方法可调用，所以持有者一侧的误用通不过编译。别名仍是
一条运行时规则：与连接互为别名的句柄（从已连接形态发出去的发布者、可共享 Broker 的克隆）在关闭之后
使用时必须报错，绝不能在一条已死的连接上悄悄地返回成功。生命周期检查同样会走到这条路径。

### `Subscribe`

在已连接形态上实现 `Subscribe`，即可支持按名字订阅。`#[subscriber("name")]` 用的就是它。

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/capability.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait Subscribe: ConnectedBroker {
    type Subscriber: Subscriber;
    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error>;
}
```

### `Subscriber`

订阅者是一个由到达消息组成的 `Stream`。背压由流天然提供。

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/subscriber.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait Subscriber: Send {
    type Message: IncomingMessage;
    type Error: std::error::Error + Send + Sync + 'static;
    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_;
}
```

`stream` 取的是 `&mut self`，因此两次 poll 之间缓冲的状态都存放在该可变借用之后，
从而保证了它的取消安全。

### `IncomingMessage`

一条投递到的消息会暴露自己的载荷和消息头，并以 ack 或 nack 结算。ack 会消费 `self`，因此两次 ack 是
编译错误。

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/message.rs, with the defaulted methods annotated inline for teaching; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait IncomingMessage: Send + Sync {
    fn payload(&self) -> &[u8];
    fn headers(&self) -> &HeaderMap;
    async fn ack(self) -> Result<(), AckError>;
    async fn nack(self, requeue: bool) -> Result<(), AckError>;

    // Defaulted: a plain nack(true). Override when the transport has native
    // delayed redelivery (JetStream NAK with delay); handlers reach it through
    // HandlerResult::retry_after.
    async fn nack_after(self, delay: Duration) -> Result<(), AckError>;

    // Defaulted: None. Override (with the Partitioned capability) to feed the
    // runtime's keyed worker lanes, workers(n, by_key).
    fn partition_key(&self) -> Option<&[u8]>;
}
```

这两个带默认实现的方法一个都不覆盖的 Broker，仍然能配合运行时的每一项功能：`retry_after` 退回为立即
重新入队，按键分道则会轮转那些没有键的消息。

### `Publisher`

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/publisher.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait Publisher: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;
    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error>;

    /// 带默认实现：这个发布者垫在每次发布下面的消息头。
    fn base_headers(&self) -> Option<&HeaderMap> { None }
}
```

`OutgoingMessage` 借用自己的名字和载荷，因此发布不会强制带来一次分配。

这是发布接口，而不是服务代码要写的那一个：应用通过构建器发布
（`publisher.message(&value).publish()`、`publisher.raw(&bytes).to(dest).publish()`），由构建器解析
目的地、编解码器和消息头，然后恰好调用一次该方法。实现 `publish`，整个构建器就随之而来；没有别的
东西要提供。

一个为一整串消息携带同一个参数的发布者（租户、分区提示、你的 Broker 用消息头表达的某个投递选项），
应当把该参数从 `base_headers` 返回，而不是在 `publish` 内部写进消息里。构建器会以这层底作为出站
映射的起点，再把调用点的消息头逐个键覆盖上去，因此调用点取胜（参见
[消息头从哪里来](../guides/publishing.md#where-the-headers-come-from)）。`Transaction` 上有同样带
默认实现的方法，因此从这样的发布者开启的事务行为完全一致。没有东西要补的发布者两个都不必实现。

### `PublishPolicy`

Broker 的发布者是一份策略（一个 exchange、一个队列超时、一个事务 id）加上活连接的组合。就沿着这条缝
把它拆开：提供一个可以随处构造的**策略**类型，它只带构建器选项，不带任何发布接口；再实现
`PublishPolicy`，把它与已连接形态配对成活的发布者。对那些在发布者激活时要真干活的 Broker（初始化
一个事务性 producer），配对是异步且可失败的；对大多数 Broker 而言，它只是一次廉价的构造调用。

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/publisher.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait PublishPolicy<C: ConnectedBroker> {
    type Live; // the live publisher (or live wiring form, for combinator stacks)
    async fn pair(self, connected: &C) -> Result<Self::Live, PairError>;
}
```

这里的错误类型是做了类型擦除的 `PairError`：用 `PairError::new` 包住你的 Broker 的失败。配对在启动时
对每个发布者只做一次，绝不出现在热路径上。

为每一种真正意义上的发布**模式**提供一对策略与活形态，并且让模式的选择成为策略类型之间的转移，而不是
一个运行时标志：普通策略配对出普通的发布者，而 `transactional_id(..)` 这一步构建器调用会转移到另一个
独立的事务性策略类型，其活形态实现 `TransactionalPublisher`，于是普通发布者上压根没有事务接口。内存
Broker 的 `MemoryPublish` / `MemoryRequest` 是最小的参考实现（没有选项，所以它们是单元标记类型）；
核心提供的类型化组合子以函子的方式实现 `PublishPolicy`，于是用户可以在你的策略配对之前，在它之上
组合编解码器和变换。

如果普通策略用默认值就能用（多数如此），那就在已连接形态上再实现 `DefaultPublish` 来指明它。随后，
挂载一个不带显式 `.publisher(..)` 的 `publish("dest")` 处理器时，运行时就会构造出默认的回复发布者：
只写 `b.include(def)` 也能编译通过。发布者总是需要显式选项的 Broker 不实现它，它们的用户要在每次
注册时附上一个策略。

<!-- inline-rust: simplified contract sketch of the real trait in src/publisher.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait DefaultPublish: ConnectedBroker {
    type Policy: PublishPolicy<Self> + Default + Send + 'static;
}
```

## 订阅来源 { #subscription-sources }

`Subscribe` 覆盖的是按名字订阅的情形。当一次订阅需要 Broker 专有的选项（一个消费者组、一个持久化名称、
一份投递策略）时，就暴露一个实现了 `SubscriptionSource` 的描述符类型：

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/subscription.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait SubscriptionSource<C: ConnectedBroker> {
    type Subscriber: Subscriber;
    fn name(&self) -> &str;
    fn subscribe(self, connected: &C) -> impl Future<Output = Result<Self::Subscriber, C::Error>> + Send;
}
```

给描述符配一个关联构造函数（`OrdersStream::new(..)`），而不是一个自由函数，这样用户就能在属性里直接
写出它的名字：`#[subscriber(OrdersStream::new("orders", "workers"))]`。宏会从这次构造调用里读出类型，
并且只要每个方法都返回 `Self`，也接受在它之上的构建器链式调用
（`#[subscriber(OrdersStream::new("orders").durable("workers"))]`）。由于 `type Subscriber` 定义在源
上，一个 Broker 可以提供多种订阅方式（pub/sub 与流），各带不同的订阅者类型；也可以像
[NATS 示例](example-nats.md)那样，用一个在内部分支的描述符把它们全部承载起来。

给描述符派生 `Clone`：它是配置，挂载点会为每次注册重新构造它，这样同一个定义可以挂到两个 Broker 上。

### 用一个字符串命名一种订阅方式

如果一种订阅方式除了名字之外没有别的标识信息，那它还会实现 `FromName`，其唯一的构造函数用该名字把
它构造出来：

<!-- inline-rust: one-impl sketch against a broker-crate descriptor that has no in-repo compiled home -->
```rust
impl FromName for OrdersStream {
    fn from_name(name: impl Into<Cow<'static, str>>) -> Self {
        Self::new(name)
    }
}
```

于是 `#[subscriber(OrdersStream)]` 就合法了：属性固定了订阅方式，值则由挂载点提供。如果一种方式确实
需要不止一个名字才能成立（既要一个主题，*又*要一个订阅名），它就不实现 `FromName`，于是这种写法对它
无法通过编译。

### 用你自己的词汇表达配置

核心无从知道一次订阅还有流、持久化名称或消费者组这些东西，所以它只暴露一个钩子：`map_source`，一个作用在
挂载点正在构造的源之上的变换；而你的 crate 在它之上叠加自己的 trait，并约束到你自己的源类型：

<!-- inline-rust: the extension-trait shape against a broker-crate descriptor with no in-repo compiled home -->
```rust
pub trait NatsSubscriber {
    fn jetstream(self, stream: impl Into<String>) -> Self;
    fn durable(self, name: impl Into<String>) -> Self;
}

impl<Def, W, F, P> NatsSubscriber for SubscriberBuilder<Def, SubscribeOptions, (W, F, P)> {
    fn jetstream(self, stream: impl Into<String>) -> Self {
        self.map_source(|source| source.jetstream(stream))
    }
    // ..
}
```

对源类型的 trait 约束意味着，这些方法在别的 Broker 的构建器上根本不存在。用户像用任何扩展 trait
那样导入它，就能用到这些方法。下文中 `Out` 槽位的词汇采用的也是同一种扩展形态。

## 能力 trait

只实现你的 Broker 真正支持的能力；它们都不属于必需接口。

| trait | 适用于支持这些能力的 Broker |
|---|---|
| `BatchSubscriber` | 批量接收消息 |
| `TransactionalPublisher` | 在句柄上围绕发布做 begin / commit / abort |
| `OwnedTransactions` / `Transaction` | 缓冲区存放在值里的事务，同一个句柄上可同时开启任意多个 |
| `RequestReply` | 原生的请求-响应（NATS 有，Kafka 没有） |
| `Partitioned` | 出站消息上的分区键 |
| `Seekable` / `Seeker` | 在可重放的日志中重新定位一个活的订阅 |
| `Positioned` | 能报告自身日志位置的投递 |
| `DescribeServer` | 为 AsyncAPI 报告一个 `ServerSpec` |

`Seekable` 会在流借用订阅者之前铸出它的 `Seeker` 句柄，因此可以从分发循环之外重新定位一个正在运行的
订阅。位置由 Broker 自己拥有（在你自己的类型上提供 `KafkaPosition` 风格的构造函数）。通过
`Positioned::position` 从一条已投递消息上捕获的位置带有钉住的契约：定位到它会精确地重新投递那一条
消息；而构造出来的位置则保持你的位置类型所记载的语义。写清楚一次定位的作用范围（一个消费者实例，
还是一个共享的组游标），并重置这次重新定位所作废的一切 ack 记账。

### 扩展 `Out` 槽位的词汇

处理器参数 `Out<impl X, Marker>` 接受运行时的 `SlotPublisher` 包装器实现了的任意 `X`；核心会转发它
自己的那套能力（`Publisher`、`TransactionalPublisher`、`OwnedTransactions`、`RequestReply`）。当你
配对出来的值提供的能力不止于此，或者它根本就不是发布者（一个按分区的 producer 缓存、一个分片路由器）
时，就声明你自己的能力 trait，为该值实现它，再用一个通过 `SlotPublisher::inner` 转发的全覆盖实现把
它嫁接到包装器上。此后处理器就用你的 trait 约束自己的槽位，而具体类型依然不会出现在应用代码里：

=== "宏"

    ```rust
    --8<-- "tests/out_slots.rs:extension"
    ```

=== "手写"

    ```rust
    --8<-- "tests/manual_out_slots.rs:extension"
    ```

通过 `inner` 取出的值所做的发布会绕过测试套件按槽位的记录（就像一个已结算的 owned 事务的缓冲区那样）；
它们仍然会出现在 Broker 的发布日志里。

## 单条投递的上下文与 `Ctx` 键

如果 Broker 有原生的投递元数据（一个分区、一个偏移量、一个流序号），就把它作为类型化的单条投递上下文
暴露出来：一个由订阅者指明的 `#[non_exhaustive]` 结构体，外加若干 `ContextField` 键类型，好让处理器能
用 [`Ctx<K>` 提取器](../guides/context.md#per-delivery-context)把单个字段绑定成参数。键是单元结构体，
值是拥有所有权的。投递路径上既没有 type-map，也没有堆分配。

<!-- inline-rust: sketch; the real trait lives in src/field.rs -->
```rust
/// Per-delivery context of this broker.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct MyContext {
    pub partition: i32,
}

/// `Ctx<Partition>` in a handler binds the delivery's partition.
#[derive(Debug, Default, Clone, Copy)]
pub struct Partition;

impl ContextField for Partition {
    type Context = MyContext;
    type Value = i32;
    fn read(self, src: &MyContext) -> i32 {
        src.partition
    }
}
```

没有任何单条投递字段的 Broker 用 `()`，整节都可以跳过。

## 异步边界上的中间件 { #middleware-on-the-async-edges }

那些需要围绕编码和解码做异步 I/O 的集成（一个 schema 注册表、一层线上格式的信封）不属于 `Codec`：核心
的编解码器是同步的，处理器也应当继续用默认的那一个。把它们放到异步边界上：在订阅的投递路径上转码入站
载荷（赶在编解码器看到它们之前），出站的则用核心的 `PublishLayer` 加上信封，通过
`RustStream::publish_layer` 在应用级别添加。发布层是异步且可失败的，而 `Outgoing::payload_mut` 的存在
正是为了包装信封。

## 配置与默认值

`Config` 归你的 crate 所有；核心不携带任何 Broker 专有的配置。如果某个配置字段没有合理的默认值，就
不要为它实现 `Default`；强迫用户显式设置，好过发出一个日后可能出问题的默认值。

## 错误

用 `thiserror` 写一个 crate 级别的错误枚举，变体按来源划分。公开的错误枚举标记 `#[non_exhaustive]`。
切勿在库 crate 里使用 `anyhow`。

## 测试支持 { #test-support }

在 `testing` feature 下提供一个进程内传输，在它的**已连接形态**上实现 `TestableBroker`（用
`register_testable_broker!` 为该已连接类型注册，因为套件会先连接每一个 Broker，然后才取回它的
传输），这样用户就能用 `TestApp` 测试套件对着你的 Broker 单元测试处理器。该传输**只做核心路由**：把
发布出去的消息分发给匹配的订阅者，并把 ack/nack 当作实质上的空操作。切勿在其中模拟 Broker 专有的语义
（持久游标、重新投递定时器、偏移量、死信路由）；那些要对着一台真实的服务器端到端地验证。

参考实现就是内存 Broker 自己的那一份（在 `ConnectedMemoryBroker` 上）：

```rust
--8<-- "src/memory/mod.rs:testable"
```

该传输在每次把消息入队给某个订阅者时调用 `Coordinator::enqueued`，在结算或丢弃一次投递时调用
`Coordinator::consumed`（这样套件才能判断反应何时尘埃落定），并把延迟的重新投递交给
`Coordinator::schedule_redelivery` 去路由。于是同一个类型既适用于 `TestApp`，也适用于 conformance
校验套件。面向用户的那一侧参见[测试](../guides/testing.md)；用 `run_suite` 和 `lifecycle` 阶梯检查来证明
你的实现，参见 [Conformance](conformance.md)。
