# 编写一个 Broker

Broker 是一个实现了核心 trait 的独立 crate。它依赖 `ruststream` 并关闭默认 feature，因此只拿到 trait
接口和运行时，不带内置的 JSON 编解码器，也不带别的 Broker：

```toml
[dependencies]
ruststream = { version = "0.7", default-features = false }
```

本页就是这份契约。实现必需的 trait，定义你自己的 `Config`，为你的 Broker 支持的功能实现能力 trait，
再用 [conformance 校验套件](conformance.md)验证结果。基于真实客户端的完整实现见
[NATS 完整示例](example-nats.md)。

## 必需的 trait

### `Broker` 与 `ConnectedBroker`

Broker 只负责生命周期。生命周期是一串状态转移：每个状态都是不同的类型，转移消费掉当前状态并交出下一
个状态，因此顺序错乱的调用无法通过编译。Broker 不指定订阅者类型，也不指定发布者类型，所以一个应用可
以混用不同种类的 Broker。

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

`shutdown` 不得阻塞，也不得 panic。所有可能返回错误的资源释放都在这里做完，并返回 `Result`。`Closed`
是关闭的见证：把清理时的诊断信息（缓冲区刷新结果、丢弃计数）当作普通数据放进去，或者直接用 `()`。

构造是**同步且不做 I/O 的**：`new(addrs)` 只记录配置。所有网络工作都发生在 `connect` 里，运行时在启
动时调用它一次。已连接形态直接持有活的客户端，因此它自身的操作不必检查“是否已经连接”。

Broker 还可以额外持有一个由 `connect` 填充的共享单元，或者像内存 Broker 那样持有可共享的进程内状态。
这样在应用还在组装、`connect` 还没运行的时候，就可以先把发布者交出去：该单元服务的是这些提前拿到的
句柄，而不是已连接形态。

[conformance 校验套件](conformance.md)会验证整条转移链，[NATS 示例](example-nats.md)则在真实客户端上
走完这条链。

在一个已经关闭的 Broker 上，没有发布或订阅方法可以调用，所以持有者一侧的误用通不过编译。共用连接只能
在运行时检查：共用它的句柄（已连接形态交出去的发布者、可共享 Broker 的克隆）在关闭之后使用时必须返回
错误，绝不能悄悄返回成功。`lifecycle` 检查也会走到这条路径。

内存 Broker 用几行就走完整条转移链。本页下面的每一段示例也都出自同一个文件，契约一改，页面上的代码就
跟着改：

```rust
--8<-- "src/memory/mod.rs:ladder"
```

`ClosedMemoryBroker` 就是上面说的那种带清理诊断的见证：它报告这次关闭移除了多少个订阅者注册。

### `Subscribe`

在已连接形态上实现 `Subscribe`，服务就能按主题、subject 或队列的名字订阅。`#[subscriber("name")]` 用
的就是它。

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/capability.rs, with the defaulted method annotated inline for teaching; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait Subscribe: ConnectedBroker {
    type Subscriber: Subscriber;
    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error>;

    // 默认 None。按订阅名发布就能到达用这个名字打开的订阅时，把名字本身返回：
    // subject、topic、流和队列名通常就是这样。
    fn redelivery_address(&self, name: &str) -> Option<RedeliveryAddress>;
}
```

要做的只有建立一条订阅，再说出发布按哪个地址能重新到达它：

```rust
--8<-- "src/memory/mod.rs:subscribe"
```

第二个答案就是运行时发布延后重试用的地址，它决定了 `#[subscriber("orders")]` 在你的 Broker 上能不
能和 `BrokerScope::retry_via` 一起用。订阅名不是发布地址的地方，保留默认值：Google Pub/Sub 的订阅
按自己的名字订阅，发布走它背后的 topic，那里由描述符来回答。

### `Subscriber`

订阅者是入站消息的 `Stream`。背压由 `Stream` 本身给出。

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/subscriber.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait Subscriber: Send {
    type Message: IncomingMessage;
    type Error: std::error::Error + Send + Sync + 'static;
    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_;
}
```

`stream` 取 `&mut self`，因此两次 poll 之间缓冲的状态都存放在这个可变借用后面，取消安全正来自这
一点。

### `IncomingMessage`

一条投递过来的消息交出自己的载荷和消息头，并由 ack 确认或由 nack 退回。ack 消费 `self`，因此两次 ack
是编译错误。

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/message.rs, with the defaulted methods annotated inline for teaching; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait IncomingMessage: Send + Sync {
    fn payload(&self) -> &[u8];
    fn headers(&self) -> &HeaderMap;
    async fn ack(self) -> Result<(), AckError>;
    async fn nack(self, requeue: bool) -> Result<(), AckError>;

    // Defaulted: false. The runtime reads this first and never calls
    // nack_after without it, so override the pair together.
    fn supports_nack_after(&self) -> bool;

    // Defaulted: AckError::Unsupported. Override when the transport has native
    // delayed redelivery (JetStream NAK with delay); handlers reach it through
    // HandlerOutcome::retry_after.
    async fn nack_after(self, delay: Duration) -> Result<(), AckError>;

    // Defaulted: None. Override (with the Partitioned capability) to feed the
    // runtime's keyed worker lanes, workers(n, by_key).
    fn partition_key(&self) -> Option<&[u8]>;
}
```

延迟重新投递由两个方法组成，运行时问的是 `supports_nack_after`。只覆盖 `nack_after`，这个标志仍然是
`false`，运行时一次也不会调用这个覆盖。`nack_after` 的默认实现返回 `AckError::Unsupported`，而不是按
一次普通的 `nack(true)` 结算：留不住消息的传输必须说出这一点，否则一次退避就变成一场重新投递的风暴。

这三个带默认实现的方法一个都不覆盖的 Broker，仍然能配合运行时的每一项功能。没有原生延迟重新投递的地
方，`retry_after` 由运行时自己完成：它丢弃这次投递，并在延迟之后发布一份副本，用的是应用通过
`BrokerScope::retry_via` 接上的那个发布者，同时把重试计数消息头加一。这份副本发往
[你的订阅给出的地址](#where-a-deferred-retry-is-published)。只有在没有这个发布者的时候，延迟才退化
成立即重新入队。按键分道的工作者池轮流分发没有键的消息。

“什么都不覆盖”会得到什么，没有哪个 Broker 可以拿来演示：这个工作区里的 Broker 个个都覆盖了这三个方
法。所以这份行为由核心的一个测试固定下来：

```rust
--8<-- "src/message.rs:incoming_defaults"
```

正是 `Unsupported` 这个答复让运行时分得清两种情况：传输没有延迟投递，还是延迟已经生效。分清之后，它
才走自己的备用路径。

### `Publisher`

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/publisher.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait Publisher: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    /// 你的 Broker 的逐条消息设置。每个字段都是可选的；没有这类设置就写 `()`。
    type Options: Send + Sync;

    async fn publish(
        &self,
        msg: OutgoingMessage<'_>,
        options: Option<&Self::Options>,
    ) -> Result<(), Self::Error>;

    /// 带默认实现：这个发布者垫在每次发布下面的消息头。
    fn base_headers(&self) -> Option<&HeaderMap> { None }
}
```

`OutgoingMessage` 借用自己的名字和载荷，因此发布不强制分配内存。

服务写的不是这个方法，而是构建器：`publisher.message(&value).publish()` 选定目的地、编解码器和消息
头，然后恰好调用一次 `publish`。实现 `publish`，整个构建器就在它之上工作起来。

`Options` 装的是属于消息而不属于句柄的东西：QoS、优先级、排序键、过期时间。每个字段都是可选的，因为
一次调用只带上它改动过的部分。它没碰的部分，就是策略在配出这个发布者时定下的值，把两者合起来是你的
`publish` 要做的第一件事。没有逐条设置的 Broker 写 `type Options = ();`。

在没有调用点可以改动设置的路径上，`options` 是 `None`：处理器的回复、延迟重投。那里由策略的设置说了
算。

`base_headers` 留给发布者自身的常量：租户、producer 名字、这个句柄每条消息都带的 schema id。构建器以
这份基础消息头为起点，再把调用点的消息头逐个键写在上面，所以同一个键上留下的是调用点的值
（参见[消息头从哪里来](../guides/publishing.md#where-the-headers-come-from)）。

`Transaction` 指定同一个 `Options`，也带同样的默认 `base_headers`，因此事务里的消息和事务外的消息带
着同样的设置离开。没有东西要补的发布者两个都不必实现。

### `PublishPolicy`

Broker 的发布者由两部分组成：一份策略（一个 exchange、一个队列超时、一个事务 id）和一条活连接。提供
一个单独的**策略**类型：它在任何地方都能构造，持有构建器选项。

在它上面实现 `PublishPolicy`：策略在已连接形态上构造出活的发布者，而 `pair` 就是这个构造函数。它是异
步的，也可以返回错误，需要初始化事务性 producer 的 Broker 就在这里做这件事。

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/publisher.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait PublishPolicy<C: ConnectedBroker> {
    type Live; // the live publisher (or live wiring form, for combinator stacks)
    async fn pair(self, connected: &C) -> Result<Self::Live, PairError>;
}
```

错误类型是做了类型擦除的 `PairError`：用 `PairError::new` 包住你的 Broker 自己的错误。策略在启动时把
发布者实例化一次，因此 `pair` 不会落到热路径上。

为每一种真正意义上的发布**模式**提供一对策略和活形态，并且让模式的选择成为策略类型之间的转移，而不是
一个运行时标志。普通策略构造出普通的发布者，而 `transactional_id(..)` 这一步构建器调用把它转成另一个
独立的事务性策略类型，它的活形态实现 `TransactionalPublisher`。于是普通发布者上根本没有事务接口。

最小的参考实现是内存 Broker 的 `MemoryPublish` 和 `MemoryRequest`：它们没有选项，所以是空结构体。

核心的类型化组合子以函子的方式实现 `PublishPolicy`，所以用户可以在策略构造出发布者之前，先在它之上组
合编解码器和变换。

如果普通策略用自己的默认值就够用（几乎总是如此），就在已连接形态上再实现 `DefaultPublish`，在那里指
名这个策略。这样，带 `publish("dest")` 的处理器在没有显式 `.out(Reply, ..)` 的情况下挂载，运行时自己
就把回复用的发布者实例化出来，只写 `b.include(def)` 也能编译。发布者总是需要显式选项的 Broker 不实现
`DefaultPublish`，它们的用户在每次注册处理器时指定策略。

<!-- inline-rust: simplified contract sketch of the real trait in src/publisher.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait DefaultPublish: ConnectedBroker {
    type Policy: PublishPolicy<Self> + Default + Send + 'static;
}
```

下面是这两半，取自一个策略完全没有选项的 Broker：

```rust
--8<-- "src/memory/mod.rs:publish_policy"
```

## 订阅来源 { #subscription-sources }

`Subscribe` 覆盖的是一个名字就够用的情形。订阅需要你的 Broker 专有的选项（消费者组、持久化名称、投递
策略）时，定义一个实现 `SubscriptionSource` 的描述符类型：

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/subscription.rs, with the defaulted method annotated inline for teaching; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait SubscriptionSource<C: ConnectedBroker> {
    type Subscriber: Subscriber;
    fn name(&self) -> &str;
    fn subscribe(self, connected: &C) -> impl Future<Output = Result<Self::Subscriber, C::Error>> + Send;

    // 默认 Ok(None)。回答发布按哪个地址能重新到达这条订阅；只有活连接知道时，
    // 就去问 Broker。
    async fn redelivery_address(&self, connected: &C) -> Result<Option<RedeliveryAddress>, C::Error>;
}
```

给描述符一个关联构造函数（`OrdersStream::new(..)`），而不是自由函数：这样用户就能在属性里直接写出
它，`#[subscriber(OrdersStream::new("orders", "workers"))]`。

宏从这次构造调用里读出类型，只要每个方法都返回 `Self`，也接受在它之上的构建器链
（`#[subscriber(OrdersStream::new("orders").durable("workers"))]`）。

`type Subscriber` 声明在来源上，所以一个 Broker 可以提供多种订阅方式（pub/sub 和流），各带不同的订阅
者类型；也可以像 [NATS 示例](example-nats.md)那样，用一个在内部分支的描述符服务全部方式。

给描述符派生 `Clone`：它是配置，挂载点为每次注册重新构造它，所以同一个定义可以同时挂到两个
Broker 上。

### 延后重试发往哪里 { #where-a-deferred-retry-is-published }

没有原生延迟重新投递时，运行时自己兑现 `retry_after`：等延迟过去，它发布一份消息的副本。副本发往哪
里，由你的描述符说出来。

```rust
--8<-- "src/memory/mod.rs:source"
```

返回的名字，要让指向你的 Broker 的发布者用它就能重新到达这条订阅：NATS 上是 subject，Kafka 上是
topic，Redis 上是流的键。在 Google Pub/Sub 上两者都不是：订阅和 topic 在那里是两种资源，答案是订
阅所绑定的那个 topic，描述符要向 API 问出来。运行时只在启动时问一次，所以这里的一次请求不摊到每条
消息上。

发布根本到不了你的订阅时，保留默认值。这样，把重试发布者接到这种订阅上的应用就起不来，并报出是哪条
订阅、来自哪个来源，而不是把每条延后的消息发到没人读的地址上。

`harness::lifecycle` 会按你给出的答案检查：发往所报地址的一次发布，必须到达报出它的那条订阅。

### 用一个字符串命名一种订阅方式

只由一个名字确定、再没有别的标识的订阅方式，还会实现 `FromName`：它唯一的构造函数用这个名字构造出
值。

```rust
--8<-- "src/memory/mod.rs:from_name"
```

于是 `#[subscriber(OrdersStream)]` 就合法了：属性指定订阅方式，值由挂载点补上。确实需要不止一个名字
才能成立的方式（既要一个主题，*又*要一个订阅名）不实现 `FromName`，这种写法对它就通不过编译。

### 用你自己的词汇表达配置

核心不知道订阅还有流、持久化名称或消费者组，所以只给出一个钩子：`map_source`，一个作用在挂载点正在
构造的来源之上的变换。你的 crate 在它之上叠加自己的 trait，并约束到你自己的来源类型：

<!-- inline-rust: the extension-trait shape against a broker-crate descriptor with no in-repo compiled home -->
```rust
use ruststream::runtime::{Declared, SubscriberBuilder, SubscriberSettings};

pub trait NatsSubscriber {
    fn jetstream(self, stream: impl Into<String>) -> Self;
    fn durable(self, name: impl Into<String>) -> Self;
}

// 四个状态槽位依次是（工作者、失败策略、起始位置、批大小）；`Codec` 是这次注册自己的解码覆盖，
// 在没人指定之前是 `()`。两者都原样传递下去。
impl<Def, Workers, Failures, StartPosition, Batch, Codec> NatsSubscriber
    for SubscriberBuilder<Def, SubscribeOptions, (Workers, Failures, StartPosition, Batch), Codec>
where
    Def: Declared,
{
    fn jetstream(self, stream: impl Into<String>) -> Self {
        self.map_source(|source| source.jetstream(stream))
    }

    fn durable(self, name: impl Into<String>) -> Self {
        self.map_source(|source| source.durable(name))
    }
}
```

对来源类型的约束意味着，这些方法在别的 Broker 的构建器上根本不存在。下文 `Out` 槽位的词汇用的也是同
一种扩展形态。

有一项核心设定改变的不是状态槽位，而是来源类型本身：`start_at(..)` 把描述符包进
`StartAt<SubscribeOptions, Position>`。于是恰恰在指定了起始位置的订阅上，你的方法不在作用域里，这种
情形由第二个针对包装后来源的 impl 补上。`StartAt::map_inner` 取出里面的描述符，并原样交还位置，所以
每个方法仍旧只有一行：

<!-- inline-rust: the second extension impl against the same broker-crate descriptor, which has no in-repo compiled home -->
```rust
use ruststream::StartAt;
use ruststream::runtime::Fixed;

// 这里起始位置槽位按构造必然是 `Fixed` - 这个包装正是 `start_at(..)` 造出来的 - 而且源类型也
// 不同，所以这个 impl 与上面那个永远不会重叠。
impl<Def, Workers, Failures, Batch, Codec, Position> NatsSubscriber
    for SubscriberBuilder<
        Def,
        StartAt<SubscribeOptions, Position>,
        (Workers, Failures, Fixed, Batch),
        Codec,
    >
where
    Def: Declared,
{
    fn jetstream(self, stream: impl Into<String>) -> Self {
        self.map_source(|source| source.map_inner(|inner| inner.jetstream(stream)))
    }

    fn durable(self, name: impl Into<String>) -> Self {
        self.map_source(|source| source.map_inner(|inner| inner.durable(name)))
    }
}
```

### 用你自己的词汇表达发布者配置

发布这一侧是对称的。挂载点用 `.out(marker, policy)` 指定发布策略：标记 `Reply` 对应带
`publish("dest")` 的处理器返回的值，槽位的标记对应 `Out` 槽位。`MapPublisher` 就是作用在这个位置所持
策略之上的钩子：

<!-- inline-rust: the extension-trait shape against a broker-crate policy with no in-repo compiled home -->
```rust
use ruststream::runtime::MapPublisher;

pub trait NatsPublish {
    fn stream(self, name: impl Into<String>) -> Self;
    fn expect_last_sequence(self, seq: u64) -> Self;
}

impl<T: MapPublisher<Policy = Publish>> NatsPublish for T {
    fn stream(self, name: impl Into<String>) -> Self {
        self.map_publisher(|policy| policy.stream(name))
    }

    fn expect_last_sequence(self, seq: u64) -> Self {
        self.map_publisher(|policy| policy.expect_last_sequence(seq))
    }
}
```

在服务里读起来是这样：

<!-- inline-rust: the call shape against the broker policy sketched above -->
```rust
b.include(confirm).out(Reply, Publish).stream("ORDERS");
b.include(mirror).out(Audit, Publish).stream("AUDIT").build();
```

约束落在策略上，而不是落在链上，所以一份实现同时适用于回复位置、每一个槽位、路由器和 Broker 作用域。

`map_publisher` 把策略换成同一类型的策略。换成另一种策略类型意味着另一种发布模式，它的位置在
`.out(marker, policy)` 调用本身。已经配置好的值也可以直接传到那里：
`.out(Reply, Publish::default().stream("ORDERS"))`。

### 发布构建器上的逐条设置 { #per-message-settings-on-the-publish-builder }

一条消息与下一条可以不同的设置 - 一个 QoS、一个优先级、一个排序键、一个过期时间 - 是你的
`Publisher::Options` 的一个字段，调用点用你加到发布构建器上的步骤去改它。发布者不被任何东西包裹，
所以这次发布仍然从挂载点自己的条目走出去，带着那个条目指定的编解码器和变换。

一共四块：一个每个字段都可选的设置类型，一份携带默认值的策略，一个把两者合起来的活发布者，以及一个
以设置类型为约束、写在 `PublishBuilder` 上的扩展 trait。正是这个约束让你的步骤出现不了在别的 Broker
的发布者的构建器上：

```rust
--8<-- "tests/publish_options.rs:broker_side"
```

服务用哪种方式挂载都不影响 Broker 这一半：两条路上它都是普通的 trait 实现。把扩展 trait 从你的
prelude 导出，就放在策略别名旁边。

逐条设置只有步骤这一种形状。不要把发送放进 trait：从你自己的值走出去的发布，槽位视图不再看得见，而
排序键这类设置恰恰是测试要断言的东西。也不要用消息头携带它：它是协议字段，在一个进程内绕一圈字符串
消息头不是协议字段。

你的 Broker 满足不了的值是一次发布错误，而不是悄悄退回默认值：调用方要的那个顺序，它拿不到。

## 能力 trait

只实现你的 Broker 真正支持的能力，它们都不属于必需接口。最接近必需的是 `BatchSubscriber`：
[能提供的地方就提供它](#batches-batchsubscriber)，因为每个批量处理器都要它，而自身没有批量能力的
传输照样可以在客户端攒批。

| trait | 适用于支持这些能力的 Broker |
|---|---|
| `BatchSubscriber` | 按批接收消息 |
| `TransactionalPublisher` | 在句柄上围绕发布做 begin / commit / abort |
| `OwnedTransactions` / `Transaction` | 同一个句柄上同时开启任意多个事务，每个各带自己的缓冲区 |
| `RequestReply` | 做原生的请求-响应 |
| `Partitioned` | 给出站消息设定分区键 |
| `Seekable` / `Seeker` | 在可重放的日志中重新定位一个活的订阅 |
| `Positioned` | 报告一次投递在日志中的位置 |
| `DescribeServer` | 为 AsyncAPI 报告一个 `ServerSpec` |

`Seekable` 在 `stream` 借用订阅者之前交出 `Seeker` 句柄，因此可以从分发循环之外重新定位一个正在
运行的订阅。

位置由 Broker 自己拥有：`KafkaPosition` 风格的构造函数由你在自己的类型上声明。通过
`Positioned::position` 从一条已投递消息上取到的位置确定了一条契约，定位到它会精确地重新投递这条
消息。构造出来的位置，语义由你的位置类型自己写明。

写清楚一次定位的作用范围（一个消费者实例，还是一个共享的组游标），并重置这次定位所作废的一切
ack 记账。

要让处理器主体能够定位，就把投递位置和订阅的 seeker 放进投递上下文的字段，并为它们发布
`ContextField` 键。范本是内存 Broker 的 `MemoryContext` 及其 `Position` 和 `SeekHandle` 键。批量的
那些写法从下面的批量上下文拿到 seeker，那里没有位置。

`DescribeServer` 给出的服务器描述，报告客户端所连接的主机和端口。凭据绝不出现在其中，因为这份文档
就是为了发布而生成的。因此，用 URL 配置的 Broker 通过 `ServerSpec::from_url` 构建描述，它会去掉
URL 里的用户名和密码，而不是只去掉协议前缀。配置了多个地址的 Broker，用 `ServerSpec::host_from_url`
把它们拼起来。

这些 trait 就是处理器主体所写的词汇。主体用它需要的那项能力约束自己的槽位
（`Out<impl TransactionalPublisher, Journal>`，手动路径上是 `where W: TransactionalPublisher`），
从不写你的任何类型。挂载点在编译期按这个约束检查一次所绑定策略的活形态。

四种发布者能力，每一种在 arena 条目上都有自己的类型化形态：发布构建器、事务作用域、拥有式事务、带
关联的请求。这些形态建立在挂载点的编解码器和标记的字典之上。服务要用到它们，只需你在活的发布者上
实现对应的 trait。

### 批：`BatchSubscriber` {#batches-batchsubscriber}

接受 `&[T]` 的处理器消费的是一批消息，挂载点为它写下一个数字，也就是批大小。运行时把这个数字直接
传给 `BatchSubscriber::batches(size)`。你的订阅者交出的批，就是处理器主体看到的批：运行时既不拆分
也不合并，所以一批里绝不会超过 `size` 条消息，传输手上只有更少的消息时就更短。

把 `size` 对应到你的客户端已有的说法上：`XREADGROUP COUNT`、JetStream 的 pull 批、Kafka 的 poll
上限。至于一批怎么攒出来，其余的事（阻塞超时、消费者组、预取窗口）仍归你自己的词汇，通过你的设置
扩展 trait 配置在订阅来源上。服务于是写成
`b.include(handler.batch(nonzero!(6)).block(Duration::from_secs(5)))`：核心的词在前，你的词在后。

把这项能力放到挂载能够到的每一个订阅者上，而不只是你自己的描述符打开的那一个。
`#[subscriber("topic")]` 走的是 `Subscribe`，所以这种写法下的 `&[T]` 主体，要的是
`Subscribe::Subscriber` 上的 `BatchSubscriber`。

只把能力挂在自家描述符订阅者上的 crate，会让字符串字面量那种写法编译不过。两者是同一个类型时无事
可做，类型不同时两边都要有。

传输一次只投递一条消息时，也要实现这项能力，用核心的 `BufferedSubscriber` 在客户端攒批：它的
`batches` 遵守拿到的批大小。批大小不由你选，让不满的批提前结束的那个截止时间由你选，而且它不必是
常量。

把这个截止时间放到你的订阅描述符上（`.max_wait(Duration::from_millis(25))`），在订阅打开时交给
包装器，这样服务可以逐个订阅去调。10 ms 的默认值是照进程内总线定的：中间一旦隔着一次网络往返，
大多数批会在一条投递上就结束，所以把这个截止时间做成描述符选项的 Broker crate 落在 10 到 50 ms
之间。

订阅者的其余部分原样穿过这层包装：

```rust
--8<-- "tests/batch_subscriber.rs:buffered_capability"
```

挂载点看不出你走的是两条路里的哪一条：服务写下批大小，就拿到批。

在攒批会破坏传输自身某项保证的地方，不提供这项能力同样是正当的答复。ZeroMQ 的 ROUTER 是现实中的
例子：它按每个对端各自的 `reply-to` 回复，而一整批只对应一个 `PublishContext`，于是这一批的回复都
会发到某一个对端的地址上。在你的 crate 文档里写明这一点：`&[T]` 主体在那种传输上编译不过。

`conformance` 的批量套件会检查这项契约：它用小于一轮消息总数的批大小打开订阅，返回的批更长的
Broker 通不过。它不在 `harness::run_suite` 之内：能力套件由你自己调用，实现了哪项能力就调哪一个。

### 你的 crate 要提供的 prelude { #broker-prelude }

你的类型由挂载点来写，而不是主体，这正是你的 crate prelude 的用处。提供一个 `prelude` 模块，按这个
顺序分三层：

1. `pub use ruststream::prelude::*;`，让一个 glob 就能服务整个文件；
2. 服务会写到的、你自己的那部分表面：Broker、它的订阅来源、它的 `Config`、它的错误，以及主体会读的
   `ContextField` 键；
3. 你的发布策略，用每个 Broker 都用的那套统一名字：`Publish`，以及在你有的时候还有
   `TransactionalPublish` 和 `Request`（`pub use crate::KafkaTransactionalPublish as
   TransactionalPublish;`）。再把你在活值上实现的能力 trait 作为一份清单加进去，这样带来策略的那个
   glob 也会把它们的操作带进作用域。

核心 prelude 在这三个名字下什么都不导出，所以挂载点在哪个 Broker 上读起来都一样。切勿把策略取名成
核心 trait 的名字（`Publisher`、`TransactionalPublisher`、`OwnedTransactions`、`RequestReply`），也
不要在这些名字下重导出别的东西：同时 glob 了两个 prelude 的主体，必须仍然把这些名字解析成核心
trait。

清单指的是你的 glob 添加的那部分，也就是主体经由你的 Broker 才够得到的消费侧 trait：`Positioned`、
`Seeker`、`Transaction` 之类。四个发布者能力已经在核心 prelude 里，再导出一遍什么也不改变。

某个 trait 的方法会和核心的默认方法冲突时，就把它留在清单外面（实践中就是
`Partitioned::partition_key` 与 `IncomingMessage::partition_key`），让需要它的服务显式导入。
`BatchSubscriber` 不属于任何清单：调用它的是框架，没有哪个主体会把它写成约束。

可以参照的现成例子是 `ruststream::memory::prelude`。

### 扩展 `Out` 槽位的词汇

处理器参数 `Out<impl X, Marker>` 接受槽位背后那个活值实现了的任意 `X`。在此之上，核心还会转发它
自己的那套能力（`Publisher`、`TransactionalPublisher`、`OwnedTransactions`、`RequestReply`）。活值
提供的能力不止于此，或者它根本就不是发布者（一个按分区的 producer 缓存、一个分片路由器）时，就
声明你自己的能力 trait，并为这个活值实现它。

处理器主体手里拿到的不是那个值，而是 arena 里的条目 `Slot<Marker, W, E, Pipe, Body>`，一扇通向它的
透明窗口。自动解引用能把方法调用送过这扇窗，却送不过 trait 约束：写成
`fn issue<L: Lanes>(lanes: &L)` 的辅助函数会以 `E0277` 拒收这个条目。

在你的 trait 旁边加上一个 blanket 实现
`impl<M, W: Lanes, E, Pipe, Body> Lanes for Slot<M, W, E, Pipe, Body>`，通过条目的 `Deref` 转发，
按能力泛型的辅助函数和主体就能原样接收这个条目。具体类型依然不会出现在应用代码里：

=== "宏"

    ```rust
    --8<-- "tests/out_slots.rs:extension"
    ```

=== "手写"

    ```rust
    --8<-- "tests/manual_out_slots.rs:extension"
    ```

决定 trait 形状的是发送发生在哪里，而形状有两种。

**路由器形状**的能力交出一个发布者，自己从不发送：上面那个按分区的 producer 缓存为某个分片挑出
发布者并把它返回。经这个发布者做的发布走在槽位视图之外，所以测试套件不会把它记到槽位名下，就像
一个已结算的 owned 事务的缓冲区那样。改在 Broker 的发布日志上断言它。这是归属的边界，也是交出
内层发布者所付的代价。

**步骤形状**的能力给一条消息设定一个参数，并以一次发布收尾：一个排序键、一个优先级、一个 QoS。
这一种根本不是能力 trait：它是你的 `Publisher::Options` 的一个字段，加上发布构建器上的一个步骤，
这样发送留在条目自己的路径上，设置也留在消息头之外。参见
[发布构建器上的逐条设置](#per-message-settings-on-the-publish-builder)。

### 你这个 crate 的 prelude

两种文件导入的东西不一样，正是这种分工让服务保持可移植。处理器主体导入 `ruststream::prelude::*`，
不导入你的任何东西：它用核心的能力 trait 去约束注入进来的槽位，也就是 `Out<impl Publisher>`、
`Out<impl TransactionalPublisher>`、`Out<impl OwnedTransactions>`、`Out<impl RequestReply>`，
于是主体说清楚它对发布者有什么要求，却从不说这是哪个 Broker 提供的。

挂载文件导入你的 prelude，因为指名 Broker 的地方就在那里。

只有一个例外：逐条设置。它天生属于某个 Broker，而调用点在主体里，所以改动它的主体为了那个步骤导入
你的 prelude，并在约束里写出你的设置类型（`Out<impl Publisher<Options = MqttOptions>, Telemetry>`）。
这样的主体绑在你的 Broker 上，它的签名也把这一点说了出来。

这样一来，你的 prelude 就是使用你这个 Broker 的服务所写的那一个导入，它的形状因此属于契约的一部分。
策略别名（`NatsPublish as Publish`、`KafkaTransactionalPublish as TransactionalPublish`、
`LapinRequest as Request`）让挂载文件在哪个 Broker 上读起来都一样，换 Broker 就是换一行导入。

你这一半的命名规则是：显式 re-export 会一声不响地盖过 glob，所以一个跟核心 trait 同名的名字，会把
那个 trait 从每个写了这行 glob 的服务手里拿走，而错误出现在服务的文件里，不在你的文件里。

用一个跟在自己 glob 后面的探针把两半都固定下来：主体所写的那个约束仍然必须解析成核心 trait，挂载点
的名字仍然必须是你的策略。

<!-- inline-rust: a compile-time probe that belongs in a broker crate, behind that crate's own prelude glob -->
```rust
// in your crate, behind your own prelude glob
use crate::prelude::*;

// A capability bound a body states: the core trait, not something of yours.
fn _p<T: Publisher>() {}

// A mount-site name: your policy, constructible with no connection in sight.
fn _q() {
    let _: Publish = Publish::default();
}
```

## 单条投递的上下文与 `Ctx` 键

Broker 有原生的投递元数据（一个分区、一个偏移量、一个流序号）时，把它作为类型化的单条投递上下文
暴露出来：一个由订阅者指明的 `#[non_exhaustive]` 结构体，外加若干 `ContextField` 键类型。处理器
按键用 [`Ctx<K>` 提取器](../guides/context.md#per-delivery-context)把单个字段绑定成参数。键是空
结构体，投递路径上既没有 type-map，也没有堆分配。

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

这段草图读的是一个 `Copy` 标量，拥有和借用没有分别。位置类型不是 `Copy` 时（Pulsar 的消息 id、
Kinesis 的分片加序列号字符串），就以借用的方式读：`Field::Value<'a>` 对来源的生命周期是泛型的，
所以键交回的是 `&'a MessageId`，用 `ctx.context(..)` 读它的主体一份都不必复制。

必须拥有所有权且为 `'static` 的只有 `ContextField::Value`，也就是 `Ctx<K>` 提取器背后的那个值，
因为提取器的值在主体运行之前就要绑定好；这个键把借用形态交回的东西克隆一份。一个键通常两个 trait
都实现，各出一种形状。

没有单条投递字段的 Broker 用 `()`。

批量订阅另有自己的上下文，因为一批横跨多次投递。把整条*订阅*共享的东西（seek 句柄、流的名字、
消费者组）攒成第二个结构体，在它上面实现 `BuildBatchContext`，再发布若干 `Field` 键，好让批量主体
用 `ctx.context(..)` 读它。运行时按批构造一个值，取自这一批的第一次投递。

逐次投递的字段不放进去：位置属于某一次投递，所以由批从元素上读。把两个结构体分开，正是让这条规则
在编译期成立的办法：投递上下文不实现 `BuildBatchContext`，批量主体也就写不出它。

范本是内存 Broker 的 `MemoryBatchContext`：订阅的 seeker 用的还是它的投递上下文所发布的那个
`SeekHandle` 键。订阅这一级上没有东西可交的 Broker 什么都不用实现，批量停在 `()` 这个默认值上。

## 异步边界上的中间件 { #middleware-on-the-async-edges }

需要围绕编码和解码做异步 I/O 的集成（一个 schema 注册表、一层传输格式的信封）不属于 `Codec`：核心的
编解码器是同步的，处理器也应当继续用默认的那一个。

把这类集成放到异步边界上。入站载荷在订阅的投递路径上转码，在编解码器看到它们之前完成。出站的用
核心的 `PublishLayer` 加上信封，通过 `RustStream::publish_layer` 添加到整个应用上。发布层是异步
的，也可以返回错误，而 `Outgoing::payload_mut` 的存在正是为了包装信封。

## 配置与默认值

`Config` 归你的 crate 所有，核心不带任何 Broker 专有的配置。某个字段没有合理的默认值时，就不要实现
`Default`：用户于是显式写下这个值，而不是继承一个日后会出问题的默认值。

## 错误

用 `thiserror` 写一个 crate 级别的错误枚举，变体按来源划分。公开的错误枚举标记 `#[non_exhaustive]`。
切勿在库 crate 里使用 `anyhow`。

## 测试支持 { #test-support }

在 `testing` feature 下提供一个进程内传输，在它的**已连接形态**上实现 `TestableBroker`。用
`register_testable_broker!` 为这个已连接类型注册：套件会先连接每一个 Broker，然后才取回它的传输。
用户于是可以借助 `TestApp`，对着你的 Broker 单元测试处理器。

该传输**只做核心路由**：把发布出去的消息分发给匹配的订阅者，对 `ack` 和 `nack` 的答复与真实传输
一致。传输能确认的地方，就在内存里结算，`nack(requeue = true)` 把这条投递放回去。传输根本无法确认
的地方（ZeroMQ、MQTT `QoS 0`、Redis pub/sub），答复仍然是 `AckError::Unsupported`。它一旦声称一次
真实传输做不到的结算，处理器里的重试就会在测试里通过，在生产中丢消息。

切勿在传输里模拟 Broker 专有的语义（持久游标、重新投递定时器、偏移量、死信路由），那些要对着一台
真实的服务器端到端地验证。

参考实现就是内存 Broker 自己的那一份（在 `ConnectedMemoryBroker` 上）：

```rust
--8<-- "src/memory/mod.rs:testable"
```

该传输在每次把消息入队给某个订阅者时调用 `Coordinator::enqueued`，在结算或丢弃一次投递时调用
`Coordinator::consumed`，套件据此判断这次反应已经结束。延迟的重新投递由它交给
`Coordinator::schedule_redelivery` 去路由。

同一个类型既适用于 `TestApp`，也适用于 conformance 校验套件。面向用户的那一侧参见
[测试](../guides/testing.md)；[Conformance](conformance.md) 讲的是怎样用 `run_suite` 和 `lifecycle`
转移链检查证明你的实现。

### 怎样写一个信得过的进程内传输 { #writing-one-you-can-trust }

一个服务的整套测试都跑在这个进程内传输上。因此它和真实传输之间的每一处差异，都会让一个测试变绿，
而它测的行为在生产里并不存在。这些差异并不冷僻，而下面每一条规则的代价大约就是一个测试。

**核心的契约套件不能只跑真实服务器，也要跑进程内传输。**套件是照着 trait 写的，并不区分应答的是
真实 Broker 还是进程内传输。一个 `#[tokio::test]` 就够：

```rust
--8<-- "tests/conformance_self.rs:run_suite"
```

先跑 `lifecycle`。它会走一遍 `new` -> `connect` -> 订阅 -> 发布 -> ack -> `shutdown`，然后问出一个
几乎没人拿去问进程内传输的问题：关闭之前创建的发布者，在关闭之后会不会返回错误？真实客户端答的是
“未连接”。而发布只是往 channel 里发一条消息的进程内传输没有理由返回错误，它会把消息收下。

```rust
--8<-- "tests/conformance_self.rs:lifecycle"
```

`capabilities::*` 套件照此加上，实现了哪项能力就加哪一个。

**提供的能力要和真实 Broker 一致。**`testing` 这个 feature 是给测试用的，release 构建会把它关掉，
两个方向的代价因此并不对等。少一项是贵的：真实 Broker 有、进程内传输没有的能力，在进程内根本挂载
不了，它背后的行为也就没人测。多一项是便宜的：只有进程内传输提供的事务或 request-reply，在你自己的
release 构建里就编译不过，恼人，但立刻就能发现。

**传输怎么结算，你就怎么结算。**真实的 `ack` 在两处返回 `AckError::Unsupported`：发完即忘的传输，
以及至多一次的服务质量。进程内传输照样返回它。为了让套件通过而回一个 `Ok(())`，会让一个返回
`HandlerOutcome::retry()` 的处理器在进程内通过测试，却在真实 Broker 上丢消息。诚实的答复，套件是
接受的。

**客户端做的事要复刻，Broker 做的事不要假造。**这条界线无关工作量，只看机制运行在哪一侧。竞争
消费、按组分发、关联与回复路由、提交前的缓冲，都由客户端或路由层完成，进程内复刻是精确的。集群
原子性、fencing、Broker 侧持有的超时和 exactly-once 由 Broker 完成，进程内复刻就是虚构。

竞争消费最该做对，因为做错了看着像成功。把每条消息都发给队列的每个订阅者，那已经不是队列。共用
一条队列的两个工作者于是各自跑完整个流，而一个统计处理条数的测试只看到消息都处理完了，一个错都
不报。

**每一处缺口都配一句注释，点名它让哪条断言站不住。**不要写“这个功能没有”，要写读者从此不该再信
哪个测试，以及真正验证它的是什么：

<!-- inline-rust: the shape of a gap comment, not code - the in-memory broker has no transactional id to be fenced on -->
```rust
// No fencing: a second producer claiming the same transactional id is not rejected here, so a
// test cannot assert the first one is fenced out. `capabilities::transactions` against a real
// server is what covers that.
```

**缺口还要用一个测试守住。**某人“修好”进程内传输，让它去路由那些它故意不路由的东西，注释就在那天
失效。而一个断言这个处理器*没有*运行的测试，会在那天失败，并且自己把话说清楚。

**挂载进程内传输的方式，要和挂载真实 Broker 一样。**你自己的订阅来源和发布策略必须原封不动地对着它
工作，这样服务测的才是它实际交付的那份路由文件。用户非得把 `OrdersStream` 换成别的东西才能把测试
跑起来，这个测试就不再检验挂载了。
