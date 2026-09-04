# 实例讲解：一个 NATS Broker

本页跟着真实的 [`ruststream-nats`](https://github.com/powersemmi/ruststream-nats) crate 走一遍，看它
如何在 [`async-nats`](https://docs.rs/async-nats) 客户端之上实现契约。它是一个麻雀虽小、五脏俱全的
Broker：`Broker` -> `ConnectedBroker` -> `Closed` 阶梯、用一个 `SubscribeOptions` 描述符同时支撑
Core NATS 与 JetStream 的单一订阅类型、一个会转发消息头的发布者，以及这套传输真正具备的各项能力。

把它当作契约的图解来读，而不是该 crate 的源码：下面的代码只保留[契约](index.md)每一条规则所要求的
部分，真实 Broker 会长出来的选项、调优参数和逐条投递的类型化上下文，都在 crate 自己那里。各个条目
的名字跟随 `async-nats` 的 API，而它会随版本变动；一个 Broker crate 跟踪哪个客户端版本是这个 crate
自己的事，由它自己的文档说明。

```toml title="Cargo.toml"
[features]
default = []
# The in-process test broker users get. The conformance harness is a broker-author tool and stays
# a dev-dependency, not a feature users can turn on.
testing = ["ruststream/testing"]

[dependencies]
ruststream = { version = "0.7", default-features = false }
```

其余的都是客户端及其配套：`async-nats`，外加 `bytes`、`futures`、`thiserror`、`tokio` 和 `tracing`。

## 错误

一个 crate 级别的枚举，按来源划分变体，并标注 `#[non_exhaustive]`，这样新增变体不构成破坏性变更。
各个来源都是装箱后的 `std` 错误，因此公开 API 不会泄漏 `async-nats` 的错误类型。

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use std::error::Error as StdError;

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum NatsError {
    #[error("nats connection error: {0}")]
    Connect(#[source] Box<dyn StdError + Send + Sync>),
    #[error("nats publish error: {0}")]
    Publish(#[source] Box<dyn StdError + Send + Sync>),
    #[error("nats subscribe error: {0}")]
    Subscribe(#[source] Box<dyn StdError + Send + Sync>),
    #[error("nats jetstream error: {0}")]
    JetStream(#[source] Box<dyn StdError + Send + Sync>),
    #[error("nats shutdown error: {0}")]
    Shutdown(#[source] Box<dyn StdError + Send + Sync>),
    #[error("nats request timed out")]
    RequestTimeout,
    /// A publisher aliasing the connection was used after the broker shut down.
    #[error("nats connection is closed; cannot reach {subject}")]
    Closed { subject: String },
    #[error("invalid subscribe options: {0}")]
    InvalidOptions(String),
}
```

`Closed` 带上了 subject，而不是只说连接已经没了：一个服务在凌晨三点读到的错误，要说清它没能到达的
是什么。

## Broker 阶梯

`new` 是同步的，只记录地址。消费 `self` 的 `connect` 负责拨号，并返回已连接形态，它直接持有活跃的
客户端：不存在“可能已连接”这种状态需要它自己的操作去检查。发布者只从已连接形态交出，别无他处，
于是“没有连接的发布者”根本无法表达。

发布者确实会比连接本身活得久，而这是类型唯一定不下来的事：它是别名问题，不是顺序问题。所以连接
自身带一个 closed 标志，在 drain 开始之前置位，每个别名句柄都通过它读取客户端。

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_nats::{Client, ConnectOptions};
use ruststream::{Broker, ConnectedBroker};

/// The live connection, shared by the connected broker and every publisher paired off it.
struct NatsConnection {
    client: Client,
    closed: AtomicBool,
}

impl NatsConnection {
    /// The client, or `Closed` once the broker has shut down. A runtime check because the force
    /// is external: aliased handles outlive the connection, and the ladder can only rule out
    /// misuse through the owner's handle.
    fn live_client(&self, subject: &str) -> Result<&Client, NatsError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(NatsError::Closed { subject: subject.to_owned() });
        }
        Ok(&self.client)
    }
}

#[derive(Debug, Clone)]
#[must_use]
pub struct NatsBroker {
    addrs: String,
    options: ConnectOptions,
}

impl NatsBroker {
    /// Records the address; dials when `Broker::connect` runs. No I/O.
    pub fn new(addrs: impl Into<String>) -> Self {
        Self { addrs: addrs.into(), options: ConnectOptions::default() }
    }

    /// Credentials, TLS, reconnect behaviour: still pure configuration, still no I/O.
    pub fn with_options(mut self, options: ConnectOptions) -> Self {
        self.options = options;
        self
    }
}

impl Broker for NatsBroker {
    type Error = NatsError;
    type Connected = ConnectedNatsBroker;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        let client = self
            .options
            .connect(self.addrs.as_str())
            .await
            .map_err(|e| NatsError::Connect(Box::new(e)))?;
        Ok(ConnectedNatsBroker::from_client(client))
    }
}

/// The typed witness that `connect` succeeded: the only value with a publish or subscribe surface.
#[derive(Debug)]
pub struct ConnectedNatsBroker {
    connection: Arc<NatsConnection>,
}

impl ConnectedNatsBroker {
    /// Adopts an already-connected client: the escape hatch for a connection built outside the
    /// framework. Only the plain `NatsBroker` slots into the synchronous app builder.
    #[must_use]
    pub fn from_client(client: Client) -> Self {
        Self {
            connection: Arc::new(NatsConnection { client, closed: AtomicBool::new(false) }),
        }
    }
}

impl ConnectedBroker for ConnectedNatsBroker {
    type Error = NatsError;
    type Closed = ClosedNatsBroker;

    async fn shutdown(self) -> Result<Self::Closed, Self::Error> {
        // Marked closed before draining: a publisher aliasing the connection must not slip a
        // message into a connection that is already going away.
        self.connection.closed.store(true, Ordering::Release);
        let client = &self.connection.client;
        let stats = client.statistics();
        client.drain().await.map_err(|e| NatsError::Shutdown(Box::new(e)))?;
        Ok(ClosedNatsBroker {
            messages_sent: stats.out_messages.load(Ordering::Relaxed),
            messages_received: stats.in_messages.load(Ordering::Relaxed),
        })
    }
}

/// The terminal witness: no publish or subscribe surface, just the drained connection's counters,
/// for a shutdown log line or a teardown assertion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosedNatsBroker {
    messages_sent: u64,
    messages_received: u64,
}
```

消费 `self` 在所有者这条路径上排除了第二次 `connect`，也排除了关闭之后再发布或再订阅。`shutdown`
完成全部可失败的拆除工作，交回那个见证值，并且绝不 panic。先前创建的发布者在此之后报告 `Closed`，
而不是对着一条已死的连接照样成功 - 这正是生命周期检查所验证的别名句柄契约。

## Core 与 JetStream 共用一种订阅

Core NATS 是发完即忘的；JetStream 则会持久化并需要确认。两者都收在一个 `SubscribeOptions` 描述符和
一个 `NatsSubscriber` 背后。`SubscribeOptions` 就是
`SubscriptionSource`；Broker 依据是否调用过 `jetstream(..)` 来分发。每个构建器方法都对应
`#[subscriber(..)]` 属性宏的一个关键字。

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use std::time::Duration;

pub use async_nats::jetstream::consumer::DeliverPolicy;
use ruststream::SubscriptionSource;

#[derive(Debug, Clone)]
#[must_use]
pub struct SubscribeOptions {
    subject: String,
    queue_group: Option<String>,
    stream: Option<String>, // Some(..) => JetStream
    durable: Option<String>,
    // JetStream tuning, elided here: filter_subject, ack_wait, max_ack_pending, deliver_policy
}

impl SubscribeOptions {
    pub fn new(subject: impl Into<String>) -> Self {
        Self { subject: subject.into(), queue_group: None, stream: None, durable: None }
    }

    /// Core-only load balancing. Rejected together with `jetstream`.
    pub fn queue_group(mut self, name: impl Into<String>) -> Self {
        self.queue_group = Some(name.into());
        self
    }

    /// Switch to a JetStream pull consumer on `stream`.
    pub fn jetstream(mut self, stream: impl Into<String>) -> Self {
        self.stream = Some(stream.into());
        self
    }

    /// Durable consumer name (JetStream only). Without it the consumer is ephemeral.
    pub fn durable(mut self, name: impl Into<String>) -> Self {
        self.durable = Some(name.into());
        self
    }

    pub fn subject(&self) -> &str {
        &self.subject
    }

    pub const fn is_jetstream(&self) -> bool {
        self.stream.is_some()
    }

    /// Reject incompatible combinations before any I/O.
    pub fn validate(&self) -> Result<(), NatsError> {
        if self.subject.is_empty() {
            return Err(NatsError::InvalidOptions("subject must be non-empty".into()));
        }
        if self.stream.is_some() && self.queue_group.is_some() {
            return Err(NatsError::InvalidOptions(
                "queue_group is Core NATS only and cannot be combined with jetstream(_)".into(),
            ));
        }
        // ...and reject the JetStream-only fields (durable, ack_wait, ...) when jetstream is unset.
        Ok(())
    }
}

impl SubscriptionSource<ConnectedNatsBroker> for SubscribeOptions {
    type Subscriber = NatsSubscriber;

    fn name(&self) -> &str {
        self.subject()
    }

    async fn subscribe(self, connected: &ConnectedNatsBroker) -> Result<NatsSubscriber, NatsError> {
        connected.subscribe(self).await
    }
}
```

由于 `#[subscriber(..)]` 宏接受构建器链式调用，整个描述符可以直接内联写在属性宏里：

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
#[subscriber(SubscribeOptions::new("orders.*").jetstream("ORDERS").durable("worker"))]
async fn handle(order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}
```

按名字订阅走的是同一条路径：实现 `Subscribe` 时委托给 `SubscribeOptions::new(name)`，于是
`#[subscriber("orders")]` 这种写法同样可用。

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use ruststream::Subscribe;

impl Subscribe for ConnectedNatsBroker {
    type Subscriber = NatsSubscriber;

    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error> {
        self.subscribe_with(SubscribeOptions::new(name)).await
    }
}
```

已连接形态自己的 `subscribe_with` 会先校验选项，然后只分支一次（`queue_group_ref`、`stream_ref` 和
`durable_ref` 是几个返回 `Option<&str>` 的小 `pub(crate)` getter）；客户端来自那条连接，closed 检查
就住在那里：

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use async_nats::jetstream::{self, consumer::pull::Config as PullConfig};

impl ConnectedNatsBroker {
    pub async fn subscribe_with(&self, opts: SubscribeOptions) -> Result<NatsSubscriber, NatsError> {
        opts.validate()?;
        if opts.is_jetstream() {
            self.subscribe_jetstream(opts).await
        } else {
            self.subscribe_core(opts).await
        }
    }

    async fn subscribe_core(&self, opts: SubscribeOptions) -> Result<NatsSubscriber, NatsError> {
        let client = self.connection.live_client(opts.subject())?;
        let subject = opts.subject().to_owned();
        let inner = match opts.queue_group_ref() {
            Some(group) => client.queue_subscribe(subject.clone(), group.to_owned()).await,
            None => client.subscribe(subject.clone()).await,
        }
        .map_err(|e| NatsError::Subscribe(Box::new(e)))?;
        // Core SUB is written without waiting for the server, so without this round trip a
        // producer on another connection can publish into a subscription the server has not
        // registered yet, and the message is simply lost.
        client.flush().await.map_err(|e| NatsError::Subscribe(Box::new(e)))?;
        Ok(NatsSubscriber::from_core(subject, inner))
    }

    async fn subscribe_jetstream(&self, opts: SubscribeOptions) -> Result<NatsSubscriber, NatsError> {
        let ctx = jetstream::new(self.connection.live_client(opts.subject())?.clone());
        let stream_name = opts.stream_ref().expect("validated").to_owned();
        let stream = ctx
            .get_stream(&stream_name)
            .await
            .map_err(|e| NatsError::JetStream(Box::new(e)))?;
        let consumer = stream
            .create_consumer(PullConfig {
                durable_name: opts.durable_ref().map(str::to_owned),
                ..Default::default() // filter_subject, ack_wait, max_ack_pending, deliver_policy
            })
            .await
            .map_err(|e| NatsError::JetStream(Box::new(e)))?;
        let messages = consumer
            .messages()
            .await
            .map_err(|e| NatsError::JetStream(Box::new(e)))?;
        Ok(NatsSubscriber::from_jetstream(opts.subject().to_owned(), stream_name, messages))
    }
}
```

## 订阅者

`NatsSubscriber` 包装的要么是 `async-nats` 的 core 订阅，要么是 JetStream 的拉取流，两者都藏在同一个
`Message` 类型之后。`stream` 用 `futures::future::Either` 做分支，并在首次轮询时把内部的流取走，所以
它只能使用一次（契约允许调用一次 `stream`）。

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use async_nats::jetstream::consumer::pull::Stream as PullStream;
use futures::{Stream, future::Either};
use ruststream::Subscriber;
use tokio_stream::StreamExt;

pub struct NatsSubscriber {
    subject: String,
    kind: SubscriberKind,
}

enum SubscriberKind {
    Core { inner: Option<async_nats::Subscriber> },
    JetStream { inner: Option<Box<PullStream>>, stream_name: String },
}

impl Subscriber for NatsSubscriber {
    type Message = NatsMessage;
    type Error = NatsError;

    fn stream(&mut self) -> impl Stream<Item = Result<NatsMessage, NatsError>> + Send + '_ {
        match &mut self.kind {
            SubscriberKind::Core { inner } => {
                let inner = inner.take().expect("stream called more than once");
                Either::Left(inner.map(|m| Ok(NatsMessage::Core(Box::new(CoreMessage::new(m))))))
            }
            SubscriberKind::JetStream { inner, .. } => {
                let inner = *inner.take().expect("stream called more than once");
                Either::Right(inner.map(|item| match item {
                    Ok(m) => Ok(NatsMessage::JetStream(Box::new(JetStreamMessage::new(m)))),
                    Err(e) => Err(NatsError::JetStream(Box::new(e))),
                }))
            }
        }
    }
}
```

## 消息

`NatsMessage` 是一个枚举：要么是 core 投递（没有 ack），要么是 JetStream 投递（有真正的 ack）。两者
都做了装箱，因为其中包装的 `async-nats` 消息很大。对 core 投递调用 `ack`/`nack` 会返回
`AckError::Unsupported`，这是运行时接受的非错误结果；在 JetStream 上它们才真正生效，其中 `nack` 在
处理器要求重新投递时映射为 `nak`（重投），在处理器不要求时映射为 `term`（丢弃毒消息）。

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use async_nats::jetstream::AckKind;
use ruststream::{AckError, HeaderMap, IncomingMessage};

pub enum NatsMessage {
    Core(Box<CoreMessage>),
    JetStream(Box<JetStreamMessage>),
}

impl IncomingMessage for NatsMessage {
    fn payload(&self) -> &[u8] {
        match self {
            Self::Core(m) => &m.inner.payload,
            Self::JetStream(m) => &m.inner.message.payload,
        }
    }

    fn headers(&self) -> &HeaderMap {
        match self {
            Self::Core(m) => &m.headers,
            Self::JetStream(m) => &m.headers,
        }
    }

    async fn ack(self) -> Result<(), AckError> {
        match self {
            Self::Core(_) => Err(AckError::Unsupported),
            Self::JetStream(m) => m.inner.ack().await.map_err(|e| AckError::Broker(box_err(e))),
        }
    }

    async fn nack(self, requeue: bool) -> Result<(), AckError> {
        match self {
            Self::Core(_) => Err(AckError::Unsupported),
            Self::JetStream(m) => {
                let kind = if requeue { AckKind::Nak(None) } else { AckKind::Term };
                m.inner.ack_with(kind).await.map_err(|e| AckError::Broker(box_err(e)))
            }
        }
    }
}
```

conformance 生命周期检查接受 `AckError::Unsupported`，所以 Core NATS 能通过它。每条消息在构造时
一次性转换自己的消息头；这两个转换函数是唯一需要跟随 `async-nats` 版本变化的地方：

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use bytes::Bytes;

fn headers_from_nats(map: Option<&async_nats::HeaderMap>) -> HeaderMap {
    let mut headers = HeaderMap::new();
    if let Some(map) = map {
        for (name, values) in map.iter() {
            if let Some(first) = values.iter().next() {
                headers.insert(name.to_string(), Bytes::copy_from_slice(first.as_ref()));
            }
        }
    }
    headers
}

fn headers_to_nats(headers: &HeaderMap) -> Option<async_nats::HeaderMap> {
    if headers.is_empty() {
        return None;
    }
    let mut map = async_nats::HeaderMap::new();
    for (name, value) in headers.iter() {
        if let Ok(text) = std::str::from_utf8(value) {
            map.insert(name, text);
        }
    }
    Some(map)
}
```

## 发布

发布者与配对出它的 Broker 共享同一条连接，每次发布都经由 closed 检查读取客户端，并在存在消息头时
把它们一并转发。

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use ruststream::{OutgoingMessage, Publisher};

#[derive(Clone)]
pub struct NatsPublisher {
    connection: Arc<NatsConnection>,
}

impl Publisher for NatsPublisher {
    type Error = NatsError;

    /// # Cancel safety
    ///
    /// Core NATS publishing is fire-and-forget: the message is handed to the connection's writer
    /// without waiting for the server. Dropping the future may leave it either sent or unsent.
    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let client = self.connection.live_client(msg.name())?.clone();
        let subject = msg.name().to_owned();
        let payload = Bytes::copy_from_slice(msg.payload());
        match headers_to_nats(msg.headers()) {
            Some(headers) => client.publish_with_headers(subject, headers, payload).await,
            None => client.publish(subject, payload).await,
        }
        .map_err(|e| NatsError::Publish(Box::new(e)))
    }
}
```

## 各项能力

NATS 原生支持请求-响应，因此在发布者上实现 `RequestReply`：用调用方给出的超时时间限定等待，并把
计时器超时映射为 `RequestTimeout`。

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use std::time::Duration;

use ruststream::RequestReply;

impl RequestReply for NatsPublisher {
    type Reply = NatsMessage;

    async fn request(
        &self,
        msg: OutgoingMessage<'_>,
        timeout: Duration,
    ) -> Result<Self::Reply, Self::Error> {
        let client = self.connection.live_client(msg.name())?.clone();
        let subject = msg.name().to_owned();
        let request = async_nats::Request::new().payload(Bytes::copy_from_slice(msg.payload()));
        let send = async {
            client
                .send_request(subject, request)
                .await
                .map_err(|e| NatsError::Publish(Box::new(e)))
        };
        let reply = tokio::time::timeout(timeout, send)
            .await
            .map_err(|_| NatsError::RequestTimeout)??;
        Ok(NatsMessage::Core(Box::new(CoreMessage::new(reply))))
    }
}
```

JetStream 拉取消费者在协议层面本来就是按批取数据，所以 `BatchSubscriber` 报告的是传输已经在做的
事，而不是模拟出来的：流里的一项就是一次取数，由一个批大小和一个过期时间限定，取空时会重试，因此
一页永远不会是空的。同一个订阅者的 Core 分支没有协议层面的批，所以那里的一页就是客户端本地已经
缓冲下来的内容，加一个上限，绝不用传输本来没有的延迟去凑数。两者都没有的 Broker 会略去这项能力，
让用户改用客户端侧的 [`buffered`](../guides/subscribers.md#batch-subscribers) 适配器。

`DescribeServer` 把该 Broker 写进生成的 AsyncAPI 文档。它落在**未连接**的 Broker 上，因为文档是从
一个尚未拨号的服务生成的：它报告的是配置里的地址。服务端自己宣告的坐标（一条集群路由、一个发现到
的对端）只有连上之后才知道，所以它们属于已连接形态上的访问器，不属于这个 trait。

其余的都略去了，因为传输层没有这些东西：NATS 没有事务，所以 `TransactionalPublisher` 和
`OwnedTransactions` 都不在，`Seekable` 也一样 - NATS 的 `Seekable` 该待的地方是 JetStream 消费者，
它的流本身就是一份可回放的日志。

## 发布策略

`NatsPublisher` 是活的那一半；声明的那一半由 `PublishPolicy` 提供，于是在任何连接存在之前，注册代码
就能指名一个发布者。Core NATS 的发布不带任何按发布者划分的选项（subject 和消息头随每条消息一起
走），所以该策略只是一个单元标记（与内存 Broker 的 `MemoryPublish` 如出一辙），而配对只是克隆一下
连接句柄。这里不会失败；如果某个 Broker 要做真正的工作才能让发布者活起来（比如事务性生产者），就用
`PairError::new` 包装它的失败。由于朴素的策略可以直接拿来用，已连接形态还实现了 `DefaultPublish`
（参见[契约](index.md#publishpolicy)），这样不指定发布者的 `publish(..)` 处理器也能通过编译。

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use ruststream::{DefaultPublish, PairError, PublishPolicy};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[must_use]
pub struct NatsPublish;

impl PublishPolicy<ConnectedNatsBroker> for NatsPublish {
    type Live = NatsPublisher;

    async fn pair(self, connected: &ConnectedNatsBroker) -> Result<Self::Live, PairError> {
        Ok(NatsPublisher { connection: Arc::clone(connected.connection()) })
    }
}
```

## 这个 crate 的 prelude

crate 的 prelude 就是挂载点会 glob 的东西：先是核心 prelude，再是 Broker 和它的描述符，最后是统一
名字下的策略（见[契约](index.md#broker-prelude)）。

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
pub use ruststream::prelude::*;

pub use crate::{NatsBroker, NatsError, NatsSource};
pub use crate::NatsPublish as Publish;

// The capabilities this broker implements on its live values.
pub use ruststream::{Positioned, RequestReply, Seekable, Seeker};
```

## 接入到应用里

有了该 Broker，应用写起来和其他任何应用完全一样；处理器和编解码器都没有任何 NATS 专有的东西。

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use ruststream_nats::prelude::*;

let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
    .with_broker(NatsBroker::new("nats://localhost:4222"), |b| {
        // `Publish` is this crate's publish policy; the runtime pairs it after connect.
        b.include(confirm).publisher(Publish::default());
    });
```

## 验证它

在 `testing` feature 下提供一个进程内传输，让它的已连接形态实现 `TestableBroker`（该已连接类型用
`register_testable_broker!` 注册），并且只做核心路由（一个 subject 匹配器，把发布出去的消息扇出给
各个订阅者），然后拿 conformance 校验套件跑它。该传输不得模拟 JetStream 的游标、重新投递计时器或
保留策略；那些要端到端地对着真实的 `nats-server` 来验证。参见 [Conformance](conformance.md)。
