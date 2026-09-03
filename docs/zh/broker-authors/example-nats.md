# 实例讲解：一个 NATS Broker

本页跟着真实的 [`ruststream-nats`](https://github.com/powersemmi/ruststream-nats) crate 走一遍，看它
如何在 [`async-nats`](https://docs.rs/async-nats) 客户端之上实现契约。它是一个麻雀虽小、五脏俱全的
Broker：`Broker` -> `ConnectedBroker` 阶梯、用一个 `SubscribeOptions` 描述符同时支撑 Core NATS 与
JetStream 的单一订阅类型、一个会转发消息头的发布者，以及原生的请求-响应能力。

!!! note
    各个条目的名字跟随你所依赖的 `async-nats` 版本（这里是 0.46）；如果该 crate 的 API 有了变动，
    就自行调整下面标注的那几处。

```toml title="Cargo.toml"
[package]
name = "ruststream-nats"
version = "0.1.0"
edition = "2024"

[features]
default = []
testing = ["ruststream/conformance"]

[dependencies]
ruststream = { version = "0.7", default-features = false }
async-nats = "0.46"
bytes = "1"
futures = "0.3"
thiserror = "2"
tokio = { version = "1", features = ["sync", "time"] }
tokio-stream = "0.1"
tracing = "0.1"
```

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
    #[error("nats request timed out")]
    RequestTimeout,
    #[error("nats broker is not connected")]
    NotConnected,
    #[error("invalid subscribe options: {0}")]
    InvalidOptions(String),
}
```

## Broker 阶梯

`new` 是同步的，只记录地址。消费 `self` 的 `connect` 负责拨号，并返回已连接形态，它直接持有活跃的
客户端。只剩下一个共享 cell：
在应用还在组装、`connect` 尚未运行的时候就可以构建发布者，而它通过 `connect` 填充的 cell 读取
客户端。该 cell 是为发布者而存在的（同一种发布者类型同时服务于早期路径和已连接路径）；已连接形态
自身的操作从不检查它。

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use std::sync::Arc;

use ruststream::{Broker, ConnectedBroker};
use tokio::sync::OnceCell;

#[derive(Clone)]
pub struct NatsBroker {
    // Shared with publishers handed out before connect; the consuming connect fills it.
    client: Arc<OnceCell<async_nats::Client>>,
    addrs: Option<String>,
}

impl NatsBroker {
    /// Records the address; dials when `Broker::connect` runs. No I/O.
    #[must_use]
    pub fn new(addrs: impl Into<String>) -> Self {
        Self {
            client: Arc::new(OnceCell::new()),
            addrs: Some(addrs.into()),
        }
    }

    /// Wraps an already-connected client (TLS, credentials, custom options); `connect` then
    /// finds the cell filled and performs no I/O.
    #[must_use]
    pub fn from_client(client: async_nats::Client) -> Self {
        Self {
            client: Arc::new(OnceCell::new_with(Some(client))),
            addrs: None,
        }
    }

    /// A publisher sharing this broker's connection cell; buildable before `connect`.
    #[must_use]
    pub fn publisher(&self) -> NatsPublisher {
        NatsPublisher::new(Arc::clone(&self.client))
    }
}

impl Broker for NatsBroker {
    type Error = NatsError;
    type Connected = ConnectedNatsBroker;

    async fn connect(self) -> Result<Self::Connected, Self::Error> {
        let client = self
            .client
            .get_or_try_init(async || {
                let addrs = self.addrs.as_deref().ok_or(NatsError::NotConnected)?;
                async_nats::connect(addrs)
                    .await
                    .map_err(|e| NatsError::Connect(Box::new(e)))
            })
            .await?
            .clone();
        Ok(ConnectedNatsBroker {
            client,
            shared: self.client,
        })
    }
}

/// The typed witness that `connect` succeeded: holds the live client directly.
pub struct ConnectedNatsBroker {
    client: async_nats::Client,
    // Keeps the cell of publishers handed out before connect alive and filled.
    shared: Arc<OnceCell<async_nats::Client>>,
}

impl ConnectedNatsBroker {
    /// A publisher from the connected form. It rides the same cell-backed publisher type as the
    /// early path; by now `connect` has filled the cell, so it resolves immediately.
    #[must_use]
    pub fn publisher(&self) -> NatsPublisher {
        NatsPublisher::new(Arc::clone(&self.shared))
    }
}

impl ConnectedBroker for ConnectedNatsBroker {
    type Error = NatsError;
    type Closed = ();

    async fn shutdown(self) -> Result<(), Self::Error> {
        let _ = self.client.drain().await; // best-effort; never blocks or panics
        Ok(())
    }
}
```

消费 `self` 在所有者这条路径上排除了第二次 `connect`，也排除了关闭之后再发布。先前创建的发布者
仍然握着它的 cell，而在 drain 之后再发布，浮现出来的是客户端自己的错误 - 这正是生命周期检查所
验证的别名句柄契约。`shutdown` 完成全部可失败的拆除工作，并且绝不 panic。

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
        ConnectedNatsBroker::subscribe(self, SubscribeOptions::new(name)).await
    }
}
```

已连接形态自己的 `subscribe` 会先校验选项，然后只分支一次（`queue_group_ref`、`stream_ref` 和
`durable_ref` 是几个返回 `Option<&str>` 的小 `pub(crate)` getter）；它直接持有客户端，因此根本没有
“未连接”这条路径需要处理：

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use async_nats::jetstream::{self, consumer::pull::Config as PullConfig};

impl ConnectedNatsBroker {
    pub async fn subscribe(&self, opts: SubscribeOptions) -> Result<NatsSubscriber, NatsError> {
        opts.validate()?;
        if opts.is_jetstream() {
            self.subscribe_jetstream(opts).await
        } else {
            self.subscribe_core(opts).await
        }
    }

    async fn subscribe_core(&self, opts: SubscribeOptions) -> Result<NatsSubscriber, NatsError> {
        let client = self.client.clone();
        let subject = opts.subject().to_owned();
        let inner = match opts.queue_group_ref() {
            Some(group) => client.queue_subscribe(subject.clone(), group.to_owned()).await,
            None => client.subscribe(subject.clone()).await,
        }
        .map_err(|e| NatsError::Subscribe(Box::new(e)))?;
        Ok(NatsSubscriber::from_core(subject, inner))
    }

    async fn subscribe_jetstream(&self, opts: SubscribeOptions) -> Result<NatsSubscriber, NatsError> {
        let ctx = jetstream::new(self.client.clone());
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

发布者持有共享的连接 cell，每次发布时从中读取客户端，并在存在消息头时把它们一并转发。

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use ruststream::{OutgoingMessage, Publisher};

#[derive(Clone)]
pub struct NatsPublisher {
    client: Arc<OnceCell<async_nats::Client>>,
}

impl Publisher for NatsPublisher {
    type Error = NatsError;

    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error> {
        let client = self.client.get().cloned().ok_or(NatsError::NotConnected)?;
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
        let client = self.client.get().cloned().ok_or(NatsError::NotConnected)?;
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

只实现传输层真正支持的能力：Core NATS 没有批量订阅、没有事务性发布，也没有可回放的日志，因此
这里略去了 `BatchSubscriber`、`TransactionalPublisher` 和 `Seekable`（NATS 的 `Seekable` 该待的地方是
JetStream 消费者，它的流本身就是一份可回放的日志）。`ruststream-nats` 目前也没有实现
`DescribeServer`；如果你希望该 Broker 在 AsyncAPI 文档里作为 server 出现，就把它补上。

## 发布策略

`NatsPublisher` 是活的那一半；声明的那一半由 `PublishPolicy` 提供，于是在任何连接存在之前，注册代码
就能指名一个发布者。NATS 的发布不带任何选项，所以该策略只是一个单元标记（与内存 Broker 的
`MemoryPublish` 如出一辙），而配对就是已连接形态自己的 `publisher()`：这里不会失败；如果某个 Broker
要做真正的工作才能让发布者活起来（比如事务性生产者），就用 `PairError::new` 包装它的失败。由于默认
配置可以直接拿来用，已连接形态还可以实现 `DefaultPublish`（参见[契约](index.md#publishpolicy)），
这样不指定发布者的 `publish(..)` 处理器也能通过编译。

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use ruststream::{PairError, PublishPolicy};

#[derive(Debug, Clone, Copy, Default)]
#[must_use]
pub struct NatsPublish;

impl PublishPolicy<ConnectedNatsBroker> for NatsPublish {
    type Live = NatsPublisher;

    async fn pair(self, connected: &ConnectedNatsBroker) -> Result<Self::Live, PairError> {
        Ok(connected.publisher())
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
