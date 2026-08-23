# Разбор примера: брокер NATS

Эта страница разбирает, как настоящий крейт
[`ruststream-nats`](https://github.com/powersemmi/ruststream-nats) реализует контракт поверх клиента
[`async-nats`](https://docs.rs/async-nats). Это полноценный брокер в миниатюре: лестница
`Broker` -> `ConnectedBroker`, один тип подписки, обслуживающий и Core NATS, и JetStream за единым
дескриптором `SubscribeOptions`, публикатор, пробрасывающий заголовки, и нативная возможность
request-reply.

!!! note
    Имена элементов зависят от версии `async-nats`, на которую вы завязаны (здесь 0.46); если API
    крейта с тех пор изменилось, поправьте отмеченные ниже места.

```toml title="Cargo.toml"
[package]
name = "ruststream-nats"
version = "0.1.0"
edition = "2024"

[features]
default = []
testing = ["ruststream/conformance"]

[dependencies]
ruststream = { version = "0.6", default-features = false }
async-nats = "0.46"
bytes = "1"
futures = "0.3"
thiserror = "2"
tokio = { version = "1", features = ["sync", "time"] }
tokio-stream = "0.1"
tracing = "0.1"
```

## Ошибки

Один enum на весь крейт, варианты по источникам, `#[non_exhaustive]` - чтобы новые варианты не
ломали совместимость. Источники хранятся как боксированные ошибки `std`, поэтому публичный API не
протекает типами ошибок `async-nats`.

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

## Лестница брокера

`new` синхронный и только записывает адрес - именно это и позволяет сервису на NATS собираться
синхронным билдером `#[ruststream::app]`. Потребляющий `connect` устанавливает соединение и
возвращает подключённую форму, которая держит живой клиент напрямую. Одна разделяемая ячейка всё же
остаётся: публикатор может быть создан ещё на сборке приложения, до вызова `connect`, и читает
клиента через ту ячейку, которую `connect` заполняет. Ячейка существует ради публикаторов (один тип
публикатора обслуживает и ранний, и подключённый путь); собственные операции подключённой формы в неё
никогда не заглядывают.

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

Раз `connect` потребляет `self`, у владельца хендла нет ни второго `connect`, ни публикации после
остановки, которую пришлось бы как-то обрабатывать; созданный раньше публикатор сохраняет свою
ячейку, а публикация после `drain` возвращает собственную ошибку клиента (это и есть контракт для
хендлов-алиасов, который и проверяет `lifecycle`). Весь способный упасть код теардауна живёт в
`shutdown`, и паниковать он не имеет права - так требует контракт.

## Одна подписка на Core и JetStream

Core NATS работает по принципу fire-and-forget, JetStream сохраняет сообщения и подтверждает их через
ack. Вместо двух типов подписчиков опишите оба режима за одним дескриптором `SubscribeOptions` и
одним `NatsSubscriber`. `SubscribeOptions` и есть `SubscriptionSource`, а брокер разветвляется по
тому, вызывали ли `jetstream(..)`. Каждый метод билдера отвечает одному именованному параметру
атрибута `#[subscriber(..)]`.

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

Макрос `#[subscriber(..)]` принимает цепочку вызовов билдера, поэтому весь дескриптор помещается
прямо в атрибут:

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
#[subscriber(SubscribeOptions::new("orders.*").jetstream("ORDERS").durable("worker"))]
async fn handle(order: &Order) -> HandlerResult {
    HandlerResult::Ack
}
```

Подписки по имени идут тем же путём: реализуйте `Subscribe`, делегируя в
`SubscribeOptions::new(name)`, - тогда заработает и форма `#[subscriber("orders")]`.

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

Собственный `subscribe` подключённой формы проверяет опции и разветвляется ровно один раз
(`queue_group_ref`, `stream_ref` и `durable_ref` - маленькие геттеры `pub(crate)`, возвращающие
`Option<&str>`); клиент он держит напрямую, поэтому обрабатывать состояние «нет соединения» здесь не
приходится:

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

## Подписчик

`NatsSubscriber` оборачивает либо core-подписку `async-nats`, либо pull-поток JetStream, пряча оба за
одним типом `Message`. `stream` разветвляется через `futures::future::Either` и забирает внутренний
поток при первом же опросе, поэтому он одноразовый (контракт разрешает ровно один вызов `stream`).

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

## Сообщение

`NatsMessage` - это enum: доставка Core (без ack) или доставка JetStream (с настоящим ack). Оба
варианта боксированы, потому что обёрнутые сообщения `async-nats` большие. `ack` и `nack` на
доставке Core возвращают `AckError::Unsupported` - это не ошибка, и рантайм такой ответ принимает;
на JetStream они подтверждают доставку, причём `nack` превращается в `nak` (повторная доставка),
когда обработчик об этом просит, и в `term` (выбросить poison-сообщение), когда не просит.

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use async_nats::jetstream::AckKind;
use ruststream::{AckError, Headers, IncomingMessage};

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

    fn headers(&self) -> &Headers {
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

Именно возврат `AckError::Unsupported` (а не настоящей ошибки) для доставок Core позволяет пройти
проверку `lifecycle` из conformance на Core NATS. Каждое сообщение конвертирует свои заголовки один раз,
при создании; эта пара конвертеров - единственное место, завязанное на версию `async-nats`:

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use bytes::Bytes;

fn headers_from_nats(map: Option<&async_nats::HeaderMap>) -> Headers {
    let mut headers = Headers::new();
    if let Some(map) = map {
        for (name, values) in map.iter() {
            if let Some(first) = values.iter().next() {
                headers.insert(name.to_string(), Bytes::copy_from_slice(first.as_ref()));
            }
        }
    }
    headers
}

fn headers_to_nats(headers: &Headers) -> Option<async_nats::HeaderMap> {
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

## Публикация

Публикатор держит разделяемую ячейку с соединением, читает из неё клиента на каждой публикации и
пробрасывает заголовки, если они есть.

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

## Возможности

NATS поддерживает request-reply нативно, поэтому реализуйте `RequestReply` на публикаторе и
ограничьте ожидание таймаутом вызывающей стороны, отображая сработавший таймер в `RequestTimeout`.

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

Реализуйте только те возможности, которые поддерживает транспорт: у Core NATS нет ни пакетной
подписки, ни транзакционной публикации, ни воспроизводимого лога, поэтому `BatchSubscriber`,
`TransactionalPublisher` и `Seekable` здесь не реализованы (место для NATS-варианта `Seekable` - это
consumer JetStream, чей поток и есть воспроизводимый лог). `DescribeServer` в `ruststream-nats` тоже
пока не реализован; добавьте его, если хотите, чтобы брокер попадал в AsyncAPI-документ как сервер.

## Политика публикации

`NatsPublisher` - живая половина, а декларативную половину даёт `PublishPolicy`: благодаря ей
регистрация может назвать публикатор ещё до того, как появится хоть какое-то соединение. Публикация в
NATS не несёт никаких опций, поэтому политика здесь - unit-маркер (по образцу `MemoryPublish` у
in-memory брокера), а сопряжение - это собственный `publisher()` подключённой формы, который здесь не
может упасть; брокер, которому для оживления публикатора нужна настоящая работа (например,
транзакционный producer), оборачивает свою неудачу через `PairError::new`. Раз конфигурация по
умолчанию годится как есть, подключённая форма может дополнительно реализовать `DefaultPublish`
(см. [контракт](index.md#publishpolicy)), и тогда обработчик с `publish(..)` компилируется без явно
указанного публикатора.

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

## Связывание с приложением

Когда брокер готов, приложение выглядит ровно так же, как любое другое: ни в обработчиках, ни в
кодеках нет ничего специфичного для NATS.

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use ruststream::runtime::{AppInfo, RustStream, TypedPublisher};

let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
    .with_broker(NatsBroker::new("nats://localhost:4222"), |b| {
        // NatsPublish is the crate's publish policy; the runtime pairs it after connect.
        b.include(confirm).publisher(TypedPublisher::new(NatsPublish::default()));
    });
```

## Как это доказать

Поставьте под фичей `testing` внутрипроцессный транспорт, реализующий `TestableBroker` на своей
подключённой форме (её тип регистрируется через `register_testable_broker!`), который умеет только
базовую маршрутизацию (сопоставление субъектов, разводящее опубликованные сообщения по подписчикам),
и прогоните на нём набор conformance. Такому транспорту нельзя эмулировать курсоры JetStream, таймеры
повторной доставки или retention: это проверяется end-to-end на настоящем `nats-server`. См.
[Conformance](conformance.md).
