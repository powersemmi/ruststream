# Разбор примера: брокер NATS

Эта страница разбирает, как настоящий крейт
[`ruststream-nats`](https://github.com/powersemmi/ruststream-nats) реализует контракт поверх клиента
[`async-nats`](https://docs.rs/async-nats). Это полноценный брокер в миниатюре: переходы
`Broker` -> `ConnectedBroker` -> `Closed`, один тип подписки на Core NATS и JetStream за единым
дескриптором `SubscribeOptions`, издатель, передающий заголовки, и те трейт-совместимости, которые
есть у транспорта.

Читайте страницу как иллюстрацию контракта, а не как исходный код крейта: код ниже урезан до того,
что требует каждое правило [контракта](index.md), а в самом крейте есть опции, тонкая настройка и
типизированный контекст доставки, которые появляются у настоящего брокера. Имена элементов взяты из
API `async-nats`, а он меняется от релиза к релизу. Версию клиента выбирает сам крейт брокера, и она
указана в его документации.

```toml title="Cargo.toml"
[features]
default = []
# The in-process test broker users get. The conformance harness is a broker-author tool and stays
# a dev-dependency, not a feature users can turn on.
testing = ["ruststream/testing"]

[dependencies]
ruststream = { version = "0.7", default-features = false }
```

Всё остальное - это клиент и его окружение: `async-nats`, а также `bytes`, `futures`, `thiserror`,
`tokio` и `tracing`.

## Ошибки

Один enum на весь крейт, варианты по источникам, `#[non_exhaustive]` - чтобы новые варианты не были
ломающим изменением. Источники хранятся как ошибки `std` в `Box`, поэтому в публичный API не
попадают типы ошибок `async-nats`.

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

`Closed` указывает субъект, а не только сам факт, что соединения больше нет: ошибка, которую сервис
читает в три часа ночи, называет то, чего он не смог достичь.

## Жизненный цикл брокера

`new` синхронный и только записывает адрес. `connect` поглощает `self`, устанавливает соединение и
возвращает подключённую форму: она хранит живой клиент напрямую, и состояния «может быть,
подключён» для её собственных операций не существует. Издателей выдаёт подключённая форма и только
она, поэтому издатель без соединения непредставим.

Издатель может пережить само соединение, и типы этого не исключают: дело не в порядке вызовов, а в
том, что на одно соединение ссылаются несколько дескрипторов. Поэтому в соединении есть флаг
закрытия: `shutdown` выставляет его до вызова `drain`, и каждый такой дескриптор читает клиента
через него.

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

Со стороны владельца поглощение `self` исключает и второй `connect`, и публикацию или подписку после
остановки. `shutdown` выполняет всё завершение работы, которое может вернуть ошибку, возвращает
свидетеля и никогда не паникует. Созданный раньше издатель после остановки возвращает `Closed`, а не
публикует успешно в закрытое соединение: это и есть контракт для таких дескрипторов, который
проверяет `lifecycle`.

## Одна подписка на Core и JetStream

Core NATS отправляет сообщения без подтверждений, JetStream их сохраняет и требует
подтверждения доставки. Оба режима описывает один дескриптор `SubscribeOptions` и один
`NatsSubscriber`. `SubscribeOptions` и есть `SubscriptionSource`, а брокер выбирает ветку по тому,
вызывали ли `jetstream(..)`. Каждый метод билдера соответствует одному именованному параметру
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
async fn handle(order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}
```

Подписки по имени субъекта используют тот же путь: вы можете реализовать `Subscribe`, делегируя в
`SubscribeOptions::new(name)`, и тогда заработает форма `#[subscriber("orders")]`.

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

Собственный `subscribe_with` подключённой формы проверяет опции и разветвляется ровно один раз
(`queue_group_ref`, `stream_ref` и `durable_ref` - маленькие геттеры `pub(crate)`, возвращающие
`Option<&str>`); клиента он берёт из соединения, где и выполняется проверка на закрытие:

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

## Подписчик

`NatsSubscriber` оборачивает либо core-подписку `async-nats`, либо pull-поток JetStream, скрывая оба
за одним типом `Message`. `stream` разветвляется через `futures::future::Either` и забирает
внутренний поток при первом же опросе, поэтому он одноразовый: контракт разрешает ровно один вызов
`stream`.

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
варианта упакованы в `Box`, потому что обёрнутые сообщения `async-nats` большие. `ack` и `nack` на
доставке Core возвращают `AckError::Unsupported` - это не ошибка, и рантайм такой ответ принимает.
На JetStream они подтверждают доставку, а `nack` отображается в `nak` (повторная доставка), когда
обработчик об этом просит, и в `term` (отбросить poison-сообщение), когда не просит.

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

Проверка `lifecycle` из conformance принимает `AckError::Unsupported`, поэтому Core NATS её проходит.
Каждое сообщение преобразует свои заголовки один раз, при создании; эти две функции - единственное
место, которое зависит от версии `async-nats`:

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

## Публикация

Издатель и подключённый брокер, на котором его инстанцировала политика, владеют соединением
совместно. При каждой публикации издатель читает клиента через проверку на закрытие и передаёт
заголовки, если они есть.

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use ruststream::{OutgoingMessage, Publisher};

#[derive(Clone)]
pub struct NatsPublisher {
    connection: Arc<NatsConnection>,
}

impl Publisher for NatsPublisher {
    type Error = NatsError;

    // Core NATS lets a message differ from the next in nothing the client exposes per publish, so
    // there is no per-message setting to carry.
    type Options = ();

    /// # Cancel safety
    ///
    /// Core NATS publishing is fire-and-forget: the message is handed to the connection's writer
    /// without waiting for the server. Dropping the future may leave it either sent or unsent.
    async fn publish(
        &self,
        msg: OutgoingMessage<'_>,
        _options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
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

## Совместимости

В NATS request-reply есть на уровне транспорта, поэтому `RequestReply` вы можете реализовать на
издателе. Ожидание ограничено таймаутом вызывающей стороны, а сработавший таймер отображается в
`RequestTimeout`.

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

Pull-консьюмер JetStream забирает сообщения пакетами на уровне протокола, поэтому `BatchSubscriber`
отдаёт пакеты транспорта, а не эмулирует их. Один элемент потока - это одна выборка, ограниченная
размером пакета и сроком ожидания, а пустая выборка повторяется, поэтому пакет никогда не приходит
пустым. У core-ветки того же подписчика пакетов на уровне протокола нет, поэтому пакет там - это то,
что клиент уже сложил в локальный буфер, и ограничен он только размером. Брокер, у которого нет ни
того, ни другого, оставляет трейт-совместимость нереализованной, а пользователи берут клиентский
адаптер [`buffered`](../guides/subscribers.md#batch-subscribers).

`DescribeServer` помещает брокера в сгенерированный AsyncAPI-документ. Он реализуется на
**неподключённом** брокере, потому что документ генерируется из сервиса, который никуда не
подключался: трейт сообщает сконфигурированный адрес. Координаты, которые объявляет сам сервер
(маршрут кластера, обнаруженный узел), известны только после подключения, поэтому им место в
геттере подключённой формы, а не в этом трейте.

Всё остальное не реализовано, потому что этого нет у транспорта. В NATS нет транзакций, поэтому нет
`TransactionalPublisher` и `OwnedTransactions`. Нет и `Seekable`: NATS-вариант `Seekable` строился
бы на консьюмере JetStream, поток которого и есть воспроизводимый лог.

## Политика публикации

`NatsPublish` - политика, которая конструирует издателя `NatsPublisher`. Политику вы указываете при
регистрации обработчика, а издателя она инстанцирует при старте, на подключённом брокере. У
публикации в Core NATS нет опций на уровне издателя, потому что субъект и заголовки задают в каждом
сообщении. Поэтому политика здесь - пустая структура, а `pair` только копирует дескриптор соединения
и не может вернуть ошибку. Брокер, у которого создание издателя может вернуть ошибку (например,
транзакционный продюсер), оборачивает её через `PairError::new`. Раз простая политика годится как
есть, подключённая форма реализует ещё и `DefaultPublish`
(см. [контракт](index.md#publishpolicy)), и тогда обработчик с `publish(..)` компилируется без явно
указанного издателя.

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

## Прелюдия

Прелюдию крейта точка монтирования подключает целиком: прелюдия ядра, затем брокер и его
дескриптор, затем политики под едиными именами
([контракт](index.md#broker-prelude)).

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
pub use ruststream::prelude::*;

pub use crate::{NatsBroker, NatsError, NatsSource};
pub use crate::NatsPublish as Publish;

// The capabilities this broker implements on its live values.
pub use ruststream::{Positioned, RequestReply, Seekable, Seeker};
```

## Связывание с приложением

Когда брокер готов, приложение выглядит так же, как любое другое: ни в обработчиках, ни в кодеках
нет ничего специфичного для NATS.

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use ruststream_nats::prelude::*;

let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
    .with_broker(NatsBroker::new("nats://localhost:4222"), |b| {
        // `Publish` is this crate's publish policy; the runtime pairs it after connect.
        b.include(confirm).out_reply(Publish::default());
    });
```

## Как это доказать

Поставьте под фичей `testing` внутрипроцессный транспорт с одной только базовой маршрутизацией:
сопоставление субъектов, при котором опубликованное сообщение доставляется всем подписчикам разом.
Он реализует `TestableBroker` на своей подключённой форме, а её тип регистрируется через
`register_testable_broker!`. Прогоните на нём набор conformance. Курсоры JetStream, таймеры
повторной доставки и срок хранения такому транспорту эмулировать нельзя: их проверяет сквозной набор
тестов на настоящем `nats-server`. См. [Conformance](conformance.md).
