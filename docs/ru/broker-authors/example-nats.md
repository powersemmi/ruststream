# Разбор примера: брокер NATS

Крейт [`ruststream-nats`](https://github.com/powersemmi/ruststream-nats) выполняет
[контракт](index.md) поверх клиента [`async-nats`](https://docs.rs/async-nats). Это полноценный
брокер в миниатюре. Страница разбирает его по частям:

- переходы `Broker` -> `ConnectedBroker` -> `Closed`;
- одна подписка на Core NATS и JetStream с общим дескриптором `SubscribeOptions`;
- издатель с заголовками;
- те трейт-совместимости, которые есть у транспорта.

Код урезан до того, что требует контракт. В самом крейте есть ещё опции, тонкая настройка и
типизированный контекст доставки.

```toml title="Cargo.toml"
[features]
default = []
# The in-process mode users test the production app with. The conformance harness is a
# broker-author tool and stays a dev-dependency, not a feature users can turn on.
testing = ["ruststream/testing"]

[dependencies]
ruststream = { version = "0.7", default-features = false }
```

Остальные зависимости - клиент и его окружение: `async-nats`, `bytes`, `futures`, `thiserror`,
`tokio` и `tracing`. Имена в коде взяты из API `async-nats`, а оно меняется от релиза к релизу.
Версию клиента выбирает крейт брокера и указывает её в своей документации.

## Ошибки

У крейта одно перечисление ошибок с вариантами по источникам:

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

`#[non_exhaustive]` делает новый вариант неломающим изменением. Источники хранятся как ошибки
`std` в `Box`, поэтому типы ошибок `async-nats` в публичный API не попадают. `Closed` называет
субъект, а не только факт, что соединения больше нет: ошибка, которую сервис читает в три часа
ночи, говорит, чего он не смог достичь.

## Жизненный цикл брокера

Брокер проходит три состояния, и у каждого свой тип:

- `NatsBroker` только записывает адрес. `new` синхронный и не выполняет ввода-вывода.
- `connect` поглощает `self`, устанавливает соединение и возвращает `ConnectedNatsBroker` с живым
  клиентом внутри. Его операциям не нужно проверять, подключены ли они. Издателей выдаёт только
  он, поэтому издатель без соединения непредставим.
- `shutdown` поглощает подключённую форму и возвращает свидетеля `ClosedNatsBroker`. `shutdown` выполняет
  всё завершение, которое может вернуть ошибку, и никогда не паникует.

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

**Что запрещают типы.** Поглощение `self` запрещает владельцу второй `connect`, а также публикацию
и подписку после остановки.

**Что проверяется во время выполнения.** Издатель может пережить соединение: его делят несколько
дескрипторов, и порядок вызовов здесь ни при чём. Поэтому у соединения есть флаг закрытия.
`shutdown` выставляет его до `drain`, а каждый дескриптор проверяет флаг, прежде чем взять
клиента. Созданный раньше издатель после остановки возвращает `Closed` и не публикует в закрытое
соединение. Это контракт разделяемых дескрипторов, и `lifecycle` его проверяет.

## Одна подписка на Core и JetStream

У NATS два режима доставки. Core NATS отправляет сообщения без подтверждений. JetStream их хранит
и требует подтверждать доставку. Крейт описывает оба одним дескриптором `SubscribeOptions`, а
отдаёт одним подписчиком `NatsSubscriber`. Дескриптор реализует `SubscriptionSource`. JetStream включает вызов `jetstream(..)`:

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
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
        connected.subscribe_with(self).await
    }
}
```

**Дескриптор в атрибуте.** Каждый метод билдера - один именованный аргумент атрибута
`#[subscriber(..)]`. Атрибут принимает цепочку вызовов целиком:

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
#[subscriber(SubscribeOptions::new("orders.*").jetstream("ORDERS").durable("worker"))]
async fn handle(order: &Order) -> HandlerOutcome {
    HandlerOutcome::ack()
}
```

**Подписка по имени.** `Subscribe` строит `SubscribeOptions::new(name)`, поэтому форма
`#[subscriber("orders")]` идёт тем же путём:

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

**Одна точка ветвления.** Обе формы приходят в `subscribe_with` подключённой формы. Метод проверяет
опции и ветвится ровно один раз. Клиента он берёт из соединения, а соединение проверяет, не
закрыто ли оно:

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

`queue_group_ref`, `stream_ref` и `durable_ref` здесь - маленькие геттеры `pub(crate)`, они
возвращают `Option<&str>`.

## Подписчик

`NatsSubscriber` оборачивает либо core-подписку `async-nats`, либо pull-поток JetStream. Для
рантайма оба выглядят одинаково: поток сообщений типа `Message`.

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

`stream` ветвится через `futures::future::Either` и забирает внутренний поток при первом опросе.
Второй раз забрать его нельзя, поэтому подписчик одноразовый. Контракт это разрешает: `stream`
вызывают ровно один раз.

## Сообщение

`NatsMessage` - перечисление из двух вариантов: доставка Core без ack и доставка JetStream с
настоящим ack. Сообщения `async-nats` большие, поэтому оба варианта лежат в `Box`.

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

**Core.** `ack` и `nack` возвращают `AckError::Unsupported`. Это не ошибка: такой ответ принимает
рантайм и проверка `lifecycle` из conformance, поэтому Core NATS её проходит.

**JetStream.** `ack` подтверждает доставку. `nack` становится `nak`, если обработчик просит
повторную доставку, и `term`, если не просит. Во втором случае poison-сообщение отбрасывается.

Заголовки сообщение преобразует один раз, при создании. От версии `async-nats` зависят только эти
две функции:

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

Издатель делит соединение с подключённым брокером, на котором политика его инстанцировала:

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use ruststream::{OutgoingMessage, Publisher};

#[derive(Clone)]
pub struct NatsPublisher {
    connection: Arc<NatsConnection>,
}

impl Publisher for NatsPublisher {
    // The client owns the payload until it has written it, so the publish hands the buffer over
    // and freezes it instead of copying the bytes.
    type Payload = Take;

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
        msg: OutgoingMessage<'_, BytesMut>,
        _options: Option<&Self::Options>,
    ) -> Result<(), Self::Error> {
        let client = self.connection.live_client(msg.name())?.clone();
        let (subject, payload, headers) = msg.into_parts();
        let subject = subject.to_owned();
        let payload = payload.freeze();
        match headers_to_nats(&headers) {
            Some(headers) => client.publish_with_headers(subject, headers, payload).await,
            None => client.publish(subject, payload).await,
        }
        .map_err(|e| NatsError::Publish(Box::new(e)))
    }
}
```

Перед каждой публикацией издатель проверяет, не закрыто ли соединение, и берёт из него клиента.
Клиент владеет нагрузкой, пока не запишет её. Поэтому издатель объявляет `Take`: он передаёт
буфер клиенту и замораживает его, а не копирует байты. Заголовки он передаёт, если они
есть. `Options` - это `()`: клиент Core NATS задаёт сообщение только субъектом, заголовками и
нагрузкой.

## Политика публикации

`NatsPublish` - политика, которая конструирует издателя `NatsPublisher`. Вы указываете её при
регистрации обработчика, а издателя она инстанцирует при старте на подключённом брокере.

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

**Пустая политика.** Субъект и заголовки задаются в каждом сообщении, поэтому у публикации в Core
NATS настроек издателя нет. Политика - пустая структура. `pair` только копирует дескриптор
соединения и не может вернуть ошибку. Брокер, у которого создание издателя может вернуть ошибку
(например, транзакционный продюсер), оборачивает её в `PairError::new`.

**Политика по умолчанию.** Подключённая форма реализует ещё и `DefaultPublish`
(см. [контракт](index.md#publishpolicy)): простая политика годится как есть. Тогда обработчик в
reply-форме, с аргументом `publish(..)` атрибута `#[subscriber]`, компилируется без явно
указанного издателя.

## Совместимости

Крейт реализует совместимости, которые есть у самого транспорта NATS.

**Запрос и ответ.** Request-reply встроен в NATS, поэтому `RequestReply` реализован на издателе.
Запрос ждёт ответа не дольше таймаута вызывающей стороны. Когда таймер срабатывает, запрос
возвращает `RequestTimeout`.

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use std::time::Duration;

use ruststream::RequestReply;

impl RequestReply for NatsPublisher {
    type Reply = NatsMessage;

    async fn request(
        &self,
        msg: OutgoingMessage<'_, BytesMut>,
        timeout: Duration,
    ) -> Result<Self::Reply, Self::Error> {
        let client = self.connection.live_client(msg.name())?.clone();
        let subject = msg.name().to_owned();
        let request = async_nats::Request::new().payload(msg.into_payload().freeze());
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

**Пакеты.** Pull-консьюмер JetStream забирает сообщения пакетами прямо в протоколе, поэтому
`BatchSubscriber` отдаёт пакеты самого транспорта. Одна выборка - один элемент потока. Её
ограничивают размер пакета и срок ожидания. Пустая выборка повторяется, так что пакет никогда не
приходит пустым. В core-ветке того же подписчика пакет - это то, что клиент уже сложил в локальный
буфер, и его ограничивает только размер. Для брокера без пакетов есть клиентский адаптер
[`buffered`](https://docs.rs/ruststream/latest/ruststream/runtime/index.html#batches).

**Описание сервера.** `DescribeServer` добавляет брокер в AsyncAPI-документ. Документ генерирует
сервис, который никуда не подключался, поэтому трейт реализован на **неподключённом** брокере и
сообщает адрес из конфигурации. Координаты, которые объявляет сам сервер (маршрут кластера,
обнаруженный узел), известны только после подключения. Их отдаёт геттер подключённой формы.

Основой для `Seekable` мог бы стать консьюмер JetStream: его поток и есть воспроизводимый журнал.

## Прелюдия

Точка монтирования подключает крейт одним glob-импортом:

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
pub use ruststream::prelude::*;

pub use crate::{NatsBroker, NatsError, SubscribeOptions};
pub use crate::NatsPublish as Publish;

// The capabilities this broker implements on its live values.
pub use ruststream::RequestReply;
```

Прелюдия собирает всё, что там нужно: прелюдию ядра, брокер и его дескриптор, затем политики под
общими именами ([контракт](index.md#broker-prelude)).

## Связывание с приложением

Готовый брокер подключается к приложению так же, как любой другой:

<!-- inline-rust: reproduces the sibling ruststream-nats crate source for teaching; that code lives in another repo and has no compilable home here -->
```rust
use ruststream_nats::prelude::*;

let app = RustStream::new(AppInfo::new("orders", "0.1.0"))
    .with_broker(NatsBroker::new("nats://localhost:4222"), |b| {
        // `Publish` is this crate's publish policy; the runtime pairs it after connect.
        b.include(confirm).out_reply(Publish::default());
    });
```

Обработчики и кодеки не знают, что работают с NATS.

## Как это доказать

Сервису нужны тесты без сервера NATS. Для них крейт поставляет под фичей `testing` режим работы
внутри процесса. `InProcess` на `NatsBroker` даёт метод `connect_in_process`. Он возвращает
подключённую форму, у которой вместо клиента - сопоставление субъектов. Сообщение, опубликованное
в субъект, получают все его подписчики разом. Подключённая форма реализует `TestableBroker`, а
`register_testable_broker!(NatsBroker)` регистрирует рабочий тип.

Наборы conformance прогоняются и внутри процесса, и против сервера. Часть поведения есть только у
сервера: курсоры JetStream, таймеры повторной доставки, срок хранения. Поэтому те же тесты идут и
против настоящего `nats-server`, через `TestApp::start_live`. Подробнее - в разделе
[Conformance](conformance.md).
