# Как написать брокер

Брокер - это самостоятельный крейт, реализующий трейты ядра. Он зависит от `ruststream` с
выключенными фичами по умолчанию, поэтому получает поверхность трейтов и рантайм без встроенного
JSON-кодека и без чужих брокеров:

```toml
[dependencies]
ruststream = { version = "0.7", default-features = false }
```

Эта страница и есть контракт. Реализуйте обязательные трейты, заведите собственный `Config`,
добавьте трейты-возможности под то, что ваш брокер умеет, и подтвердите результат
[обвязкой conformance](conformance.md). Полная реализация поверх настоящего клиента разобрана в
[примере с NATS](example-nats.md).

## Обязательные трейты

### `Broker` и `ConnectedBroker`

Брокер - это чистый жизненный цикл, а сам цикл - лестница потребляющих переходов: каждое состояние
представлено отдельным типом, поэтому вызовы не в том порядке просто не компилируются. Брокер не
несёт ни типа подписчика, ни типа публикатора, так что одно приложение может смешивать брокеры
разных видов.

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

`shutdown` нельзя ни блокировать, ни доводить до паники; всё освобождение ресурсов, способное
завершиться ошибкой, делайте здесь и возвращайте `Result`. У свидетеля `Closed` нет ни поверхности
публикации, ни подписки; складывайте в него диагностику остановки (результаты сброса буферов,
счётчики потерь) как обычные данные - или
возьмите `()`.

Конструирование **синхронно и без I/O**: `new(addrs)` только записывает конфигурацию, вся сетевая
работа происходит в `connect` (рантайм вызывает его один раз на старте), а подключённая форма
держит живого клиента напрямую - её собственные операции никогда не проверяют состояние «вроде бы
подключены». Дополнительно брокер может держать разделяемую ячейку, которую заполняет `connect`
(или разделяемое внутрипроцессное состояние, как это делает in-memory брокер), чтобы публикаторов
можно было раздавать, пока приложение ещё собирается, до вызова `connect`; ячейка обслуживает
именно эти ранние хендлы, а не подключённую форму. Вариант с ячейкой показан в
[примере с NATS](example-nats.md). [Обвязка conformance](conformance.md) проверяет лестницу от
начала до конца.

У брокера, который вы уже остановили, нечего вызвать - ни публикации, ни подписки, поэтому ошибка со
стороны владельца не компилируется. Рантайм-правилом остаётся разделение соединения: хендлы,
разделяющие его (публикаторы, розданные из подключённой формы, клоны разделяемого брокера), обязаны
возвращать ошибку при использовании после остановки - и никогда не отрабатывать молча и успешно на
мёртвом соединении. Проверка `lifecycle` проходит и по этому пути.

### `Subscribe`

Реализуйте `Subscribe` на подключённой форме, чтобы поддержать подписку по имени. Именно этим
пользуется `#[subscriber("name")]`.

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/capability.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait Subscribe: ConnectedBroker {
    type Subscriber: Subscriber;
    async fn subscribe(&self, name: &str) -> Result<Self::Subscriber, Self::Error>;
}
```

### `Subscriber`

Подписчик - это `Stream` входящих сообщений. Обратное давление достаётся от стрима бесплатно.

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/subscriber.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait Subscriber: Send {
    type Message: IncomingMessage;
    type Error: std::error::Error + Send + Sync + 'static;
    fn stream(&mut self) -> impl Stream<Item = Result<Self::Message, Self::Error>> + Send + '_;
}
```

`stream` берёт `&mut self`, поэтому любое состояние, буферизованное между опросами, живёт за
мутабельным заимствованием - это и даёт безопасность при отмене.

### `IncomingMessage`

Доставленное сообщение отдаёт свою полезную нагрузку и заголовки, и его либо подтверждают через
ack, либо отклоняют через nack. Ack потребляет `self`, поэтому двойной ack - ошибка компиляции.

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/message.rs, with the defaulted methods annotated inline for teaching; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait IncomingMessage: Send + Sync {
    fn payload(&self) -> &[u8];
    fn headers(&self) -> &Headers;
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

Брокер, который не переопределил ни одного из двух методов с реализацией по умолчанию, всё равно
работает со всеми возможностями рантайма: `retry_after` сводится к немедленному возврату в очередь,
а полосы воркеров по ключу раскладывают сообщения без ключа по кругу.

### `Publisher`

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/publisher.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait Publisher: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;
    async fn publish(&self, msg: OutgoingMessage<'_>) -> Result<(), Self::Error>;

    /// С реализацией по умолчанию: заголовки, которые этот публикатор кладёт под каждую публикацию.
    fn base_headers(&self) -> Option<&Headers> { None }
}
```

`OutgoingMessage` заимствует и имя, и полезную нагрузку, поэтому публикация не вынуждает выделять
память.

Это интерфейс публикации, а не то, что пишет сервис: приложения публикуют через билдер
(`publisher.message(&value).publish()`, `publisher.raw(&bytes).to(dest).publish()`), который
разбирается с назначением, кодеком и заголовками, а затем делает ровно один вызов этого метода.
Реализуйте `publish` - и весь билдер прилагается; предоставлять больше нечего.

Публикатор, который несёт один аргумент для целой серии сообщений (арендатор, подсказка о партиции,
опция доставки, выражаемая вашим брокером через заголовок), возвращает этот аргумент из
`base_headers`, а не вписывает его в сообщение внутри `publish`. Билдер начинает исходящую карту с
этой основы и кладёт поверх, ключ за ключом, заголовки места вызова, поэтому побеждает место вызова
(см. [откуда берутся заголовки](../guides/publishing.md#where-the-headers-come-from)).
У `Transaction` есть тот же метод с реализацией по умолчанию, поэтому открытая от такого публикатора
транзакция ведёт себя так же. Публикатору, которому добавить нечего, не нужен ни один из них.

### `PublishPolicy`

Публикатор брокера - это связка политики (exchange, таймаут очереди, транзакционный id) и живого
соединения. Разрежьте его по этому шву: поставьте свободно конструируемый тип **политики** с
опциями билдера и без поверхности публикации, а затем реализуйте `PublishPolicy`, чтобы соединить
его с подключённой формой в живой публикатор. Соединение асинхронное и может завершиться ошибкой -
ради брокеров, которые делают настоящую работу, когда публикатор оживает (инициализация
транзакционного продюсера); для большинства это дешёвый вызов конструктора.

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/publisher.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait PublishPolicy<C: ConnectedBroker> {
    type Live; // the live publisher (or live wiring form, for combinator stacks)
    async fn pair(self, connected: &C) -> Result<Self::Live, PairError>;
}
```

Ошибка здесь - `PairError` со стёртым типом: заворачивайте отказ своего брокера через
`PairError::new`. Соединение выполняется по одному разу на публикатор на старте и никогда не
попадает на горячий путь.

Поставляйте по одной паре «политика / живая форма» на каждый настоящий **режим** публикации, а
выбор режима делайте переходом типа политики, а не рантайм-флагом: обычная политика соединяется в
обычный публикатор, а шаг билдера `transactional_id(..)` переводит в отдельный тип транзакционной
политики, живая форма которой реализует `TransactionalPublisher`, - и тогда у обычного публикатора
транзакционной поверхности нет вовсе. Минимальный эталон - `MemoryPublish` / `MemoryRequest` из
in-memory брокера (опций нет, поэтому это unit-маркеры); типизированные комбинаторы ядра реализуют
`PublishPolicy` функториально, и именно это позволяет пользователям составлять кодеки и
преобразования поверх вашей политики ещё до того, как она соединится.

Если обычная политика годится со своими умолчаниями (а так почти всегда), реализуйте на
подключённой форме ещё и `DefaultPublish`, чтобы её назвать. Именно это позволяет рантайму собрать
публикатор ответа по умолчанию, когда обработчик с `publish("dest")` монтируется без явного
`.publisher(..)` - то есть `b.include(def)` компилируется сам по себе. Брокеры, публикаторам
которых всегда нужны явные опции, его не реализуют, и их пользователи прикладывают политику при
каждой регистрации.

<!-- inline-rust: simplified contract sketch of the real trait in src/publisher.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait DefaultPublish: ConnectedBroker {
    type Policy: PublishPolicy<Self> + Default + Send + 'static;
}
```

## Источники подписки {#subscription-sources}

`Subscribe` закрывает случай «по имени». Когда подписке нужны специфичные для брокера опции
(consumer group, durable-имя, политика доставки), заведите тип-дескриптор, реализующий
`SubscriptionSource`:

<!-- inline-rust: simplified contract sketch of the real RPITIT trait in src/subscription.rs; a compiled copy would just duplicate the source with more noise -->
```rust
pub trait SubscriptionSource<C: ConnectedBroker> {
    type Subscriber: Subscriber;
    fn name(&self) -> &str;
    fn subscribe(self, connected: &C) -> impl Future<Output = Result<Self::Subscriber, C::Error>> + Send;
}
```

Дайте дескриптору ассоциированный конструктор (`OrdersStream::new(..)`), а не свободную функцию,
чтобы пользователи могли назвать его прямо в декораторе:
`#[subscriber(OrdersStream::new("orders", "workers"))]`. Макрос вычитывает тип из вызова
конструктора и принимает на нём ещё и цепочку билдера
(`#[subscriber(OrdersStream::new("orders").durable("workers"))]`), пока каждый метод возвращает
`Self`. Поскольку `type Subscriber` живёт на источнике, один брокер может предложить несколько
видов подписки (pub/sub против стримов) с разными типами подписчиков - или, как в
[примере с NATS](example-nats.md), обслуживать их все одним дескриптором, который ветвится внутри.

Выведите на дескрипторе `Clone`: это конфигурация, и монтирование пересобирает её на каждую
регистрацию, поэтому одно определение можно смонтировать сразу на два брокера.

### Как назвать вид одной строкой

Вид, который определяется именем и больше ничем, реализует ещё и `FromName` - его единственный
конструктор строит значение из этого имени:

<!-- inline-rust: one-impl sketch against a broker-crate descriptor that has no in-repo compiled home -->
```rust
impl FromName for OrdersStream {
    fn from_name(name: impl Into<Cow<'static, str>>) -> Self {
        Self::new(name)
    }
}
```

Именно это делает `#[subscriber(OrdersStream)]` законным: атрибут фиксирует вид, а значение
подставляет точка монтирования. Вид, которому для существования по-настоящему нужно больше одного
имени (топик *и* имя подписки), его не реализует, и такая форма для него не компилируется.

### Настройки на вашем собственном языке

Ядро не может знать, что у подписки есть стрим, durable-имя или consumer group, поэтому оно даёт
один хук - `map_source`, преобразование над источником, который собирает точка монтирования, - а
ваш крейт кладёт сверху свой трейт, привязанный к вашему типу источника:

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

Ограничение по типу источника означает, что на билдере другого брокера этих методов просто не
существует. Пользователи
импортируют трейт, чтобы до них добраться, как и в случае любого расширяющего трейта. Ниже словарь
слотов `Out` пользуется тем же приёмом с расширением.

## Трейты-возможности

Реализуйте только те возможности, которые ваш брокер поддерживает; ни одна из них не входит в
обязательный интерфейс.

| Трейт | Для брокеров, которые умеют |
|---|---|
| `BatchSubscriber` | получать сообщения пачками |
| `TransactionalPublisher` | begin / commit / abort вокруг публикаций на самом хендле |
| `OwnedTransactions` / `Transaction` | транзакции, чей буфер живёт в значении: один хендл держит сколько угодно сразу |
| `RequestReply` | нативный request-reply (у NATS есть, у Kafka нет) |
| `Partitioned` | ключ партиционирования на исходящих сообщениях |
| `Seekable` / `Seeker` | перемещение живой подписки по воспроизводимому логу |
| `Positioned` | доставки, которые сообщают собственную позицию в логе |
| `DescribeServer` | сообщать `ServerSpec` для AsyncAPI |

`Seekable` выдаёт свой хендл `Seeker` до того, как стрим заимствует подписчика, поэтому работающую
подписку можно переместить извне цикла диспетчеризации. Позиции принадлежат брокеру (конструкторы в
духе `KafkaPosition` на вашем собственном типе); позиция, снятая с доставленного сообщения через
`Positioned::position`, несёт закреплённый контракт - перемотка на неё повторно доставит ровно
это сообщение, - а у сконструированных позиций семантика та, которую описывает ваш тип позиции.
Опишите, на что распространяется одна перемотка (на экземпляр консьюмера или на общий курсор
группы), и сбрасывайте всю бухгалтерию ack, которую перемещение делает недействительной.

### Расширение словаря слотов `Out`

Параметр обработчика `Out<impl X, Marker>` принимает любой `X`, который реализует обёртка
`SlotPublisher` из рантайма; ядро делегирует собственный набор возможностей (`Publisher`,
`TransactionalPublisher`, `OwnedTransactions`, `RequestReply`). Когда соединённое вами значение
умеет больше - или вовсе не является публикатором (кэш продюсеров по партициям, шардирующий
роутер), - объявите собственный трейт-возможность, реализуйте его для этого значения и привейте к
обёртке одним blanket-impl, делегирующим через `SlotPublisher::inner`. После этого обработчики
ограничивают свой слот вашим трейтом, а конкретный тип по-прежнему не появляется в коде приложения:

```rust
--8<-- "tests/out_slots.rs:extension"
```

Публикации, сделанные через значения, полученные из `inner`, обвязка не приписывает слоту (как и
буфер уже завершённой owned-транзакции); в логе публикаций брокера они остаются видны.

## Контекст доставки и ключи `Ctx`

Брокер с нативными метаданными доставки (партиция, offset, номер в стриме) отдаёт их типизированным
контекстом доставки: это структура с `#[non_exhaustive]`, которую называет подписчик, плюс
типы-ключи `ContextField`, чтобы обработчики могли привязывать отдельные поля как параметры через
[экстрактор `Ctx<K>`](../guides/context.md#per-delivery-context). Ключи - это unit-структуры,
значения - владеемые. Ни type-map, ни кучи на пути доставки.

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

Брокер, у которого своих полей доставки нет, берёт `()` и пропускает всё это.

## Middleware на асинхронных краях {#middleware-on-the-async-edges}

Интеграциям, которым нужен асинхронный I/O вокруг кодирования и декодирования (schema registry,
конверт поверх формата на проводе), не место в `Codec`: кодек ядра синхронный, а обработчикам стоит
оставаться на кодеке по умолчанию. Ставьте такие интеграции на асинхронные края: входящие полезные
нагрузки перекодируйте на пути доставки подписки (до того, как их увидит кодек), а исходящие
оборачивайте слоем `PublishLayer` из ядра, добавленным на всё приложение через
`RustStream::publish_layer`. Слой публикации асинхронный и может завершиться ошибкой,
а `Outgoing::payload_mut` существует ровно для оборачивания в конверт.

## Конфигурация и умолчания

Свой `Config` принадлежит вашему крейту; ядро не несёт никакой специфичной для брокера
конфигурации. Если у поля конфигурации нет разумного умолчания, не реализуйте для него `Default`:
пусть пользователь задаст значение явно, чем вы поставите умолчание, которое потом сломается.

## Ошибки

Возьмите `thiserror` и один enum ошибок на весь крейт, с вариантами по источнику. Публичные
перечисления ошибок помечайте `#[non_exhaustive]`. В библиотечном крейте `anyhow` не используйте
никогда.

## Поддержка тестирования {#test-support}

Поставляйте внутрипроцессный транспорт, реализующий `TestableBroker` на **подключённой форме**, под
фичей `testing` (зарегистрированный через `register_testable_broker!` именно для этого подключённого
типа, поскольку обвязка подключает каждый брокер, прежде чем достать его транспорт), - тогда
пользователи смогут писать модульные тесты обработчиков против вашего брокера с обвязкой `TestApp`.
Транспорт делает **только маршрутизацию ядра**: раздаёт опубликованные сообщения подходящим
подписчикам и трактует ack/nack фактически как no-op. Не имитируйте в нём специфичную для брокера
семантику (durable-курсоры, таймеры повторной доставки, смещения, маршрутизацию в dead-letter) - её
проверяют сквозными тестами против настоящего сервера.

Эталон - собственная реализация in-memory брокера (на `ConnectedMemoryBroker`):

```rust
--8<-- "src/memory/mod.rs:testable"
```

Транспорт вызывает `Coordinator::enqueued` на каждую постановку в очередь подписчика и
`Coordinator::consumed`, когда доставка завершена или отброшена (так обвязка понимает, что реакция
улеглась), а отложенные повторные доставки проводит через `Coordinator::schedule_redelivery`. Один
такой тип работает затем и с `TestApp`, и с набором conformance. Пользовательская сторона описана в
разделе [Тестирование](../guides/testing.md), а [Conformance](conformance.md) показывает, как
подтвердить реализацию через `run_suite` и лестничную проверку `lifecycle`.
