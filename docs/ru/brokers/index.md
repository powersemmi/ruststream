# Брокеры

Обработчики, роутеры, кодеки и middleware не зависят от брокера. Перевести сервис на другой брокер
вы можете, изменив одну строку в `with_broker`.

В составе фреймворка идёт полноценный in-memory брокер для очередей внутри одного приложения.
Для внешнего брокера очереди вы добавляете в зависимости отдельный крейт - адаптацию его
клиентской библиотеки.

У каждой адаптации библиотеки брокера свой сайт документации: ссылки собраны в столбце «Документация» и в меню
**Брокеры**.

| Брокер | Крейт | Транспорт | Документация |
|---|---|---|---|
| [Memory](memory.md) | `ruststream` (фича `memory`) | очередь внутри процесса, без сервера и без зависимостей | этот сайт |
| NATS | [`ruststream-nats`](https://github.com/powersemmi/ruststream-nats) | Core NATS и JetStream | [powersemmi.github.io/ruststream-nats](https://powersemmi.github.io/ruststream-nats/) |
| Redis | [`ruststream-fred`](https://github.com/powersemmi/ruststream-fred) | Redis Streams (standalone, кластер, sentinel) | [powersemmi.github.io/ruststream-fred](https://powersemmi.github.io/ruststream-fred/) |
| RabbitMQ | [`ruststream-lapin`](https://github.com/powersemmi/ruststream-lapin) | AMQP 0.9.1 (очереди, обменники, подтверждения издателя, direct reply-to) | [powersemmi.github.io/ruststream-lapin](https://powersemmi.github.io/ruststream-lapin/) |
| Kafka | [`ruststream-rdkafka`](https://github.com/powersemmi/ruststream-rdkafka) | Apache Kafka (группы консьюмеров, отслеживаемая фиксация смещений, транзакции, конвейеры exactly-once) | [powersemmi.github.io/ruststream-rdkafka](https://powersemmi.github.io/ruststream-rdkafka/) |
| AMQP 1.0 | [`ruststream-amqp`](https://github.com/powersemmi/ruststream-amqp) | ActiveMQ Artemis, RabbitMQ 4.x, Azure Service Bus и остальное семейство AMQP 1.0 (request-reply, транзакции) | [powersemmi.github.io/ruststream-amqp](https://powersemmi.github.io/ruststream-amqp/) |
| Google Cloud Pub/Sub | [`ruststream-gcp-pubsub`](https://github.com/powersemmi/ruststream-gcp-pubsub) | Pub/Sub через официальную клиентскую библиотеку (ключи упорядочивания, подтверждение exactly-once, политики DLQ) | [powersemmi.github.io/ruststream-gcp-pubsub](https://powersemmi.github.io/ruststream-gcp-pubsub/) |
| AWS SQS / SNS | [`ruststream-sqs-sns`](https://github.com/powersemmi/ruststream-sqs-sns) | Очереди SQS с доставкой через SNS всем подписчикам разом (FIFO-группы, управление видимостью, встроенная отложенная повторная доставка) | [powersemmi.github.io/ruststream-sqs-sns](https://powersemmi.github.io/ruststream-sqs-sns/) |
| Apache Pulsar | [`ruststream-pulsar`](https://github.com/powersemmi/ruststream-pulsar) | Темы и шаблоны Pulsar (режимы подписки, политики DLQ, перемотка) | [powersemmi.github.io/ruststream-pulsar](https://powersemmi.github.io/ruststream-pulsar/) |
| MQTT 5 | [`ruststream-rumqttc`](https://github.com/powersemmi/ruststream-rumqttc) | MQTT v5 (уровни QoS, shared-группы, retained-сообщения) | [powersemmi.github.io/ruststream-rumqttc](https://powersemmi.github.io/ruststream-rumqttc/) |
| ZeroMQ | [`ruststream-zeromq`](https://github.com/powersemmi/ruststream-zeromq) | Без брокера очереди: PUSH/PULL, PUB/SUB и request-reply на DEALER/ROUTER поверх TCP и IPC | [powersemmi.github.io/ruststream-zeromq](https://powersemmi.github.io/ruststream-zeromq/) |
| Файлы потоков / stdio | [`ruststream-sea-file`](https://github.com/powersemmi/ruststream-sea-file) | Долговечные воспроизводимые файлы потоков и конвейеры оболочки; нулевая инфраструктура, перемотка на любую позицию | [powersemmi.github.io/ruststream-sea-file](https://powersemmi.github.io/ruststream-sea-file/) |
| AWS Kinesis | [`ruststream-kinesis`](https://github.com/powersemmi/ruststream-kinesis) | Потоки данных Kinesis (аренда шардов, контрольные точки, перемотка) | [powersemmi.github.io/ruststream-kinesis](https://powersemmi.github.io/ruststream-kinesis/) |
| SQL-базы данных | [`ruststream-sqlx`](https://github.com/powersemmi/ruststream-sqlx) | Очереди задач в собственных таблицах сервиса (Postgres, MySQL/MariaDB, SQLite) и транзакционный outbox поверх любого брокера; SQL-запросы проверяются при запуске или на этапе компиляции | [репозиторий](https://github.com/powersemmi/ruststream-sqlx#readme) |

Как написать адаптацию библиотеки брокера для другого транспорта, объясняет раздел
[Авторам брокеров](../broker-authors/index.md).

## Переключение брокеров {#switching-brokers}

Чтобы перейти на другой брокер, замените импорт его типа и строку, которая его создаёт.
Остальной код в примерах ниже одинаков. Любой брокер создаётся синхронно, а подключает его рантайм на старте
приложения.

=== "Memory"

    <!-- inline-rust: side-by-side broker-switch comparison; the NATS half depends on the external ruststream-nats crate and has no in-repo compiled home, so both halves stay inline to read in parallel -->
    ```rust
    use ruststream::memory::MemoryBroker;
    use ruststream::runtime::{AppInfo, RustStream};

    #[ruststream::app]
    fn app() -> RustStream {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .with_broker(MemoryBroker::new(), |b| b.include_router(routes::orders()))
    }
    ```

=== "NATS"

    <!-- inline-rust: NATS half of the broker-switch comparison; depends on the external ruststream-nats crate, no in-repo compiled home -->
    ```rust
    use ruststream::runtime::{AppInfo, RustStream};
    use ruststream_nats::NatsBroker;

    #[ruststream::app]
    fn app() -> RustStream {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .with_broker(NatsBroker::new("nats://localhost:4222"), |b| {
                b.include_router(routes::orders())
            })
    }
    ```

=== "Redis"

    <!-- inline-rust: Redis half of the broker-switch comparison; depends on the external ruststream-fred crate, no in-repo compiled home -->
    ```rust
    use ruststream::runtime::{AppInfo, RustStream};
    use ruststream_fred::RedisBroker;

    #[ruststream::app]
    fn app() -> RustStream {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .with_broker(RedisBroker::standalone("redis://localhost:6379"), |b| {
                b.include_router(routes::orders())
            })
    }
    ```

=== "RabbitMQ"

    <!-- inline-rust: RabbitMQ half of the broker-switch comparison; depends on the external ruststream-lapin crate, no in-repo compiled home -->
    ```rust
    use ruststream::runtime::{AppInfo, RustStream};
    use ruststream_lapin::LapinBroker;

    #[ruststream::app]
    fn app() -> RustStream {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .with_broker(LapinBroker::new("amqp://localhost:5672"), |b| {
                b.include_router(routes::orders())
            })
    }
    ```

=== "Kafka"

    <!-- inline-rust: Kafka half of the broker-switch comparison; depends on the external ruststream-rdkafka crate, no in-repo compiled home -->
    ```rust
    use ruststream::runtime::{AppInfo, RustStream};
    use ruststream_rdkafka::KafkaBroker;

    #[ruststream::app]
    fn app() -> RustStream {
        RustStream::new(AppInfo::new("orders", "0.1.0"))
            .with_broker(
                KafkaBroker::new(["localhost:9092"]).default_group("orders"),
                |b| {
                    b.include_router(routes::orders())
                },
            )
    }
    ```

Параметры подключения каждая адаптация библиотеки брокера документирует сама.

Если подписке нужны опции конкретного брокера очереди (группы консьюмеров, durable-имена), вы можете
передать его дескриптор аргументом макроса `#[subscriber(..)]`. Об этом рассказывает раздел
[дескрипторы конкретных брокеров](https://docs.rs/ruststream/latest/ruststream/runtime/index.html#the-subscription-source).
