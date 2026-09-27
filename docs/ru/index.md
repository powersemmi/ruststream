# RustStream

**RustStream** подписывает Rust-сервис на потоки событий и публикует в них сообщения. Сервис при
этом не привязан к одному брокеру сообщений. Ядро - это трейты и рантайм с роутером. Вместе с ядром
поставляются кодеки, генерация AsyncAPI, метрики Prometheus и набор проверок `conformance` для
авторов брокеров.

Фреймворк определяют два архитектурных обязательства:

1. **Полноценный интерфейс для сторонних брокеров.** Ядро содержит только трейты и типы, ни одной
   зависимости от брокера. Каждый брокер - самостоятельный крейт. Соблюдение контракта проверяет
   набор `conformance`.
2. **Конфигурация брокера остаётся в его крейте.** В ядре нет ни настроек, ни умолчаний,
   привязанных к конкретному брокеру. Каждый крейт брокера объявляет свой `Config`. Поэтому
   изменение на стороне брокера затрагивает только его крейт, а не фреймворк.

=== "Макросы"

    ```rust
    --8<-- "examples/quickstart.rs"
    ```

=== "Вручную"

    ```rust
    --8<-- "examples/manual/quickstart.rs"
    ```

`#[ruststream::app]` генерирует `main` со всем шаблонным кодом рантайма. Поэтому `cargo run -- run`
запускает сервис, а `cargo run -- asyncapi gen` печатает его AsyncAPI-документ.

## Принципы устройства

- **Полностью асинхронный, на tokio.** В публичном API нет блокирующих вызовов.
- **Обобщённое ядро, никакого `dyn` в контракте.** Контракт построен на ассоциированных типах и
  нативном `async fn in trait`. Стирание типов, если оно нужно сервису, выполняет рантайм.
- **Подписчики - это `Stream`, а не колбэки.** Обратное давление обеспечивает сам `Stream`. Колбэки
  надстраивает рантайм.
- **Ack поглощает `self`.** Второй ack - ошибка компиляции.
- **Трейты-совместимости для необязательных возможностей.** `BatchSubscriber`,
  `TransactionalPublisher`, `RequestReply`, `Partitioned` и `Seekable` не входят в обязательный
  интерфейс.

## Куда идти дальше

<div class="grid cards" markdown>

- :material-download: **[Установка](getting-started/installation.md)** - подключение крейта и выбор фич.
- :material-rocket-launch: **[Быстрый старт](getting-started/quickstart.md)** - сервис из шаблона за одну команду `cargo generate`.
- :material-school: **[Учебник](getting-started/tutorial.md)** - первый сервис шаг за шагом.
- :material-test-tube: **[Тестирование](https://docs.rs/ruststream/latest/ruststream/testing/index.html)** - тесты обработчиков внутри процесса, без сервера.
- :material-web: **[HTTP-фреймворки](https://docs.rs/ruststream/latest/ruststream/runtime/index.html#running-beside-another-server)** - сервис рядом с axum и транзакционный outbox.
- :material-transit-connection-variant: **[Брокеры](brokers/index.md)** - in-memory брокер и крейты остальных брокеров.
- :material-server-network: **[Авторам брокеров](broker-authors/index.md)** - свой брокер: контракт и проверки `conformance`.

</div>

## Что входит в этот репозиторий

Этот сайт документирует `ruststream` - ядро, не зависящее от брокера. Конкретные брокеры (NATS,
Kafka, RabbitMQ, Redis, MQTT и другие) поставляются отдельными крейтами. Каждый такой крейт
подключает `ruststream` с crates.io.

Справочник по Rust API опубликован на [docs.rs](https://docs.rs/ruststream) - см.
[Справочник API](reference.md).
