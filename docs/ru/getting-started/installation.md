# Установка

RustStream поставляется одним крейтом `ruststream`, поверхность которого включается аддитивными
фичами cargo. Добавьте его в `Cargo.toml`:

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "memory", "json"] }
serde = { version = "1", features = ["derive"] }
```

`serde` - прямая зависимость вашего сервиса, потому что ваши типы сообщений выводят
`Deserialize` / `Serialize`.

!!! note "Редакция и MSRV"
    RustStream рассчитан на **редакцию 2024** и минимальную версию Rust **1.88**. Пропишите
    `edition = "2024"` в своём `Cargo.toml`.
    Крейту брокера может понадобиться более свежая версия Rust, чем ядру, если её требует
    клиентская библиотека. Точную границу указывает поле `rust-version` крейта брокера.

## Фичи

Трейты ядра, объект приложения `RustStream`, `Router`, middleware и диспетчеризация сообщений
подписчикам компилируются всегда. Всё остальное - аддитивные фичи, которые вы включаете по
необходимости.

| Фича | Зависимости | Что даёт |
|---|---|---|
| `json` *(по умолчанию)* | serde_json | `JsonCodec` |
| `msgpack` | rmp-serde | `MsgpackCodec` |
| `cbor` | ciborium | `CborCodec` |
| `memory` | - | `MemoryBroker`, эталонный in-memory брокер |
| `macros` | ruststream-macros | `#[subscriber]`, `#[ruststream::app]` и derive-макросы (`Outgoing`, `OutSlot`, `OutMessages`, `Deserialized`, `Serialized`, `FromRef`, `MessageInfo`) |
| `asyncapi` | schemars, serde_norway | генерация AsyncAPI и HTML-просмотрщик |
| `metrics` | prometheus | middleware и экспортёр Prometheus |
| `logging` | tracing-subscriber | `ruststream::logging`, цветной консольный логгер ([Логирование](../guides/logging.md)) |
| `otel` | opentelemetry, opentelemetry-otlp | экспорт трасс и метрик по OTLP и передача trace-context по W3C ([OpenTelemetry](../guides/opentelemetry.md)) |
| `testing` | inventory | `TestApp` и построители утверждений ([Тестирование](../guides/testing.md)) |
| `conformance` | inventory | обвязка conformance для авторов брокеров |
| `cli` | clap, anyhow | бинарник `ruststream` |

В сервисе можно включить сразу несколько кодеков (см. [Кодеки](../guides/codecs.md)). Чтобы убрать
встроенный JSON-кодек (например, в крейте брокера, которому нужны только трейты и рантайм),
отключите фичи по умолчанию:

```toml
[dependencies]
ruststream = { version = "0.7", default-features = false }
```

## CLI

Бинарник `ruststream` поставляется вместе с крейтом за фичей cargo `cli`. Он вызывает `cargo` с
подкомандами фреймворка (`run`, `asyncapi gen`); установка и команды описаны в
[руководстве по CLI](../guides/cli.md). Заготовку нового проекта создаёт `cargo generate` по
шаблону, о чём рассказано в [быстром старте](quickstart.md).

## Конкретные брокеры

Брокер `memory` встроен в крейт и не требует внешнего сервиса. Чтобы работать с брокером за
пределами процесса, подключите крейт этого брокера: он реэкспортирует из `ruststream` всё, что ему
нужно.

У каждого брокера своя версия и свой цикл выпуска, поэтому точную строку зависимости - с текущей
версией и фичей `testing` для тестов обработчиков - смотрите в его собственной документации. Там же
описаны `Config` и список совместимостей.

Доступные брокеры перечислены в разделе [Брокеры](../brokers/index.md); оттуда ведёт ссылка на
документацию каждого из них с инструкцией по установке. Если вы пишете свой брокер, смотрите
[Авторам брокеров](../broker-authors/index.md).
