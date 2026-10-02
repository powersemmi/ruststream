# Установка

RustStream поставляется одним крейтом `ruststream`, а всё необязательное в нём включается
аддитивными фичами cargo. Добавьте крейт в `Cargo.toml`:

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "memory", "json"] }
serde = { version = "1", features = ["derive"] }
```

`serde` - прямая зависимость сервиса, потому что типы сообщений выводят `Deserialize` и
`Serialize`.

!!! note "Редакция и MSRV"
    Нужны **редакция 2024** и Rust **1.95** или новее. Укажите `edition = "2024"` в своём
    `Cargo.toml`. Адаптации библиотеки брокера может понадобиться более свежий Rust, если его требует
    клиентская библиотека. Точная граница записана в поле `rust-version` этой адаптации.

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
| `logging` | tracing-subscriber | `ruststream::logging`, цветной консольный логгер ([Логирование](https://docs.rs/ruststream/latest/ruststream/logging/index.html)) |
| `otel` | opentelemetry, opentelemetry-otlp | экспорт трасс и метрик по OTLP и передача trace-context по W3C ([OpenTelemetry](https://docs.rs/ruststream/latest/ruststream/otel/index.html)) |
| `testing` | inventory | `TestApp` и построители утверждений ([Тестирование](https://docs.rs/ruststream/latest/ruststream/testing/index.html)) |
| `conformance` | inventory | обвязка conformance для авторов адаптаций библиотек брокеров |
| `cli` | clap, anyhow | бинарник `ruststream` |

В сервисе можно включить сразу несколько кодеков (см. [Кодеки](https://docs.rs/ruststream/latest/ruststream/codec/index.html)). Чтобы убрать
встроенный JSON-кодек (например, в адаптации библиотеки брокера, которой нужны только трейты и рантайм),
отключите фичи по умолчанию:

```toml
[dependencies]
ruststream = { version = "0.7", default-features = false }
```

## CLI

Фича `cli` добавляет бинарник `ruststream`. Он запускает `cargo` с подкомандами фреймворка: `run` и
`asyncapi gen`. Установка и команды описаны в модуле
[`runtime::cli`](https://docs.rs/ruststream/latest/ruststream/runtime/cli/index.html).

Новый проект создаётся из шаблона командой `cargo generate`, это показано в
[быстром старте](quickstart.md).

## Конкретные брокеры

Брокер `memory` входит в ядро. Он работает внутри вашего процесса, поэтому ему не нужны ни сервер,
ни дополнительные зависимости.

Чтобы подключиться к внешнему брокеру очереди, добавьте адаптацию его клиентской библиотеки.
Например, для NATS:

```toml
[dependencies]
ruststream-nats = "0.7"
```

У каждой адаптации библиотеки брокера своя версия и свой график выпусков. Точную строку
зависимости, фичу `testing` для тестов обработчиков, настройки подключения и совместимости смотрите
в её документации. Все адаптации перечислены в разделе [Брокеры](../brokers/index.md), а как
написать свою, рассказывает раздел [Авторам брокеров](../broker-authors/index.md).
