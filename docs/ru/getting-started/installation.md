# Установка

RustStream поставляется одним крейтом `ruststream`, вся поверхность которого закрыта аддитивными
фичами cargo. Добавьте его в `Cargo.toml`:

```toml
[dependencies]
ruststream = { version = "0.7", features = ["macros", "memory", "json"] }
serde = { version = "1", features = ["derive"] }
```

`serde` попадает в прямые зависимости вашего сервиса, потому что ваши типы сообщений выводят
`Deserialize` / `Serialize`.

!!! note "Редакция и MSRV"
    RustStream рассчитан на **редакцию 2024** и минимальную версию Rust **1.85** (нативный
    `async fn in trait`). Пропишите `edition = "2024"` в своём `Cargo.toml`. CI собирает и
    тестирует крейт на нижней границе и на текущем stable, а также собирает на beta, так что
    подойдёт любая версия начиная с 1.85.
    Крейту брокера может требоваться более свежий тулчейн, чем ядру, когда этого требует его
    клиентская библиотека; смотрите `rust-version` в самом крейте брокера.

## Фичи

Трейты ядра, объект приложения `RustStream`, `Router`, middleware и диспетчеризация сообщений
подписчикам компилируются всегда. Всё остальное - аддитивные фичи, которые включаются по желанию.

| Фича | Что тянет | Что даёт |
|---|---|---|
| `json` *(по умолчанию)* | serde_json | `JsonCodec` |
| `msgpack` | rmp-serde | `MsgpackCodec` |
| `cbor` | ciborium | `CborCodec` |
| `memory` | - | `MemoryBroker`, эталонный in-memory брокер |
| `macros` | ruststream-macros | `#[subscriber]`, `#[ruststream::app]`, `#[derive(MessageInfo)]` |
| `asyncapi` | schemars, serde_norway | генерация AsyncAPI и HTML-просмотрщик |
| `metrics` | prometheus | middleware и экспортёр Prometheus |
| `logging` | tracing-subscriber | `ruststream::logging`, цветной консольный логгер ([Логирование](../guides/logging.md)) |
| `otel` | opentelemetry, opentelemetry-otlp | экспорт трасс и метрик по OTLP и перенос trace-context по W3C ([OpenTelemetry](../guides/opentelemetry.md)) |
| `testing` | inventory | `TestApp` и построители утверждений ([Тестирование](../guides/testing.md)) |
| `conformance` | - | обвязка conformance для авторов брокеров |
| `cli` | clap, anyhow | бинарник `ruststream` |

Фичи кодеков совместимы друг с другом, включайте сколько нужно (см.
[Кодеки](../guides/codecs.md)). Чтобы выкинуть встроенный JSON-кодек (например, в крейте брокера,
которому нужны только трейты и рантайм), отключите умолчания:

```toml
[dependencies]
ruststream = { version = "0.7", default-features = false }
```

## CLI

Необязательный бинарник `ruststream` поставляется вместе с крейтом за фичей cargo `cli` и вызывает
`cargo` с подкомандами фреймворка (`run`, `asyncapi gen`); установка и команды описаны в
[руководстве по CLI](../guides/cli.md). Для создания заготовки нового проекта он не нужен - это
делает `cargo generate` по шаблону, о чём рассказано в [быстром старте](quickstart.md).

## Конкретные брокеры

Брокер `memory` встроен в крейт и не требует внешнего сервиса. Чтобы выйти на брокер за пределами
процесса, подключайте крейт брокера: он реэкспортирует из `ruststream` всё, что ему нужно. Каждый
брокер версионируется и релизится независимо, поэтому точную строку зависимости (включая текущую
версию и фичу `testing` для тестов обработчиков), а вместе с ней `Config` и список
совместимостей, несёт его собственная документация.

Список доступных брокеров лежит в разделе [Брокеры](../brokers/index.md); оттуда есть ссылка на
документацию каждого брокера с инструкцией по установке. Если хотите написать свой, смотрите
[Авторам брокеров](../broker-authors/index.md).
