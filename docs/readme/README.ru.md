<h1 align="center">RustStream</h1>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream/blob/main/README.md">English</a> | <b>Русский</b> | <a href="https://github.com/powersemmi/ruststream/blob/main/docs/readme/README.zh.md">中文</a>
</p>

<p align="center">
  <i>Асинхронный фреймворк обмена сообщениями для Rust: трейты, не привязанные к брокеру, рантайм с роутером, кодеки, генерация AsyncAPI, наблюдаемость через Prometheus и OpenTelemetry и набор проверок соответствия для авторов брокеров.</i>
</p>

<p align="center">
  <a href="https://github.com/powersemmi/ruststream/actions/workflows/ci.yml"><img src="https://github.com/powersemmi/ruststream/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="https://coveralls.io/github/powersemmi/ruststream?branch=main"><img src="https://coveralls.io/repos/github/powersemmi/ruststream/badge.svg?branch=main" alt="Coverage"></a>
  <a href="https://crates.io/crates/ruststream"><img src="https://img.shields.io/crates/v/ruststream.svg" alt="crates.io"></a>
  <a href="https://crates.io/crates/ruststream"><img src="https://img.shields.io/crates/dr/ruststream" alt="Recent downloads"></a>
  <a href="https://docs.rs/ruststream"><img src="https://img.shields.io/docsrs/ruststream" alt="docs.rs"></a>
  <img src="https://img.shields.io/badge/MSRV-1.95-blue.svg" alt="MSRV 1.95">
  <img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License">
  <img src="https://img.shields.io/badge/unsafe-none-success.svg" alt="100% safe Rust">
  <a href="https://t.me/ruststream_community"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=News" alt="Telegram news channel"></a>
  <a href="https://t.me/ruststream_communuty_ru_chat"><img src="https://img.shields.io/badge/-Telegram-blue?logo=telegram&label=RU" alt="Telegram RU chat"></a>
  <a href="https://context7.com/powersemmi/ruststream"><img src="https://img.shields.io/badge/Context7-Ask_AI-6200ee" alt="Ask AI"></a>
  <a href="https://www.greptile.com/?utm_source=oss_badge&amp;utm_medium=readme&amp;utm_campaign=greptile_for_open_source"><img src="https://www.greptile.com/badge.svg" alt="Greptile: The War on Bugs"></a>
</p>

<p align="center">
  <b><a href="https://powersemmi.github.io/ruststream/latest/ru/">Документация</a></b>
</p>

---

RustStream подключает ваш сервис к брокеру очереди через небольшой набор обобщённых трейтов. Поверх
них он даёт роутер, middleware, кодеки и инструменты. Ядро не зависит ни от одного брокера: адаптация
каждого брокера - отдельный крейт, и все они соблюдают один контракт. Ядро целиком написано на
безопасном Rust.

## Возможности

- **Ядро не привязано к брокеру.** Адаптации брокеров - отдельные крейты, и каждая проходит набор
  проверок соответствия.
- **Неверное использование не компилируется.** Двойной `ack`, вызовы жизненного цикла не по порядку и
  публикация без обязательных параметров дают ошибку компиляции, и она называет исправление.
- **Три рантайма.** `tokio` (по умолчанию), `compio` с отдельным рантаймом на каждом ядре процессора,
  на `io_uring` там, где его поддерживает ядро ОС, и `embassy` для устройства, с кучей или без неё.
  Обработчики на всех трёх одинаковы, а исполнитель задаётся в приложении.
- **Подключаемые кодеки:** JSON, MessagePack и CBOR включаются фичами cargo, postcard и JSON на
  `serde-json-core` - для устройства без кучи, а без кодека остаются сырые байты.
- **Бинарники без шаблонного кода.** Макросы `#[subscriber]` и `#[ruststream::app]` и CLI, который
  запускает сервис и печатает его документ AsyncAPI.
- **Наблюдаемость:** AsyncAPI 3.1, метрики Prometheus, состояние здоровья сервиса и OpenTelemetry.
- **Тесты без брокера.** Собственное приложение сервиса работает в процессе теста под тестовой
  обвязкой.
- **Ошибка публикации возвращает сообщение.** Если публикацию отклонили там, где клиент брокера не
  забрал полезную нагрузку, неотправленное сообщение возвращается вызывающему, и его можно отправить
  в другое место.
- **Трейты-совместимости** для пакетов, транзакций, запроса с ответом, разбиения на разделы и
  перемотки; брокер реализует только то, что поддерживает.

## Установка

```toml
[dependencies]
ruststream = { version = "0.8", features = ["macros", "memory", "json"] }
serde = { version = "1", features = ["derive"] }
```

CLI входит в крейт и включается фичей `cli`:

```bash
cargo install ruststream --features cli
```

## Навыки для агентов

В репозитории лежат навыки для ИИ-агентов, которые пишут код. С ними агент пишет сервисы на
RustStream, их тесты и адаптации брокеров так, как рекомендует фреймворк. В Claude Code:

```text
/plugin marketplace add powersemmi/ruststream
/plugin install ruststream@ruststream
```

Cursor и Codex читают их из
[`.agents/skills/`](https://github.com/powersemmi/ruststream/tree/main/.agents/skills); список
навыков - на странице
[Навыки для агентов](https://powersemmi.github.io/ruststream/latest/ru/guides/getting-started/agent-skills/).

## Пример сервиса

```rust
use ruststream::memory::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize, Outgoing)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn handle(order: &Order) -> HandlerOutcome {
    println!("got order {}", order.id);
    HandlerOutcome::ack()
}

#[ruststream::app]
fn app() -> RustStream {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(handle))
}
```

`#[ruststream::app]` генерирует `main`, поэтому код запуска рантайма писать не нужно. Обработчику
достаточно `Deserialize`; остальные derive нужны для теста ниже.

## Запуск

```bash
ruststream run                 # start the service (or: cargo run -- run)
ruststream asyncapi gen        # print the AsyncAPI document
```

Команде `asyncapi gen` нужна фича `asyncapi` крейта `ruststream`.

Чтобы начать с шаблона, создайте проект командой `cargo generate`:

```bash
cargo generate --git https://github.com/powersemmi/ruststream templates/memory --name my-service
```

`templates/compio-memory` создаёт сервис на рантайме compio, а `templates/embassy-memory` - прошивку
для Cortex-M на embassy без кучи. В каждом шаблоне есть тесты, которые запускают его приложение в
процессе. Несколько адаптаций брокеров поставляют собственные шаблоны и называют их в своей
документации. Первый запуск разобран на странице
[Быстрый старт](https://powersemmi.github.io/ruststream/latest/ru/guides/getting-started/quickstart/).

## Тестирование

`TestApp` запускает приложение сервиса в процессе, без внешнего брокера. Его добавляет фича
`testing`, которую сервис включает для своих тестов:

```toml
[dev-dependencies]
ruststream = { version = "0.8", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

Тест публикует заказ через брокер и проверяет, каким исходом обработчик завершил доставку:

```rust
#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::memory::prelude::*;
    use ruststream::testing::TestApp;

    use super::{Order, app};

    #[tokio::test]
    async fn an_order_is_acked() -> Result<(), Box<dyn Error>> {
        let tb = TestApp::start(app()).await?;

        // Inject an order; the publish drives the handler to a standstill before returning.
        tb.broker::<MemoryBroker>()
            .message(&Order { id: 42 })
            .to("orders")
            .publish()
            .await?;

        // The handler ran once, decoded the order, and acked.
        tb.broker::<MemoryBroker>()
            .subscriber("orders")
            .assert_called_once()
            .with(&Order { id: 42 })
            .settled(HandlerOutcome::ack());
        Ok(())
    }
}
```

Остальное об обвязке тестов - на страницах
[Тестирование](https://powersemmi.github.io/ruststream/latest/ru/components/application/testing/).

## Брокеры

| Брокер | Крейт |
|---|---|
| NATS | [`ruststream-nats`](https://github.com/powersemmi/ruststream-nats) |
| Redis / Valkey | [`ruststream-fred`](https://github.com/powersemmi/ruststream-fred) |
| RabbitMQ (AMQP 0.9.1) | [`ruststream-lapin`](https://github.com/powersemmi/ruststream-lapin) |
| Apache Kafka | [`ruststream-rdkafka`](https://github.com/powersemmi/ruststream-rdkafka) |
| AMQP 1.0 | [`ruststream-amqp`](https://github.com/powersemmi/ruststream-amqp) |
| Google Cloud Pub/Sub | [`ruststream-gcp-pubsub`](https://github.com/powersemmi/ruststream-gcp-pubsub) |
| Amazon SQS / SNS | [`ruststream-sqs-sns`](https://github.com/powersemmi/ruststream-sqs-sns) |
| Apache Pulsar | [`ruststream-pulsar`](https://github.com/powersemmi/ruststream-pulsar) |
| MQTT 5 | [`ruststream-rumqttc`](https://github.com/powersemmi/ruststream-rumqttc) |
| MQTT 5 на embassy и compio, есть вариант для устройства без кучи | [`ruststream-rust-mqtt`](https://github.com/powersemmi/ruststream-rust-mqtt) |
| ZeroMQ | [`ruststream-zeromq`](https://github.com/powersemmi/ruststream-zeromq) |
| Файлы и stdio | [`ruststream-sea-file`](https://github.com/powersemmi/ruststream-sea-file) |
| Amazon Kinesis | [`ruststream-kinesis`](https://github.com/powersemmi/ruststream-kinesis) |
| Базы данных SQL (Postgres, MySQL / MariaDB, SQLite) | [`ruststream-sqlx`](https://github.com/powersemmi/ruststream-sqlx) |

Что умеет каждый брокер, показано на странице
[Брокеры](https://powersemmi.github.io/ruststream/latest/ru/brokers/). Чтобы написать свой брокер,
начните с [трейта `Broker`](https://docs.rs/ruststream/latest/ruststream/trait.Broker.html).

## Документация

- Сайт: <https://powersemmi.github.io/ruststream/latest/ru/>
- Справочник API и обзоры тем: <https://docs.rs/ruststream>

## Минимальная поддерживаемая версия Rust

MSRV - **1.95**, редакция 2024. Политика скользящая, как в tower: MSRV поднимается только до версии
Rust, вышедшей не менее шести месяцев назад, и только в минорном выпуске. Адаптации библиотеки брокера
может понадобиться более свежий Rust, если его требует клиентская библиотека.

## Участие в разработке

См. [CONTRIBUTING.md](https://github.com/powersemmi/ruststream/blob/main/CONTRIBUTING.md).

## Лицензия

Распространяется по лицензии [Apache-2.0](https://github.com/powersemmi/ruststream/blob/main/LICENSE).

<sub>Проект вдохновлён [FastStream](https://github.com/ag2ai/faststream).</sub>
