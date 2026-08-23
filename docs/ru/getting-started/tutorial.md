# Учебник: собираем первый сервис

Этот учебник собирает сервис заказов с нуля и разбирает каждую его часть. Сервис работает на
in-memory брокере, поэтому запускать что-то внешнее не нужно; переход на настоящий брокер - правка в
одну строку, о ней в конце.

## 1. Создайте крейт

```bash
cargo new orders-service
cd orders-service
```

```toml title="Cargo.toml"
[package]
name = "orders-service"
version = "0.1.0"
edition = "2024"

[dependencies]
ruststream = { version = "0.6", features = ["macros", "memory", "json", "asyncapi"] }
serde = { version = "1", features = ["derive"] }
```

## 2. Опишите сообщение и обработчик

Обработчик - это `async fn`, первый параметр которой - декодированная полезная нагрузка. Макрос
`#[subscriber]` превращает функцию в определение, готовое к монтированию, и называет его по имени
самой функции.

```rust title="src/orders.rs"
use ruststream::runtime::HandlerResult;
use ruststream::subscriber;
use serde::{Deserialize, Serialize};

--8<-- "examples/tutorial/orders.rs:order"
```

Обработчик возвращает [`HandlerResult`](../guides/subscribers.md#acking): либо `Ack`, либо `nack`,
который отбрасывает сообщение или возвращает его в очередь. Подойдёт и `()`, и `Result<(), E>` - они
преобразуются в результат (`Ok` подтверждает, `Err` отбрасывает).

## 3. Свяжите обработчик с приложением

```rust title="src/main.rs"
mod orders;

use ruststream::memory::MemoryBroker;
use ruststream::runtime::{AppInfo, RustStream};

use crate::orders::handle;

--8<-- "examples/quickstart.rs:app"
```

Макрос превращает `handle` в значение с тем же именем, что у функции, поэтому его достаточно
импортировать и передать напрямую.

!!! tip "Кодек по умолчанию"
    `include` декодирует кодеком по умолчанию (`json`, если фича включена, иначе `cbor`, иначе
    `msgpack`), поэтому аргумент с кодеком ему не нужен. Чтобы везде декодировать другим, задайте его
    один раз через `with_broker_codec(broker, codec, |b| ...)`. Полные правила разрешения - в разделе
    [Кодеки](../guides/codecs.md).

Запустите:

```bash
cargo run -- run
```

## 4. Ответьте на сообщения

Чтобы опубликовать ответ, верните значение ответа и назовите адресата через `publish(..)`:

```rust title="src/orders.rs"
--8<-- "examples/tutorial/orders.rs:confirm"
```

Монтируется он обычным `include`: ответ уходит через политику публикации брокера по умолчанию и
кодируется кодеком по умолчанию (чтобы назвать кодек ответа или добавить преобразования, добавьте в
цепочку `.publisher(..)` со стеком `TypedPublisher`):

<!-- inline-rust: minimal mount fragment isolating the reply wiring; the full compiled program is examples/tutorial/main.rs:main, pulled in below -->
```rust
// inside with_broker(...), with `confirm` imported from the orders module
b.include(confirm);
```

Полная картина, включая публикацию изнутри обработчика, - в разделе
[Публикация и ответы](../guides/publishing.md).

## 5. Наведите порядок роутером

Когда обработчиков становится много, держите их в отдельном модуле и собирайте в
[`Router`](../guides/routing.md):

```rust title="src/routes.rs"
--8<-- "examples/tutorial/routes.rs:routes"
```

```rust title="src/main.rs"
--8<-- "examples/tutorial/main.rs:main"
```

## 6. Посмотрите AsyncAPI-документ

```bash
cargo run -- asyncapi gen
```

Каждый подписчик превращается в канал и операцию `receive`; типы полезной нагрузки, для которых
выведен `schemars::JsonSchema`, добавляют ещё и схемы. Флаги вывода (`-o`, `--yaml`) и сам документ
разобраны в [руководстве по AsyncAPI](../guides/asyncapi.md).

## 7. Перейдите на настоящий брокер

Ничто из написанного выше не привязано к in-memory брокеру. Брокер выбирается в `with_broker`,
поэтому замена сводится к одной строке: добавьте крейт брокера в зависимости и создайте его там
(например, `NatsBroker::new("nats://localhost:4222")` вместо `MemoryBroker::new()`); обработчики,
роутер и кодеки остаются прежними. Список доступных брокеров и замена для каждого из них - в разделе
[Брокеры](../brokers/index.md#switching-brokers).

!!! info "Готовый сервис - это компилируемый пример"
    Каждый фрагмент на этой странице подтянут из
    [`examples/tutorial`](https://github.com/powersemmi/ruststream/tree/main/examples/tutorial)
    в репозитории, который CI собирает при каждом изменении. Запустить его самостоятельно можно
    командой `cargo run --example tutorial --features macros,memory,json -- run`.

## Что дальше

- [Middleware](../guides/middleware.md) - сквозная логика вокруг обработчиков.
- [Жизненный цикл](../guides/lifespan.md) - общее состояние и хуки старта и остановки.
- [Тестирование](../guides/testing.md) - тесты только что написанных обработчиков прямо в процессе.
- [Метрики](../guides/metrics.md) - счётчики и гистограммы Prometheus.
