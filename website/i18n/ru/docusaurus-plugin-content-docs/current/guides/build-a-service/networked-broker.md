---
title: Переход на сетевой брокер
description: "Магазин переходит на NATS заменой брокера, на котором он собран. Подписчики и настройки роутеров остаются прежними."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Переход на сетевой брокер

Магазин переходит на NATS заменой брокера, на котором он собран. Подписчики и настройки роутеров остаются прежними.

Брокер выбирается в одном месте: это значение, которое передаётся в `shop`.
Крейт брокера предоставляет тип брокера и прелюдию с теми же именами, что и у прелюдии in-memory брокера, в том числе `Publish`.
Поэтому роутер выглядит одинаково на любом брокере, и меняется только тип брокера, который он указывает.
Тела подписчиков ничего не импортируют из брокера, поэтому не меняются совсем.
У каждого брокера свой [крейт](../../brokers/index.md). Эта глава использует [`ruststream-nats`](https://docs.rs/ruststream-nats).

## Зависимость

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.8", features = ["macros", "json", "cbor", "asyncapi", "logging"] }
ruststream-nats = { version = "0.8", features = ["asyncapi"] }
```

Фича `memory` больше не нужна.
Фичи `testing` обоих крейтов переносятся в `[dev-dependencies]`.

## Замена брокера

Точка входа собирает магазин на `NatsBroker`. Как и in-memory брокер, он подключается только при запуске сервиса.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

{/* inline-rust: the shop on NATS needs the ruststream-nats crate, which this repository does not depend on; this code was built and run against a NATS 2 server for this page */}
```rust title="main.rs"
use ruststream_nats::prelude::*;

use crate::state::Shop;

#[ruststream::app]
fn app() -> impl App<State = Shop> {
    service::shop(NatsBroker::new("nats://localhost:4222"))
}
```

</TabItem>
<TabItem value="manual" label="Вручную">

{/* inline-rust: the shop on NATS needs the ruststream-nats crate, which this repository does not depend on; this code was built and run against a NATS 2 server for this page */}
```rust title="main.rs"
use std::process::ExitCode;

use ruststream::runtime::cli::run_main;
use ruststream_nats::prelude::*;

use crate::state::Shop;

fn app() -> impl App<State = Shop> {
    service::shop(NatsBroker::new("nats://localhost:4222"))
}

fn main() -> ExitCode {
    run_main(app)
}
```

</TabItem>
</Tabs>

Остальные файлы меняются в двух местах.
Каждый импортирует `ruststream_nats::prelude::*` вместо `ruststream::memory::prelude::*`.
Каждый роутер возвращает `RouterDef<NatsBroker, Shop>`, а `shop` принимает брокер типа `NatsBroker`.
Кодеки, слоты, ограничение попыток и тема недоставленных сообщений остаются прежними.
`http.rs` так же получает токен издателя через `NatsBroker::new(..).bindable()`.

## Тесты

У `NatsBroker` есть внутрипроцессный режим, поэтому `TestApp` запускает магазин без сервера, как и на in-memory брокере.
Тесты берут брокер у функции `test_broker` из `service.rs`, и теперь она создаёт `NatsBroker`:

{/* inline-rust: the tests' broker helper on NATS needs the ruststream-nats crate, which this repository does not depend on; this code was built and its tests run in process for this page */}
```rust title="service.rs"
#[cfg(test)]
pub(crate) fn test_broker() -> NatsBroker {
    NatsBroker::new("nats://localhost:4222")
}
```

Внутри процесса адрес только разбирается, соединение по нему не устанавливается.
Брокер тесты запрашивают по типу: `tb.broker::<NatsBroker>()`.
Больше в них ничего не меняется, включая задержку из главы 8 и AsyncAPI-документ.

## Что меняет NATS

```bash
docker run --rm -p 4222:4222 nats:2-alpine
cargo run -- run
```

В Core NATS имя темы - это субъект.
Core NATS не хранит сообщения: сообщение, опубликованное, когда никто не подписан, пропадает, а у доставки нет подтверждения.
`retry` и `retry_after` публикуют копию обратно в субъект, а ограничение попыток считает эти копии.
Если сообщения темы должны пережить перезапуск, подписка читает вместо субъекта стрим JetStream: [`JetStreamSubject`](https://docs.rs/ruststream-nats/latest/ruststream_nats/struct.JetStreamSubject.html) в атрибуте и долговременный консьюмер.
Пример крейта [`nats_jetstream`](https://github.com/powersemmi/ruststream-nats/blob/main/crates/ruststream-nats/examples/nats_jetstream.rs) показывает такую подписку, а [`nats_retry`](https://github.com/powersemmi/ruststream-nats/blob/main/crates/ruststream-nats/examples/nats_retry.rs) - как переобрабатывает сообщения подписка каждого вида.
