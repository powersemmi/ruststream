---
title: Сервис для устройства на локальном in-memory брокере
description: "Плата с термометром запускает сервис RustStream на рантайме embassy без кучи, а тесты на хосте проваливаются при любой аллокации фреймворка."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Сервис для устройства на локальном in-memory брокере

Плата с термометром запускает сервис RustStream на рантайме embassy без кучи, а тесты на хосте идут под аллокатором, который проваливает тест при любой аллокации фреймворка.

Магазин из прошлых глав работает на `MemoryBroker` и рантайме tokio.
У прошивки своя пара: [встраиваемый рантайм](../../components/application/embedded-runtime.md) и [локальный in-memory брокер](../../brokers/local-memory.md), очередь между частями одной платы.
Подписчики выглядят так же, а монтирование строится той же цепочкой.
Эта глава строит небольшую плату: датчик публикует показания, плата пересчитывает их в градусы и поднимает тревогу, когда показание слишком высокое.

## Зависимость

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.8", default-features = false, features = ["embassy", "memory", "macros", "postcard"] }
serde = { version = "1", default-features = false, features = ["derive"] }
```

`default-features = false` отключает `tokio` и `json`, а `embassy` подключает запуск для прошивок.
`postcard` - кодек: он кодирует в срез и декодирует из среза, поэтому плате не нужна куча для буферов.
Ничто в этом списке не включает фичу `alloc` фреймворка, поэтому каждая часть, размер которой определяла бы куча, имеет фиксированную форму.

## Шина

Плата объявляет шину как `static`, а функция приложения получает брокер, построенный поверх неё.
Каждая подписка хранит четыре сообщения, включая то, что сейчас у обработчика.
Реализацию `critical-section`, внутри которой работает шина, предоставляет бинарный крейт.
На ядре без атомарного сравнения с обменом, например на Cortex-M0+, прошивка ещё включает у `portable-atomic` фичу `critical-section`.

```rust reference="crates/ruststream/tests/doc_device.rs#bus"
```

## Обработчик и ответ

Показание и отчёт - обычные типы `serde`, которые знают, куда их отправлять.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device.rs#messages"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device_manual.rs#messages"
```

</TabItem>
</Tabs>

Обработчик принимает показание и возвращает отчёт.
Ответ уходит по адресу, который указан в типе отчёта, поэтому сам обработчик ничего не публикует.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device.rs#reading-handler"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device_manual.rs#reading-handler"
```

</TabItem>
</Tabs>

Приложение монтирует обработчик на шину платы.
`out_reply` публикует отчёт через шину, `codec` кодирует его postcard, а `encode_capacity` отдаёт каждому исполнителю столько байт для кодирования ответа, сколько указано; по умолчанию 256.
Ответ, который в них не помещается, отклоняется с `CodecFailure::BufferFull` и никогда не обрезается: доставка подтверждается, а сбой попадает в продолжение `.error` обработчика.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device.rs#reply-mount"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device_manual.rs#reply-mount"
```

</TabItem>
</Tabs>

## Публикация в другую тему

Второй обработчик читает отчёты и поднимает тревогу, когда температура выше пятидесяти градусов.
Тревога уходит через слот `Out`, как [уведомления магазина](./replies.md), и несёт номер датчика одним байтом.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device.rs#alarm"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device_manual.rs#alarm"
```

</TabItem>
</Tabs>

Монтирование добавляет обработчик и называет издателя, который стоит за слотом.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device.rs#alarm-mount"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="device.rs" reference="crates/ruststream/tests/doc_device_manual.rs#alarm-mount"
```

</TabItem>
</Tabs>

На устройстве `#[ruststream::app(runtime = embassy)]` ставит эту функцию перед исполнителем платы.
Остальную поверхность для устройства показывает страница [встраиваемого рантайма](../../components/application/embedded-runtime.md): повторы по часам платы, продолжения и пакеты.

## Тестирование на хосте

Тесты запускаются на хосте, с `std`, через встроенный [`TestApp`](../../components/application/testing/embedded-app.md).
Он запускает то же приложение, что и плата, без изменений, а шина сама служит внутрипроцессным режимом, поэтому подставной брокер не нужен.
Обвязка хранит свой учёт (что получила каждая подписка и что опубликовал каждый слот) в куче хоста.
Фреймворк под ней остаётся в форме устройства: тестовая сборка не включает `alloc`, и приложение опрашивается в фиксированной форме.

Тест может доказать, что сервис ничего не выделяет.
Бинарный файл теста ставит `assert_no_alloc::AllocDisabler` глобальным аллокатором, и любая аллокация фреймворка при опросе приложения проваливает тест.
Собственный учёт обвязки выполняется там, где аллокатор его пропускает.
Проверка работает в отладочной сборке, а именно её собирает `cargo test`.

```toml title="Cargo.toml"
[dev-dependencies]
ruststream = { version = "0.8", default-features = false, features = ["testing"] }
assert_no_alloc = "1.1"
futures = "0.3"
```

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="tests/device.rs" reference="crates/ruststream/tests/doc_device.rs#allocator"
```

```rust title="tests/device.rs" reference="crates/ruststream/tests/doc_device.rs#device-test"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="tests/device.rs" reference="crates/ruststream/tests/doc_device_manual.rs#allocator"
```

```rust title="tests/device.rs" reference="crates/ruststream/tests/doc_device_manual.rs#device-test"
```

</TabItem>
</Tabs>

Тест публикует показание 1200 милливольт, которое обработчик пересчитывает в 70 градусов.
Он проверяет, что отчёт дошёл до `reports` и что тревога для датчика 2 вышла через свой слот.
Публикация из теста возвращается, когда всё, что она запустила, уже обработано, поэтому проверки идут сразу за ней.
Часы платы двигаются только по вызову `advance`, как на приостановленных часах [тестов магазина](./testing.md).

## Тест в форме устройства

Cargo собирает крейт один раз за сборку, с объединением фич, которые запрашивают все крейты этой сборки.
Если зависимость для разработки или любой другой крейт тестовой сборки включает фичу `alloc` фреймворка, она включается и для тестируемого сервиса.
Тогда тест проверяет форму фреймворка с кучей, и аллокатор ничего не доказывает про устройство.

Обычная причина - строка зависимости с фичами по умолчанию, `ruststream = { version = "0.8", features = ["testing"] }`: она подключает `json` и `tokio`, а обе фичи включают `alloc`.
То же делают `alloc`, `tokio`, `compio`, `tracing`, `json`, `msgpack`, `cbor` и `conformance`.
Поэтому каждая строка `ruststream` в сервисе, включая зависимости для разработки, содержит `default-features = false`, и никакой другой крейт тестовой сборки не запрашивает ни одну из этих фич.

Ядро держит свои сборки без `alloc` отдельно, в [`checks/noalloc`](https://github.com/powersemmi/ruststream/tree/main/checks/noalloc): зависимости для разработки, нужные собственным модульным тестам крейта, включают `alloc`.
Рецепт `just test-no-alloc` выводит дерево фич каждой сборки командой `cargo tree -e features` и завершается ошибкой, если в нём есть `alloc`.
Та же проверка подходит сервису:

```bash
cargo tree -e features,normal,build,dev -i ruststream-core | grep 'feature "alloc"'
```

Пока сервис остаётся в форме устройства, команда ничего не выводит.
