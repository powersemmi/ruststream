---
title: Сервис и его первый подписчик
description: "Сначала магазин - это один подписчик, который принимает каждый оформленный заказ. Небольшое приложение монтирует его на in-memory брокере."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Сервис и его первый подписчик

Сначала магазин - это один подписчик, который принимает каждый оформленный заказ. Небольшое приложение монтирует его на in-memory брокере.

Сервис на RustStream - это [приложение](../../components/application/index.md): билдер, который перечисляет брокеры сервиса и монтирует на них подписчиков.
[Подписчик](../../components/subscribers/index.md) - это `async fn`, первый параметр которой - сообщение. Макрос [`#[subscriber]`](api:ruststream::subscriber) задаёт тему, которую он читает.
Рантайм декодирует полезную нагрузку до запуска тела обработчика, по умолчанию из JSON.
Значение, которое возвращает тело, завершает доставку: [`HandlerOutcome::ack`](api:ruststream::runtime::HandlerOutcome::ack) сообщает брокеру, что сообщение обработано.
[In-memory брокер](../../components/memory/index.md) передаёт сообщения внутри процесса, поэтому магазину ничего не нужно устанавливать.
Глава 11 заменяет его сетевым брокером, а подписчики остаются прежними.

## Заказ

Заказ - это тип `serde`.
Derive-макрос [`Outgoing`](api:ruststream::Outgoing) задаёт тему, в которую публикуется заказ: `orders.placed`.
Сам магазин пока не публикует заказы. Это делает тест, а в главе 10 - HTTP-витрина.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust reference="examples/guide/first.rs#order"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust reference="examples/manual/guide/first.rs#order"
```

</TabItem>
</Tabs>

Ручная форма содержит две реализации, которые генерирует derive-макрос: адрес публикации и заголовки заказа (пока их нет).

## Первый подписчик

`accept` читает тему `orders.placed` и записывает каждый заказ в журнал через `tracing`.
Ручная форма - это именованный тип с реализацией [`impl Handle<Order>`](api:ruststream::runtime::Handle): `Ok(())` подтверждает сообщение, а `Err(outcome)` завершает доставку этим исходом.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust reference="examples/guide/first.rs#accept"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust reference="examples/manual/guide/first.rs#accept"
```

</TabItem>
</Tabs>

## Приложение

[`RustStream`](api:ruststream::runtime::RustStream) - это приложение. [`with_broker`](api:ruststream::runtime::RustStream::with_broker) регистрирует брокер и монтирует на нём подписчиков через `include`.
Билдер только запоминает переданное: брокер подключается при запуске сервиса.
Атрибут [`#[ruststream::app]`](../../components/application/app-macro.md) превращает билдер в `main` с командами `run` и `asyncapi gen`.
В ручной форме этот `main` написан явно: [`run_main`](api:ruststream::runtime::cli::run_main) поверх билдера.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust reference="examples/guide/first.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust reference="examples/manual/guide/first.rs#app"
```

</TabItem>
</Tabs>

## Запуск

```bash
cargo run --example guide_first --features testing,macros,memory,json,logging -- run
```

Фича `logging` выводит журнал сервиса в терминал.
Сервис подключает брокер, создаёт подписку на тему и работает до нажатия <kbd>Ctrl</kbd>+<kbd>C</kbd>:

```text
INFO ruststream::lifecycle starting service service=shop version=0.1.0 brokers=1 subscribers=1
INFO ruststream::lifecycle broker connected broker="ruststream::memory::MemoryBroker"
INFO ruststream::dispatch subscriber started subscriber=orders.placed input="guide_first::Order"
INFO ruststream::lifecycle service running subscribers=1
```

Остальные главы превращают этот файл в магазин из [`examples/guide`](https://github.com/powersemmi/ruststream/tree/main/examples/guide).
