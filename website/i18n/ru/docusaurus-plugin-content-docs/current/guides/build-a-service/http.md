---
title: Рядом с HTTP-сервером, outbox
description: "Витрина принимает заказы по HTTP и сохраняет их вместе с событиями. Ретранслятор передаёт события брокеру, а магазин работает рядом с сервером."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Рядом с HTTP-сервером, outbox

Витрина принимает заказы по HTTP и сохраняет их вместе с событиями. Ретранслятор передаёт события брокеру, а магазин работает рядом с сервером.

Когда сервис также обслуживает HTTP, [процессом управляет HTTP-сервер](../../components/application/running/http.md), а магазин работает рядом с ним.
[`start`](../../components/application/running/start.md) запускает магазин в фоне и возвращает управление, когда магазин работает. Поэтому если брокер отказывает в подключении, процесс останавливается раньше, чем сервер примет первый запрос.
HTTP-запрос не должен сам публиковать сообщения в брокер. Иначе сбой брокера приведёт к ошибке запроса, а заказ, сохранённый без события, никогда не дойдёт до магазина.
Транзакционный outbox решает обе проблемы.
Запрос записывает заказ и его событие в одной транзакции, а ретранслятор позже публикует сохранённые события.
Сбой брокера только откладывает события, и ни одно из них не теряется. Если процесс аварийно завершится между публикацией и удалением события, событие будет опубликовано снова, поэтому магазин получает каждый заказ хотя бы один раз.

## Хранилище

В настоящей базе данных заказы и outbox - это две таблицы, которые записываются в одной транзакции.
Здесь эту роль играет мьютекс над парой.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="http.rs" reference="examples/guide/http.rs#store"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="http.rs" reference="examples/manual/guide/http.rs#store"
```

</TabItem>
</Tabs>

HTTP-часть и outbox - обычные функции, поэтому обе вкладки показывают один и тот же код.

## Конечная точка

`POST /orders` принимает заказ в JSON, а идентификатор запроса - из заголовка `x-request-id`.
Она только записывает в хранилище, поэтому отвечает `202 Accepted`, не дожидаясь брокера.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="http.rs" reference="examples/guide/http.rs#endpoint"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="http.rs" reference="examples/manual/guide/http.rs#endpoint"
```

</TabItem>
</Tabs>

## Ретранслятор

Ретранслятор публикует сначала самое старое событие вместе с его контрактом заголовков и удаляет событие только после успешной публикации.
Если публикация вернула ошибку, строка остаётся до следующего прохода.
Издатель - любой [`Publisher`](../../components/subscribers/publishing/publishers/publisher.md), поэтому ретранслятор не меняется при смене брокера.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="http.rs" reference="examples/guide/http.rs#relay"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="http.rs" reference="examples/manual/guide/http.rs#relay"
```

</TabItem>
</Tabs>

## Процесс

`bindable` позволяет процессу получить токен издателя до запуска магазина, а [`publisher`](api:ruststream::runtime::RunningApp::publisher) превращает токен в работающего издателя после подключения брокера.
Сервер останавливается по <kbd>Ctrl</kbd>+<kbd>C</kbd> или когда [`stopping`](api:ruststream::runtime::RunningApp::stopping) сообщает, что магазин остановился сам.
Затем магазин дорабатывает начатые доставки и останавливается через [`shutdown`](../../components/application/running/shutdown.md).
`main`, написанный вручную, сам устанавливает логгер через `ruststream::logging::init()`.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="http.rs" reference="examples/guide/http.rs#main"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="http.rs" reference="examples/manual/guide/http.rs#main"
```

</TabItem>
</Tabs>

Запустите магазин и оформите заказ:

```bash
cargo run --example guide_http --features testing,macros,memory,json,cbor,asyncapi,logging
curl -X POST http://127.0.0.1:8080/orders -H 'content-type: application/json' \
  -H 'x-request-id: req-1' \
  -d '{"id":1,"customer":"ada@example.com","lines":[{"sku":"mug","quantity":12,"price_cents":1200}]}'
```

Журнал из главы 5 показывает путь заказа через магазин.
[Конечная точка проверки состояния](../../components/application/running/health.md) того же сервера сообщает оркестратору о состоянии магазина.
