---
title: "Завершение доставки: retry, retry_after, недоставленные сообщения"
description: "charge отвечает на каждый отказ платёжного провайдера нужным исходом: переобработкой сразу или позже, отбрасыванием или темой недоставленных сообщений."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Завершение доставки: retry, retry_after, недоставленные сообщения

`charge` отвечает на каждый отказ платёжного провайдера нужным исходом: переобработкой сразу или позже, отбрасыванием или темой недоставленных сообщений.

Каждая доставка завершается одним [исходом](../../components/subscribers/messages/handler-outcome.md).
[`ack`](api:ruststream::runtime::HandlerOutcome::ack) завершает её, [`drop`](api:ruststream::runtime::HandlerOutcome::drop) отбрасывает, а [`retry`](api:ruststream::runtime::HandlerOutcome::retry) просит брокер доставить сообщение повторно.
[`retry_after`](../../components/subscribers/messages/retry-after.md) доставляет его повторно после задержки и не задерживает доставки, идущие следом.
Переобработка, которая никогда не завершается успехом, повторялась бы бесконечно, поэтому точка монтирования ограничивает число попыток.
Когда попытки [исчерпаны](../../components/subscribers/retry-cap.md), доставка уходит в тему для недоставленных сообщений (dead letter), где её может изучить оператор или другой сервис.

## Подписчик

Платёжный провайдер отказывает в списании тремя способами, и каждому нужен свой исход.
`Busy` означает, что лимит запросов провайдера пока исчерпан. Немедленная переобработка снова получит отказ, поэтому `retry_after` ждёт секунду.
`Unavailable` означает, что провайдер не ответил: `retry` повторяет попытку сразу.
`Declined` означает, что на карте не хватает средств. Повторная попытка получит тот же ответ, поэтому платёж отбрасывается.
Клиент провайдера берётся из состояния приложения, о котором рассказывает глава 7.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="payments.rs" reference="examples/guide/payments.rs#charge"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="payments.rs" reference="examples/manual/guide/payments.rs#charge"
```

</TabItem>
</Tabs>

## Ограничение попыток и тема недоставленных сообщений

[`max_attempts`](api:ruststream::runtime::RouterWith::max_attempts) считает все доставки одного сообщения, включая первую.
Пятая доставка не переобрабатывается: [`dead_letter`](api:ruststream::runtime::RouterWith::dead_letter) публикует её в `payments.dead` с полезной нагрузкой и заголовками в том виде, в каком они пришли.
Роутер декодирует запросы из CBOR и так же кодирует ответ.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="payments.rs" reference="examples/guide/payments.rs#routes"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="payments.rs" reference="examples/manual/guide/payments.rs#routes"
```

</TabItem>
</Tabs>

In-memory брокер сам доставляет сообщение повторно после задержки, которую запросил `retry_after`.
Если брокер не умеет откладывать доставку, рантайм публикует копию сообщения обратно в подписку. Подробнее об этом - в разделе [куда уходит копия для переобработки](../../components/subscribers/retry-destination.md).
Глава 8 проверяет все три пути, включая задержку, и не ждёт её истечения.
