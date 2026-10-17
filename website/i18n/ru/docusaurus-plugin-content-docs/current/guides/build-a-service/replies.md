---
title: Ответы и публикация в другие темы
description: "accept отвечает на каждый заказ запросом оплаты и уведомляет покупателя через слот Out. Роутер задаёт, как публикуются оба сообщения."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Ответы и публикация в другие темы

`accept` отвечает на каждый заказ запросом оплаты и уведомляет покупателя через слот `Out`. Роутер задаёт, как публикуются оба сообщения.

Подписчик публикует сообщения двумя способами.
[Ответ](../../components/subscribers/publishing/replies.md) - это значение, которое возвращает тело обработчика. Рантайм кодирует его и публикует после успешного завершения тела.
[Слот `Out`](../../components/subscribers/publishing/out-slots.md) - это издатель, которого тело получает параметром и вызывает само, столько раз, сколько нужно.
Адрес сообщения - свойство его типа: [`#[outgoing(name = ..)]`](../../components/subscribers/publishing/outgoing.md) задаёт тему, поэтому ни тело, ни точка монтирования её не повторяют.
Слот перечисляет типы, которые через него публикуются, поэтому публикация любого другого типа не компилируется.
Точка монтирования связывает каждую позицию с политикой публикации, здесь это обычная политика брокера `Publish`.

## Сообщения

Платёжная часть получает `PaymentRequest`, а `Paid` - это её ответ из следующей главы.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="payments.rs" reference="examples/guide/payments.rs#request"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="payments.rs" reference="examples/manual/guide/payments.rs#request"
```

</TabItem>
</Tabs>

`Notice` - это уведомление, которое магазин отправляет покупателю, а маркер [`OutSlot`](api:ruststream::OutSlot) `Notices` - слот, через который оно публикуется.
Ручная форма содержит то, что генерирует `#[derive(OutSlot)]`: имя слота, типы, которые он публикует, и их записи в AsyncAPI-документе.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="notifications.rs" reference="examples/guide/notifications.rs#notice"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="notifications.rs" reference="examples/manual/guide/notifications.rs#notice"
```

</TabItem>
</Tabs>

## Подписчик

`accept` отбрасывает заказ без строк: оплачивать нечего.
Через слот он сообщает покупателю, что заказ получен, и просит переобработку, если эта публикация вернула ошибку.
Затем он возвращает запрос оплаты, и рантайм публикует его как ответ.
`Ok` публикует ответ и подтверждает сообщение, `Err` ничего не публикует и завершает доставку указанным исходом.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="orders.rs" reference="examples/guide/orders.rs#accept"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="orders.rs" reference="examples/manual/guide/orders.rs#accept"
```

</TabItem>
</Tabs>

В ручной форме слот - это элемент [`Outs`](api:ruststream::runtime::Outs), и `outs.get(Notices)` возвращает его издателя.

## Роутер

[`Router`](api:ruststream::runtime::Router) объединяет подписчиков одного модуля с настройками их позиций.
[`out_reply`](api:ruststream::runtime::Mounting::out_reply) связывает ответ с политикой, а `.codec(INTERNAL)` кодирует его в CBOR.
[`out`](api:ruststream::runtime::Mounting::out) связывает слот. Слот сохраняет кодек роутера, здесь это JSON.
Роутер указывает брокер и тип состояния приложения в [`RouterDef<MemoryBroker, Shop>`](api:ruststream::runtime::RouterDef). Тип `Shop` появляется в главе 7.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="orders.rs" reference="examples/guide/orders.rs#routes"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="orders.rs" reference="examples/manual/guide/orders.rs#routes"
```

</TabItem>
</Tabs>

Ручная форма связывает каждого подписчика с его темой через [`subscriber(..)`](api:ruststream::runtime::subscriber), а `.describe(..)` задаёт описание, которое макросу даёт doc-комментарий.
[`include_router`](api:ruststream::runtime::BrokerScope::include_router) монтирует роутер на брокере. Приложение, которое это делает, показывает глава 5.
