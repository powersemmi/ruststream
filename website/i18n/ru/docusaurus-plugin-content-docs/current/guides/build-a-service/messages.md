---
title: "Сообщения: полезная нагрузка, заголовки, кодеки"
description: "Заказ получает строки, контракт заголовков, обязательный при каждой публикации, и кодек, выбранный для каждой темы."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Сообщения: полезная нагрузка, заголовки, кодеки

Заказ получает строки, контракт заголовков, обязательный при каждой публикации, и кодек, выбранный для каждой темы.

Полезная нагрузка - это любой тип `serde`. Кодек превращает её в байты и обратно.
Заголовки передаются рядом с полезной нагрузкой. [Контракт заголовков](../../components/subscribers/typed-headers/index.md) - это структура `serde`, в которой каждое поле соответствует одному заголовку.
Тип сообщения, объявивший контракт, нельзя опубликовать без заголовков: такая публикация не компилируется.
Подписчик читает контракт через параметр [`Headers<T>`](api:ruststream::runtime::Headers), который разбирается до запуска тела обработчика.
Доставка с заголовками, которые не удалось разобрать, не доходит до тела и отбрасывается, как и полезная нагрузка, которую не удалось декодировать.
[Кодек](../../components/subscribers/codecs/index.md) задаётся в точке монтирования, а не на типе. Поэтому один тип может передаваться в одной теме в JSON, а в другой - в CBOR.

## Заказ

Дальше код магазина разложен по модулям.
`orders.rs` содержит заказ: его строки, итоговую сумму и контракт заголовков, с которым он публикуется.
Границы ограничивают, сколько может запросить один заказ, а сумма считается с проверкой переполнения, поэтому у заказа за этими границами суммы нет.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="orders.rs" reference="examples/guide/orders.rs#order"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="orders.rs" reference="examples/manual/guide/orders.rs#order"
```

</TabItem>
</Tabs>

`headers = RequestMeta` делает контракт частью типа.
Ручная форма выражает то же через `type Contract = WithHeaders<RequestMeta>` и задаёт имя сообщения в AsyncAPI-документе реализацией [`MessageInfo`](api:ruststream::MessageInfo).

## Контракт заголовков

Каждое поле контракта - один заголовок.
`request_id` указывает запрос к витрине, который оформил заказ, чтобы каждый шаг мог записать его в журнал.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="orders.rs" reference="examples/guide/orders.rs#meta"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="orders.rs" reference="examples/manual/guide/orders.rs#meta"
```

</TabItem>
</Tabs>

## Чтение заголовков

`audit` - второй подписчик темы `orders.placed`. Он записывает каждый заказ в журнал аудита вместе с запросом, который его оформил.
Каждая подписка получает все сообщения своей темы, поэтому каждый заказ видят и `audit`, и `accept` из следующей главы.
Ручная форма принимает на вход пару [`Message<RequestMeta, Order>`](api:ruststream::runtime::Message): обе части декодируются до запуска тела, а AsyncAPI-документ показывает схему заголовков.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="orders.rs" reference="examples/guide/orders.rs#audit"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="orders.rs" reference="examples/manual/guide/orders.rs#audit"
```

</TabItem>
</Tabs>

## Кодек внутренних тем магазина

JSON - кодек по умолчанию. Магазин оставляет его для тем `orders.placed` и `notices`, которые читают другие команды.
Темы между шагами самого магазина передают сообщения в [CBOR](../../components/subscribers/codecs/cbor.md): этот формат компактнее, а других читателей у этих тем нет.
Кодек задаётся одной константой, поэтому роутеры следующих глав не могут разойтись в выборе.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="payments.rs" reference="examples/guide/payments.rs#codec"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="payments.rs" reference="examples/manual/guide/payments.rs#codec"
```

</TabItem>
</Tabs>

[Роутер](../../components/subscribers/router.md) декодирует сообщения кодеком, который задаёт [`with_codec`](api:ruststream::runtime::Router::with_codec), а каждая позиция публикации задаёт свой кодек через `.codec(..)`.
Следующая глава использует обе возможности.
Для CBOR нужна фича `cbor` крейта.
