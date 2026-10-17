---
title: AsyncAPI-документ
description: "asyncapi gen печатает AsyncAPI-документ магазина: все темы, кто их читает и пишет, схемы, ограничение попыток и описание приложения."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# AsyncAPI-документ

`asyncapi gen` печатает AsyncAPI-документ магазина: все темы, кто их читает и пишет, схемы, ограничение попыток и описание, которое приложение даёт себе.

Документ строится по самому приложению, поэтому он не может описать тему, которую код не использует.
Каждая подписка - это операция `receive`, каждый ответ и каждый слот - операция `send`.
Схема полезной нагрузки берётся из её derive-макроса [`JsonSchema`](../../components/asyncapi/index.md), а doc-комментарий становится описанием.
`max_attempts` и `dead_letter` указываются на операции, поэтому читатель видит, куда попадает сообщение, обработка которого не удаётся.
Раздел [AsyncAPI](../../components/asyncapi/index.md) описывает серверы, схемы безопасности и раздачу документа по HTTP.
Для документа нужна фича `asyncapi` крейта.

## Точка входа

`main.rs` собирает магазин на in-memory брокере.
[`#[ruststream::app]`](api:ruststream::app) даёт исполняемому файлу команды: `run` запускает магазин, `asyncapi gen` печатает документ.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="main.rs" reference="examples/guide/main.rs#main"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="main.rs" reference="examples/manual/guide/main.rs#main"
```

</TabItem>
</Tabs>

## Что магазин сообщает о себе

[`AppInfo`](api:ruststream::runtime::AppInfo) - это блок `info` документа: название, версия, описание, контакт, лицензия и теги.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="service.rs" reference="examples/guide/service.rs#info"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="service.rs" reference="examples/manual/guide/service.rs#info"
```

</TabItem>
</Tabs>

## Документ

```bash
cargo run --example guide --features testing,macros,memory,json,cbor,asyncapi -- asyncapi gen --yaml
```

Операция подписчика `charge` читает тему, отвечает в `payments.settled` и объявляет ограничение попыток:

```yaml
receive_payments_requested:
  action: receive
  channel:
    $ref: '#/channels/payments.requested'
  messages:
  - $ref: '#/channels/payments.requested/messages/PaymentRequest'
  description: Charges an order through the payment provider.
  reply:
    channel:
      $ref: '#/channels/payments.settled'
    messages:
    - $ref: '#/channels/payments.settled/messages/Paid'
  x-ruststream-retry:
    maxAttempts: 5
    deadLetter: payments.dead
```

Обе формы магазина выдают одинаковый документ.
Тест в каждой из них сравнивает документ с [`examples/guide/asyncapi.json`](https://github.com/powersemmi/ruststream/blob/main/examples/guide/asyncapi.json), поэтому любое изменение темы видно на ревью.
