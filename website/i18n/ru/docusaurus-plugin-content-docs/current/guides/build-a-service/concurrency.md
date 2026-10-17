---
title: "Параллельная обработка: воркеры и потоки"
description: "Упаковка - это вычисления, поэтому ей выделены два собственных потока. Бронирование курьера ждёт сети, поэтому четыре бронирования идут одновременно."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Параллельная обработка: воркеры и потоки

Упаковка - это вычисления, поэтому ей выделены два собственных потока. Бронирование курьера ждёт сети, поэтому четыре бронирования идут одновременно.

По умолчанию подписка обрабатывает доставки по одной, в том порядке, в каком их передаёт брокер.
[Воркеры](../../components/subscribers/dispatch/workers.md) позволяют ей обрабатывать несколько доставок одновременно на рантайме приложения. Они подходят для обработчика, который ждёт ввода-вывода.
[Потоки](../../components/subscribers/dispatch/threads.md) выделяют подписке собственные потоки. Они подходят для обработчика, который ведёт вычисления: иначе он задерживал бы рантайм, который обслуживает все остальные подписки.
И воркеры, и потоки - настройки подписки, тело обработчика при этом не меняется.
Подписка с воркерами или потоками больше не сохраняет порядок доставок. `workers(n, by_key)` сохраняет порядок для каждого ключа.

## Упаковка на потоках

`pack` разбивает оплаченный заказ на посылки и отвечает планом упаковки.
Оплаченный заказ за границами магазина отбрасывается до того, как спланирована хоть одна посылка.
`threads(2)` запускает вместе с подпиской два потока, и доставки распределяются между ними по очереди.
Декодирование, тело обработчика, ответ и завершение доставки выполняются в потоке, который получил доставку.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="shipping.rs" reference="examples/guide/shipping.rs#pack"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="shipping.rs" reference="examples/manual/guide/shipping.rs#pack"
```

</TabItem>
</Tabs>

## Бронирование на воркерах

`dispatch` бронирует курьера для упакованного заказа и сообщает покупателю код отслеживания.
`workers(4)` допускает до четырёх бронирований одновременно. Следующая доставка ждёт свободного воркера.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="shipping.rs" reference="examples/guide/shipping.rs#dispatch"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="shipping.rs" reference="examples/manual/guide/shipping.rs#dispatch"
```

</TabItem>
</Tabs>

## Роутер

Роутер отгрузки читает внутренние темы магазина, поэтому декодирует CBOR.
Ответ использует кодек по умолчанию, если его позиция не задаёт другой, поэтому ответ `pack` задаёт CBOR.
Слот использует кодек роутера, если не задаёт свой, поэтому уведомления задают JSON.
В ручной форме обе настройки - шаги цепочки монтирования: [`threads`](api:ruststream::runtime::SubscriberSettings::threads) и [`workers`](api:ruststream::runtime::SubscriberSettings::workers).

Счётная часть работает на своих потоках, поэтому рантайм приложения только ждёт ввода-вывода, и магазин запускает его в потоке, который вызывает `main`: `#[ruststream::app(flavor = "current_thread")]`, а в ручной форме [`run_main_on(AppRuntime::CurrentThread, app)`](api:ruststream::runtime::cli::run_main_on). Точка входа с HTTP из главы про HTTP делает то же через `#[tokio::main(flavor = "current_thread")]`.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="shipping.rs" reference="examples/guide/shipping.rs#routes"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="shipping.rs" reference="examples/manual/guide/shipping.rs#routes"
```

</TabItem>
</Tabs>

Потоки окупаются, когда вычисления занимают от десяти микросекунд на доставку. На более лёгком обработчике они обходятся дороже, чем экономят.
[Диагностика опроса](../../components/subscribers/dispatch/poll-diagnostics.md) находит обработчики, которые задерживают рантайм.
