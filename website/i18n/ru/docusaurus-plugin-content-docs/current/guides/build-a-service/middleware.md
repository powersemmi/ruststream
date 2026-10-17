---
title: Middleware и наблюдаемость
description: "Приложение оборачивает каждого подписчика в два слоя: один пишет в журнал доставки и их исходы, другой, собственный, предупреждает о медленных обработчиках."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Middleware и наблюдаемость

Приложение оборачивает каждого подписчика в два слоя: один пишет в журнал доставки и их исходы, другой, собственный, предупреждает о медленных обработчиках.

[`Layer`](../../components/subscribers/middleware/layer.md) оборачивает обработчик в другой обработчик.
Слой выполняет код до тела обработчика, после него или вместо него и передаёт исход дальше.
Слой приложения оборачивает всех подписчиков, которых монтирует приложение. Слой роутера оборачивает только подписчиков этого роутера.
RustStream сам записывает события через `tracing`: запуск сервиса, подписку на каждую тему, остановку сервиса.
[`TracingLayer`](api:ruststream::runtime::layers::TracingLayer) добавляет событие для каждой доставки: `delivery received` на уровне `DEBUG`, затем её исход. Подтверждение (ack) записывается на уровне `INFO`, переобработка и отбрасывание - на уровне `WARN`.
Фича `logging` выводит эти события, как показала глава 1. Вместо неё подходит любой другой подписчик `tracing`.
Раздел [Наблюдаемость](../../components/observability/index.md) описывает метрики Prometheus и экспорт в OpenTelemetry. Они подключаются как слои тем же способом.

## Собственный слой магазина

`SlowHandlers` замеряет время каждого обработчика и предупреждает, если обработчик работает дольше порога.
Предупреждение называет подписку, имя которой слой читает из [контекста](../../components/subscribers/context/index.md) доставки.
У слоя нет формы на макросах: в обеих вкладках это один и тот же обычный тип.
[`BlanketLayer`](api:ruststream::runtime::BlanketLayer) позволяет приложению применять его к обработчикам, типы которых приложение скрывает.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="observability.rs" reference="examples/guide/observability.rs#layer"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="observability.rs" reference="examples/manual/guide/observability.rs#layer"
```

</TabItem>
</Tabs>

## Приложение

`service.rs` собирает магазин.
[`layer`](api:ruststream::runtime::RustStream::layer) добавляет слой приложению. Первый добавленный слой - внешний.
Роутер каждого модуля монтируется через [`include_router`](api:ruststream::runtime::BrokerScope::include_router), и слои приложения оборачивают его подписчиков.
`on_startup` создаёт разделяемое состояние из главы 7, а `info()` - описание из главы 9.
Функция принимает брокер параметром, поэтому точка входа командной строки и HTTP-сервер из главы 10 собирают один и тот же магазин.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="service.rs" reference="examples/guide/service.rs#shop"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="service.rs" reference="examples/manual/guide/service.rs#shop"
```

</TabItem>
</Tabs>

## Что показывает журнал

Заказ, оформленный запросом с идентификатором `req-1`, оставляет по строке на каждый шаг:

```text
INFO guide_http::orders order placed request=req-1 order=1 total_cents=14400
INFO ruststream::dispatch handler ack
INFO ruststream::dispatch handler ack
INFO ruststream::dispatch handler ack
INFO guide_http::notifications mail sent server=smtp://mail.internal:25 to="ada@example.com" text="We have your order 1."
INFO ruststream::dispatch handler ack
INFO ruststream::dispatch handler ack
INFO ruststream::dispatch handler ack
INFO guide_http::notifications mail sent server=smtp://mail.internal:25 to="ada@example.com" text="Order 1 is on its way: TRK-1-2."
INFO ruststream::dispatch handler ack
```

`RUST_LOG` настраивает, что выводится, например `RUST_LOG=ruststream=debug,info`.
