# Middleware

Middleware оборачивает обработчики сквозной логикой: трассировка, метрики, авторизация, повторы. В
RustStream две области middleware. Обе строятся на трейте `Layer` и применяются в разных точках пути
диспетчеризации.

## Области middleware {#middleware-scopes}

Области вкладываются друг в друга: внешняя - стек приложения, внутренняя - собственный стек роутера.

**Область приложения.** `RustStream::layer` добавляет слой на всё приложение, до `with_broker`. Слой
оборачивает каждый обработчик, зарегистрированный после него: и в области брокера, и в роутере,
смонтированном через `include_router`. Порядок проверяется во время компиляции: первый `with_broker`
переводит билдер в фазу без `layer`, `publish_layer` и `on_startup`. Слой, который не обернул бы уже
зарегистрированные обработчики, - ошибка компиляции, а не молчаливое бездействие:

=== "Макросы"

    ```rust
    --8<-- "examples/middleware_app_scope.rs:app_scope"
    ```

=== "Вручную"

    ```rust
    --8<-- "examples/manual/middleware_app_scope.rs:app_scope"
    ```

**Область роутера.** `Router::layer` даёт роутеру собственный стек: он оборачивает каждый обработчик
этого роутера при монтировании (см. [Роутинг](routing.md#router-middleware)). Обработчики,
смонтированные прямо в области брокера, в него не попадают:

=== "Макросы"

    ```rust
    --8<-- "examples/middleware_router_scope.rs:router_scope"
    ```

=== "Вручную"

    ```rust
    --8<-- "examples/manual/middleware_router_scope.rs:router_scope"
    ```

Обе программы целиком -
[`middleware_app_scope.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/middleware_app_scope.rs)
и
[`middleware_router_scope.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/middleware_router_scope.rs).
`LogLayer` - написанный вручную слой из следующего раздела. Встроенный `layers::TracingLayer`
монтируется так же.

Первый добавленный слой - самый внешний. Оба стека статические: диспетчеризация в рантайме ничего не
стоит, а тип стека растёт с каждым вызовом `layer`.

!!! note "Чтобы дотянуться до обработчиков роутера, нужен `BlanketLayer`"
    Слой, который оборачивает обработчики роутера (стек приложения на `include_router` или
    `Router::layer`), обязан реализовать `BlanketLayer` - один обобщённый метод, оборачивающий любой
    обработчик. Поставляемые слои его реализуют; для своего слоя это несколько строк рядом с его
    impl `Layer` (см. `LogLayer` в примерах выше).

## Как написать слой

Слой превращает один обработчик в другой. Реализуйте `Layer<H>`:

```rust
use ruststream::runtime::{Context, Handler, HandlerOutcome, Layer};

--8<-- "examples/middleware.rs:layer_impl"
```

`Identity` - слой, который ничего не делает. Это глобальный стек по умолчанию. `Stack<Inner, Outer>`
соединяет два слоя. `ctx` - тот же [`Context`](context.md) доставки, который получает обработчик,
поэтому слой может проставить значения в [рабочую копию
заголовков](context.md#the-headers-working-copy) до того, как обработчик её прочитает.

## Middleware для одной регистрации

Слой можно поставить на одну регистрацию вместо всего приложения. `.layer(..)` после `include` на
роутере относится к этой регистрации, как и остальные шаги цепочки относятся к позиции, названной
перед ними.

<!-- inline-rust: the call shape; the LogLayer impl it composes is compiled in middleware.rs:layer_impl, shown above -->
```rust
let router = Router::<MemoryBroker>::new().include(handle).layer(LogLayer);
```

Одна регистрация - то, что нужно, когда слой требуется только части обработчиков. Это единственное
место для слоя без `BlanketLayer`: тип обработчика здесь ещё конкретен, поэтому достаточно обычного
`Layer<H>`. Слой стоит снаружи шага декодирования, поэтому видит сырое сообщение брокера. Он
сочетается со стеком приложения и стеком роутера.

## Сколько стоит слой

Статические слои на горячем пути ничего не стоят. Динамические добавляют накладные расходы на каждое
сообщение, поэтому они нужны там, где цепочка собирается в рантайме.

## Динамическое middleware

Состав цепочки иногда известен только в рантайме: слои включает конфигурация или они скрыты за
`dyn`. Для таких обработчиков возьмите динамический стек: `DynStack`, `DynMiddleware` и `Next`.
`DynMiddleware` получает вход и контекст, а дальше либо вызывает `next.run(..)` и продолжает
цепочку, либо обрывает её собственным результатом. Возвращаемый тип вы записываете явно:

```rust
use std::future::Future;
use std::pin::Pin;

use ruststream::runtime::{Context, DynMiddleware, HandlerOutcome, Next};

--8<-- "examples/middleware.rs:dyn_middleware"
```

Динамичен только *список*. Соберите его в рантайме и передайте в `DynStack`: результат - обычный
статический `Layer`, привязанный к одному типу входа. Поэтому его ставят на одну регистрацию через
`.layer(..)`, а не в стек приложения, который принимает только слои, реализующие `BlanketLayer`.
Остальная цепочка диспетчеризации остаётся статической, накладные расходы есть только у самого
стека:

=== "Макросы"

    ```rust
    use std::sync::Arc;

    use ruststream::memory::MemoryMessage;
    use ruststream::runtime::DynStack;

    --8<-- "examples/middleware.rs:dyn_stack"
    ```

=== "Вручную"

    ```rust
    use std::sync::Arc;

    use ruststream::memory::{MemoryBroker, MemoryMessage};
    use ruststream::prelude::*;
    use ruststream::runtime::{DynMiddleware, DynStack};

    --8<-- "examples/manual/middleware.rs:dyn_stack"
    ```

Полная программа, где цепочку переключает переменная окружения, -
[`examples/middleware.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/middleware.rs).

`DynStack<I>` обобщён по входу, который оборачивает. На регистрации он оборачивает декодирующий
обработчик целиком, поэтому строится над сырым типом сообщения брокера (выше это
`DynStack<MemoryMessage>`) и работает до декодирования. Middleware, обобщённое по `I` (как `Audit`),
работает с любым типом входа. Внутри одного `DynStack` middleware выполняется в порядке списка,
первым - самый внешний.

## Middleware на стороне публикации {#publish-side-middleware}

Всё middleware выше работает на пути потребления, на входящих сообщениях. У пути публикации свой
конвейер, см. [Публикация и ответы](publishing.md#the-publish-pipeline).

## Встроенные слои {#built-in-layers}

- `layers::TracingLayer` выдаёт событие трассировки на каждое сообщение: DEBUG при поступлении, INFO
  на ack, WARN на nack. Чтобы увидеть эти события в консоли, включите фичу `logging`, см.
  [Логирование](logging.md).
- Слой из фичи `metrics` пишет счётчики Prometheus и гистограмму длительности, см.
  [Метрики](metrics.md).
