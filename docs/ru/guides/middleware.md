# Middleware

Middleware оборачивает обработчики сквозной логикой: трассировка, метрики, авторизация, повторы. В
RustStream две области middleware; обе построены на одной и той же машинерии `Layer`, но применяются
в разных точках пути диспетчеризации.

## Области middleware {#middleware-scopes}

Области вкладываются друг в друга: стек приложения - внешний, собственный стек роутера сидит внутри
него.

**Область приложения.** Слой на всё приложение добавляется через `RustStream::layer`, до
`with_broker`. Обёрнутым оказывается каждый обработчик, зарегистрированный после него: и те, что
регистрируются прямо в области брокера, и те, что приносит роутер через `include_router`. Порядок
держится на времени компиляции: первый же `with_broker` переводит билдер в фазу, где `layer` (а
также `publish_layer` и `on_startup`) больше не существует, поэтому слой, который не смог бы
обернуть уже зарегистрированные обработчики, - ошибка компиляции, а не молчаливый no-op:

=== "Макросы"

    ```rust
    --8<-- "examples/middleware_app_scope.rs:app_scope"
    ```

=== "Вручную"

    ```rust
    --8<-- "examples/manual/middleware_app_scope.rs:app_scope"
    ```

**Область роутера.** Собственное middleware роутеру даёт `Router::layer`: он оборачивает каждый
обработчик этого роутера в момент монтирования (см. [Роутинг](routing.md#router-middleware)).
Обработчики, смонтированные прямо в области брокера, остаются снаружи:

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
[`middleware_router_scope.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/middleware_router_scope.rs);
`LogLayer` - это написанный руками слой из следующего раздела, а встроенный
`layers::TracingLayer` монтируется точно так же.

Добавленный первым слой оказывается самым внешним. Оба стека статические: диспетчеризация в рантайме
не стоит ничего, а тип растёт с каждым вызовом `layer`.

!!! note "Чтобы дотянуться до обработчиков роутера, нужен `BlanketLayer`"
    Слой, который оборачивает обработчики роутера (стек приложения на `include_router` или
    `Router::layer`), обязан реализовать `BlanketLayer` - один обобщённый метод, оборачивающий любой
    обработчик. Поставляемые слои его реализуют; для своего слоя это несколько строк рядом с его
    impl `Layer` (см. `LogLayer` в примерах выше).

## Как написать слой

Слой превращает один обработчик в другой. Реализуйте `Layer<H>`:

```rust
use ruststream::runtime::{Context, Handler, HandlerResult, Layer};

--8<-- "examples/middleware.rs:layer_impl"
```

`Identity` - слой, который ничего не делает (глобальный стек по умолчанию), а `Stack<Inner, Outer>`
соединяет два слоя. `ctx` здесь - тот же самый [`Context`](context.md) доставки, который получает
обработчик, поэтому слой может обогатить [рабочую копию
заголовков](context.md#the-headers-working-copy) до того, как обработчик её прочитает.

## Middleware для одного обработчика

`HandlerExt::with` оборачивает один обработчик вместо всего приложения:

<!-- inline-rust: HandlerExt::with API-shape fragment with placeholder handler and layer; the LogLayer impl it composes is compiled in middleware.rs:layer_impl, shown above -->
```rust
use ruststream::runtime::HandlerExt;

let handler = base_handler.with(LogLayer);
```

Это то, что нужно, когда слой требуется только части обработчиков. С глобальным стеком он
сочетается.

## Сколько стоит слой

Статические слои на горячем пути ничего не стоят. Динамические платят на каждом сообщении, поэтому
берите их тогда, когда цепочка собирается в рантайме.

## Динамическое middleware

Когда цепочка решается в рантайме (слои включаются конфигом или лежат за `dyn`), включите
динамический стек ровно для этих обработчиков: `DynStack`, `DynMiddleware` и `Next`. У
`DynMiddleware` сигнатура вида around/next: он получает на вход сообщение и контекст, а дальше либо
вызывает `next.run(..)`, чтобы продолжить, либо коротко замыкает цепочку собственным результатом.
Свой возвращаемый тип он записывает явно:

```rust
use std::future::Future;
use std::pin::Pin;

use ruststream::runtime::{Context, DynMiddleware, HandlerResult, Next};

--8<-- "examples/middleware.rs:dyn_middleware"
```

Динамичен только *список*. Соберите его в рантайме, заморозьте в `DynStack` - и результат станет
обычным статическим `Layer`, который добавляется в стек приложения через `layer` ровно так же, как
написанный руками. Остальная цепочка диспетчеризации остаётся статической, накладные расходы есть
только у самого стека:

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

Полная программа, где цепочка переключается переменной окружения, -
[`examples/middleware.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/middleware.rs).

`DynStack<I>` обобщён по входу, который оборачивает. В стеке приложения он оборачивает целиком
декодирующий обработчик, поэтому строится над сырым типом сообщения брокера (выше это
`DynStack<MemoryMessage>`) и работает до декодирования; middleware, обобщённое по `I` (как `Audit`),
годится для любого из уровней. Чтобы работать с уже декодированным значением, соберите
`DynStack<Order>` и наденьте его на внутренний типизированный обработчик через `with` (форма ручной
регистрации). Middleware внутри одного `DynStack` выполняется в порядке списка, начиная с самого
внешнего. Держите статическую цепочку как вариант по умолчанию и беритесь за `DynStack` только там,
где сборка в рантайме себя окупает.

## Middleware на стороне публикации {#publish-side-middleware}

Всё middleware выше работает на пути потребления (входящие сообщения). У пути публикации свой
конвейер, см. [Публикация и ответы](publishing.md#the-publish-pipeline).

## Встроенные слои {#built-in-layers}

- `layers::TracingLayer` выдаёт событие трассировки на каждое сообщение (DEBUG при поступлении, INFO
  на ack, WARN на nack). Чтобы эти события отображались в консоли, включите фичу `logging`, см.
  [Логирование](logging.md).
- Фича `metrics` поставляет слой, который пишет счётчики Prometheus и гистограмму длительности, см.
  [Метрики](metrics.md).
