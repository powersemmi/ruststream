# Роутинг

Когда сервис растёт, обработчики выносят из `main.rs` в отдельные модули. `Router` собирает
обработчики одного модуля в одну группу. `include_router` монтирует эту группу на область брокера.

## Сборка роутера

`Router` повторяет область брокера. `include` - единственная точка входа. Она монтирует определение
любой формы: обычной, сырой, пакетной, с публикацией ответа, с внедрением. Форму выбирает само
определение. `with_codec` переключает кодек декодирования для цепочки (см.
[Кодеки](codecs.md#per-handler)).

Источник подписки задаёт само определение: `#[subscriber(..)]` берёт выражение-источник брокера
вместе с цепочкой вызовов, поэтому в точке монтирования источник не указывают. Каждый вызов
поглощает роутер и возвращает новый, поэтому регистрации выстраиваются в цепочку:

=== "Макросы"

    ```rust title="routes.rs"
    use ruststream::runtime::Router;

    --8<-- "examples/routing.rs:builders"
    ```

=== "Вручную"

    ```rust title="routes.rs"
    use ruststream::runtime::Router;

    --8<-- "examples/manual/routing.rs:builders"
    ```

<!-- inline-rust: minimal mount fragment with placeholder routes module; the full compiled program is examples/routing.rs (merge form pulled in below) -->
```rust title="main.rs"
RustStream::new(info).with_broker(broker, |b| {
    b.include_router(routes::orders());
});
```

Обработчики с привязкой - издателем ответа или слотом
[`Out`](publishing.md#publishing-from-inside-a-handler) - регистрируются на роутере так же, как на
области, с одним отличием: регистрацию фиксирует явный `.build()`. `.out(marker, policy)` задаёт
политику публикации для одной позиции: `Reply` - для ответа, маркер слота - для слота `Out`. Когда
`.out(Reply, ..)` не указан, `.build()` берёт для ответа политику публикации брокера по умолчанию.

Цепочка без `.build()` не станет роутером, поэтому не скомпилируется. Политики остаются чистой
декларацией, поэтому и такому роутеру брокер не нужен:

=== "Макросы"

    ```rust title="routes.rs"
    --8<-- "examples/tutorial/routes.rs:routes"
    ```

=== "Вручную"

    ```rust title="routes.rs"
    --8<-- "examples/manual/tutorial/routes.rs:routes"
    ```

## Middleware роутера {#router-middleware}

У роутера может быть собственный стек слоёв: `Router::layer` оборачивает в него каждый обработчик
роутера при монтировании. Вокруг этого стека `include_router` оборачивает глобальный стек
приложения, добавленный через `RustStream::layer`. Области вкладываются друг в друга, и самая
внешняя - приложение:

=== "Макросы"

    ```rust title="main.rs"
    --8<-- "examples/logging_middleware.rs:layered_router"
    ```

=== "Вручную"

    ```rust title="main.rs"
    --8<-- "examples/manual/logging_middleware.rs:layered_router"
    ```

Роутер скрывает конкретные типы своих обработчиков, поэтому слой, который их оборачивает, обязан
быть `BlanketLayer`. Обе области, требование `BlanketLayer` и написание собственного слоя разбирает
раздел [Middleware](middleware.md#middleware-scopes).

## Композиция и монтирование

Соберите по роутеру на модуль и комбинируйте их так, как удобно сервису:

<!-- inline-rust: illustrative multi-router composition with placeholder route modules; the compiled merge form is examples/routing.rs:merge, pulled in below -->
```rust
// Mount several routers on one broker - include_router can be called more than once.
RustStream::new(info).with_broker(broker, |b| {
    b.include_router(routes::orders());
    b.include_router(routes::shipping());
});
```

Или слейте группы в один роутер до монтирования (полная программа -
[`examples/routing.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/routing.rs)):

=== "Макросы"

    ```rust
    --8<-- "examples/routing.rs:merge"
    ```

=== "Вручную"

    ```rust
    --8<-- "examples/manual/routing.rs:merge"
    ```

`merge` дописывает регистрации другого роутера по порядку. Каждый роутер сохраняет свой кодек и свой
стек слоёв. При монтировании слои внешнего роутера (и глобальный стек приложения) оборачиваются
вокруг слоёв присоединённого роутера.

## Что дальше

- Контракт обработчика и макрос `#[subscriber]` - [Подписчики](subscribers.md).
- Выбор кодека декодирования для `include` - [Кодеки](codecs.md).
