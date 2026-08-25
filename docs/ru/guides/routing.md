# Роутинг

Когда сервис растёт, обработчики переезжают из `main.rs` в собственные модули. `Router` собирает
обработчики одного модуля в одну монтируемую группу, а `include_router` монтирует всю группу на
область брокера.

## Сборка роутера

`Router` повторяет область брокера: `include` - единственная точка входа, она монтирует любую форму
определения (обычную, сырую, пакетную, с публикацией ответа, с внедрением), а какую именно -
выбирает само определение; рядом стоят `with_codec` (переключает кодек декодирования для цепочки, см.
[Кодеки](codecs.md#per-handler)) и ручные регистрации `handle` / `subscribe`. Источник подписки
всегда приходит из определения - `#[subscriber(..)]` берёт выражение-источник самого брокера вместе
с цепочкой билдера, - так что в точке монтирования называть нечего. Каждый вызов потребляет роутер и
возвращает новый, поэтому регистрации выстраиваются в цепочку:

```rust title="routes.rs"
use ruststream::runtime::Router;

--8<-- "examples/routing.rs:builders"
```

<!-- inline-rust: minimal mount fragment with placeholder routes module; the full compiled program is examples/routing.rs (merge form pulled in below) -->
```rust title="main.rs"
RustStream::new(info).with_broker(broker, |b| {
    b.include_router(routes::orders());
});
```

Обработчики, которым нужна привязка - публикатор ответа или слот
[`Out`](publishing.md#publishing-from-inside-a-handler), - регистрируются на роутере так же, как на
области, с одним отличием: регистрация фиксируется явным терминалом. `.publisher(policy)` задаёт
связывание, `.mount()` берёт собственную политику публикации брокера по умолчанию, а
`.out(marker, policy)` привязывает один именованный слот перед `.mount()`. Забытый терминал никогда
не станет роутером, поэтому цепочка не скомпилируется. Политики остаются чистой декларацией, поэтому
роутеру по-прежнему не нужен брокер:

```rust title="routes.rs"
--8<-- "examples/tutorial/routes.rs:routes"
```

## Middleware роутера {#router-middleware}

У роутера может быть собственный стек слоёв: `Router::layer` оборачивает каждый обработчик этого
роутера в момент монтирования. Глобальный стек приложения (добавленный через `RustStream::layer`)
оборачивается вокруг него на `include_router`: области вкладываются друг в друга, самая внешняя -
приложение:

```rust title="main.rs"
--8<-- "examples/logging_middleware.rs:layered_router"
```

Роутер скрывает конкретные типы своих обработчиков, поэтому слой, который до них дотягивается,
обязан быть `BlanketLayer`. Обе области, требование `BlanketLayer` и написание собственного слоя
разобраны в разделе [Middleware](middleware.md#middleware-scopes).

## Композиция и монтирование

Соберите по роутеру на модуль, а дальше комбинируйте их так, как удобно сервису:

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

```rust
--8<-- "examples/routing.rs:merge"
```

`merge` дописывает регистрации другого роутера по порядку. Каждый роутер сохраняет свой кодек и свой
стек слоёв; когда результат монтируется, слои внешнего роутера (и глобальный стек приложения)
оборачиваются вокруг слоёв влитого роутера.

## Что дальше

- Контракт обработчика и макрос `#[subscriber]` - [Подписчики](subscribers.md).
- Как для `include` выбирается кодек декодирования - [Кодеки](codecs.md).
