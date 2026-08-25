# Логирование

RustStream шлёт структурированные события [`tracing`](https://docs.rs/tracing) на всём пути
диспетчеризации, при публикации и на переходах жизненного цикла сервиса. Сам он не ставит ни одного
подписчика `tracing` - этот выбор за приложением. Фича `logging` предлагает готовый вариант: цветной
консольный подписчик, управляемый через `RUST_LOG`.

Это не то же самое, что middleware [`TracingLayer`](middleware.md#built-in-layers). `TracingLayer`
*порождает* событие на каждое сообщение, а фича `logging` ставит подписчика, который *отрисовывает*
события (и собственные события RustStream, и ваши) в терминал. Чтобы видеть логи по каждому
сообщению, используйте их вместе.

## Со сгенерированным CLI

Когда фича `logging` включена, CLI из `#[ruststream::app]` сам вызывает установку логгера на команде
`run`, поэтому сгенерированный по шаблону сервис логирует сразу:

```toml
ruststream = { version = "0.7", features = ["macros", "memory", "json", "logging"] }
```

```bash
RUST_LOG=ruststream=debug,info cargo run -- run
```

Вывод идёт в **stderr** (чтобы stdout оставался чистым для `asyncapi gen`), а цвета включаются
автоматически, когда stderr - это терминал.

## Вручную

Установите логгер по умолчанию один раз, в самом начале `main`:

<!-- inline-rust: manual logger-init fragment; the shipped logging example uses the automatic #[ruststream::app] installer, so there is no compiled call site for the by-hand path -->
```rust
ruststream::logging::init()?;
tracing::info!("service starting");
```

`init` берёт фильтр из `RUST_LOG`, а если переменной нет - откатывается на `info`. Умолчания
настраиваются через билдер `Logging`:

<!-- inline-rust: manual Logging-builder fragment; the by-hand init path has no compiled call site (the logging example uses the automatic installer) -->
```rust
use ruststream::logging::Logging;

Logging::new()
    .with_default_filter("ruststream=debug,info")  // used when RUST_LOG is unset
    .with_target(false)                            // hide the event target column
    .try_init()?;
```

`init` и `try_init` никогда не заменяют уже установленного подписчика: повторный вызов (или вызов
после того, как подписчика поставил другой крейт) возвращает `LoggingInitError::AlreadyInitialized`,
а не паникует.

## Свой подписчик

Фича `logging` - необязательный сахар. RustStream только шлёт события `tracing`, поэтому подойдёт
любой подписчик: поставьте `tracing-subscriber`, `tracing-bunyan-formatter`, слой OpenTelemetry или
то, что принято в вашем стеке, - через него потекут те же самые события. В этом случае фичу `logging`
можно не включать.
