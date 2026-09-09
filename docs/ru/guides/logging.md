# Логирование

RustStream выдаёт структурированные события [`tracing`](https://docs.rs/tracing) при
диспетчеризации, при публикации и на переходах жизненного цикла сервиса. Подписчика событий
`tracing` ставит само приложение. Фича `logging` предлагает готовый вариант: цветной консольный
подписчик, управляемый через `RUST_LOG`.

Событие на каждое сообщение выдаёт middleware [`TracingLayer`](middleware.md#built-in-layers), а
подписчик из фичи `logging` его печатает.

## Со сгенерированным CLI

С включённой фичей `logging` CLI из `#[ruststream::app]` ставит подписчика сам, по команде `run`.

```toml
ruststream = { version = "0.7", features = ["macros", "memory", "json", "logging"] }
```

```bash
RUST_LOG=ruststream=debug,info cargo run -- run
```

Подписчик пишет в **stderr**, чтобы stdout оставался чистым для `asyncapi gen`. Цвета включаются,
когда stderr - это терминал.

## Вручную

Поставьте подписчика по умолчанию один раз, в самом начале `main`:

<!-- inline-rust: manual logger-init fragment; the shipped logging example uses the automatic #[ruststream::app] installer, so there is no compiled call site for the by-hand path -->
```rust
ruststream::logging::init()?;
tracing::info!("service starting");
```

Без `RUST_LOG` фильтр - `info`. Изменить умолчания можно через билдер `Logging`:

<!-- inline-rust: manual Logging-builder fragment; the by-hand init path has no compiled call site (the logging example uses the automatic installer) -->
```rust
use ruststream::logging::Logging;

Logging::new()
    .with_default_filter("ruststream=debug,info")  // used when RUST_LOG is unset
    .with_target(false)                            // hide the event target column
    .try_init()?;
```

`init` и `try_init` не заменяют подписчика, которого уже поставили вы или другой крейт: такой вызов
возвращает `LoggingInitError::AlreadyInitialized`.

## Свой подписчик

Вместо фичи `logging` вы можете поставить любого подписчика событий `tracing`: собранного на крейте
`tracing-subscriber`, на `tracing-bunyan-formatter`, со слоем OpenTelemetry или принятого в вашем
стеке.
