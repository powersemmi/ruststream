# Метрики

Фича `metrics` собирает метрики Prometheus по обработанным и опубликованным сообщениям. Она построена
прямо на крейте `prometheus` и отдаёт данные в формате экспозиции Prometheus.

```toml
ruststream = { version = "0.6", features = ["macros", "memory", "metrics"] }
```

## Связывание

Создайте `Metrics`, установите его слои для потребления и публикации и сохраните хендл, чтобы позже
выгружать данные:

```rust
--8<-- "examples/metrics_http.rs:wiring"
```

`consume_layer` учитывает каждое обработанное сообщение, `publish_layer` - каждое опубликованное.
Чтобы собирать метрики в уже существующий реестр, а не в новый, используйте
`Metrics::with_registry(registry)`.

## Какие метрики отдаются

| Метрика | Тип | Метки |
|---|---|---|
| `ruststream_messages_consumed_total` | counter | `name`, `status` |
| `ruststream_consume_duration_seconds` | histogram | `name` |
| `ruststream_messages_published_total` | counter | `name`, `status` |

`name` - это имя подписки или адресата публикации, `status` - исход (`ack` или `nack` для
потребления, `ok` или `error` для публикации).

## Выгрузка

`export` отдаёт текущие значения в формате экспозиции Prometheus:

<!-- inline-rust: one-line export() API shape; the complete server, including this call, is compiled in metrics_http.rs and pulled in below -->
```rust
let body = metrics.export()?;
```

Как и с AsyncAPI, размещение остаётся на вас: отдавайте `export()` на маршруте `/metrics` в своём
HTTP-стеке или отправляйте результат в push-gateway. `metrics.registry()` возвращает лежащий в основе
`prometheus::Registry` - на случай, если вы хотите зарегистрировать рядом с метриками RustStream свои
коллекторы или использовать уже имеющийся экспортер.

## Полноценный сервер

Пример [`metrics_http`](https://github.com/powersemmi/ruststream/blob/main/examples/metrics_http.rs)
отдаёт `/metrics` через [axum](https://github.com/tokio-rs/axum) и публикует заказы по маршруту
`/orders`, так что счётчики крутит обычный HTTP-клиент. Запустите его командой
`cargo run --example metrics_http --features macros,memory,metrics`, а затем:

```bash
curl -X POST http://127.0.0.1:8080/orders -d '{"id":1,"quantity":3}'
curl http://127.0.0.1:8080/metrics
```

```rust
--8<-- "examples/metrics_http.rs"
```

Если сервис выгружает метрики через фичу `otel`, для полного набора метрик есть готовый дашборд
Grafana в [`ruststream-grafana`](https://github.com/powersemmi/ruststream-grafana); см.
[руководство по OpenTelemetry](opentelemetry.md).
