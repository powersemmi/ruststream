# Метрики

Фича `metrics` собирает метрики Prometheus по обработанным и опубликованным сообщениям. Она
построена напрямую на крейте `prometheus`.

```toml
ruststream = { version = "0.7", features = ["macros", "memory", "metrics"] }
```

## Связывание

Создайте `Metrics`, добавьте его слои потребления и публикации и сохраните дескриптор, чтобы позже
выгружать метрики:

=== "Макросы"

    ```rust
    --8<-- "examples/metrics_http.rs:wiring"
    ```

=== "Вручную"

    ```rust
    --8<-- "examples/manual/metrics_http.rs:wiring"
    ```

`consume_layer` учитывает каждое обработанное сообщение, `publish_layer` - каждую попытку
публикации, включая те, что вернули ошибку.
`Metrics::with_registry(registry)` собирает метрики в ваш реестр вместо реестра по умолчанию.

## Какие метрики отдаются

| Метрика | Тип | Метки |
|---|---|---|
| `ruststream_messages_consumed_total` | counter | `name`, `status` |
| `ruststream_consume_duration_seconds` | histogram | `name` |
| `ruststream_messages_published_total` | counter | `name`, `status` |

`name` - имя подписки или адресата публикации. `status` - исход: `ack` или `nack` при потреблении,
`ok` или `error` при публикации.

## Выгрузка

`export` возвращает текущие значения в формате экспозиции Prometheus:

<!-- inline-rust: one-line export() API shape; the complete server, including this call, is compiled in metrics_http.rs and pulled in below -->
```rust
let body = metrics.export()?;
```

Отдавайте результат `export()` на маршруте `/metrics` в своём HTTP-стеке или отправляйте его в
push-gateway.

`metrics.registry()` возвращает сам `prometheus::Registry`. Вы можете добавить в него свои
коллекторы рядом с метриками RustStream или передать его готовому экспортеру.

## Полноценный сервер

Пример [`metrics_http`](https://github.com/powersemmi/ruststream/blob/main/examples/metrics_http.rs)
отдаёт `/metrics` через [axum](https://github.com/tokio-rs/axum) и публикует заказы, которые
приходят на маршрут `/orders`, так что счётчики увеличивает обычный HTTP-клиент. Запустите его
командой `cargo run --example metrics_http --features macros,memory,metrics`, а затем:

```bash
curl -X POST http://127.0.0.1:8080/orders -d '{"id":1,"quantity":3}'
curl http://127.0.0.1:8080/metrics
```

=== "Макросы"

    ```rust
    --8<-- "examples/metrics_http.rs"
    ```

=== "Вручную"

    ```rust
    --8<-- "examples/manual/metrics_http.rs"
    ```

Если сервис выгружает метрики через фичу `otel`, готовый дашборд Grafana по полному набору метрик
лежит в [`ruststream-grafana`](https://github.com/powersemmi/ruststream-grafana); см.
[руководство по OpenTelemetry](opentelemetry.md).
