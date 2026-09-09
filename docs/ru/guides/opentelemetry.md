# OpenTelemetry

Фича `otel` даёт сервису распределённую трассировку: одна трасса охватывает всю цепочку
«принял - преобразовал - отправил», от входящего сообщения до ответов на него.

```toml
ruststream = { version = "0.7", features = ["macros", "memory", "json", "otel"] }
```

Распространение контекста переносит [W3C Trace Context](https://www.w3.org/TR/trace-context/) и
открывает спаны `tracing`. Оно не зависит от брокера и работает без экспортера.

## Связывание

Создайте `OpenTelemetry`, поставьте его слой потребления на уровне приложения и добавьте его
распространение в связывание ответа:

=== "Макросы"

    ```rust
    --8<-- "tests/opentelemetry.rs:wiring"
    ```

=== "Вручную"

    ```rust
    --8<-- "tests/manual_opentelemetry.rs:wiring"
    ```

- `consume_layer()` - [слой](middleware.md) на стороне потребления. На каждую доставку он читает
  входящий `traceparent`, открывает спан `tracing` для обработчика и записывает спан *потребителя*
  в рабочую копию заголовков. Он действует и на обработчики, смонтированные напрямую, и на
  смонтированные через [роутер](routing.md).
- `propagation()` - статическое [преобразование публикации](publishing.md). Оно копирует
  `traceparent` (и `tracestate`) из рабочей копии заголовков в каждый ответ, поэтому сервис ниже по
  потоку видит спан потребителя родителем ответа. Для пакетного издателя то же преобразование
  переиспользуют через `for_batch(otel.propagation())`.

## Что именно распространяется

Доставка с `traceparent` вида `00-<trace-id>-<span-id>-01` продолжает эту трассу: ответ сохраняет
тот же `trace-id` и получает новый `span-id`, спан потребителя. Доставка без `traceparent` начинает
новую корневую трассу, помеченную как сэмплируемая. Слой потребления пишет спаны под целью
`ruststream.consume` с полями `trace_id` / `span_id` / `subscription`.

## Как прочитать трассу в обработчике

Контекст трассировки потребителя записан в рабочую копию заголовков, поэтому обработчик читает его
как любой другой заголовок - через [контекст](context.md):

<!-- inline-rust: one-line read of the working traceparent inside a handler; the full traced app, including this access, is compiled in tests/opentelemetry.rs and embedded above -->
```rust
let traceparent = ctx.headers().get_str("traceparent");
```

Разобрать значение можно через `TraceContextPropagator` из SDK OpenTelemetry - тот же разбор, что
делает слой потребления. Он возвращает `opentelemetry::trace::SpanContext`, откуда читаются
`trace_id()`, `span_id()` и `is_sampled()`.

## Экспорт в коллектор

Распространение останавливается на W3C-контексте и спанах `tracing`. Экспорт входит в ту же фичу:
[SDK OpenTelemetry и экспортеры OTLP](#the-otel-feature-sdk-otlp-and-the-metrics-inventory) ставит
один вызов `Otel::builder().init()`. Начинайте с него.

Собрать [`tracing-opentelemetry`](https://docs.rs/tracing-opentelemetry) и экспортер самостоятельно,
прямо в бинарнике, - путь для сервиса, у которого уже есть свой стек подписчиков. Выбор подписчика
фреймворк оставляет за вами так же, как в [логировании](logging.md).

## Фича otel: SDK, OTLP и набор метрик {#the-otel-feature-sdk-otlp-and-the-metrics-inventory}

`Otel::builder().init()` собирает экспортеры OTLP, ставит провайдеры трассировщика и измерителя
OpenTelemetry **глобально** для процесса и включает мост спанов `tracing`. Спаны, которые открывает
распространение, экспортируются без дополнительного связывания:

=== "Макросы"

    ```rust
    --8<-- "examples/otel_export.rs:init"
    ```

=== "Вручную"

    ```rust
    --8<-- "examples/manual/otel_export.rs:init"
    ```

Метрики диспетчеризации собирают два middleware. Инструменты размечены по обработчику
(`messaging.destination.name`) и названы по семантическим соглашениям для messaging или в
пространстве имён `ruststream.*`:

| Инструмент | Тип | Что измеряет |
|---|---|---|
| `messaging.client.consumed.messages` | counter | принятые доставки |
| `messaging.process.duration` | histogram (корзины semconv) | время работы обработчика |
| `ruststream.messages.processed` | counter, атрибут `outcome` | исход доставки: `ack`, `nack_requeue`, `nack_drop`, `retry_after` |
| `ruststream.messages.in_flight` | up-down counter | доставки внутри обработчиков: насыщение пула относительно `workers(n)` |
| `ruststream.message.queue_time` | histogram | задержка от публикации до старта обработчика, по проставленному заголовку с временем публикации |
| `ruststream.messages.decode_failures` | counter | доставки с полезной нагрузкой, отвергнутой кодеком |
| `ruststream.messages.panics` | counter | вызовы обработчика, завершившиеся паникой |
| `messaging.client.sent.messages` | counter, при ошибке с `error.type` | публикации |
| `messaging.client.operation.duration` | histogram | операция публикации |
| `ruststream.message.payload.size` | histogram (`By`) | размеры опубликованной полезной нагрузки |
| `ruststream.batch.size` | histogram | размеры декодированных пакетов, переданных пакетным обработчикам |
| `ruststream.app.state` | observable gauge | состояние жизненного цикла, из [`RunningApp::health`](http.md#a-healthz-endpoint) через `otel.observe_health(running.health())` |

Пакетные обработчики идут мимо слоя потребления, который работает на каждое сообщение
(задокументированное исключение в [middleware](middleware.md)), поэтому `ruststream.batch.size`
пишет сама пакетная диспетчеризация через глобальный измеритель. Метрика записывается, как только
`init()` поставит глобальные провайдеры. При голом `attach()` она остаётся пустой, пока вы не
поставите свой провайдер глобально.

Бизнес-метрикам не нужна отдельная обвязка экспорта. Соберите инструменты один раз на старте в один
объект-хранилище и раздайте его через типизированное состояние (внедряется как `State<..>` через
`FromRef`) - они экспортируются тем же конвейером OTLP:

=== "Макросы"

    ```rust
    --8<-- "examples/otel_export.rs:business_metric"
    ```

=== "Вручную"

    ```rust
    --8<-- "examples/manual/otel_export.rs:business_metric"
    ```

Готовый дашборд Grafana ровно по этому набору лежит в
[`ruststream-grafana`](https://github.com/powersemmi/ruststream-grafana): импортируйте
`dashboards/ruststream.json` и направьте его на любой Prometheus-совместимый бэкенд, принимающий
метрики OTLP, - панели заполнятся по каждому обработчику. README оттуда служит контрактом метрик.

Вызовите `otel.shutdown()` в конце `main`, после штатной остановки приложения, чтобы дослать
последние спаны и точки метрик. Чтобы встроить мост спанов в свой стек подписчиков (например,
вместе со слоем fmt из фичи `logging`), соберите `Otel` с `.tracing_bridge(false)` и поставьте мост
сами. `.messaging_system("kafka")` проставляет атрибут системы из semconv, который ядро, не
зависящее от брокера, вывести не может.
