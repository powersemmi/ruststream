# OpenTelemetry

Фича `otel` даёт сервису распределённую трассировку: трасса идёт от входящего сообщения к ответам,
которые оно породило, поэтому одна трасса покрывает всю цепочку «принял - преобразовал - отправил».
Построена она на типизированном контексте пути публикации - на том же стыке, благодаря которому
преобразование публикации может прочитать доставку, породившую ответ.

```toml
ruststream = { version = "0.6", features = ["macros", "memory", "json", "otel"] }
```

У фичи две половины. Распространение переносит
[W3C Trace Context](https://www.w3.org/TR/trace-context/) и порождает span-ы `tracing`; оно не
зависит от брокера и работает вообще без экспортера. Экспорт идёт в комплекте:
[SDK OpenTelemetry и экспортеры OTLP](#the-otel-feature-sdk-otlp-and-the-metrics-inventory) спрятаны
за `Otel::builder().init()`, который ставит глобальные провайдеры и перекидывает в них span-ы. Либо
соберите собственный подписчик (например, на
[`tracing-opentelemetry`](https://docs.rs/tracing-opentelemetry)) - ровно так же, как руководство по
[логированию](logging.md) оставляет выбор подписчика за вами.

## Связывание

Создайте `OpenTelemetry`, добавьте его слой потребления на уровне приложения и вшейте его
распространение в публикатор ответов:

```rust
--8<-- "tests/opentelemetry.rs:wiring"
```

- `consume_layer()` - это [слой](middleware.md) на стороне потребления: на каждую доставку он читает
  входящий `traceparent`, открывает span `tracing` для обработчика и записывает span *потребителя* в
  рабочие заголовки. Он действует и на обработчики, смонтированные напрямую, и на смонтированные
  через [роутер](routing.md).
- `propagation()` - это статический [слой публикации](publishing.md): он копирует рабочий
  `traceparent` (и `tracestate`) в каждый ответ, поэтому сервис ниже по потоку видит span
  потребителя как родителя ответа. Для пакетного публикатора переиспользуйте его через
  `for_batch(otel.propagation())`.

## Что именно распространяется

Доставка с `00-<trace-id>-<span-id>-01` продолжает эту трассу: ответ сохраняет тот же `trace-id` и
несёт новый `span-id` (span потребителя), поэтому трасса связана из конца в конец. Доставка без
`traceparent` начинает новую корневую трассу, помеченную как сэмплируемая. Span-ы уходят под целью
`ruststream.consume` с полями `trace_id` / `span_id` / `subscription`.

## Как прочитать трассу в обработчике

Контекст трассировки потребителя лежит в рабочих заголовках, поэтому обработчик читает его так же,
как любой другой заголовок, - через [контекст](context.md):

<!-- inline-rust: one-line read of the working traceparent inside a handler; the full traced app, including this access, is compiled in tests/opentelemetry.rs and embedded above -->
```rust
let traceparent = ctx.headers().get_str("traceparent");
```

Разобрать значение можно через `TraceContextPropagator` из SDK OpenTelemetry (тот же парсер, что и в
слое потребления) в `opentelemetry::trace::SpanContext` - чтобы прочитать `trace_id()` / `span_id()`
или проверить `is_sampled()`.

## Экспорт в коллектор

Модуль распространения останавливается на W3C-контексте и span-ах `tracing`; доставить их в коллектор
можно двумя способами. Либо соберите `tracing-opentelemetry` и экспортер сами, прямо в бинарнике (то
же разделение ответственности, что и в [логировании](logging.md)), либо доверьте это
`Otel::builder().init()` - о нём ниже.

## Фича otel: SDK, OTLP и набор метрик {#the-otel-feature-sdk-otlp-and-the-metrics-inventory}

`Otel::builder().init()` собирает экспортеры OTLP, ставит провайдеры трассировщика и измерителя
OpenTelemetry **глобально** для процесса и перекидывает в них span-ы `tracing` - так что span-ы,
которые слой распространения и так открывает, экспортируются без всякого дополнительного связывания:

```rust
--8<-- "examples/otel_export.rs:init"
```

Метрики диспетчеризации несут два middleware; метки проставляются по обработчику
(`messaging.destination.name`) по семантическим соглашениям для messaging плюс пространство имён
`ruststream.*`:

| Инструмент | Тип | Что измеряет |
|---|---|---|
| `messaging.client.consumed.messages` | counter | принятые доставки |
| `messaging.process.duration` | histogram (корзины semconv) | время обработки в обработчике |
| `ruststream.messages.processed` | counter, атрибут `outcome` | исходы завершения доставки: `ack`, `nack_requeue`, `nack_drop`, `retry_after` |
| `ruststream.messages.in_flight` | up-down counter | доставки внутри обработчиков (насыщение пула относительно `workers(n)`) |
| `ruststream.message.queue_time` | histogram | задержка от публикации до старта обработчика, по проставленному заголовку с временем публикации |
| `ruststream.messages.decode_failures` | counter | доставки, чью полезную нагрузку кодек отверг |
| `ruststream.messages.panics` | counter | вызовы обработчика, завершившиеся паникой |
| `messaging.client.sent.messages` | counter, при ошибке с `error.type` | публикации |
| `messaging.client.operation.duration` | histogram | операция публикации |
| `ruststream.message.payload.size` | histogram (`By`) | размеры опубликованной полезной нагрузки |
| `ruststream.batch.size` | histogram | размеры раскодированных пакетов, переданных пакетным обработчикам |
| `ruststream.app.state` | observable gauge | состояние жизненного цикла, из [`RunningApp::health`](http.md#a-healthz-endpoint) через `otel.observe_health(running.health())` |

Пакетные обработчики обходят слой потребления, работающий на каждое сообщение (задокументированное
исключение в [middleware](middleware.md)), поэтому `ruststream.batch.size` пишет сама пакетная
диспетчеризация через глобальный измеритель: метрика оживает, как только `init()` поставит
глобальные провайдеры, и молчит при голом `attach()`, пока вы не поставите свой провайдер глобально
сами.

Раз `init()` ставит глобальные провайдеры, бизнес-метрикам не нужна отдельная обвязка экспорта:
соберите инструменты один раз на старте в один объект-хранилище, раздайте его через типизированное
состояние (внедряется как `State<..>` через `FromRef`) - и всё, что в нём лежит, поедет по тому же
конвейеру OTLP:

```rust
--8<-- "examples/otel_export.rs:business_metric"
```

Готовый дашборд Grafana ровно по этому набору лежит в
[`ruststream-grafana`](https://github.com/powersemmi/ruststream-grafana): импортируйте
`dashboards/ruststream.json`, направьте его на любой Prometheus-совместимый бэкенд, принимающий
метрики OTLP, - и панели заполнятся по каждому обработчику; README оттуда заодно служит контрактом
метрик.

Вызовите `otel.shutdown()` в конце `main`, после штатной остановки приложения, чтобы дослать
последние span-ы и точки. Чтобы вписать мост span-ов в свой стек подписчиков (например, вместе со
слоем fmt из фичи `logging`), собирайте с `.tracing_bridge(false)` и ставьте мост сами;
`.messaging_system("kafka")` проставляет атрибут системы из semconv, который ядро, оставаясь
независимым от брокера, вывести не может.
