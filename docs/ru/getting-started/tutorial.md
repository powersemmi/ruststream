# Учебник: собираем первый сервис

Этот учебник собирает сервис заказов с нуля и разбирает каждую его часть. Сервис работает на
in-memory брокере, поэтому запускать что-то внешнее не нужно. Переход на настоящий брокер - правка
в одну строку, её показывает шаг 7.

## 1. Создайте крейт

```bash
cargo new orders-service
cd orders-service
```

```toml title="Cargo.toml"
[package]
name = "orders-service"
version = "0.1.0"
edition = "2024"

[dependencies]
ruststream = { version = "0.7", features = ["macros", "memory", "json", "asyncapi"] }
serde = { version = "1", features = ["derive"] }
```

## 2. Опишите сообщение и обработчик

Обработчик - это `async fn`. Её первый параметр - полезная нагрузка, уже декодированная из
сообщения. Макрос `#[subscriber]` превращает функцию в определение подписчика с тем же именем.

=== "Макросы"

    ```rust title="src/orders.rs"
    --8<-- "examples/tutorial/orders.rs:order"
    ```

=== "Вручную"

    ```rust title="src/orders.rs"
    --8<-- "examples/manual/tutorial/orders.rs:order"
    ```

Обработчик возвращает [`HandlerOutcome`](https://docs.rs/ruststream/latest/ruststream/runtime/index.html#subscribers).
`ack` подтверждает сообщение, `nack` отбрасывает его или возвращает в очередь. Вместо исхода можно
вернуть `()` или `Result<(), E>`: `Ok` подтверждает сообщение, `Err` отбрасывает.

`JsonSchema` добавляет схему полезной нагрузки в AsyncAPI-документ из шага 6. Doc-комментарий типа
становится описанием сообщения. Фича `asyncapi` реэкспортирует `schemars`, поэтому отдельная
зависимость не нужна.

## 3. Свяжите обработчик с приложением

Приложение - это функция с атрибутом `#[ruststream::app]`. `with_broker` подключает брокер, а
`include` регистрирует на нём обработчик.

=== "Макросы"

    ```rust title="src/main.rs"
    --8<-- "examples/tutorial/first_app.rs:app"
    ```

=== "Вручную"

    ```rust title="src/main.rs"
    --8<-- "examples/manual/tutorial/first_app.rs:app"
    ```

!!! tip "Кодек по умолчанию"
    `include` декодирует сообщения кодеком по умолчанию. Это `json`, если фича включена, иначе
    `cbor`, иначе `msgpack`. Другой кодек для всех обработчиков брокера задаётся один раз через
    `with_broker_codec`. Правила выбора кодека описаны в разделе
    [Кодеки](https://docs.rs/ruststream/latest/ruststream/codec/index.html).

Запустите сервис:

```bash
cargo run -- run
```

## 4. Ответьте на сообщения

Чтобы ответить на сообщение, верните ответ из обработчика и укажите `publish` в `#[subscriber]`.
Адрес ответа объявляет его тип через derive `Outgoing`:

=== "Макросы"

    ```rust title="src/orders.rs"
    --8<-- "examples/tutorial/orders.rs:confirm"
    ```

=== "Вручную"

    ```rust title="src/orders.rs"
    --8<-- "examples/manual/tutorial/orders.rs:confirm"
    ```

Подключите `confirm` тем же `include`, что и `handle`:

=== "Макросы"

    ```rust title="src/main.rs"
    --8<-- "examples/tutorial/reply_app.rs:reply"
    ```

=== "Вручную"

    ```rust title="src/main.rs"
    --8<-- "examples/manual/tutorial/reply_app.rs:reply"
    ```

Ответ публикуется политикой брокера по умолчанию и кодируется кодеком по умолчанию. Другие способы
публикации, в том числе изнутри обработчика, описаны в разделе
[Публикация и ответы](https://docs.rs/ruststream/latest/ruststream/runtime/index.html#publishing).

## 5. Наведите порядок роутером

Когда обработчиков становится много, вынесите их в отдельный модуль и соберите в
[`Router`](https://docs.rs/ruststream/latest/ruststream/runtime/index.html#routing). Роутер - это
набор обработчиков, который приложение подключает к брокеру одним вызовом.

=== "Макросы"

    ```rust title="src/routes.rs"
    --8<-- "examples/tutorial/routes.rs:routes"
    ```

=== "Вручную"

    ```rust title="src/routes.rs"
    --8<-- "examples/manual/tutorial/routes.rs:routes"
    ```

`include` добавляет в роутер обычный обработчик. Обработчик с ответом возвращает цепочку:
`.out_reply(..)` задаёт политику публикации ответа, `.build()` завершает регистрацию. Без
`.out_reply(..)` метод `.build()` берёт политику брокера по умолчанию, ту же, что `include` в
шаге 4.

Приложение подключает роутер через `include_router`:

=== "Макросы"

    ```rust title="src/main.rs"
    --8<-- "examples/tutorial/main.rs:main"
    ```

=== "Вручную"

    ```rust title="src/main.rs"
    --8<-- "examples/manual/tutorial/main.rs:main"
    ```

Остальные возможности роутера описаны в разделе
[Роутинг](https://docs.rs/ruststream/latest/ruststream/runtime/index.html#routing).

## 6. Посмотрите AsyncAPI-документ

```bash
cargo run -- asyncapi gen
```

Каждый подписчик добавляет в документ канал и операцию `receive`. `handle` и `confirm` слушают
канал `orders`, но у каждого своя подписка и своя операция. Ответ добавляет операцию `send` на
канале `confirmations`.

Схемы полезных нагрузок лежат в `components.messages`. Флаги вывода (`-o`, `--yaml`) и устройство
документа описаны в [руководстве по AsyncAPI](https://docs.rs/ruststream/latest/ruststream/asyncapi/index.html).

## 7. Перейдите на настоящий брокер

Ничто из написанного выше не привязано к in-memory брокеру: замена сводится к одной строке в
`with_broker`. Добавьте крейт брокера в зависимости и создайте его вместо `MemoryBroker::new()` -
например, `NatsBroker::new("nats://localhost:4222")`. Обработчики, роутер и кодеки остаются
прежними. Список брокеров и замену для каждого из них даёт раздел
[Брокеры](../brokers/index.md#switching-brokers).

!!! info "Готовый сервис - это компилируемый пример"
    Каждый фрагмент этой страницы взят из
    [`examples/tutorial`](https://github.com/powersemmi/ruststream/tree/main/examples/tutorial),
    который CI собирает при каждом изменении. `first_app.rs` и `reply_app.rs` - это сервис после
    шагов 3 и 4, а `main.rs` - готовый. Запустить его можно командой
    `cargo run --example tutorial --features macros,memory,json,asyncapi -- run`.

## Что дальше

- [Middleware](https://docs.rs/ruststream/latest/ruststream/runtime/index.html#middleware) - сквозная логика вокруг обработчиков.
- [Жизненный цикл](https://docs.rs/ruststream/latest/ruststream/runtime/index.html#lifecycle) - разделяемое состояние и хуки старта и остановки.
- [Тестирование](https://docs.rs/ruststream/latest/ruststream/testing/index.html) - тесты только что написанных обработчиков прямо в процессе.
- [Метрики](https://docs.rs/ruststream/latest/ruststream/metrics/index.html) - счётчики и гистограммы Prometheus.
