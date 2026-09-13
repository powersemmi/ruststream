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

Обработчик - это `async fn`, первый параметр которой - декодированная полезная нагрузка. Макрос
`#[subscriber]` превращает функцию в определение подписчика и называет его по имени самой функции.

=== "Макросы"

    ```rust title="src/orders.rs"
    --8<-- "examples/tutorial/orders.rs:order"
    ```

=== "Вручную"

    ```rust title="src/orders.rs"
    --8<-- "examples/manual/tutorial/orders.rs:order"
    ```

Обработчик возвращает [`HandlerOutcome`](../guides/subscribers.md#acking): либо `ack`, либо `nack`,
который отбрасывает сообщение или возвращает его в очередь. Вместо исхода можно вернуть `()` или
`Result<(), E>`, где `Ok` подтверждает, а `Err` отбрасывает.

Вывод `JsonSchema` добавляет схему полезной нагрузки в AsyncAPI-документ шага 6. Описанием
сообщения в документе служит doc-комментарий типа. Отдельная зависимость для этого не нужна: фича
`asyncapi` реэкспортирует `schemars`.

## 3. Свяжите обработчик с приложением

=== "Макросы"

    ```rust title="src/main.rs"
    --8<-- "examples/tutorial/first_app.rs:app"
    ```

=== "Вручную"

    ```rust title="src/main.rs"
    --8<-- "examples/manual/tutorial/first_app.rs:app"
    ```

!!! tip "Кодек по умолчанию"
    `include` декодирует кодеком по умолчанию, поэтому аргумент с кодеком ему не нужен. По
    умолчанию берётся `json`, если фича включена, иначе `cbor`, иначе `msgpack`. Другой кодек для
    всех обработчиков брокера вы можете задать один раз через
    `with_broker_codec(broker, codec, |b| ...)`. Полные правила выбора - в разделе
    [Кодеки](../guides/codecs.md).

Запустите:

```bash
cargo run -- run
```

## 4. Ответьте на сообщения

Чтобы опубликовать ответ, верните его из обработчика и напишите у подписчика `publish`. Адресата
объявляет derive `Outgoing` на типе ответа:

=== "Макросы"

    ```rust title="src/orders.rs"
    --8<-- "examples/tutorial/orders.rs:confirm"
    ```

=== "Вручную"

    ```rust title="src/orders.rs"
    --8<-- "examples/manual/tutorial/orders.rs:confirm"
    ```

Смонтируйте `confirm` рядом с `handle` тем же `include`. Ответ публикуется политикой публикации
брокера по умолчанию и кодируется кодеком по умолчанию.

=== "Макросы"

    ```rust title="src/main.rs"
    --8<-- "examples/tutorial/reply_app.rs:reply"
    ```

=== "Вручную"

    ```rust title="src/main.rs"
    --8<-- "examples/manual/tutorial/reply_app.rs:reply"
    ```

Публикацию изнутри обработчика и остальные способы разбирает раздел
[Публикация и ответы](../guides/publishing.md).

## 5. Наведите порядок роутером

Когда обработчиков становится много, держите их в отдельном модуле и собирайте в
[`Router`](../guides/routing.md):

=== "Макросы"

    ```rust title="src/routes.rs"
    --8<-- "examples/tutorial/routes.rs:routes"
    ```

=== "Вручную"

    ```rust title="src/routes.rs"
    --8<-- "examples/manual/tutorial/routes.rs:routes"
    ```

Обработчик с ответом монтируется на роутер цепочкой: `.out_reply(..)` задаёт политику публикации
ответа, а `.build()` фиксирует регистрацию. Без `.out_reply(..)` `.build()` берёт ту же политику
публикации брокера по умолчанию, что и `include` в шаге 4. Остальные возможности роутера разбирает
раздел [Роутинг](../guides/routing.md).

=== "Макросы"

    ```rust title="src/main.rs"
    --8<-- "examples/tutorial/main.rs:main"
    ```

=== "Вручную"

    ```rust title="src/main.rs"
    --8<-- "examples/manual/tutorial/main.rs:main"
    ```

## 6. Посмотрите AsyncAPI-документ

```bash
cargo run -- asyncapi gen
```

Каждый подписчик добавляет в документ канал и операцию `receive`. Обработчики `handle` и `confirm`
делят канал `orders`, но операция у каждого своя: подписки у них разные. Ответ добавляет на канале
`confirmations` операцию `send`.

Схемы полезных нагрузок документ хранит в `components.messages`. Флаги вывода (`-o`, `--yaml`) и
сам документ разобраны в [руководстве по AsyncAPI](../guides/asyncapi.md).

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

- [Middleware](../guides/middleware.md) - сквозная логика вокруг обработчиков.
- [Жизненный цикл](../guides/lifespan.md) - разделяемое состояние и хуки старта и остановки.
- [Тестирование](../guides/testing.md) - тесты только что написанных обработчиков прямо в процессе.
- [Метрики](../guides/metrics.md) - счётчики и гистограммы Prometheus.
