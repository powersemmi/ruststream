# AsyncAPI

С фичей `asyncapi` RustStream строит документ [AsyncAPI 3.0](https://www.asyncapi.com/) по
обработчикам приложения. Каждый подписчик становится каналом и операцией `receive`, а типы полезной
нагрузки дают схемы. Несколько обработчиков могут делить один канал. Тогда документ показывает по
операции на обработчик: подписка у каждого своя.

```toml
ruststream = { version = "0.7", features = ["macros", "memory", "asyncapi"] }
```

## Генерация документа

Быстрее всего - через CLI. Он запускает генератор вашего сервиса и печатает документ:

```bash
ruststream asyncapi gen                  # JSON to stdout
ruststream asyncapi gen -o asyncapi.json
ruststream asyncapi gen --yaml
```

В коде спецификацию по приложению строит `build_spec`, а сериализуют её `to_json` или `to_yaml`:

```rust
--8<-- "examples/asyncapi_http.rs:generate"
```

`#[ruststream::app]` сам связывает команду `asyncapi gen` с `build_spec`, поэтому CLI и вызов из
вашего кода дают один и тот же документ.

## Схемы полезной нагрузки {#payload-schemas}

Тип полезной нагрузки обработчика попадает в документ схемой, если выводит `JsonSchema`.
RustStream реэкспортирует `schemars`, поэтому отдельная зависимость не нужна:

=== "Макросы"

    ```rust
    --8<-- "examples/asyncapi_http.rs:payload"
    ```

=== "Вручную"

    ```rust
    --8<-- "examples/manual/asyncapi_http.rs:payload"
    ```

На пути `#[subscriber]` тип без `JsonSchema` тоже годится в полезную нагрузку, только схемы
документу не даёт. Генератор пишет `WARN` на каждый такой пробел: по одному на обработчик или на
объявленное исходящее сообщение, с именем подписки или канала и с типом.
`Spec::messages_without_schema()` перечисляет затронутые компоненты сообщений. Проверьте в тесте,
что список пуст, и CI не пропустит сообщение без схемы.

Ручная регистрация - цепочка `subscriber(..)` - строже. Она документируется по умолчанию, поэтому с
фичей `asyncapi` требует схему от своих типов сообщений. Тип без `JsonSchema` на этом пути даёт
ошибку компиляции, и она называет недостающий вывод. `.undocumented()` убирает одну регистрацию из
документа и снимает с неё требование схемы.

Сообщение со своим форматом передачи - намеренное исключение. Вход
[`Deserialized`](subscribers.md#raw-subscribers) и исходящее сообщение, которое публикуется уже
сериализованным (ответ с [`#[derive(Serialized)]`](subscribers.md#raw-subscribers) или член
`Serialized` в списке `#[publishes(..)]` слота), попадают в документ под собственным именем и без
схемы нагрузки. Генератор о них не предупреждает, и `messages_without_schema()` их не перечисляет:
формат здесь - сами байты, схеме сказать нечего.

Кроме полезных нагрузок в документе есть **схемы заголовков** - из параметра `Headers<T>`
обработчика или из контракта, объявленного на самом типе сообщения, - и **операции `send`** на каждое
объявленное исходящее сообщение: на ответ формы `publish(..)` и на каждый тип сообщения, объявленный
слотом `Out`. См. [типизированные заголовки](headers.md).

Тип сообщения с шаблонным именем (`#[outgoing(name = "orders.{tenant}.v1")]`) объявляется по этому
шаблонному адресу, а блок **parameters** канала заполняют его подстановки. Тип, который не объявляет
адресата, канала не даёт. См. [публикацию](publishing.md#declaring-where-a-message-goes).

## Имена и описания сообщений

Тип полезной нагрузки со схемой задаёт компонент сообщения сам: doc-комментарий типа становится
описанием сообщения, а `#[schemars(title = "...")]` или переименование даёт компоненту имя. Без
схемы компонент называется по типу полезной нагрузки, а описание берётся из doc-комментария
обработчика; он же описывает операцию `receive`. В ручной цепочке описание операции задаёт
`.describe(..)`.

Задать метаданные явно, в том числе для типа без `JsonSchema`, вы можете через трейт `MessageInfo`:
он важнее схемы. Вывод `MessageInfo` берёт имя типа и его doc-комментарий:

<!-- inline-rust: minimal MessageInfo-derive sketch; the compiled form (asyncapi_http.rs:payload) also derives JsonSchema, which would obscure the point that MessageInfo takes precedence over the schema -->
```rust
use ruststream::MessageInfo;

/// An order placed by a customer.
#[derive(MessageInfo, serde::Deserialize)]
struct Order {
    id: u64,
}
// In the document: components.messages.Order with that description.
```

Ручной `impl MessageInfo` может назвать компонент иначе, чем называется тип Rust
(`const NAME: &'static str = "CustomOrder";`), - так контракт передачи не меняется при
переименовании типа.

## Серверы

Опишите серверы, к которым подключается сервис, чтобы они попали в раздел `servers` документа.
`ServerSpec` вы строите напрямую:

=== "Макросы"

    ```rust
    --8<-- "examples/asyncapi_http.rs:server"
    ```

=== "Вручную"

    ```rust
    --8<-- "examples/manual/asyncapi_http.rs:server"
    ```

Крейт брокера может реализовать совместимость `DescribeServer`. Тогда спецификацию отдаёт
`broker.describe_server()`, а `with_broker_labeled` записывает её под меткой брокера. У всех
поставляемых брокеров эта совместимость есть.

## Безопасность сервера

Как аутентифицируются клиенты, объявляет `ServerSpec::with_security`. Каждая схема попадает в
`components.securitySchemes`, а список `security` сервера на неё ссылается:

```rust
--8<-- "examples/asyncapi_http.rs:security"
```

У `SecurityScheme` есть конструкторы для видов схем AsyncAPI 3.0: `user_password`, `plain`,
`scram_sha256` / `scram_sha512`, `gssapi`, `api_key`, `x509`, `http`, `http_api_key`,
`open_id_connect` и `oauth2`, который принимает объект flows сырым JSON. Схему, которой среди них
нет, задаёт `SecurityScheme::custom(json)`.

Безопасность объявляет автор сервиса, а не брокер: `DescribeServer` о ней не сообщает. Чтобы закрыть
сервер, зарегистрированный брокером автоматически (`with_broker_labeled`), объявите его явно:
`.server(label, broker.describe_server().with_security(..))` с той же меткой.

## Как отдавать документ

`build_spec` и `to_json` / `to_yaml` дают байты документа, а отдаёте вы их тем HTTP-стеком, который
у вас уже работает: axum, actix или любым другим.

Интерактивный просмотрщик даёт `render_viewer_html`: он возвращает самодостаточную HTML-страницу,
которая загружает React-компонент AsyncAPI и показывает в нём вашу спецификацию по её URL:

<!-- inline-rust: two-line API-shape fragment; the compiled call lives in asyncapi_http.rs:generate -->
```rust
use ruststream::asyncapi::{render_viewer_html, ViewerOptions};

let html = render_viewer_html("/asyncapi.json", &ViewerOptions::default());
```

Отдавайте этот HTML и JSON спецификации двумя маршрутами своего сервера. По умолчанию просмотрщик
загружает ресурсы с CDN. Для офлайна или закрытого контура вы можете задать другой базовый адрес
через `ViewerOptions::with_cdn_base`, а `with_title` задаёт заголовок страницы.

## Полноценный сервер

Пример [`asyncapi_http`](https://github.com/powersemmi/ruststream/blob/main/examples/asyncapi_http.rs)
отдаёт документ и просмотрщик через [axum](https://github.com/tokio-rs/axum). Запустите его командой
`cargo run --example asyncapi_http --features macros,memory,asyncapi` и откройте
<http://127.0.0.1:8080/>.

=== "Макросы"

    ```rust
    --8<-- "examples/asyncapi_http.rs"
    ```

=== "Вручную"

    ```rust
    --8<-- "examples/manual/asyncapi_http.rs"
    ```
