# AsyncAPI

С фичей `asyncapi` RustStream строит документ [AsyncAPI 3.1](https://www.asyncapi.com/) по
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
обработчика или из контракта, объявленного на самом типе сообщения, - и **операция `send`** на
каждый тип сообщения, объявленный слотом `Out`. См. [типизированные заголовки](headers.md).

Тип сообщения с шаблонным именем (`#[outgoing(name = "orders.{tenant}.v1")]`) объявляется по этому
шаблонному адресу, а блок **parameters** канала заполняют его подстановки. Тип, который не объявляет
адресата, канала не даёт. См. [публикацию](publishing.md#declaring-where-a-message-goes).

## Запрос и ответ

Обработчик, который возвращает значение, отвечает на доставку, и документ это говорит: операция
`receive` несёт поле `reply` с каналом ответа и сообщением, которое там поедет.

```rust
--8<-- "examples/asyncapi_http.rs:reply"
```

```json
"receive_requests": {
  "action": "receive",
  "channel": { "$ref": "#/channels/requests" },
  "messages": [{ "$ref": "#/channels/requests/messages/Order" }],
  "reply": {
    "channel": { "$ref": "#/channels/responses" },
    "messages": [{ "$ref": "#/channels/responses/messages/Confirmed" }]
  }
}
```

Канал ответа остаётся в документе наравне с остальными: трафик там настоящий. Своей операции `send`
у него нет. Ровно ради этого поле `reply` и существует: иначе читателю пришлось бы сопоставлять две
несвязанные операции по имени.

## На каком сервере живёт канал

Канал называет серверы, на которых он есть, и сервис на двух брокерах перестаёт показывать каждый
канал на обоих. Имя берётся из метки, под которой зарегистрирован брокер:

```rust
--8<-- "examples/asyncapi_http.rs:server"
```

Сервис с одним сервером называет его на каждом канале, с меткой или без: выбирать не из чего. Сервис
с несколькими серверами и регистрацией без метки поле опускает, а это по спецификации значит "канал
доступен на всех серверах". Регистрируйте брокеров через `with_broker_labeled`, и вопрос не
возникает.

Один случай документ описать не может. Обработчик, который публикует через межброкерный токен,
попадает на брокера этого токена, а не на брокера регистрации, и канал сообщает про сервер
регистрации.

## Media type полезной нагрузки

Каждое сообщение называет media type своей нагрузки; он берётся из кодека, который её разбирает:
`application/json`, `application/cbor`, `application/msgpack`. Сервис, который везде разбирает один
формат, называет его ещё и один раз в корне, полем `defaultContentType`. Сервис с двумя кодеками
корневое поле опускает: читатель принял бы его за весь документ.

Свой кодек называет свой media type одной ассоциированной константой:

<!-- inline-rust: a one-line trait constant; the compiled custom codec lives in examples/custom_codec.rs, which predates this constant and keeps the default -->
```rust
impl Codec for ProtobufCodec {
    const CONTENT_TYPE: &'static str = "application/vnd.google.protobuf";
    // encode / decode как обычно
}
```

Без константы кодек сообщает `application/octet-stream`. Вход `Deserialized` не сообщает ничего:
кодек над ним не работает, значит и media type назвать некому.

## Повторы и недоставленные сообщения

Регистрация, которая объявила предел попыток или адресата недоставленных, говорит об этом на своей
операции `receive`, в расширении `x-ruststream-retry`:

```json
"receive_orders": {
  "action": "receive",
  "channel": { "$ref": "#/channels/orders" },
  "x-ruststream-retry": { "maxAttempts": 5, "deadLetter": "orders.dead" }
}
```

Адресат недоставленных при этом остаётся каналом с операцией `send`: недоставленное сообщение
действительно уходит из сервиса. Отличить его от бизнес-назначения даёт как раз расширение: очередь
недоставленных в спецификации несут только биндинги `sqs` и `sns`, а предел попыток - вообще ни
один. См. [предел повторов](subscribers.md#capping-the-retries).

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

Свои каналы, операции и сообщения брокер описывает ещё и словами собственного протокола:
долговечность очереди, группа потребителей, QoS. Эти **биндинги** попадают в документ сами, а что
именно заполняет конкретный брокер, написано в его документации.

Версию протокола, на которой говорят клиенты, называет `with_protocol_version`. Заполнять её стоит
там, где одно имя протокола покрывает несовместимые версии: AMQP 0.9.1 и AMQP 1.0 в документе оба
`amqp`, а общего у них больше ничего нет.

## Описание сервиса

Раздел `info` несёт, что за сервис перед читателем и кто им занимается. Всё это принимает `AppInfo`:

```rust
--8<-- "examples/asyncapi_http.rs:describe"
```

Собственный идентификатор сервиса задаёт `with_id`. Это URI, и `AppId` разбирает его при
конструировании, поэтому заголовок, набранный не в тот билдер, падает на месте вызова, а не в уже
опубликованном документе:

<!-- inline-rust: two lines of a fallible parse; putting a `?` or an unwrap in the compiled example would either add an error type to it or panic at startup -->
```rust
let info = AppInfo::new("orders", "0.1.0").with_id("urn:example:orders".parse()?);
```

## Безопасность сервера

Как аутентифицируются клиенты, объявляет `ServerSpec::with_security`. Каждая схема попадает в
`components.securitySchemes`, а список `security` сервера на неё ссылается:

```rust
--8<-- "examples/asyncapi_http.rs:security"
```

У `SecurityScheme` есть конструкторы для видов схем AsyncAPI: `user_password`, `plain`,
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
