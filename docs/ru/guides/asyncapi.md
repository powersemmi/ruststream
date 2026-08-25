# AsyncAPI

С фичей `asyncapi` RustStream генерирует документ [AsyncAPI 3.0](https://www.asyncapi.com/) по
обработчикам приложения: каждый подписчик становится каналом и операцией `receive`, а типы полезной
нагрузки дают схемы. У обработчиков, которые делят один канал, операция остаётся своя у каждого: они
открывают отдельные подписки, поэтому документ показывает по операции на обработчик.

```toml
ruststream = { version = "0.7", features = ["macros", "memory", "asyncapi"] }
```

## Генерация документа

Быстрее всего - через CLI: он запускает генератор вашего сервиса и печатает документ:

```bash
ruststream asyncapi gen                  # JSON to stdout
ruststream asyncapi gen -o asyncapi.json
ruststream asyncapi gen --yaml
```

В коде спецификация строится из приложения через `build_spec`, а сериализуется через `to_json` или
`to_yaml`:

```rust
--8<-- "examples/asyncapi_http.rs:generate"
```

`#[ruststream::app]` сам связывает команду `asyncapi gen` с `build_spec`, поэтому CLI и написанный
руками вызов дают один и тот же документ.

## Схемы полезной нагрузки

Тип полезной нагрузки обработчика попадает в документ схемой, если он выводит `JsonSchema`.
RustStream реэкспортирует `schemars`, так что прямая зависимость не нужна:

```rust
--8<-- "examples/asyncapi_http.rs:payload"
```

Тип без `JsonSchema` тоже работает как полезная нагрузка обработчика, просто не даёт документу схемы.
При генерации на каждый такой пробел пишется `WARN` (по одному на обработчик или на исходящую
декларацию, с именем подписки или канала и с типом; намеренно бессхемные сообщения из сырых байтов не
отмечаются), а `Spec::messages_without_schema()` перечисляет затронутые компоненты сообщений:
проверьте в тесте, что список пуст, - и покрытие схемами станет гейтом в CI.

Кроме полезных нагрузок, документ несёт **схемы заголовков** (из параметра `FromHeaders<T>`
обработчика или из объявленного на типе контракта `headers = ..`) и **операции `send`** на каждое
объявленное исходящее сообщение - ответ формы `publish(..)` и каждый тип сообщения, который объявляет
слот `Out`. См. [типизированные заголовки](headers.md).

Тип сообщения, объявленный шаблонным именем (`#[outgoing(name = "orders.{tenant}.v1")]`),
объявляется по этому шаблонному адресу, а блок **parameters** канала заполняется из его подстановок.
Тип, который не объявляет адресата, не даёт и канала. См.
[публикацию](publishing.md#declaring-where-a-message-goes).

## Имена и описания сообщений

Задокументированный тип полезной нагрузки наполняет компонент сообщения сам: при выводе `JsonSchema`
doc-комментарий типа становится описанием сообщения, а `#[schemars(title = "...")]` (или
переименование) даёт компоненту имя. Без схемы компонент называется по типу полезной нагрузки, а
описание берётся из doc-комментария обработчика (он же документирует операцию `receive`).

Чтобы задать метаданные явно - в том числе для типов без `JsonSchema`, - реализуйте трейт `Message`:
он важнее схемы. Или выведите его, тогда в дело пойдут имя типа и его doc-комментарий:

<!-- inline-rust: minimal Message-derive sketch; the compiled form (asyncapi_http.rs:payload) also derives JsonSchema, which would obscure the point that Message takes precedence over the schema -->
```rust
use ruststream::Message;

/// An order placed by a customer.
#[derive(Message, serde::Deserialize)]
struct Order {
    id: u64,
}
// In the document: components.messages.Order with that description.
```

Ручной `impl Message` может назвать компонент иначе, чем называется тип Rust
(`const NAME: &'static str = "CustomOrder";`), - так контракт на проводе переживает переименования.

## Серверы

Опишите серверы, к которым подключается сервис, чтобы они попали в раздел `servers` документа.
`ServerSpec` строится напрямую:

```rust
--8<-- "examples/asyncapi_http.rs:server"
```

Крейт брокера может реализовать и совместимость `DescribeServer` - тогда спецификацию отдаст
`broker.describe_server()` (у всех поставляемых брокеров это так), а `with_broker_labeled` запишет её
автоматически под меткой брокера.

## Безопасность сервера

Как аутентифицируются клиенты, объявляет `ServerSpec::with_security`: каждая схема попадает в
`components.securitySchemes`, а список `security` сервера на неё ссылается:

```rust
--8<-- "examples/asyncapi_http.rs:security"
```

У `SecurityScheme` есть конструкторы для видов схем AsyncAPI 3.0 - `user_password`, `plain`,
`scram_sha256` / `scram_sha512`, `gssapi`, `api_key`, `x509`, `http`, `http_api_key`,
`open_id_connect` и `oauth2` (он принимает объект flows сырым JSON), - плюс
`SecurityScheme::custom(json)` как запасной выход для всего, что ими не описывается. Без
`with_security` в документе не будет ни одного раздела про безопасность.

`DescribeServer` о безопасности не сообщает: это заявление автора сервиса, а не брокера. Чтобы
закрыть сервер, который брокер зарегистрировал автоматически (`with_broker_labeled`), объявите его
явно: `.server(label, broker.describe_server().with_security(..))` с той же меткой.

## Как отдавать документ

Хостинг - не часть фреймворка. `build_spec` и `to_json` / `to_yaml` отдают байты, а вы монтируете их
в тот HTTP-стек, который у вас уже работает (axum, actix или любой другой).

Для интерактивного просмотрщика есть `render_viewer_html`: он возвращает самодостаточную
HTML-страницу, которая загружает React-компонент AsyncAPI и направляет его на URL вашей спецификации:

<!-- inline-rust: two-line API-shape fragment; the compiled call lives in asyncapi_http.rs:generate -->
```rust
use ruststream::asyncapi::{render_viewer_html, ViewerOptions};

let html = render_viewer_html("/asyncapi.json", &ViewerOptions::default());
```

Отдавайте этот HTML и JSON спецификации двумя маршрутами своего сервера. По умолчанию просмотрщик
тянет ресурсы с CDN; для офлайна или закрытых контуров переопределите базовый URL через
`ViewerOptions::with_cdn_base` (`with_title` задаёт заголовок страницы).

## Полноценный сервер

Пример [`asyncapi_http`](https://github.com/powersemmi/ruststream/blob/main/examples/asyncapi_http.rs)
отдаёт документ и просмотрщик через [axum](https://github.com/tokio-rs/axum). Запустите его командой
`cargo run --example asyncapi_http --features macros,memory,asyncapi`, а затем откройте
<http://127.0.0.1:8080/>.

```rust
--8<-- "examples/asyncapi_http.rs"
```
