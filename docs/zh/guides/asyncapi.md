# AsyncAPI

启用 `asyncapi` feature 后，RustStream 从应用的处理器生成一份
[AsyncAPI 3.0](https://www.asyncapi.com/) 文档。每个订阅者成为一个通道和一个 `receive` 操作，
载荷类型给出 schema。多个处理器可以共用一个通道。这时文档为每个处理器给出一个操作：它们的订阅各自独立。

```toml
ruststream = { version = "0.7", features = ["macros", "memory", "asyncapi"] }
```

## 生成文档

最快的做法是用 CLI。它运行你服务里的生成器，并打印文档：

```bash
ruststream asyncapi gen                  # JSON to stdout
ruststream asyncapi gen -o asyncapi.json
ruststream asyncapi gen --yaml
```

在代码里，`build_spec` 从应用构建 spec，`to_json` 或 `to_yaml` 把它序列化：

```rust
--8<-- "examples/asyncapi_http.rs:generate"
```

`#[ruststream::app]` 会把 `asyncapi gen` 命令绑定到 `build_spec`。
因此 CLI 和你代码里的调用产出的是同一份文档。

## 载荷 schema { #payload-schemas }

处理器的载荷类型只要派生 `JsonSchema`，就会作为一个 schema 进入文档。
RustStream 重导出了 `schemars`，你不需要单独依赖它：

=== "宏"

    ```rust
    --8<-- "examples/asyncapi_http.rs:payload"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/asyncapi_http.rs:payload"
    ```

在 `#[subscriber]` 这条路径上，没有 `JsonSchema` 的类型照样能作为处理器的载荷，只是文档里不会有它的
schema。生成器为每一处这样的缺口写一条 `WARN`：每个处理器或每条已声明的出站消息各一条，
写明订阅或通道的名字和类型。`Spec::messages_without_schema()` 列出受影响的消息组件。
在测试里断言该列表为空，CI 就不会放过缺少 schema 的消息。

手写的注册，也就是 `subscriber(..)` 这条链，更严格。它默认进入文档，因此在 `asyncapi` feature 下
要求自己的消息类型给出 schema。这条路径上没有 `JsonSchema` 的类型是一个编译错误，
错误会指出该加上哪个 `derive`。`.undocumented()` 把一条注册排除在文档之外，
同时免去它给出 schema 的义务。

自带传输格式的消息是刻意留下的例外。[`Deserialized`](subscribers.md#raw-subscribers) 输入，以及已经
序列化之后才发出的出站消息（带 [`#[derive(Serialized)]`](subscribers.md#raw-subscribers) 的回复，或者
槽位 `#[publishes(..)]` 列表里的 `Serialized` 成员），都以自己的名字进入文档，不带载荷 schema。生成器
不会为它们告警，`messages_without_schema()` 也不会列出它们：格式就是那些字节，schema 无话可说。

除了载荷，文档还带上**消息头 schema**（来自处理器的 `Headers<T>` 参数，或者消息类型自身声明的契约）。
每一条已声明的出站消息都有自己的 **`send` 操作**：`publish(..)` 形式的回复，以及 `Out` 槽位声明的
每一种消息类型。参见[类型化消息头](headers.md)。

消息类型声明的名字如果是模板（`#[outgoing(name = "orders.{tenant}.v1")]`），它就声明在该模板化的
地址上，通道的 **parameters** 块由模板里的占位符填充。不声明目的地的类型不给出通道。参见
[发布](publishing.md#declaring-where-a-message-goes)。

## 消息的名字与描述

带 schema 的载荷类型自己就定义了消息组件：类型的文档注释成为消息的描述，
`#[schemars(title = "...")]` 或者重命名给出组件的名字。没有 schema 时，组件按载荷类型命名，
描述取自处理器的文档注释；这条注释同时也是 `receive` 操作的说明。手写的链上，`.describe(..)`
设置这次操作的描述。

要显式指定这些元数据，包括为没有 `JsonSchema` 的类型指定，你可以实现 `MessageInfo` trait：
它的优先级高于 schema。派生 `MessageInfo` 则取类型自身的名字和文档注释：

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

手写的 `impl MessageInfo` 可以给组件起一个与 Rust 类型不同的名字
（`const NAME: &'static str = "CustomOrder";`）。类型改名时，传输契约保持不变。

## 服务器

记录你的服务连接的服务器，它们就会出现在文档的 `servers` 一节里。直接构建一个 `ServerSpec`：

=== "宏"

    ```rust
    --8<-- "examples/asyncapi_http.rs:server"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/asyncapi_http.rs:server"
    ```

Broker crate 也可以实现 `DescribeServer` 能力。这时 `broker.describe_server()` 给出这份 spec，
`with_broker_labeled` 把它记在该 Broker 的标签之下。随框架发布的 Broker 都有该能力。

## 服务器安全

`ServerSpec::with_security` 声明客户端如何认证。每个方案落进 `components.securitySchemes`，
服务器的 `security` 列表引用它：

```rust
--8<-- "examples/asyncapi_http.rs:security"
```

`SecurityScheme` 为 AsyncAPI 3.0 的各种方案提供构造函数：`user_password`、`plain`、
`scram_sha256` / `scram_sha512`、`gssapi`、`api_key`、`x509`、`http`、`http_api_key`、
`open_id_connect` 和 `oauth2`。`oauth2` 接收原始 JSON 形式的 flows 对象。这些构造函数没有覆盖的
方案，用 `SecurityScheme::custom(json)` 声明。

安全由服务作者声明，而不是由 Broker 声明：`DescribeServer` 不报告安全。
要给自动注册的服务器（`with_broker_labeled`）加上安全声明，就用同一个标签显式写出
`.server(label, broker.describe_server().with_security(..))`。

## 把文档提供出去

`build_spec` 和 `to_json` / `to_yaml` 给出文档的字节。用你已经在跑的 HTTP 栈把它们提供出去：
axum、actix 或者别的。

需要交互式查看器时，`render_viewer_html` 返回一个自包含的 HTML 页面。它加载 AsyncAPI 的 React 组件，
并在其中按 URL 显示你的 spec：

<!-- inline-rust: two-line API-shape fragment; the compiled call lives in asyncapi_http.rs:generate -->
```rust
use ruststream::asyncapi::{render_viewer_html, ViewerOptions};

let html = render_viewer_html("/asyncapi.json", &ViewerOptions::default());
```

把该 HTML 和 spec 的 JSON 放在你自己服务器的两条路由上。查看器默认从 CDN 加载资源。离线或者
封闭网络下，你可以用 `ViewerOptions::with_cdn_base` 换掉基础 URL，`with_title` 设置页面标题。

## 一个完整的服务器

[`asyncapi_http`](https://github.com/powersemmi/ruststream/blob/main/examples/asyncapi_http.rs)
示例用 [axum](https://github.com/tokio-rs/axum) 提供文档和查看器。用
`cargo run --example asyncapi_http --features macros,memory,asyncapi` 运行它，然后打开
<http://127.0.0.1:8080/>。

=== "宏"

    ```rust
    --8<-- "examples/asyncapi_http.rs"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/asyncapi_http.rs"
    ```
