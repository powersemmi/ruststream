# AsyncAPI

启用 `asyncapi` feature 之后，RustStream 会从应用的处理器生成一份
[AsyncAPI 3.0](https://www.asyncapi.com/) 文档：每个订阅者变成一个通道和一个 `receive` 操作，载荷
类型则贡献出 schema。共用同一个通道的多个处理器各自保留自己的操作，因为它们打开的是各自独立的订阅，
所以文档里每个处理器都会有一条。

```toml
ruststream = { version = "0.7", features = ["macros", "memory", "asyncapi"] }
```

## 生成文档

最快的路径是用 CLI，它会运行你服务里的生成器并把文档打印出来：

```bash
ruststream asyncapi gen                  # JSON to stdout
ruststream asyncapi gen -o asyncapi.json
ruststream asyncapi gen --yaml
```

在代码里，用 `build_spec` 从应用构建 spec，再用 `to_json` 或 `to_yaml` 把它序列化：

```rust
--8<-- "examples/asyncapi_http.rs:generate"
```

`#[ruststream::app]` 已经替你把 `asyncapi gen` 命令接到了 `build_spec` 上，因此 CLI 和手写调用产出的
是同一份文档。

## 载荷 schema

处理器的载荷类型只要 derive 了 `JsonSchema`，就会作为一个 schema 出现。RustStream 重导出了 `schemars`，
所以你不需要直接依赖它：

```rust
--8<-- "examples/asyncapi_http.rs:payload"
```

没有 `JsonSchema` 的类型照样可以作为处理器的载荷，只是它不会给文档贡献 schema。生成文档时，每出现
一处这样的缺口就会打一条 `WARN` 日志（每个处理器或每条出站声明只报一次，并写明是哪个订阅或哪个通道、
以及是什么类型；刻意不带 schema 的原始字节消息不在此列）。`Spec::messages_without_schema()` 会列出
受影响的消息组件；在测试里断言它为空，就能在 CI 里卡住 schema 覆盖率。

除了载荷之外，文档还会带上**消息头的 schema**（来自处理器的 `FromHeaders<T>` 参数，或者某个类型
声明的 `headers = ..` 契约），以及为每一条已声明的出站消息生成的 **`send` 操作**，包括 `publish(..)`
形式的回复，以及 `Out` 槽位声明的每一种消息类型。参见[类型化消息头](headers.md)。

如果一个消息类型声明的名字是一个模板（`#[outgoing(name = "orders.{tenant}.v1")]`），它就声明在
该模板化的地址上，通道的 **parameters** 块由模板里的占位符填出。没有声明目的地的类型不贡献任何
通道。参见
[发布](publishing.md#declaring-where-a-message-goes)。

## 消息的名字与描述

一个写了文档注释的载荷类型自己就能喂饱消息组件：加上 `JsonSchema` derive 之后，类型的文档注释会成为
消息的 description，而 `#[schemars(title = "...")]`（或者 rename）决定组件的名字。没有 schema 时，
组件以载荷类型命名，description 退回到处理器的文档注释（这条注释同时也是 `receive` 操作的说明）。

若要显式控制这些元数据，包括为没有 `JsonSchema` 的类型控制，就实现 `Message` trait，它的优先级高于
schema；或者 derive 它，那样会使用类型自身的名字和文档注释：

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

手写的 `impl Message` 可以让组件名与 Rust 类型名不同
（`const NAME: &'static str = "CustomOrder";`），这样即使类型改名，线上契约也保持稳定。

## 服务器

把你的服务所连接的服务器记录下来，它们就会出现在文档的 `servers` 一节里。直接构建一个 `ServerSpec`：

```rust
--8<-- "examples/asyncapi_http.rs:server"
```

Broker crate 也可以实现 `DescribeServer` 能力，这样 `broker.describe_server()` 就会替你产出这份 spec
（随框架发布的那几个 Broker 都实现了），而 `with_broker_labeled` 会自动把它记在该 Broker 的标签之下。

## 服务器安全

用 `ServerSpec::with_security` 声明客户端如何认证；每个方案都会落进 `components.securitySchemes`，
并由服务器的 `security` 列表引用：

```rust
--8<-- "examples/asyncapi_http.rs:security"
```

`SecurityScheme` 为 AsyncAPI 3.0 的各种方案类型提供了构造函数：`user_password`、`plain`、
`scram_sha256` / `scram_sha512`、`gssapi`、`api_key`、`x509`、`http`、`http_api_key`、
`open_id_connect`，以及 `oauth2`（它接收以原始 JSON 表示的 flows 对象），此外还有
`SecurityScheme::custom(json)`，作为它们没有建模的一切的逃生口。不调用 `with_security` 的话，文档里
根本不会有任何安全相关的部分。

`DescribeServer` 从不报告安全，因为这是服务作者的声明，而不是 Broker 的声明。要给一个自动注册的
Broker（`with_broker_labeled`）加上安全声明，改为显式声明：用同一个标签写
`.server(label, broker.describe_server().with_security(..))`。

## 把文档提供出去

托管不属于框架的一部分。`build_spec` 和 `to_json` / `to_yaml` 把字节交给你；至于挂到哪个 HTTP 栈上，
用你已经在跑的那个就行（axum、actix 或者别的）。

如果需要一个可交互的查看器，`render_viewer_html` 会返回一个自包含的 HTML 页面，它会加载 AsyncAPI 的
React 组件，并把它指向你的 spec URL：

<!-- inline-rust: two-line API-shape fragment; the compiled call lives in asyncapi_http.rs:generate -->
```rust
use ruststream::asyncapi::{render_viewer_html, ViewerOptions};

let html = render_viewer_html("/asyncapi.json", &ViewerOptions::default());
```

把该 HTML 和 spec JSON 从你自己服务器的两个路由上提供出去即可。查看器默认从 CDN 加载它的资源；
对于离线或受限的部署，用 `ViewerOptions::with_cdn_base` 覆盖基础 URL（`with_title` 设置页面标题）。

## 一个完整的服务器

[`asyncapi_http`](https://github.com/powersemmi/ruststream/blob/main/examples/asyncapi_http.rs)
示例用 [axum](https://github.com/tokio-rs/axum) 同时提供文档和查看器。用
`cargo run --example asyncapi_http --features macros,memory,asyncapi` 运行它，然后打开
<http://127.0.0.1:8080/>。

```rust
--8<-- "examples/asyncapi_http.rs"
```
