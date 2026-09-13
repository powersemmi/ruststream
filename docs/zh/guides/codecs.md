# 编解码器与序列化

编解码器把消息的字节变成带类型的载荷，再变回去。它与 Broker 无关。接收一侧的流水线是
`字节 -> Codec -> 带类型的载荷 -> 处理器`，发布一侧反向运行同一条流水线。编解码器在挂载处理器
时就已固定，因此在投递路径上不花任何代价。

## 内置的编解码器

| 编解码器 | feature | 引入依赖 | 传输格式 |
|---|---|---|---|
| `JsonCodec` | `json` *（默认）* | serde_json | JSON |
| `MsgpackCodec` | `msgpack` | rmp-serde | MessagePack |
| `CborCodec` | `cbor` | ciborium | CBOR |

编解码器的 feature 严格可加，需要几个就启用几个。消息类型只需 derive `serde::Deserialize`，
回复类型再加一个 `Serialize`。

## 默认编解码器 { #the-default-codec }

`DefaultCodec` 是按启用的 feature 选出的别名：启用了 `json` 就是它，否则是 `cbor`，再否则是
`msgpack`。哪里都没有指定编解码器时，`include(def)` 和停在 `.out_reply(policy)` 的回复链用的
就是它。这两种写法都不接收编解码器参数。

一个编解码器 feature 都不启用时，就没有东西可以编码和解码。凡是会用到默认编解码器的写法都成为
编译错误，错误本身列出几条出路：启用一个编解码器 feature、显式指定一个编解码器，或者把消息
放到字节路径上。

字节路径从不需要编解码器。[`Deserialized` 输入](subscribers.md#raw-subscribers)从本次投递的
字节里构造自己，`Serialized` 值自己产出字节。只使用自有传输格式的服务，一个编解码器 feature
都不开也照样运行，什么都不会少。

## 二进制协议不是编解码器 { #binary-protocols-are-not-codecs }

编解码器的位置意味着“一个值，由挂载点选定的编解码器来编码”。生成出来的 Protobuf 消息放不进
这个位置：它本身就是自己的编码，从头到尾拥有字节布局。放进编解码器的位置，一处挂载会把 `Order`
发成 JSON，另一处发成 Protobuf；字节路径防的正是这种混淆。所以二进制协议走字节路径，类型与
传输字节之间没有编解码器。

生成出来的代码不靠手改，但每个生成器都能给自己产出的代码添加属性。`prost-build` 接受
`message_attribute`，因此整套做法就是给每个生成的消息加两个属性：

<!-- inline-rust: the service's own build script, which has no compiled home in this repository -->
```rust
// build.rs
prost_build::Config::new()
    .message_attribute(
        ".",
        "#[derive(ruststream::Serialized, ruststream::Deserialized, ruststream::Outgoing)]",
    )
    .message_attribute(".", "#[wire(prost)]")
    .compile_protos(&["proto/orders.proto"], &["proto"])?;
```

第三个 derive 声明消息发往何处，类型化发布和回复都读这份声明。

此后 schema 里的每个消息一到手就已经在字节路径上，如同手写出来的一样。“手写”标签页是同一个
消息展开 `derive` 之后的样子：

=== "宏"

    ```rust
    --8<-- "examples/protobuf.rs:message"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/protobuf.rs:message"
    ```

处理器在入口和出口都不指定编解码器：

=== "宏"

    ```rust
    --8<-- "examples/protobuf.rs:handler"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/protobuf.rs:handler"
    ```

`#[wire(prost)]` 是某一个生成器那对函数的简写。通用形式自己点名这两个函数：
`#[wire(encode = <path>, decode = <path>)]`。`encode` 是 `fn(&Self, &mut BytesMut)`，可以不返回
值，也可以返回 `Result`；`decode` 是 `fn(&[u8]) -> Result<Self, E>`。

Cap'n Proto、FlatBuffers 和自己手写的帧走同一套机制。不需要为每种格式单独加一个 cargo
feature：本 crate 只调用属性点名的函数，不依赖其中任何一个，依赖由服务自己声明。

这两种写法都套不进去的格式，就照“手写”标签页的样子写这个消息。`wire_bytes` 和
`from_payload` 是公开 trait 的方法，因此字节路径不需要 `macros` feature 也能用。

模型类型在要紧的地方仍然看得见：挂载点上写的是它，`Out` 槽位的词典列出的是它，生成的
`AsyncAPI` 文档报告的也是它。换成预先编码好的字节 newtype，这三处都会把类型藏起来。这两个
服务见
[`examples/protobuf.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/protobuf.rs)
和
[`examples/manual/protobuf.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/manual/protobuf.rs)。

## 解码用的编解码器从哪里来 { #where-the-decode-codec-comes-from }

解码用的编解码器在编译期就已固定。`include` 不接收编解码器参数，而是从你指定过的最具体的层级
取一个，由窄到宽依次是：

### 按处理器 { #per-handler }

覆盖单个挂载的编解码器：

=== "Router"

    <!-- inline-rust: standalone Router-builder fragment; the compiled form is the with_broker tab below (codecs.rs:per_handler), which mounts the same chain via include_router -->
    ```rust
    router.with_codec(CborCodec).include(handle);
    ```

=== "with_broker"

    === "宏"

        ```rust
        --8<-- "examples/codecs.rs:per_handler"
        ```

    === "手写"

        ```rust
        --8<-- "examples/manual/codecs.rs:per_handler"
        ```

### 按作用域

为一个 `with_broker` 作用域内的所有处理器设置同一个编解码器：

=== "宏"

    ```rust
    use ruststream::codec::CborCodec;

    --8<-- "examples/codecs.rs:scope"
    ```

=== "手写"

    ```rust
    use ruststream::codec::CborCodec;

    --8<-- "examples/manual/codecs.rs:scope"
    ```

### 默认

以上层级都没有指定编解码器时，`include` 使用 [`DefaultCodec`](#the-default-codec)。

## 发布一侧 { #the-publish-side }

发布者遵循同样的规则：`.out_reply(policy)` 用默认编解码器编码回复，
`.out_reply(policy).codec(codec)` 显式指定一个，单个 `Out` 槽位的
`.out(marker, policy).codec(codec)` 也一样。

传入的请求用 `with_broker_codec` 设置的作用域编解码器解码，或者用 `Router::with_codec` 设置的
路由器链编解码器，两者都没有就用默认编解码器。回复用的编解码器由挂载链决定，因此请求和回复
可以用不同的格式。

编解码器是挂载的属性，不是消息类型的属性。同一个类型可以在一个订阅上按 JSON 解码，在另一个
订阅上按 CBOR 解码，挂载点是唯一说明用哪一个的地方。

## 解码失败 { #decode-failures }

消息解码失败时，怎么处理它由失败策略决定。默认是丢弃消息：不重新入队的 nack。你可以用
`on_failure(decode = ..)` 子句为订阅者设置自己的策略：

=== "宏"

    ```rust
    use ruststream::subscriber;

    --8<-- "examples/codecs.rs:decode_failure"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/codecs.rs:decode_failure"
    ```

各个策略取值（`Drop`、`Retry`、`RetryAfter(..)`、`Skip`、`FailFast`）、默认值以及重试方面的
注意事项，参见[失败策略](failure-policy.md)。上面这些编解码器示例出自
[`examples/codecs.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/codecs.rs)。

## 自定义编解码器

编解码器就是任何实现了 `Codec` trait 的类型。凡是接收内置编解码器的地方，都可以传入你自己的
编解码器。

让它对另一个编解码器泛型，它就成了组合式的：内层编解码器决定载荷的格式，外层包装只变换它周围
的字节。下面这个编解码器给内层输出的字节加上两个字节的版本头。schema 注册中心的信封和加密包装
也是同样的结构。

```rust
--8<-- "examples/custom_codec.rs:codec"
```

包装的两侧都返回 `CodecError` 错误。内层编解码器的错误已经是这个类型，`?` 原样把它向上传递。
包装自身的错误变成 `CodecError::Decode`（或 `CodecError::Encode`），来源是你自己的错误类型。
因此错误文本会指出是哪一层拒绝了载荷，以及原因：`decode failed: not an envelope: leading
byte 0x7b`。

自定义编解码器能挂载的层级和内置编解码器完全一样，共三个。下面的例子一次把三个都写全：

=== "宏"

    ```rust
    --8<-- "examples/custom_codec.rs:mount"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/custom_codec.rs:mount"
    ```

## 同步边界 { #the-synchronous-boundary }

`Codec::encode` 和 `Codec::decode` 是同步的，这就划定了编解码器里能放什么：只有常量和已经拿到
的字节所决定的东西，比如上面那个版本标记。序列化时需要 I/O 的集成放不进来，例如到注册中心查
schema id、从 KMS 取密钥。在编解码器里放一个阻塞调用会停住投递任务。

把这类集成放到编解码器周围的异步位置上：传入的载荷在编解码器看到之前，先在订阅的投递路径上
转码；出站的载荷用 [`PublishLayer`](middleware.md#publish-side-middleware) 包裹。这两处都是
异步的，也都可以返回错误。同一条边界在 Broker 一侧的说法，参见
[Broker 作者](../broker-authors/index.md#middleware-on-the-async-edges)。

这个编解码器出自
[`examples/custom_codec.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/custom_codec.rs)。
