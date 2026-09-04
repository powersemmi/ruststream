# 编解码器与序列化

编解码器负责把线上的字节变成带类型的载荷，再变回去。它与 Broker 是彼此独立的接缝：消费一侧的管线是
`bytes -> Codec -> typed payload -> handler`，发布一侧则反向走一遍。编解码器在处理器挂载时就已固定，
因此在投递路径上不花任何代价。

## 内置的编解码器

| 编解码器 | feature | 引入依赖 | 传输格式 |
|---|---|---|---|
| `JsonCodec` | `json` *（默认）* | serde_json | JSON |
| `MsgpackCodec` | `msgpack` | rmp-serde | MessagePack |
| `CborCodec` | `cbor` | ciborium | CBOR |

各个编解码器 feature 严格可加，需要几个就开几个。消息类型只需要 derive `serde::Deserialize`
（回复还需要 `Serialize`）。

## 默认编解码器 { #the-default-codec }

`DefaultCodec` 是一个由 feature 选出的别名：启用了 `json` 就是它，否则是 `cbor`，再否则是
`msgpack`。当没有任何地方指定编解码器时，`include(def)` 和停在 `.out(Reply, policy)` 的回复链用的
就是它；这两者都不接收编解码器参数。

一个编解码器 feature 都不启用时，没有任何东西能编码或解码，于是凡是会用到默认编解码器的写法都是
编译错误，而且错误会点明几条出路：启用一个编解码器 feature、显式指定一个编解码器，或者把这个消息
放到字节路径上。字节路径从不需要编解码器 - [`Deserialized` 输入](subscribers.md#raw-subscribers)
从这次投递的字节里构造自己，`Serialized` 值自己产出字节 - 所以只讲自家传输格式的服务，一个编解码器
feature 都不开也照样运行，什么都不会少。

## 二进制协议不是编解码器 { #binary-protocols-are-not-codecs }

编解码器这个位置的含义是「一个值，由挂载点选定的编解码器来编码」。生成出来的 Protobuf 消息放不进
这个位置：它本身就是自己的编码，字节布局从头到尾归它所有。放进编解码器的位置，一处挂载就可能把
`Order` 发成 JSON，另一处发成 Protobuf - 字节路径存在的意义正是杜绝这种混淆。所以二进制协议走
字节路径，类型与线之间不解析任何东西。

生成出来的代码不靠手改，但给自己产出的代码加注解，是每个生成器都会的。`prost-build` 接受
`message_attribute`，于是整份配方就是构建配置里的两行：

<!-- inline-rust: the service's own build script, which has no compiled home in this repository -->
```rust
// build.rs
prost_build::Config::new()
    .message_attribute(".", "#[derive(ruststream::Serialized, ruststream::Deserialized)]")
    .message_attribute(".", "#[wire(prost)]")
    .compile_protos(&["proto/orders.proto"], &["proto"])?;
```

此后 schema 里的每个消息一到手就已经在字节路径上，如同手写出来的一样。「手写」这一栏是同一个消息把
derive 展开后的样子：两个字节路径的 impl、它们选定的传输方式，以及出站声明：

=== "宏"

    ```rust
    --8<-- "examples/protobuf.rs:message"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/protobuf.rs:message"
    ```

处理器进出两侧都不指定编解码器，因为这个类型根本不解析编解码器：

=== "宏"

    ```rust
    --8<-- "examples/protobuf.rs:handler"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/protobuf.rs:handler"
    ```

`#[wire(prost)]` 是某一个生成器那两条路径的简写。通用写法自己点名这两个函数 -
`#[wire(encode = <path>, decode = <path>)]`：`encode` 是 `fn(&Self, &mut BytesMut)`，返回空或者
`Result`；`decode` 是 `fn(&[u8]) -> Result<Self, E>`。Cap'n Proto、FlatBuffers 和自己手写的帧都走
同一套机制，不需要为每种格式加一个 cargo feature：本 crate 只调用属性点名的东西，不依赖其中任何
一个，依赖它的是服务自己。哪种格式这两种写法都套不进去，就照「手写」那一栏写这个消息的办法来写：
`wire_bytes` 和 `from_payload` 都是公开 trait 的方法，整条字节路径根本不需要 `macros` feature。

模型类型在要紧的地方仍然看得见。挂载点点的是它，`Out` 槽位的词典列的是它，生成的 `AsyncAPI`
文档报告的也是它 - 这三处，预先编码好的字节 newtype 全都藏在一袋字节后面。这两个服务见
[`examples/protobuf.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/protobuf.rs)
和
[`examples/manual/protobuf.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/manual/protobuf.rs)。

## 解码用的编解码器从哪里来 { #where-the-decode-codec-comes-from }

解码用的编解码器在编译期就已固定。`include` 不接收编解码器参数；它会从你设置过的最具体的层级解析出
一个，由窄到宽依次是：

### 按处理器 { #per-handler }

覆盖单次挂载：

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

以上都没有指定编解码器时，`include` 使用 [`DefaultCodec`](#the-default-codec)。

## 发布一侧 { #the-publish-side }

发布者遵循同样的规则：`.out(Reply, policy)` 用默认编解码器编码回复，
`.out(Reply, policy).codec(codec)` 则显式指定一个 - 单个 `Out` 槽位的
`.out(marker, policy).codec(codec)` 同理。传入请求的解码遵循所在作用域（用
`with_broker_codec` 设置的作用域编解码器，或路由器链上的 `Router::with_codec`，再否则是默认值）。
回复用的编解码器则随着这条链搭好的接线传递，因此请求和回复的格式可以自由地不同。

编解码器是挂载的属性，而不是消息类型的属性：同一个类型可以在这个订阅上按 JSON 解码，在另一个订阅上
按 CBOR 解码，而挂载点是唯一说明用哪一个的地方。

## 解码失败 { #decode-failures }

解码失败时，由失败策略决定这条消息的去向；默认是丢弃（不重新入队的 nack）。该策略按订阅者设置，用
`on_failure(decode = ..)` 子句：

=== "宏"

    ```rust
    use ruststream::subscriber;

    --8<-- "examples/codecs.rs:decode_failure"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/codecs.rs:decode_failure"
    ```

各个策略取值（`Drop`、`Retry`、`RetryAfter(..)`、`Skip`、`FailFast`）、默认值以及重试方面的注意事项，
参见[失败策略](failure-policy.md)。上面这些编解码器示例出自
[`examples/codecs.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/codecs.rs)。

## 自定义编解码器

编解码器就是任何实现了 `Codec` trait 的类型，因此你可以提供自己的实现，并把它传到任何接受内置编解码器
的地方。让它对另一个编解码器泛型，它就能组合起来：内层编解码器决定载荷的格式，外层包装只变换它周围的
字节。下面这个编解码器给内层的输出加上两个字节的版本头，schema 注册中心的信封和加密包装也是同样的形状。

```rust
--8<-- "examples/custom_codec.rs:codec"
```

包装的两侧都通过 `CodecError` 上报错误。内层编解码器的失败本身就是一个 `CodecError`，用 `?` 原样向上
传递。包装自身的失败则变成 `CodecError::Decode`（或 `CodecError::Encode`），并把你自己的错误类型作为
来源带上，因此错误消息会指出是哪一层拒绝了载荷、以及为什么：`decode failed: not an envelope: leading
byte 0x7b`。

自定义编解码器可以挂载的层级和内置编解码器完全一样，共三个。这里三个一次写全，作用域、路由器链和回复
各占一行：

=== "宏"

    ```rust
    --8<-- "examples/custom_codec.rs:mount"
    ```

=== "手写"

    ```rust
    --8<-- "examples/manual/custom_codec.rs:mount"
    ```

## 同步边界 { #the-synchronous-boundary }

`Codec::encode` 和 `Codec::decode` 是同步的，这就定死了编解码器里能放什么：常量和手头的字节已经能决定
的东西，比如上面那个版本标记。序列化时需要 I/O 的集成放不进来，例如到注册中心解析 schema id、从 KMS
取密钥；在里面包一个阻塞调用会拖住投递任务。

把这类集成放到异步的边缘：传入的载荷在订阅的投递路径上转码，赶在编解码器看到它之前；出站的则用
[`PublishLayer`](middleware.md#publish-side-middleware) 加封。两者都是异步的，也都可以返回错误。同一条
边界在 Broker 一侧的说法，参见 [Broker 作者](../broker-authors/index.md#middleware-on-the-async-edges)。

这个编解码器出自
[`examples/custom_codec.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/custom_codec.rs)。
