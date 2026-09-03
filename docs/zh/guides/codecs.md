# 编解码器与序列化

编解码器负责把线上的字节变成带类型的载荷，再变回去。它与 Broker 是彼此独立的接缝：消费一侧的管线是
`bytes -> Codec -> typed payload -> handler`，发布一侧则反向走一遍。编解码器在处理器挂载时就已固定，
因此在投递路径上不花任何代价。

## 内置的编解码器

| 编解码器 | feature | 引入依赖 | 线上格式 |
|---|---|---|---|
| `JsonCodec` | `json` *（默认）* | serde_json | JSON |
| `MsgpackCodec` | `msgpack` | rmp-serde | MessagePack |
| `CborCodec` | `cbor` | ciborium | CBOR |

各个编解码器 feature 严格可加，需要几个就开几个。消息类型只需要 derive `serde::Deserialize`
（回复还需要 `Serialize`）。

## 默认编解码器 { #the-default-codec }

`DefaultCodec` 是一个由 feature 选出的别名：启用了 `json` 就是它，否则是 `cbor`，再否则是
`msgpack`。当没有任何地方指定编解码器时，`include(def)` 和 `TypedPublisher::new(publisher)` 用的
就是它；这两者都不接收编解码器参数。它只在至少启用一个编解码器 feature 时才存在；一个编解码器
feature 都不开时，就只剩下需要显式指定编解码器的那些方法。

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

发布者遵循同样的规则：`TypedPublisher::new(policy)` 用默认编解码器编码回复，
`TypedPublisher::with_codec(policy, codec)` 则显式指定一个。传入请求的解码遵循所在作用域（用
`with_broker_codec` 设置的作用域编解码器，或路由器链上的 `Router::with_codec`，再否则是默认值）。
回复用的编解码器则随着 `.publisher(..)` 附上的那一层传递，因此请求和回复的格式可以自由地不同。

不存在按消息类型指定的编解码器（消息 trait 上没有关联的编解码器）：编解码器是挂载的属性，而不是类型
的属性。

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

通过底层的 `handle` SPI 注册时，`typed(codec, handler)` 返回的 `Typed` 包装器通过
`on_decode_failure` 接收同样的策略。

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
