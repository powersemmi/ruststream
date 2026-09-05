# Codecs and serialization

A codec turns wire bytes into your typed payload and back. It is a separate seam from the broker:
the pipeline on the consume side is `bytes -> Codec -> typed payload -> handler`, and the publish
side runs it in reverse. The codec is fixed when the handler is mounted, so it costs nothing on
the delivery path.

## Built-in codecs

| Codec | Feature | Pulls in | Wire format |
|---|---|---|---|
| `JsonCodec` | `json` *(default)* | serde_json | JSON |
| `MsgpackCodec` | `msgpack` | rmp-serde | MessagePack |
| `CborCodec` | `cbor` | ciborium | CBOR |

Codec features are strictly additive; enable as many as you need. Message types only need to derive
`serde::Deserialize` (and `Serialize` for replies).

## The default codec

`DefaultCodec` is a feature-selected alias: `json` if enabled, otherwise `cbor`, otherwise
`msgpack`. It is what `include(def)` and a reply chain that stops at `.out(Reply, policy)` use when
nothing names a codec; neither takes a codec argument.

With no codec feature enabled at all, nothing can encode or decode, so anything that would need
the default codec is a compile error naming the ways out: enable a codec feature, name a codec
explicitly, or put the message on a byte lane. The byte lanes never need a codec - a
[`Deserialized` input](subscribers.md#raw-subscribers) builds itself from the delivery's bytes,
and a `Serialized` value produces its own - so a service that speaks only its own wire formats
runs with no codec feature and loses nothing.

## Binary protocols are not codecs

A codec position means "a value, encoded by a codec the mount site chose". A generated Protobuf
message does not fit there: it *is* its encoding, and it owns the byte layout end to end. Put it
in a codec position and one mounting could send `Order` as JSON while another sends it as
Protobuf - the confusion the byte lanes exist to prevent. So a binary protocol goes on the lanes,
and nothing is resolved between the type and the wire.

Generated code is not edited by hand, but every generator can decorate what it emits.
`prost-build` takes `message_attribute`, so the whole recipe is two lines of build configuration:

<!-- inline-rust: the service's own build script, which has no compiled home in this repository -->
```rust
// build.rs
prost_build::Config::new()
    .message_attribute(".", "#[derive(ruststream::Serialized, ruststream::Deserialized)]")
    .message_attribute(".", "#[wire(prost)]")
    .compile_protos(&["proto/orders.proto"], &["proto"])?;
```

Every message in the schema then arrives on the lanes already, as if it had been written out. The
Manual tab is the same message with the derives expanded - two lane impls, the wire spellings they
pick, and the outgoing declaration:

=== "Macros"

    ```rust
    --8<-- "examples/protobuf.rs:message"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/protobuf.rs:message"
    ```

The handler names no codec, on the way in or the way out, because none is resolved for this type:

=== "Macros"

    ```rust
    --8<-- "examples/protobuf.rs:handler"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/protobuf.rs:handler"
    ```

`#[wire(prost)]` is a shorthand for one generator's two paths. The general form names the
functions itself - `#[wire(encode = <path>, decode = <path>)]` - where `encode` is a
`fn(&Self, &mut BytesMut)` returning either nothing or a `Result`, and `decode` a
`fn(&[u8]) -> Result<Self, E>`. Cap'n Proto, FlatBuffers and a hand-rolled frame ride the same
mechanism, with no cargo feature per format: this crate calls what the attribute names and
depends on none of them, while the service depends on the one it uses. A format that fits neither
shape is written the way the Manual tab writes this one: `wire_bytes` and `from_payload` are
public trait methods, so the whole lane is reachable without the `macros` feature at all.

The model type stays visible where it matters. It is what the mount site names, what an `Out`
slot's dictionary lists, and what the generated `AsyncAPI` document reports - all three of which a
pre-encoded byte newtype hides behind a bag of bytes. These services are
[`examples/protobuf.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/protobuf.rs)
and
[`examples/manual/protobuf.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/manual/protobuf.rs).

## Where the decode codec comes from

The decode codec is fixed at compile time. `include` takes no codec argument; it resolves one from
the most specific level you set, from narrowest to widest:

### Per handler

Override a single mounting:

=== "Router"

    <!-- inline-rust: standalone Router-builder fragment; the compiled form is the with_broker tab below (codecs.rs:per_handler), which mounts the same chain via include_router -->
    ```rust
    router.with_codec(CborCodec).include(handle);
    ```

=== "with_broker"

    === "Macros"

        ```rust
        --8<-- "examples/codecs.rs:per_handler"
        ```

    === "Manual"

        ```rust
        --8<-- "examples/manual/codecs.rs:per_handler"
        ```

### Per scope

Set one codec for every handler in a `with_broker` scope:

=== "Macros"

    ```rust
    use ruststream::codec::CborCodec;

    --8<-- "examples/codecs.rs:scope"
    ```

=== "Manual"

    ```rust
    use ruststream::codec::CborCodec;

    --8<-- "examples/manual/codecs.rs:scope"
    ```

### Default

When nothing above names a codec, `include` uses [`DefaultCodec`](#the-default-codec).

## The publish side

Publishers mirror the same rules: `.out(Reply, policy)` encodes replies with the default codec,
and `.out(Reply, policy).codec(codec)` names one - as does `.out(marker, policy).codec(codec)` for
one `Out` slot. Decoding of the incoming request follows the scope
(the scope codec set with `with_broker_codec`, or the router chain's `Router::with_codec`, else
the default), while the reply codec travels on the wiring the chain built - so the request and
reply formats differ freely.

The codec is a property of the mounting, not of the message type: one type decodes as JSON on one
subscription and as CBOR on another, and the mount site is the single place that says which.

## Decode failures

When decoding fails, the failure policy decides what happens to the message; by default it is
dropped (a nack without requeue). The policy is set per subscriber with the
`on_failure(decode = ..)` clause:

=== "Macros"

    ```rust
    use ruststream::subscriber;

    --8<-- "examples/codecs.rs:decode_failure"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/codecs.rs:decode_failure"
    ```

The policy values (`Drop`, `Retry`, `RetryAfter(..)`, `Skip`, `FailFast`), the defaults, and the
retry caveats live in [Failure policy](failure-policy.md). The codec examples above are
[`examples/codecs.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/codecs.rs).

## Custom codecs

A codec is any type implementing the `Codec` trait, so you can supply your own and pass it
anywhere a built-in codec goes. Making it generic over another codec makes it composable: the
inner codec decides the payload format, the wrapper only transforms the bytes around it. The
codec below frames the inner one's output with a two-byte versioned header, the shape a
schema-registry envelope or an encrypting wrapper takes as well.

```rust
--8<-- "examples/custom_codec.rs:codec"
```

Both sides of the wrapper report through `CodecError`. A failure of the inner codec already is
one and travels up with `?`, unchanged. A failure of the wrapper itself becomes
`CodecError::Decode` (or `CodecError::Encode`) carrying your own error type as its source, so the
message names which layer rejected the payload and why: `decode failed: not an envelope: leading
byte 0x7b`.

A custom codec mounts at the same three levels as a built-in one - here all three at once, so
that the scope, the chain and the reply each read their own line:

=== "Macros"

    ```rust
    --8<-- "examples/custom_codec.rs:mount"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/custom_codec.rs:mount"
    ```

## The synchronous boundary

`Codec::encode` and `Codec::decode` are synchronous, which fixes what can live in a codec: what a
constant and the bytes at hand already decide, like the version tag above. An integration that
needs I/O to serialize - resolving a schema id against a registry, fetching a key from a KMS -
does not fit, and wrapping a blocking call in it stalls the delivery task.

Put those on the async edges instead: transcode incoming payloads on the subscription's delivery
path, before the codec sees them, and frame outgoing ones with a
[`PublishLayer`](middleware.md#publish-side-middleware). Both are async and fallible.
[Broker authors](../broker-authors/index.md#middleware-on-the-async-edges) covers the same
boundary from the broker side.

This codec is [`examples/custom_codec.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/custom_codec.rs).
