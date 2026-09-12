# Codecs and serialization

A codec turns wire bytes into your typed payload and back. It is not tied to the broker. On the
consume side the pipeline is `bytes -> Codec -> typed payload -> handler`. On the publish side the
same pipeline runs in reverse. The codec is fixed where the handler is mounted, so it costs nothing
on the delivery path.

## Built-in codecs

| Codec | Feature | Pulls in | Wire format |
|---|---|---|---|
| `JsonCodec` | `json` *(default)* | serde_json | JSON |
| `MsgpackCodec` | `msgpack` | rmp-serde | MessagePack |
| `CborCodec` | `cbor` | ciborium | CBOR |

Codec features are strictly additive: you can enable as many as you need. A message type only needs
to derive `serde::Deserialize`, and a reply type also `Serialize`.

## The default codec

`DefaultCodec` is an alias selected by the enabled features: `json` when it is enabled, otherwise
`cbor`, otherwise `msgpack`. When nothing names a codec, `include(def)` and a reply chain that
stops at `.out_reply(policy)` take it. Neither of them takes a codec argument.

With no codec feature enabled, there is nothing to encode or decode with. Anything that would need
the default codec becomes a compile error, and the error lists the ways out: enable a codec
feature, name a codec explicitly, or move the message to a byte lane.

The byte lanes never need a codec. A [`Deserialized` input](subscribers.md#raw-subscribers) builds
itself from the delivery's bytes, and a `Serialized` value produces its own. A service that speaks
only its own wire formats runs with no codec feature and loses nothing.

## Binary protocols are not codecs

A codec position means "a value encoded by a codec the mount site chose". A generated Protobuf
message does not fit there: it *is* its encoding, and it owns the byte layout end to end. In a
codec position one mounting would send `Order` as JSON and another as Protobuf, and that is the
confusion the byte lanes prevent. So a binary protocol goes on the lanes, where there is no codec
between the type and its bytes.

Generated code is not edited by hand, but every generator can add attributes to what it emits.
`prost-build` takes `message_attribute`, so the whole recipe is two attributes on every generated
message:

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

The third derive declares where the message is sent, which is what a typed publish and a reply
both read.

Every message in the schema is then already on the lanes, as if it had been written by hand. The
Manual tab is the same message with the derives expanded:

=== "Macros"

    ```rust
    --8<-- "examples/protobuf.rs:message"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/protobuf.rs:message"
    ```

The handler names no codec, on the way in or on the way out:

=== "Macros"

    ```rust
    --8<-- "examples/protobuf.rs:handler"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/protobuf.rs:handler"
    ```

`#[wire(prost)]` is a shorthand for one generator's pair of functions. The general form names the
functions itself: `#[wire(encode = <path>, decode = <path>)]`, where `encode` is a
`fn(&Self, &mut BytesMut)` returning either nothing or a `Result`, and `decode` is a
`fn(&[u8]) -> Result<Self, E>`.

Cap'n Proto, FlatBuffers and a hand-rolled frame use the same mechanism. No cargo feature per
format is needed: this crate calls what the attribute names and depends on none of these formats,
while the service declares the one it uses.

A format that fits neither shape is written the way the Manual tab writes this one: `wire_bytes`
and `from_payload` are public trait methods, so the whole lane is reachable without the `macros`
feature.

The model type stays visible where it matters: the mount site names it, an `Out` slot's dictionary
lists it, and the generated `AsyncAPI` document reports it. A newtype of pre-encoded bytes would
hide the type in all three places. These services are
[`examples/protobuf.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/protobuf.rs)
and
[`examples/manual/protobuf.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/manual/protobuf.rs).

## Where the decode codec comes from

The decode codec is fixed at compile time. `include` takes no codec argument. The codec comes from
the most specific level where you named one, from narrowest to widest:

### Per handler

Override the codec of a single mounting:

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

When no level above names a codec, `include` uses [`DefaultCodec`](#the-default-codec).

## The publish side

Publishers follow the same rules: `.out_reply(policy)` encodes replies with the default codec, and
`.out_reply(policy).codec(codec)` names one explicitly, as does `.out(marker, policy).codec(codec)`
for a single `Out` slot.

The incoming request decodes with the scope codec set through `with_broker_codec`, or with the
router chain's codec from `Router::with_codec`, otherwise with the default codec. The mounting
chain sets the reply codec, so the request and reply formats differ freely.

The codec is a property of the mounting, not of the message type: one type decodes as JSON on one
subscription and as CBOR on another, and the mount site is the single place that says which.

## Decode failures

The failure policy decides what happens to a message that could not be decoded. By default the
message is dropped: a nack without requeue. You can set the policy per subscriber with
`on_failure(decode = ..)`:

=== "Macros"

    ```rust
    use ruststream::subscriber;

    --8<-- "examples/codecs.rs:decode_failure"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/codecs.rs:decode_failure"
    ```

The policy values (`Drop`, `Retry`, `RetryAfter(..)`, `Skip`, `FailFast`), the defaults and the
retry caveats are described in [Failure policy](failure-policy.md). The codec examples above come
from [`examples/codecs.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/codecs.rs).

## Custom codecs

A codec is any type that implements the `Codec` trait. You can pass your own codec anywhere a
built-in one is accepted.

A codec generic over another codec is composable: the inner codec decides the payload format, and
the wrapper only transforms the bytes around it. The codec below adds a two-byte versioned header
to the inner codec's bytes. A schema-registry envelope and an encrypting wrapper have the same
shape.

```rust
--8<-- "examples/custom_codec.rs:codec"
```

Both sides of the wrapper return `CodecError`. An error from the inner codec already has that type,
and `?` passes it up unchanged. An error from the wrapper itself becomes `CodecError::Decode` (or
`CodecError::Encode`) with your own error type as its source. The error message then names the
layer that rejected the payload and the reason:
`decode failed: not an envelope: leading byte 0x7b`.

A custom codec mounts at the same three levels as a built-in one, and the example below sets all
three at once:

=== "Macros"

    ```rust
    --8<-- "examples/custom_codec.rs:mount"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/custom_codec.rs:mount"
    ```

## The synchronous boundary

`Codec::encode` and `Codec::decode` are synchronous, and that sets the boundary of what fits in a
codec: only what a constant and the bytes at hand already decide, like the version tag above. An
integration that needs I/O to serialize (resolving a schema id against a registry, fetching a key
from a KMS) does not fit. A blocking call inside a codec stalls the delivery task.

Put such integrations around the codec, on the async edges: transcode incoming payloads on the
subscription's delivery path, before the codec sees them, and frame outgoing ones with a
[`PublishLayer`](middleware.md#publish-side-middleware). Both edges are async and can return an
error. The [Broker authors](../broker-authors/index.md#middleware-on-the-async-edges) page describes
the same boundary from the broker side.

This codec is [`examples/custom_codec.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/custom_codec.rs).
