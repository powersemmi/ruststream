# Codecs and serialization

A codec turns wire bytes into your typed payload and back. It is a separate seam from the broker:
the pipeline on the consume side is `bytes -> Codec -> typed payload -> handler`, and the publish
side runs it in reverse. Codecs are compile-time types, so decoding has no dynamic dispatch.

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
`msgpack`. It is what `include(def)` and `TypedPublisher::new(publisher)` use when nothing names a
codec, which is why neither takes a codec argument. It exists only when at least one codec feature
is enabled; with no codec features, only the explicit-codec methods are available.

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

    ```rust
    --8<-- "examples/codecs.rs:per_handler"
    ```

### Per scope

Set one codec for every handler in a `with_broker` scope:

```rust
use ruststream::codec::CborCodec;

--8<-- "examples/codecs.rs:scope"
```

### Default

When nothing above names a codec, `include` uses [`DefaultCodec`](#the-default-codec).

## The publish side

Publishers mirror the same rules: `TypedPublisher::new(publisher)` encodes replies with the default
codec, and `TypedPublisher::with_codec(publisher, codec)` names one. `include_publishing(def,
publisher)` reuses the publisher's codec to decode the incoming request, so one mounting names one
codec. The request and reply formats can still differ: set the decode codec on the scope
(`with_broker_codec`) or on the router chain (`Router::with_codec`) and keep the reply codec on the
`TypedPublisher`.

There is no per-message-type codec (no associated codec on a message trait): the codec is a
property of the mounting, not of the type.

## Decode failures

When decoding fails, the message is dropped by default (a nack without requeue). The policy is
configurable on the typed adapter when you build handlers by hand: `typed(codec, handler)` returns
a `Typed` wrapper whose `on_decode_failure` accepts a `FailurePolicy` (`Drop`, `Retry`,
`RetryAfter(..)`, `Skip`, or `FailFast`). On a macro handler the same policy is set with the
`on_failure(decode = ..)` clause (see the failure-policy guide).

```rust
use ruststream::runtime::{FailurePolicy, typed};

// inside with_broker(...):
--8<-- "examples/codecs.rs:decode_failure"
```

Retry with care: a payload that can never decode will redeliver forever unless the broker has a
dead-letter or max-deliveries policy. The codec examples above are
[`examples/codecs.rs`](https://github.com/powersemmi/ruststream/blob/main/examples/codecs.rs).

## Custom codecs

A codec is any type implementing the `Codec` trait, so you can supply your own (a schema-registry
envelope, an encrypting wrapper) and pass it anywhere a built-in codec goes:
`with_broker_codec`, `Router::with_codec`, or `TypedPublisher::with_codec`.
