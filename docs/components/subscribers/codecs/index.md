---
description: "A codec turns a payload into a message type and back; it is a compile-time type, chosen at the mount site, and JSON is the default."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Codecs

A codec turns a payload into a message type and back; it is a compile-time type, chosen at the mount site, and JSON is the default.

A [`Codec`](api:ruststream::codec::Codec) decodes the bytes of a delivery into the type the handler reads, and encodes what the handler publishes.
It belongs to the mounting, not to the message type, so one type can arrive in JSON on one subscription and in CBOR on another.
The codec is resolved when the app is built, so choosing one costs nothing per message.

A handler decodes with the codec of the most specific level that names one:

| Level | Macros | Manual |
|---|---|---|
| registration | `Router::new().with_codec(codec).include(def)` | `subscriber(..).codec(codec)` |
| scope | `with_broker_codec(broker, codec, \|b\| ..)` | the same |
| default | [`DefaultCodec`](api:ruststream::codec::DefaultCodec) | the same |

[`with_broker_codec`](api:ruststream::runtime::RustStream::with_broker_codec) names the codec for every handler of a broker scope.
A [router](../router.md) names one for the handlers it mounts with [`Router::with_codec`](api:ruststream::runtime::Router::with_codec).
`DefaultCodec` is the first enabled of the `json`, `cbor` and `msgpack` features, in that order.
With no codec feature, a mounting that needs the default does not compile, and the error lists the ways out.

The publish side names its codec at its own position.
A reply takes the default codec unless `.out_reply(policy).codec(codec)` names another, so a request and its reply may use different formats.
An `Out` slot takes `.out(marker, policy).codec(codec)`; without it a slot encodes with the codec of the router or scope it is mounted in, not the default codec a reply falls back to. A single publish takes `.with_codec(codec)` on the builder.

A payload that does not decode never reaches the body.
The subscriber's `on_failure(decode = ..)` [policy](../failure-policy.md) settles it, a drop by default.
A type that carries its own bytes skips the codec: [Protobuf](./protobuf.md) and the [byte lanes](./bytes.md) need no codec feature.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/codecs.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/codecs.rs#app"
```

</TabItem>
</Tabs>
