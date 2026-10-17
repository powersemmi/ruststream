---
sidebar_position: 4
title: Bindings
description: "Bindings carry what one protocol says about a server, a channel, an operation or a message, in that protocol's AsyncAPI vocabulary."
---

# Bindings

Bindings carry what one protocol says about a server, a channel, an operation or a message, in that protocol's AsyncAPI vocabulary.

A [`Binding`](api:ruststream::asyncapi::Binding) is one protocol's object.
[`Binding::new`](api:ruststream::asyncapi::Binding::new) takes the protocol key, the binding's version and a `serde` body, and writes `bindingVersion` into the body itself.
A key outside the specification's list fails construction with a [`BindingError`](api:ruststream::asyncapi::BindingError), at startup rather than in a published file.
[`Binding::extension`](api:ruststream::asyncapi::Binding::extension) carries an `x-` object for a transport the specification has no binding for.
[`Bindings`](api:ruststream::asyncapi::Bindings) holds one body per protocol, and an empty set leaves the object out of the document.
Broker crates fill most bindings: a server through `DescribeServer`, a subscription through its descriptor's [`channel_bindings`](api:ruststream::SubscriptionSource::channel_bindings) and its siblings, a publish position through its [`PublishPolicy`](api:ruststream::PublishPolicy).
A service attaches a server binding itself with [`ServerSpec::bindings`](api:ruststream::ServerSpec::bindings).
The example runs with `cargo test --example asyncapi_servers --features macros,memory,asyncapi`.

## Example

{/* single-form: a binding is a plain value, the same with and without the macros */}
```rust reference="examples/asyncapi_servers.rs#bindings"
```
