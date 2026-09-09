# Typed headers

Message headers are an untyped `name -> bytes` map. Typed headers are a struct that declares which
headers a message has and of which types. You can declare it on the subscriber and on the publisher.

## The contract

A header contract is a flat struct with scalar fields: numbers, booleans, strings, raw bytes,
unit-only enums.

```rust
--8<-- "examples/typed_headers.rs:contracts"
```

If a header name is not a valid Rust identifier, you can set it with
`#[serde(rename = "x-task-id")]`. An `Option` field declares an optional header.

A header value is stored as a string: the framework parses `"3"` into a `u32` field and writes it
back the same way.

## Receiving: the `Headers` extractor

`Headers<T>` is an extractor: the runtime parses the delivery's headers into the contract `T` before
the body runs.

=== "Macros"

    ```rust
    --8<-- "examples/typed_headers.rs:handler"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/typed_headers.rs:handler"
    ```

When a required header is missing or a value does not parse, the delivery never reaches the body.
The subscriber's `on_failure(decode = ..)` policy settles it, the same policy that covers a payload
that does not decode (`drop` by default). Before that the framework writes a `WARN` naming the
subscription and the contract type.

`Headers` composes with every other extractor and with a body that deserializes itself.

In a batch the headers stay per-delivery, so the input of a batch handler is `&[Message<H, T>]`.

=== "Macros"

    ```rust
    --8<-- "examples/typed_headers.rs:batch"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/typed_headers.rs:batch"
    ```

An element whose payload or headers do not parse never reaches the handler: the same policy settles
it. A `Headers<..>` parameter does not compile here, and the error names the pair input.

You mount a batch handler like any other: `b.include(bulk)` on a broker scope, `Router::include` on
the router path.

When one subscription receives messages with different sets of headers, you can write your own
[`FromContext`] extractor instead of `Headers`: it reads the discriminator header from the untyped
map ([`HeaderMap::get_str`]) and builds the contract that kind of event calls for. Declare the union
of the shapes on the input type (see the next section).

[`FromContext`]: https://docs.rs/ruststream/latest/ruststream/runtime/trait.FromContext.html
[`HeaderMap::get_str`]: https://docs.rs/ruststream/latest/ruststream/struct.HeaderMap.html#method.get_str

## Declaring a contract on a message type

`#[derive(Outgoing)]` takes `headers = Meta` next to the destination: the contract becomes part of
the type. How the destination is declared is covered in
[publishing](publishing.md#declaring-where-a-message-goes).

=== "Macros"

    ```rust
    --8<-- "examples/typed_headers.rs:messages"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/typed_headers.rs:messages"
    ```

## Publishing: the contract at the call site

An `Out` slot's marker lists the message types the slot may publish:

=== "Macros"

    ```rust
    --8<-- "examples/typed_headers.rs:dictionary"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/typed_headers.rs:dictionary"
    ```

The `Out` parameter's optional third position declares the message set this handler publishes:

- `Out<impl Publisher, Events>` (or an explicit `()`) - unrestricted: any declared message;
- `Out<impl Publisher, Events, (ChunkDone, Progress)>` - an inline list;
- `Out<impl Publisher, Events, ChunkDone>` - one declared type (a `#[derive(Outgoing)]` type
  declares itself);
- `Out<impl Publisher, Events, ConvertSends>` - a `#[derive(OutMessages)]` enum whose variants each
  wrap one model: a named set several handlers can share. The enum is a type-level declaration and
  is never constructed.

The body then publishes through the publish builder (the handler above), and the compiler checks the
whole declaration:

- a `message(..)` of a type outside the declared set does not compile;
- a type declaring `headers = Meta` publishes only through `.message(&value).with_headers(&meta)`;
- the destination comes from the type's own declaration: a fixed name needs nothing at the call
  site, a templated one demands its placeholders;
- the capability position is checked against the policy you name when registering the handler:
  `Out<impl TransactionalPublisher, Events, (ChunkDone, Progress)>` demands a policy that constructs
  a transactional publisher, and the declared publishes run inside that publisher's transaction,
  under the same declaration.

You can wrap a payload the service already holds encoded, or a foreign type that takes no
declaration of its own (`Vec<Frame>`), in a newtype that derives `Outgoing` and
[`Serialized`](subscribers.md#raw-subscribers). It declares its headers like any model and publishes
through the same `out.message(&export)`, byte for byte.

A publisher can set a base of headers of its own, and the contract's fields are written over that
base; see [where the headers come from](publishing.md#where-the-headers-come-from).

## The reply form

A handler with `publish("dest")` needs no extra declaration: the destination is in the attribute,
the headers are in the reply type's contract.

=== "Macros"

    ```rust
    --8<-- "examples/typed_headers.rs:reply"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/typed_headers.rs:reply"
    ```

A `PublishTransform` on the reply publisher sets the reply headers: inside a transform,
[`HeaderMap::insert_typed`] writes a contract value into the map.

[`HeaderMap::insert_typed`]: https://docs.rs/ruststream/latest/ruststream/struct.HeaderMap.html#method.insert_typed

## What the document shows

With the `asyncapi` feature, `build_spec` adds a headers schema to every message in the document:

- for a received message, from the handler's `Headers<T>` parameter, or from the input type's
  `#[message(headers(..))]` contract when the handler extracts the headers by hand;
- for a sent message, from the contract declared on the type itself.

Schemas describe the logical field types: `task_id: integer`.

## Testing

The in-process harness drives the whole path: `with_headers(&meta)` on the injection builder sends a
delivery with a typed contract, and the publish log shows the headers a typed publish produced.

```rust
--8<-- "examples/typed_headers.rs:drive"
```
