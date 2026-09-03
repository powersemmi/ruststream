# Typed headers

Message headers travel as an untyped `name -> bytes` map, `HeaderMap`. When an application
carries a real contract in them (ids, sequence numbers, totals), one struct can declare that
contract and drive all three surfaces at once: runtime extraction on the consume side, the
publish builder on the produce side, and the headers schema in the generated AsyncAPI document.

## The contract

A header contract is a flat struct: each field names a header, values are scalars (numbers,
booleans, strings, raw bytes, unit-only enums) or `Option`s of them. On the wire every value is
string-encoded - the framework parses `"3"` into a `u32` field and writes it back the same way -
while schemas keep describing the logical types.

```rust
--8<-- "examples/typed_headers.rs:contracts"
```

Field names are the wire names; use `#[serde(rename = "x-task-id")]` for names that are not
Rust identifiers. An `Option` field is `None` when the header is absent; a missing non-`Option`
header is a contract violation.

## Receiving: the `Headers` extractor

`Headers<T>` is an extractor parameter: the runtime parses the delivery headers into `T`
before the body runs, so the handler starts from validated, typed values. A violation (missing
header, unparsable value) never reaches the body - the delivery settles by the subscriber's
`on_failure(decode = ..)` policy, the same one that covers a payload that does not decode
(drop by default), after a `WARN` naming the subscription and the contract type.

=== "Macros"

    ```rust
    --8<-- "examples/typed_headers.rs:handler"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/typed_headers.rs:handler"
    ```

`Headers` composes with a byte body (`&[u8]`, typed headers) and with every other extractor.

On a batch handler the headers stay per-delivery, so the parameter takes one contract per
element: `Headers<Vec<T>>`. `meta[i]` belongs to `chunks[i]`, and the two line up by
construction - an element whose payload or headers fail to materialize is settled by the same
`on_failure(decode = ..)` policy and never reaches the handler, exactly as on the single-message
path. The bare `Headers<T>` is rejected there, naming the vector form.

=== "Macros"

    ```rust
    --8<-- "examples/typed_headers.rs:batch"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/typed_headers.rs:batch"
    ```

Mounting reads the same as every other form and on both surfaces: `b.include(bulk)` on a broker
scope, `Router::include` on the router path. The contract type travels in the route, and the
definition's own form token is what picks that route.

When one channel carries messages whose headers differ per event kind, keep the standard
extractor out of it and write your own [`FromContext`] extractor: read the discriminator
header off the untyped map ([`HeaderMap::get_str`]), then build the contract that kind calls
for. Declare the union of shapes on the input type (see the next section) so the document
still shows the full contract.

[`FromContext`]: https://docs.rs/ruststream/latest/ruststream/runtime/trait.FromContext.html
[`HeaderMap::get_str`]: https://docs.rs/ruststream/latest/ruststream/struct.HeaderMap.html#method.get_str

## Declaring a contract on a message type

`#[derive(Outgoing)]` accepts `headers = Meta` next to the destination: the contract becomes
part of the type. The publish builder then demands exactly those headers, and the AsyncAPI
document renders the schema next to the payload wherever the type appears. See
[publishing](publishing.md#declaring-where-a-message-goes) for the destination half.

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
- `Out<impl Publisher, Events, ConvertSends>` - a `#[derive(OutMessages)]` enum whose variants
  each wrap one model: a reusable, named set (the enum is a type-level declaration and is
  never constructed).

The body then publishes through the builder (the handler above), and the compiler enforces the
whole declaration:

- a `message(..)` of a type outside the declared set does not compile - the handler publishes
  what it declared, nothing else;
- a type declaring `headers = Meta` publishes only through
  `.message(&value).with_headers(&meta)` - forgetting the headers, or passing the wrong headers
  type, does not compile;
- the destination comes from the type's own declaration, so a fixed name needs nothing at the
  call site and a templated one demands its placeholders;
- the capability position is checked against the include-site policy statically, as always:
  `Out<impl TransactionalPublisher, Events, (ChunkDone, Progress)>` demands a policy whose
  live publisher is transactional, and the declared publishes ride inside its transactions.

A payload the service already holds encoded, or a foreign type that cannot carry a declaration
(a bare `Vec<Frame>`), goes out through the byte entry point:
`out.raw(&bytes).to(dest).publish()`. It takes the same headers positions and no codec; wrap
the payload in a `#[derive(Outgoing)]` newtype when it deserves a declaration of its own.

The contract fills that position once. What the publisher itself contributes travels
underneath: a handle carrying an argument for every message it sends exposes it as a base, and
the contract's fields serialize over that base field by field - see
[where the headers come from](publishing.md#where-the-headers-come-from).

## The reply form

A `publish("dest")` handler needs no extra declaration: the reply type's own contract feeds
the document, and the destination is already in the attribute.

=== "Macros"

    ```rust
    --8<-- "examples/typed_headers.rs:reply"
    ```

=== "Manual"

    ```rust
    --8<-- "examples/manual/typed_headers.rs:reply"
    ```

At runtime, reply headers stay where they were: a `PublishTransform` on the reply publisher
sets them, and [`HeaderMap::insert_typed`] serializes a contract value into the map from inside
a transform.

[`HeaderMap::insert_typed`]: https://docs.rs/ruststream/latest/ruststream/struct.HeaderMap.html#method.insert_typed

## What the document shows

With the `asyncapi` feature, `build_spec` renders:

- the headers schema of every receive message - from the handler's `Headers<T>` parameter,
  or from the input type's `#[message(headers(..))]` contract when the handler extracts by
  hand;
- a `send` operation per declared outgoing message - the reply of every `publish(..)` form and
  every message type a slot declares - each with its payload and headers schemas.

Schemas describe the logical field types (`task_id: integer`), while wire values are
string-encoded headers.

## Testing

The in-process harness drives the whole path: `with_headers(&meta)` on the injection builder sends
a delivery carrying a typed contract, and the publish log shows the headers a typed publish
produced.

```rust
--8<-- "examples/typed_headers.rs:drive"
```
