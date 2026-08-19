# Typed headers

Message headers travel as an untyped `name -> bytes` map. When an application carries a real
contract in them (ids, sequence numbers, totals), one struct can declare that contract and
drive all three surfaces at once: runtime extraction on the consume side, the typed publish
path on the produce side, and the headers schema in the generated AsyncAPI document.

## The contract

A header contract is a flat struct: each field names a header, values are scalars (numbers,
booleans, strings, raw bytes, unit-only enums) or `Option`s of them. On the wire every value is
string-encoded - the framework parses `"3"` into a `u32` field and writes it back the same way -
while schemas keep describing the logical types, which is the AsyncAPI convention for header
documentation.

--8<-- "examples/typed_headers.rs:contracts"

Field names are the wire names; use `#[serde(rename = "x-task-id")]` for names that are not
Rust identifiers. An `Option` field is `None` when the header is absent; a missing non-`Option`
header is a contract violation.

## Receiving: the `FromHeaders` extractor

`FromHeaders<T>` is an extractor parameter: the runtime parses the delivery headers into `T`
before the body runs, so the handler starts from validated, typed values. A violation (missing
header, unparsable value) never reaches the body - the delivery settles by the subscriber's
`on_failure(decode = ..)` policy, the same one that covers a payload that does not decode
(drop by default), after a `WARN` naming the subscription and the contract type.

--8<-- "examples/typed_headers.rs:handler"

`FromHeaders` composes with a byte body (`&[u8]`, typed headers) and with every other extractor.

On a batch handler the headers stay per-delivery, so the parameter takes one contract per
element: `FromHeaders<Vec<T>>`. `meta[i]` belongs to `chunks[i]`, and the two line up by
construction - an element whose payload or headers fail to materialize is settled by the same
`on_failure(decode = ..)` policy and never reaches the handler, exactly as on the single-message
path. The bare `FromHeaders<T>` is rejected there, naming the vector form.

--8<-- "examples/typed_headers.rs:batch"

Mounting reads the same as every other form and on both surfaces: `b.include(bulk)` on a broker
scope, `Router::include` on the router path. The contract type travels in the route, and the
definition's own form token is what picks that route.

When one channel carries messages whose headers differ per event kind, keep the standard
extractor out of it and write your own [`FromContext`] extractor: read the discriminator
header, then parse the matching contract with [`Headers::to_typed`] - the same machinery
`FromHeaders` uses. Declare the union of shapes on the input type (see the next section) so
the document still shows the full contract.

[`FromContext`]: https://docs.rs/ruststream/latest/ruststream/runtime/trait.FromContext.html
[`Headers::to_typed`]: https://docs.rs/ruststream/latest/ruststream/struct.Headers.html#method.to_typed

## Declaring a contract on a message type

`#[derive(Message)]` accepts `#[message(headers(Meta))]`: the contract becomes part of the
type. The typed publish path then demands exactly those headers, and the AsyncAPI document
renders the schema next to the payload wherever the type appears.

--8<-- "examples/typed_headers.rs:messages"

## Publishing: the slot dictionary

An `Out` slot's marker can declare what it publishes and where, with
`#[publishes(Type = "channel", ..)]`:

--8<-- "examples/typed_headers.rs:dictionary"

The `Out` parameter's optional third position declares the message set the handler publishes:

- `Out<impl Publisher, Events>` (or an explicit `()`) - unrestricted: `publish_typed` accepts
  any type in the marker's dictionary;
- `Out<impl Publisher, Events, (ChunkDone, Progress)>` - an inline list;
- `Out<impl Publisher, Events, ChunkDone>` - one declared type (a `#[derive(Message)]` type
  declares itself);
- `Out<impl Publisher, Events, ConvertSends>` - a `#[derive(OutMessages)]` enum whose variants
  each wrap one model: a reusable, named set (the enum is a type-level declaration and is
  never constructed).

The body then publishes by value alone: the destination comes from the dictionary, the payload
encodes with the include site's scope codec, and the compiler enforces the whole declaration.

- a declared message type outside the marker's dictionary does not compile (the error names
  the type and the slot);
- a `publish_typed` of a type outside the declared set does not compile - the handler
  publishes what it declared, nothing else;
- a type declaring `#[message(headers(..))]` publishes only through
  `.with_headers(&meta).publish_typed(&value)` - forgetting the headers, or passing the wrong
  headers type, does not compile;
- the capability position is checked against the include-site policy statically, as always:
  `Out<impl TransactionalPublisher, Events, (ChunkDone, Progress)>` demands a policy whose
  live publisher is transactional, and the declared publishes ride inside its transactions;
- several types may share one channel; one type maps to one channel per slot.

A bare collection works as a model - `#[publishes(Vec<Frame> = "chunks.frames")]`, declared in
a list or a set enum, published with `publish_typed(&frames)`; its header contract is none by
definition (wrap it in a `#[derive(Message)]` newtype to declare one).

The typed path needs a named marker (the dictionary lives on it), and destinations are fixed
by the declaration. The value derefs to the slot's publisher, so the declared capability's
whole surface stays reachable - including the byte-level
`out.publish(OutgoingMessage::new(dest, ..))` for a destination computed per message, which is
inherently undocumentable in a static AsyncAPI document.

## The reply form

A `publish("dest")` handler needs no extra declaration: the reply type's own contract feeds
the document, and the destination is already in the attribute.

--8<-- "examples/typed_headers.rs:reply"

At runtime, reply headers stay where they were: a `PublishTransform` on the reply publisher
sets them, and [`Headers::insert_typed`] serializes a contract value into the map from inside
a transform (or anywhere an `OutgoingMessage` is built).

[`Headers::insert_typed`]: https://docs.rs/ruststream/latest/ruststream/struct.Headers.html#method.insert_typed

## What the document shows

With the `asyncapi` feature, `build_spec` renders:

- the headers schema of every receive message - from the handler's `FromHeaders<T>` parameter,
  or from the input type's `#[message(headers(..))]` contract when the handler extracts by
  hand;
- a `send` operation per declared outgoing message - the reply of every `publish(..)` form and
  every entry of every slot dictionary - each with its payload and headers schemas.

Schemas describe the logical field types (`task_id: integer`), while wire values are
string-encoded headers; that convention matches how header contracts are documented across the
AsyncAPI ecosystem, which deliberately leaves value encoding to the protocol.

## Testing

The in-process harness drives the whole path: `publish_with_headers` injects a delivery with a
typed contract, and the publish log shows the headers a typed publish produced.

--8<-- "examples/typed_headers.rs:drive"
