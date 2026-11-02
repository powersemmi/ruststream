# A device service on the embassy runtime

A device service is written with the same handlers as a server: `#[subscriber]`, `.then`,
`.error`, `Out` slots and typed headers. The app is built by
`#[ruststream::app(runtime = embassy)]` as an `EmbeddedApp` and an embassy task, `app::task`,
that the board's `main` spawns.

## Pick the form of the build

The embassy runtime builds in two forms, and one build holds one of them. Features add up, so a
crate of the graph that turns `alloc` on turns it on for the whole build; `tokio`, `json`, `cbor`
and `msgpack` turn it on.

- A device with a heap enables `embassy`, `macros`, a codec feature (`json` or `cbor`) and
  `memory`. Headers are `HeaderMap`, continuations are boxed, codecs are `Codec`.
- A device with no heap enables `embassy`, `memory`, `macros`, a slice codec (`postcard` or
  `json-core`) and a logger (`log` or `defmt`) with `default-features = false`. The build
  allocates nothing at startup or per message, and builds for `thumbv6m-none-eabi`.

```toml
ruststream = { version = "0.8", default-features = false, features = ["embassy", "memory", "macros", "postcard", "defmt"] }
```

## What the build without `alloc` holds

- Headers are `FixedHeaders` of 8 entries and 256 bytes; `insert` returns an error when full.
- The codec is a `SliceCodec`, named at the mount: `b.with_codec(PostcardCodec)` for handlers and
  `.out_reply(policy).codec(PostcardCodec)` for a reply. A type that is its own encoding
  (`Deserialized`, `Serialized`) needs no codec.
- A continuation (`and_after`, `.then`, `.error`) lives in a room of 128 bytes; a larger capture
  is a build error, an overflow in `ROOM - size_of::<Work>()` that prints both sizes.
- `.error` borrows the failures: `async move |failures: &Failures| ..`.
- `ctx.after(..)`, `ctx.after_ack(..)` and `ctx.after_settle(..)` return a `Result`: a delivery
  holds four registrations and a fifth is refused, never dropped.
- The local bus is a `LocalMemoryBus<MESSAGES, PAYLOAD, SUBSCRIPTIONS>` declared as a `static`;
  the app function receives `LocalMemoryBroker::new(&BUS)`. The binary supplies
  `critical-section`.
- A batch is `.batch::<N>()` with N from 1 to 16, of messages that deserialize themselves.
- State is borrowed, `async |state: &State| ..`, and a state that holds a heap type (`Rc`) frees
  it inside the app's poll; a `static` counter does not.

## Capacities a mount names

`.retry_capacity::<CAP>()` (delayed retries waiting at once), `.encode_capacity::<N>()` (bytes a
reply is encoded into, 256 by default, at most 256 for a publisher that keeps the payload),
`.pool::<N>()` and `.keyed::<N>()`, `.batch::<N>()`. A count of zero or past the limit does not
compile. A reply that does not fit is `CodecFailure::BufferFull`, and a retry that finds the store
full is the "no free record" error; both reach the handler's `.error`.

## Diagnostics

`tracing` needs the heap, so the build without `alloc` reports through `log` or `defmt`, whichever
feature is on (both may be). Each line names the subscription and the message type.

## Test it

`ruststream::testing::embedded::TestApp` follows the form of the build. Install
`assert_no_alloc::AllocDisabler` as the test binary's global allocator and the harness arms it
around every poll of the app, so a test fails on an allocation of the framework. Drive time with
`tb.advance(..)` on the mock clock. See the `ruststream-testing` skill.
