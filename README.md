# ruststream-rs

Pure Rust core of the RustStream messaging framework: traits, codecs, router runtime, conformance harness for broker authors.

This repository holds the Rust-only part of the project. No PyO3, no Python bindings.

| Crate | Purpose |
|---|---|
| `ruststream-core` | `Broker`, `Subscriber`, `Publisher`, `IncomingMessage` traits; capability traits; message types; headers; errors. Zero broker dependencies. |
| `ruststream-codec` | Serde codecs (json/msgpack/cbor) gated by cargo features. |
| `ruststream-runtime` | Router, middleware, lifecycle, dispatch. |
| `ruststream-conformance` | Generic in-memory `MemoryBroker` (reference impl) plus contract tests for broker authors. |

PyO3 helper crates for Python bindings live in the [`ruststream-py`](https://github.com/powersemmi/ruststream-py) repository. Concrete brokers (NATS, Kafka, RabbitMQ, Redis, MQTT) live in their own repositories and pull `ruststream-core` from crates.io.

## Quick start

```bash
just check
just test
```

## License

Apache-2.0.
