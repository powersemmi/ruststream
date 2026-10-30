---
title: Design
description: "The core knows no broker: a broker crate implements its traits and owns every broker-specific setting."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The service, the broker-agnostic core, and broker crates that plug into its traits",
  "text": "The diagram as text",
  "service": "Your service",
  "handlers": ["#[subscriber]", "handlers"],
  "app": ["#[ruststream::app]", "builder"],
  "core": "ruststream",
  "runtime": ["runtime: routing, dispatch,", "settlement, publishing"],
  "codecs": ["codecs: JSON, CBOR,", "MessagePack, or none", "chosen at compile time"],
  "modules": ["features: memory broker,", "AsyncAPI, metrics,", "OpenTelemetry, testing"],
  "traits": ["Broker, Subscriber,", "Publisher, sources,", "policies"],
  "capabilities": ["capabilities: batches,", "transactions, request", "and reply, seeking"],
  "crates": "Broker crates",
  "nats": ["ruststream-nats", "subjects, streams,", "policies, options,", "mount steps"],
  "kafka": ["ruststream-rdkafka", "topics, groups,", "policies, options,", "mount steps"],
  "mounted": "mounted on",
  "implements": "implements",
  "wraps": "wraps"
};

# Design

The core knows no broker: a broker crate implements its traits and owns every broker-specific
setting.

<Diagram name="design" words={words} />

The core depends on no broker client. Its required traits are the lifecycle of a connection, a
subscriber that is a `Stream` of deliveries, and a publisher. Batches, transactions, request and
reply, seeking and values a delivery already holds are capability traits a broker implements only
when its client has them.

A broker crate owns its vocabulary. The descriptor of a subscription form, the policy of a
publisher, the per-message settings a publish carries and the mount steps that read the chain's
settings all live there. The core has no broker-specific setting and no default for one.

The codec is a type: [the codec](../components/subscribers/codecs/index.md) a mounting names, or
the default one the enabled feature selects. A service without a codec feature has no default, and
a mounting that needs one does not compile.

The traits, the runtime and each optional layer are crates of their own, and `ruststream`
re-exports them under one set of paths. The compiler holds the layering: the contract crate,
`ruststream-core`, depends on no other crate of the framework. It builds with `#![no_std]` and
needs only an allocator, so a device can depend on it; `ruststream` turns on its `std` feature.
The delivery body (decoding, middleware, the handler, the reply, the settlement) lives in
`ruststream-runtime`, which names no executor. It is generic over a runtime's clock and over the
wait on the app's shutdown. `ruststream-runtime-tokio` runs it on tokio and owns what is tokio's:
the app's lifecycle, spawning, the worker pools, the dedicated threads and the signals. Service
code that waits calls `ruststream::time::sleep`, which sleeps on the app's clock, so the test
harness moves it with every other timer.
A cargo feature of `ruststream`
turns an optional layer on: the [memory broker](../components/memory/index.md), AsyncAPI,
metrics, logging, OpenTelemetry, [testing](../components/application/testing/index.md) and the
command line. Features only add, and none of them changes what another one does.
