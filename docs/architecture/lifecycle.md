---
title: Lifecycle
description: "A broker's states are types: a configuration, a connection and a closed connection, each reached by a call that consumes the one before."
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "The broker's states as types, and the app steps that move it from one to the next",
  "text": "The diagram as text",
  "brokerSide": "Broker",
  "appSide": "App",
  "unconnected": ["B::new(config)", "sync, no I/O"],
  "connected": ["B::Connected", "subscribe, publish"],
  "closed": ["Closed", "teardown report"],
  "builder": ["The builder", "registers brokers", "and handlers"],
  "running": ["RunningApp", "connected, loops", "running"],
  "stopped": ["Stopped", "loops drained,", "brokers closed"],
  "connect": "connect(self)",
  "shutdown": "shutdown(self)",
  "start": "start()",
  "stop": "shutdown()",
  "afterShutdown": ["A publisher that outlives its broker's connection", "answers with an error, never with a silent success"]
};

# Lifecycle

A broker's states are types: a configuration, a connection and a closed connection, each reached by
a call that consumes the one before.

<Diagram name="lifecycle" words={words} />

A broker is built without I/O, so the [`#[ruststream::app]`](../components/application/app-macro.md)
builder stays synchronous whatever the broker. Every network call waits for `start()`.

Subscribing and publishing are methods of the connected form only. Code that holds a broker
before `connect` has nothing to subscribe with, and `shutdown` consumes the connection it closes,
so these mistakes do not compile.

[`start()`](../components/application/running/start.md) connects the brokers in the order they
were registered, opens every subscription against its connected broker and runs the
`after_startup` hooks. A failure on the way closes what is already open, in reverse order.
[`shutdown()`](../components/application/running/shutdown.md) stops the loops, drains them and
closes the brokers in reverse registration order. Where each
[hook](../components/application/lifecycle.md) runs is drawn on [Async](async.md#shutdown).

A publisher handed out earlier can outlive the connection. Using it after shutdown returns an
error. The conformance suite holds every broker crate to this rule.
