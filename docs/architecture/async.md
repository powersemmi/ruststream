---
title: Async
description: "The tasks a running service holds, the way one delivery passes through them, and the order a shutdown stops them in."
---

import {Diagram} from '@site/src/components/Diagrams';

export const topology = {
  "title": "The tasks of each dispatch form on the app runtime, and the dedicated threads beside it",
  "text": "The diagram as text",
  "broker": "Subscription streams of the connected brokers",
  "runtime": "App runtime: Tokio, multi-thread by default",
  "loop": ["default", "one task runs", "every step"],
  "pool": ["workers(n)", "one loop task", "hands them out"],
  "feeder": ["threads(n)", "one feeder task", "fills the rings"],
  "workerTasks": ["n worker tasks", "an inbox of one"],
  "rings": ["n rings", "of eight slots"],
  "tracker": ["Tracked tasks: and_after,", "hooks, retry_after timers"],
  "osThreads": ["n OS threads", "a Tokio runtime", "on each"],
  "poll": "poll_next",
  "inbox": "send",
  "push": "push",
  "pop": "pop",
  "spawn": "spawn"
};

export const timeline = {
  "title": "One delivery, step by step, in the column of the task that runs each step",
  "text": "The diagram as text",
  "loopLane": "Loop task",
  "handlerLane": "Handling task",
  "laterLane": "Later, tracked",
  "pollStep": ["poll the", "stream"],
  "context": ["build the", "context"],
  "layers": ["consume", "layers"],
  "decode": "decode",
  "body": ["handler", "body"],
  "reply": ["publish", "the reply"],
  "settle": "settle",
  "next": ["poll the", "next one"],
  "nextTurn": "next turn",
  "after": ["continuations", "and timers"],
  "spawn": "spawn"
};

export const shutdown = {
  "title": "What a graceful shutdown does, in order",
  "text": "The diagram as text",
  "stop": ["A signal, a fail-fast", "or RunningApp::shutdown"],
  "onShutdown": ["on_shutdown hooks run", "while the brokers are connected"],
  "cancel": ["The loops stop", "taking new deliveries"],
  "finish": ["Deliveries in hand", "are handled and settled"],
  "drain": ["Tracked continuations", "and retry timers finish"],
  "brokers": ["Brokers shut down", "in reverse registration order"],
  "afterShutdown": ["after_shutdown hooks run", "with the brokers closed"]
};

# Async

The tasks a running service holds, the way one delivery passes through them, and the order a
shutdown stops them in.

<Diagram name="asyncTopology" words={topology} />

The service runs on Tokio. [`#[ruststream::app]`](../components/application/app-macro.md) builds a
multi-thread runtime. `flavor = "current_thread"` picks a single-threaded one, and
`worker_threads = n` sets the number of threads.

A subscriber is a `Stream` of deliveries, and back-pressure comes from polling it. The default
loop polls for the next delivery once the previous one is settled.
[`workers(n)`](../components/subscribers/dispatch/workers.md) keeps at most `n` deliveries in
flight. `workers(n, by_key)` sends every delivery of one key to the same worker, so one key keeps
its order. [`threads(n)`](../components/subscribers/dispatch/threads.md) reads ahead at most
`n × 9` deliveries: one in the hands of each thread and eight in its ring.

A dedicated thread runs the whole delivery, from decode to settlement, on its own current-thread
runtime. Work the handler hands to [`MainRuntime`](api:ruststream::runtime::MainRuntime), taken as
[`Ctx<MainRuntime>`](../components/subscribers/context/ctx.md), runs back on the app runtime.

## One delivery

<Diagram name="asyncTimeline" words={timeline} />

The handling task is the loop task itself by default, a worker task under `workers(n)`, and a
dedicated thread under `threads(n)`. An [`and_after`](../components/subscribers/messages/and-after.md)
continuation, a post-settle hook and a [`retry_after`](../components/subscribers/messages/retry-after.md)
timer run after the settlement, as tasks the app tracks. They run at most once: the delivery is
already settled.

## Shutdown

<Diagram name="asyncShutdown" words={shutdown} />

A [shutdown timeout](../components/application/running/shutdown.md) bounds the wait. When it
expires, loops still running are aborted, and tracked tasks still pending are left to finish on
their own. Without a timeout, a shutdown waits for all of them. A task spawned with plain
`tokio::spawn` on a dedicated thread is not tracked and ends with its thread.
