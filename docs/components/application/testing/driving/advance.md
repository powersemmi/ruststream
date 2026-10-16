---
sidebar_position: 7
title: advance
description: "TestApp::advance lets a delay pass, fires the delayed redeliveries now due, and drives their reaction to a standstill."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `advance`

`TestApp::advance` lets a delay pass, fires the delayed redeliveries now due, and drives their reaction to a standstill.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_driving.rs#advance"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_driving.rs#advance"
```

</TabItem>
</Tabs>

## How it works

A handler that answers `retry_after(delay)` settles at once, and the publish returns with the delivery pending.
`advance(delay)` then delivers the message again, by a broker's native delayed redelivery or by the runtime's deferred copy.
In process it moves the paused clock, so the test starts paused with `#[tokio::test(start_paused = true)]`.
On a running clock the in-process `advance` panics.
In live mode it lets the time pass for real, because the delay is the broker's own timer.
A copy the runtime publishes for a zero delay arrives in the same reaction, without `advance`.

## See also

- [`settle`](./settle.md): drive a reaction without moving the clock.
- [`start_live`](../starting-and-stopping/start-live.md): the real clock of a live test.
- [`TestApp::advance`](api:ruststream::testing::TestApp::advance), [`HandlerOutcome::retry_after`](api:ruststream::runtime::HandlerOutcome::retry_after)
