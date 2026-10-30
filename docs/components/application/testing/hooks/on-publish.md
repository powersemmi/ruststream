---
sidebar_position: 2
title: on_publish
description: "on_publish runs an async closure each time a message is published to a destination, handing it that destination's assertions as they stood at that publish."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# on_publish

[`on_publish(hook)`](api:ruststream::testing::PublishedAssertions::on_publish) runs an async closure each time a message is published to a destination, handing it that destination's assertions as they stood at that publish.

It sees the test's own publishes and everything a publisher wired by the runtime sends: replies, `Out` slot publishes, retry copies and dead letters.
On the record of a slot, [`out::<M>()`](../records/out.md), it runs for each publish through that slot.
The hook gets [`PublishedAssertions`](api:ruststream::testing::PublishedAssertions) with the new message already in the record and the later ones left out, so [`decoded`](../published-assertions/decoded.md) and [`with`](../published-assertions/with.md) work inside it.
The record is the message as the framework handed it to the broker, in process and against a running stand alike.
The publish the test made returns only after the hook finished, and a failed assertion inside the hook fails it, or else the test's `shutdown` or the end of the test.
A hook applies from the moment it is registered, and several hooks on one destination run in the order they were registered.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_hooks.rs#on-publish"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_hooks.rs#on-publish"
```

</TabItem>
</Tabs>
