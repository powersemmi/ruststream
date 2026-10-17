---
sidebar_position: 5
title: Shortcuts
description: "TestApp::publish and BrokerHandle::publish send any serde value to a named destination in one call; publish_with_headers adds headers."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Shortcuts

[`TestApp::publish`](api:ruststream::testing::TestApp::publish) and [`BrokerHandle::publish`](api:ruststream::testing::BrokerHandle::publish) send any serde value to a named destination in one call; [`publish_with_headers`](api:ruststream::testing::BrokerHandle::publish_with_headers) adds headers.

They are for a value whose type the test does not own, such as a `serde_json::Value`.
The builder of [`message(&v)`](./message.md) needs the type to declare a destination, and the orphan rule keeps a foreign type from declaring one.
The value is encoded with the default codec and sent to `name`; the call then drives the reaction like [`publish()`](./publish.md).
`publish_with_headers` serializes a typed header contract into the delivery's headers, the input a `Message<H, P>` or `Headers<H>` handler reads.
`TestApp::publish` needs a single broker and fails with `TestError::Ambiguous` otherwise.
A value that does not encode fails the call with `TestError::Encode`.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_driving.rs#publish-shortcuts"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_driving.rs#publish-shortcuts"
```

</TabItem>
</Tabs>
