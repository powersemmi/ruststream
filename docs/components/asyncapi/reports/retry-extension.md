---
sidebar_position: 5
title: Retry
description: "The attempt cap and the dead-letter destination of a registration appear on its receive operation as the x-ruststream-retry extension."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Retry

The attempt cap and the dead-letter destination of a registration appear on its `receive` operation as the `x-ruststream-retry` extension.

[`max_attempts(n)`](../../subscribers/retry-cap.md) is `maxAttempts`, and `dead_letter(name)` is `deadLetter`.
AsyncAPI has a field for neither: a binding models a dead-letter queue only for SQS and SNS, and nothing models an attempt cap.
So the document carries them as an [extension](api:ruststream::asyncapi::RetryExtension) of the operation.
The dead-letter destination is a channel of its own, with a `send` operation, since the registration publishes there.
Retry copies sent to a named destination add `retryDestination`, with its channel (see [Retry slot](../../subscribers/retry-destination.md)).
Copies that return to the subscription's own address add nothing: that channel is already in the document.
A registration that declares none of this has no extension.
The example runs with `cargo test --example asyncapi_reports --features macros,memory,asyncapi`.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/asyncapi_reports.rs#retry"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/asyncapi_reports.rs#retry"
```

</TabItem>
</Tabs>
