---
sidebar_position: 16
title: panicked
description: "panicked() checks that the handler panicked on its latest call."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# panicked

[`panicked()`](api:ruststream::testing::SubscriberAssertions::panicked) checks that the handler panicked on its latest call.

The harness runs dispatch under the app's real failure policy.
Under the default policy, `fail_fast`, a panic shuts the service down, which [`tb.assert_shut_down()`](api:ruststream::testing::TestApp::assert_shut_down) states.
The delivery stays unsettled, so no `settled(..)` matches it; [`assert_outcome(Outcome::Panicked)`](./assert-outcome.md) is the same check by outcome.
The test output carries the handler's panic message, and the test itself passes.
[`assert_last_failed_to_decode`](./assert-last-failed-to-decode.md) checks the other failure a call can end in.
The [`assert_shut_down`](../service-state/assert-shut-down.md) and [`run_result`](../service-state/run-result.md) pages show what the panic did to the service.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_subscriber_calls.rs#panicked"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_subscriber_calls.rs#panicked"
```

</TabItem>
</Tabs>
