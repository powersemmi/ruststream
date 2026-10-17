---
sidebar_position: 2
title: assert_running
description: "TestApp::assert_running asserts the service is still up: no fail-fast failure has shut it down."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# assert_running

[`TestApp::assert_running`](api:ruststream::testing::TestApp::assert_running) asserts the service is still up: no fail-fast failure has shut it down.

It is for a failure the service must survive.
Under the default [`FailurePolicy`](api:ruststream::runtime::FailurePolicy) a delivery that does not decode is dropped, and the service keeps running; [`assert_last_failed_to_decode`](../subscriber-assertions/assert-last-failed-to-decode.md) checks that delivery.
A handler panic under the default policy stops the service, and so does a decode failure under a `fail_fast` decode policy.
On failure the assertion names the failure that stopped the service.
[`assert_shut_down`](./assert-shut-down.md) is the opposite assertion, and [`run_result`](./run-result.md) gives the same state as a value.

## Example

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/testing_service_state.rs#assert-running"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/testing_service_state.rs#assert-running"
```

</TabItem>
</Tabs>
