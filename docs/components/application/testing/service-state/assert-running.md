---
sidebar_position: 2
title: assert_running
description: "TestApp::assert_running asserts the service is still up: no fail-fast failure has shut it down."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# `assert_running`

`TestApp::assert_running` asserts the service is still up: no fail-fast failure has shut it down.

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

## How it works

It is for a failure the service must survive.
Under the default failure policy a delivery that does not decode is dropped, and the service keeps running.
A handler panic under the default policy stops the service, and so does a decode failure under a `fail_fast` decode policy.
On failure the assertion names the failure that stopped the service.

## See also

- [`assert_shut_down`](./assert-shut-down.md): the opposite assertion.
- [`run_result`](./run-result.md): the same state as a value.
- [`assert_last_failed_to_decode`](../subscriber-assertions/assert-last-failed-to-decode.md): the delivery that did not decode.
- [`TestApp::assert_running`](api:ruststream::testing::TestApp::assert_running), [`FailurePolicy`](api:ruststream::runtime::FailurePolicy)
