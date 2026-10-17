---
sidebar_position: 5
title: Options
description: "A broker's per-message settings, such as a priority, a QoS or a key: defaulted by the publish policy, set per publish by the broker's own steps."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Options

A broker's per-message settings, such as a priority, a `QoS` or a key: defaulted by the publish policy, set per publish by the broker's own steps.

A broker declares its settings as [`Publisher::Options`](api:ruststream::Publisher), a type of its own with every field optional.
The publish policy at the mount site holds the defaults.
A body that sets one bounds its slot `Out<impl Publisher<Options = ..>, _>` and imports the broker's prelude, whose extension trait adds the steps to the [builder](./publish-builder.md).
A step sets one field for one publish; a field no step touches keeps the policy's default.
A reply and a retry copy have no call site, so a [transform](./transforms/publish-transform.md) that writes the broker's options sets them there.
A transform runs after the call site, so it wins a field both of them set.
A setting reaches the broker's publisher as a value, not as a header, and a test reads it back with [`with_options`](../../application/testing/published-assertions/with-options.md).

The memory broker's publisher has no settings, so the example wraps it with a priority the way a networked broker ships one, and turns the priority into a header the test can read.
A service on a networked broker imports the broker's own options and steps instead.

## Example

The body sets the priority of an express order; the mount site sets the default.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/per_message_options.rs#handler"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/per_message_options.rs#handler"
```

</TabItem>
</Tabs>

A reply takes its settings from a transform.

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/per_message_options.rs#transform"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/per_message_options.rs#transform"
```

</TabItem>
</Tabs>

<Tabs groupId="api">
<TabItem value="macros" label="Macros">

```rust reference="examples/per_message_options.rs#app"
```

</TabItem>
<TabItem value="manual" label="Manual">

```rust reference="examples/manual/per_message_options.rs#app"
```

</TabItem>
</Tabs>
