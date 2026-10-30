The application object, the handler surface and dispatch: a map of the module, each topic linked
to its page on the site.

A service is one [`RustStream`] value: brokers are registered with
[`with_broker`](RustStream::with_broker), handlers are mounted inside that scope with `include`,
and [`run`](RustStream::run) owns the process. The [`#[ruststream::app]`](macro@crate::app)
attribute writes the `main` that calls it, and [`cli`] is the command it builds. Every topic
below shows its attribute form and its manual form ([`subscriber`], [`Handle`]) on its page.

- [Application](https://powersemmi.github.io/ruststream/latest/components/application/): the
  [`RustStream`] builder, [`AppInfo`] and the [`App`] it produces.
- [App](https://powersemmi.github.io/ruststream/latest/components/application/app-macro/): the
  `#[ruststream::app]` attribute that writes `main`.
- [CLI](https://powersemmi.github.io/ruststream/latest/components/application/cli/): the command
  the attribute builds, in [`cli`].

# Subscribers

- [Subscribers](https://powersemmi.github.io/ruststream/latest/components/subscribers/): a
  handler is an `async fn` over `&T` (or `&[T]`) under `#[subscriber]`, mounted with `include`;
  the manual form is [`subscriber`] over a [`Handle`].
- [Source](https://powersemmi.github.io/ruststream/latest/components/subscribers/subscription-source/):
  a subscription named by a string ([`Subscribe`](crate::Subscribe)) or by the broker's
  descriptor ([`SubscriptionSource`](crate::SubscriptionSource),
  [`FromName`](crate::FromName)).
- [Settings](https://powersemmi.github.io/ruststream/latest/components/subscribers/mount-settings/):
  name, workers, failure policy, batch size and start position at the mount site,
  [`SubscriberSettings`]. A broker crate's own mount steps read what the chain fixed through
  [`SubscriberBuilder::dispatch`], [`SubscriberBuilder::batch_size`] and
  [`SubscriberBuilder::subscription_name`].
- [Batches](https://powersemmi.github.io/ruststream/latest/components/subscribers/batches/): a
  `&[T]` handler, `batch(n)`, and a `Vec<HandlerOutcome>` that settles each element.
- [Failures](https://powersemmi.github.io/ruststream/latest/components/subscribers/failure-policy/):
  what a panic or a payload that does not decode settles as, [`FailurePolicies`] and
  [`FailurePolicy`].
- [Attempts](https://powersemmi.github.io/ruststream/latest/components/subscribers/retry-cap/):
  `max_attempts(n)` and `dead_letter(name)` end a message that keeps coming back.
- [Retry](https://powersemmi.github.io/ruststream/latest/components/subscribers/retry-destination/):
  the [`Retry`] slot a `retry_after` copy leaves through, and where it goes
  ([`RedeliveryAddressed`](crate::RedeliveryAddressed), [`NamedCopies`](crate::NamedCopies)).
- [Routers](https://powersemmi.github.io/ruststream/latest/components/subscribers/router/): a
  [`Router`] groups one module's handlers, mounted with
  [`include_router`](BrokerScope::include_router).
- [Codecs](https://powersemmi.github.io/ruststream/latest/components/subscribers/codecs/): the
  codec is a compile-time type chosen at the mount site; payloads that carry their own bytes
  derive [`Deserialized`](macro@crate::Deserialized) and [`Serialized`](macro@crate::Serialized)
  (see [`codec`](crate::codec)). A value the broker already holds reaches a handler as `&T` with
  no codec ([`SoloCarried`], [`Carries`](crate::Carries)).
- [Message sets](https://powersemmi.github.io/ruststream/latest/components/subscribers/message-sets/):
  several message types on one subscription, declared as an enum that derives
  [`InMessages`](macro@crate::InMessages); a rule picks the variant (a header, a function, a
  [`CodecDiscriminator`] that asks the decode codec, or the order of the variants), each model decodes through its own lane ([`InVariant`]), and the
  document lists one message per variant ([`IncomingMessageMetadata`]).

## Messages

- [Messages](https://powersemmi.github.io/ruststream/latest/components/subscribers/messages/):
  what a handler returns settles its delivery.
- [Outcomes](https://powersemmi.github.io/ruststream/latest/components/subscribers/messages/handler-outcome/):
  [`HandlerOutcome::ack`], [`retry`](HandlerOutcome::retry) and
  [`drop`](HandlerOutcome::drop), `()` and `Result`.
- [retry_after](https://powersemmi.github.io/ruststream/latest/components/subscribers/messages/retry-after/):
  [`HandlerOutcome::retry_after`] and the framework's [`RETRY_COUNT_HEADER`].
- [and_after](https://powersemmi.github.io/ruststream/latest/components/subscribers/messages/and-after/):
  [`HandlerOutcome::and_after`], a continuation once the delivery is settled.
- [Hooks](https://powersemmi.github.io/ruststream/latest/components/subscribers/messages/hooks/):
  [`Context::after_ack`], [`Context::after_settle`] and [`Context::after`].

## Dispatch

- [Dispatch](https://powersemmi.github.io/ruststream/latest/components/subscribers/dispatch/):
  how many deliveries of one subscription run at once, and where, as a [`Workers`] with its
  [`Placement`].
- [Workers](https://powersemmi.github.io/ruststream/latest/components/subscribers/dispatch/workers/):
  `workers(n)` and `workers(n, by_key)` on the app's runtime.
- [Threads](https://powersemmi.github.io/ruststream/latest/components/subscribers/dispatch/threads/):
  `threads(n)` gives a handler that computes threads of its own; [`MainRuntime`] sends work back
  to the app's runtime.
- [Diagnostics](https://powersemmi.github.io/ruststream/latest/components/subscribers/dispatch/poll-diagnostics/):
  [`PollDiagnostics`] names the handlers that belong on `threads(n)`.

## Replay

- [Replay](https://powersemmi.github.io/ruststream/latest/components/subscribers/replay/): a
  subscription over a replayable log ([`Seekable`](crate::Seekable)).
- [start_at](https://powersemmi.github.io/ruststream/latest/components/subscribers/replay/start-at/):
  where a subscription opens, [`SubscriberSettings::start_at`].
- [seek](https://powersemmi.github.io/ruststream/latest/components/subscribers/replay/seek/): a
  handler moves its own subscription through the broker's context keys.

# Publishing

- [Publishing](https://powersemmi.github.io/ruststream/latest/components/subscribers/publishing/):
  a handler answers by returning a value or publishes through an `Out` parameter; the mount site
  names a [`PublishPolicy`](crate::PublishPolicy) that startup pairs with the connected broker.
- [Replies](https://powersemmi.github.io/ruststream/latest/components/subscribers/publishing/replies/):
  the `reply` clause and [`RouterWith::out_reply`].
- [Destinations](https://powersemmi.github.io/ruststream/latest/components/subscribers/publishing/outgoing/):
  a message type derives [`Outgoing`](macro@crate::Outgoing) and declares where it goes.
- [Slots](https://powersemmi.github.io/ruststream/latest/components/subscribers/publishing/out-slots/):
  [`Out`] parameters, [`OutSlot`](crate::OutSlot) markers and [`DefaultSlot`].
- [Builder](https://powersemmi.github.io/ruststream/latest/components/subscribers/publishing/publish-builder/):
  `message(&value)`, the positions its type leaves open, then `publish()`.
- [Options](https://powersemmi.github.io/ruststream/latest/components/subscribers/publishing/publish-options/):
  a broker's per-message settings, [`Publisher::Options`](crate::Publisher::Options).
- [Bindable](https://powersemmi.github.io/ruststream/latest/components/subscribers/publishing/cross-broker/):
  another broker's publisher through [`bindable`](crate::Broker::bindable), [`Bindable::bind`]
  and a [`Bound`] token.

## Publishers

- [Publishers](https://powersemmi.github.io/ruststream/latest/components/subscribers/publishing/publishers/):
  one capability trait each.
- [Publisher](https://powersemmi.github.io/ruststream/latest/components/subscribers/publishing/publishers/publisher/):
  [`Publisher`](crate::Publisher), the capability every slot names.
- [TransactionalPublisher](https://powersemmi.github.io/ruststream/latest/components/subscribers/publishing/publishers/transactional-publisher/):
  [`TransactionalPublisher`](crate::TransactionalPublisher) and its [`TransactionScope`].
- [OwnedTransactions](https://powersemmi.github.io/ruststream/latest/components/subscribers/publishing/publishers/owned-transactions/):
  [`OwnedTransactions`](crate::OwnedTransactions) and [`TypedTransaction`].
- [RequestReply](https://powersemmi.github.io/ruststream/latest/components/subscribers/publishing/publishers/request-reply/):
  [`RequestReply`](crate::RequestReply), a request awaited from a handler.

## Transforms

- [Transforms](https://powersemmi.github.io/ruststream/latest/components/subscribers/publishing/transforms/):
  a static step on one publish position.
- [PublishTransform](https://powersemmi.github.io/ruststream/latest/components/subscribers/publishing/transforms/publish-transform/):
  [`PublishTransform`] over [`ForReply`] ([`PublishContext`]) or [`ForSlot`]
  ([`SlotContext`]), declaring [`Reads`].
- [Naming](https://powersemmi.github.io/ruststream/latest/components/subscribers/publishing/transforms/naming-transform/):
  a transform that declares [`Names`] and calls [`Outgoing::set_name`].
- [BatchPublishTransform](https://powersemmi.github.io/ruststream/latest/components/subscribers/publishing/transforms/batch-transform/):
  `.batch_transform(..)` on a batch reply, and [`for_batch`].

# Routing

- [Routers](https://powersemmi.github.io/ruststream/latest/components/subscribers/router/):
  [`Router`], [`Router::with_codec`], [`Router::layer`] and [`Router::merge`].

# Context and state

- [Context and extractors](https://powersemmi.github.io/ruststream/latest/components/subscribers/context/):
  a handler's parameters after the message.
- [Context](https://powersemmi.github.io/ruststream/latest/components/subscribers/context/delivery-context/):
  [`Context`], one delivery's channel, headers, state and broker fields.
- [Ctx](https://powersemmi.github.io/ruststream/latest/components/subscribers/context/ctx/):
  [`Ctx<K>`] binds one broker field as a parameter.
- [Extractors](https://powersemmi.github.io/ruststream/latest/components/subscribers/context/from-context/):
  a type of your own implementing [`FromContext`].
- [State](https://powersemmi.github.io/ruststream/latest/components/application/state/): one
  value built by [`on_startup`](RustStream::on_startup), read as [`State<T>`] through
  [`FromRef`](macro@crate::FromRef).

# Typed headers

- [Headers](https://powersemmi.github.io/ruststream/latest/components/subscribers/typed-headers/):
  a `serde` struct as the header contract, read with [`Headers<T>`].
- [Message](https://powersemmi.github.io/ruststream/latest/components/subscribers/typed-headers/message/):
  [`Message<H, T>`](Message), the payload and its contract as one input.
- [Outgoing](https://powersemmi.github.io/ruststream/latest/components/subscribers/typed-headers/outgoing/):
  `#[outgoing(headers = Meta)]` and [`PublishBuilder::with_headers`].
- [insert_typed](https://powersemmi.github.io/ruststream/latest/components/subscribers/typed-headers/insert-typed/):
  [`HeaderMap::insert_typed`](crate::HeaderMap::insert_typed) in a transform.

# Lifecycle

- [Lifecycle](https://powersemmi.github.io/ruststream/latest/components/application/lifecycle/):
  [`on_startup`](RustStream::on_startup), [`after_startup`](BrokerScope::after_startup),
  [`on_shutdown`](RustStream::on_shutdown), [`after_shutdown`](RustStream::after_shutdown) and
  [`shutdown_timeout`](RustStream::shutdown_timeout).
- [Running](https://powersemmi.github.io/ruststream/latest/components/application/running/): a
  service runs until a signal, until a future of yours, or beside another server.
- [Run](https://powersemmi.github.io/ruststream/latest/components/application/running/run/):
  [`RustStream::run`].
- [Until](https://powersemmi.github.io/ruststream/latest/components/application/running/until/):
  [`RustStream::run_until`].
- [Start](https://powersemmi.github.io/ruststream/latest/components/application/running/start/):
  [`RustStream::start`] and the [`RunningApp`] it returns.
- [Shutdown](https://powersemmi.github.io/ruststream/latest/components/application/running/shutdown/):
  [`RunningApp::shutdown`].
- [Health](https://powersemmi.github.io/ruststream/latest/components/application/running/health/):
  [`RunningApp::health`] and its [`HealthProbe`], the [`SubscriptionCount`] it reports, and what
  [`RustStream::on_subscription_end`] makes of a stream that ends while the service runs.

# Middleware

- [Middleware](https://powersemmi.github.io/ruststream/latest/components/subscribers/middleware/):
  layers wrap handlers and publishes, app-wide, per router or per registration.
- [Layer](https://powersemmi.github.io/ruststream/latest/components/subscribers/middleware/layer/):
  [`Layer`], [`Handler`] and [`BlanketLayer`], added with [`RustStream::layer`] or
  [`Router::layer`].
- [DynStack](https://powersemmi.github.io/ruststream/latest/components/subscribers/middleware/dyn-stack/):
  a chain assembled at startup, [`DynStack`] of [`DynMiddleware`].
- [PublishLayer](https://powersemmi.github.io/ruststream/latest/components/subscribers/middleware/publish-layer/):
  [`PublishLayer`], added with [`RustStream::publish_layer`].
- [PublishDynStack](https://powersemmi.github.io/ruststream/latest/components/subscribers/middleware/publish-dyn-stack/):
  the publish side's chain assembled at startup.
- [TracingLayer](https://powersemmi.github.io/ruststream/latest/components/observability/tracing-layer/):
  the bundled [`layers::TracingLayer`].

# Failure policy

- [Failures](https://powersemmi.github.io/ruststream/latest/components/subscribers/failure-policy/):
  [`FailurePolicies`] per subscriber, for a panic and for a decode failure.

# Running beside another server

- [HTTP](https://powersemmi.github.io/ruststream/latest/components/application/running/http/):
  [`RustStream::start`] beside an HTTP server, [`RunningApp::stopping`] for its graceful
  shutdown and [`RunningApp::publisher`] for a live publisher on the request path.

# Runtimes

The delivery body is shared and generic over a runtime's [`Clock`] and over the wait on the app's
shutdown, [`ShutdownWait`]. The tokio runner, [`Tokio`], runs it: the app's lifecycle, spawning,
`workers(n)`, `threads(n)` and the signals are its own. Service code that waits calls
[`crate::time::sleep`], which sleeps on the app's clock, so the test harness moves it with
`advance` like every timer the framework arms.

The runtime is a cargo feature of the crate, and the two exclude each other. `tokio`, the default,
selects the tokio runner and everything above. `embassy` selects the embedded runner on embassy's
executor and clock: its application object, `EmbeddedApp`, with the mount steps `MountSteps`, its
shutdown and health, `Control`, `Health` and `Stopping`, and its clock, `EmbassyClock`, are
re-exported in this module, and their reference is the documentation of
[`ruststream-runtime-embassy`](https://docs.rs/ruststream-runtime-embassy).
