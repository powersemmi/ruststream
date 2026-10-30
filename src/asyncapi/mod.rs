//! `AsyncAPI` 3.1 document generation from a [`RustStream`](crate::runtime::RustStream) service.
//!
//! [`build_spec`] turns the app `main` runs into a [`Spec`]: each subscriber becomes a channel
//! and a `receive` operation, a reply rides that operation, every `Out` slot message gets a `send`
//! operation, and payload and header types contribute schemas.
//!
//! # Generation
//!
//! - [AsyncAPI][asyncapi]: what the `asyncapi` feature adds to a service.
//! - [Generation][generation]: how the document is produced, serialized and served.
//! - [build_spec][build-spec]: [`build_spec`] reads the document from the app value.
//! - [Formats][formats]: [`Spec::to_json`] and [`Spec::to_yaml`].
//! - [Command][command]: `ruststream asyncapi gen` prints the document from the generated `main`
//!   (see [`cli`](crate::runtime::cli)).
//! - [Viewer][viewer]: [`render_viewer_html`] and [`ViewerOptions`] serve a page that renders it.
//! - [describe][describe]: the operation description of a manual registration.
//! - [undocumented][undocumented]: a manual registration left out of the document.
//!
//! # Schemas
//!
//! - [Schemas][schemas]: a payload type that derives `JsonSchema` (re-exported as [`schemars`])
//!   gets a payload schema.
//! - [MessageInfo][message-info]: [`MessageInfo`](crate::MessageInfo) names and describes a
//!   message component whatever the Rust type is called.
//! - [messages_without_schema][without-schema]: [`Spec::messages_without_schema`] lists the gaps
//!   for a test to fail on.
//!
//! # What the document reports
//!
//! - [What the document reports][reports]: what it says beyond the channels.
//! - [Replies][replies]: a reply is the `reply` of the operation it answers.
//! - [Parameters][parameters]: a name template becomes a channel with `parameters`.
//! - [Headers][headers]: a header contract becomes the message's headers schema.
//! - [Content][content]: each message names its codec's
//!   [`CONTENT_TYPE`](crate::codec::Codec::CONTENT_TYPE).
//! - [Message sets][message-sets]: a subscription over an
//!   [`InMessages`](crate::runtime::InMessages) set lists one message per variant, each with
//!   its own format, and the header rule as a `const` in its headers schema.
//! - [Retry][retry]: the attempt cap and the retry destinations as `x-ruststream-retry`.
//!
//! # Servers
//!
//! - [Servers][servers]: the brokers a service connects to, how clients authenticate, and the
//!   protocol bindings.
//! - [with_broker_labeled][with-broker-labeled]:
//!   [`with_broker_labeled`](crate::runtime::RustStream::with_broker_labeled) registers a broker
//!   under its server name, and a broker implementing [`DescribeServer`](crate::DescribeServer)
//!   describes the server.
//! - [Server][server]: [`server`](crate::runtime::RustStream::server) declares one from a
//!   [`ServerSpec`](crate::ServerSpec).
//! - [Security][security]: [`ServerSpec::security`](crate::ServerSpec::security) with a
//!   [`SecurityScheme`](crate::SecurityScheme).
//! - [Bindings][bindings]: [`Bindings`] carry what one protocol says about a server, a channel,
//!   an operation or a message.
//!
//! # Service info
//!
//! - [Service info][service-info]: the `info` section, what the service is and who owns it.
//! - [Info][app-info]: [`AppInfo`](crate::runtime::AppInfo), the title, the version and the
//!   description.
//! - [AppId][app-id]: [`AppId`](crate::AppId), the service's identifier, parsed as a URI.
//! - [Contact][contact]: [`Contact`](crate::Contact), who answers for the service.
//! - [License][license]: [`License`](crate::License).
//! - [Tag][tag]: [`Tag`](crate::Tag), a label that groups services.
//! - [ExternalDocs][external-docs]: [`ExternalDocs`](crate::ExternalDocs), documentation kept
//!   outside the document.
//!
//! [asyncapi]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/
//! [generation]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/generation/
//! [reports]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/reports/
//! [build-spec]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/generation/build-spec/
//! [formats]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/generation/to-json-yaml/
//! [command]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/generation/cli-gen/
//! [viewer]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/generation/viewer/
//! [describe]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/generation/describe/
//! [undocumented]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/generation/undocumented/
//! [schemas]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/generation/schemas/
//! [message-info]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/generation/message-info/
//! [without-schema]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/generation/messages-without-schema/
//! [replies]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/reports/replies/
//! [parameters]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/reports/channel-parameters/
//! [headers]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/reports/header-schemas/
//! [content]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/reports/content-type/
//! [message-sets]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/reports/message-sets/
//! [retry]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/reports/retry-extension/
//! [servers]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/servers/
//! [with-broker-labeled]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/servers/with-broker-labeled/
//! [server]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/servers/server/
//! [security]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/servers/security-schemes/
//! [bindings]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/servers/bindings/
//! [service-info]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/service-info/
//! [app-info]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/service-info/app-info/
//! [app-id]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/service-info/app-id/
//! [contact]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/service-info/contact/
//! [license]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/service-info/license/
//! [tag]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/service-info/tag/
//! [external-docs]: https://powersemmi.github.io/ruststream/latest/components/asyncapi/service-info/external-docs/

#[doc(inline)]
pub use ruststream_asyncapi::*;
