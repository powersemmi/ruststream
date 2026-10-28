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
//! - [Tag][tag]: [`Tag`], a label that groups services.
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

use std::collections::BTreeMap;

mod bindings;
mod messages;
mod operations;
mod servers;
mod spec;
mod viewer;

pub use bindings::{Binding, BindingError, Bindings, PublishBindings, SubscriptionBindings};
pub(crate) use messages::{join_headers, schema_json};
pub use spec::{
    Channel, Components, Info, MessageObject, Operation, OperationReply, Parameter, Reference,
    ReplyAddress, RetryExtension, Server, Spec,
};
pub use viewer::{ViewerOptions, render_viewer_html};

use crate::describe::Tag;
use crate::runtime::{App, OutgoingKind};

use operations::{add_outgoing, add_receive, send_operation_id};
use servers::{build_servers, resolve_server};
use spec::ASYNCAPI_VERSION;

/// Builds an [`AsyncAPI`](Spec) 3.1.0 document from a service's handlers and metadata.
///
/// Each registered subscriber becomes a channel (addressed by its name), a `receive` operation,
/// and a message component named after the handler's input type.
///
/// # Examples
///
/// ```
/// # #[cfg(all(feature = "macros", feature = "memory", feature = "json"))]
/// # mod demo {
/// use std::error::Error;
/// use std::fs;
///
/// use ruststream::asyncapi::build_spec;
/// use ruststream::memory::prelude::*;
/// use ruststream::schemars::JsonSchema;
/// use serde::Deserialize;
///
/// #[derive(Deserialize, JsonSchema)]
/// struct Order {
///     id: u64,
/// }
///
/// #[subscriber("orders")]
/// async fn accept(order: &Order) -> HandlerOutcome {
///     tracing::info!(order.id, "accepted");
///     HandlerOutcome::ack()
/// }
///
/// pub fn app() -> RustStream {
///     RustStream::new(AppInfo::new("orders", "1.0.0")).with_broker(MemoryBroker::new(), |b| {
///         b.include(accept);
///     })
/// }
///
/// /// Writes the document the service publishes next to its code, from the app `main` runs.
/// pub fn write_document() -> Result<(), Box<dyn Error>> {
///     let spec = build_spec(&app());
///     fs::write("asyncapi.json", spec.to_json()?)?;
///     Ok(())
/// }
/// # }
/// # fn main() {}
/// ```
#[must_use]
pub fn build_spec<A: App>(app: &A) -> Spec {
    let contact = app.info().contact.clone();
    let info = Info {
        title: app.info().title.clone(),
        version: app.info().version.clone(),
        description: app.info().description.clone(),
        terms_of_service: app.info().terms_of_service.clone(),
        contact: (!contact.is_empty()).then_some(contact),
        license: app.info().license.clone(),
        tags: app.info().tags.clone(),
        external_docs: app.info().external_docs.clone(),
    };

    let (servers, security_schemes) = build_servers(app);

    let mut channels = BTreeMap::new();
    let mut operations = BTreeMap::new();
    let mut messages = BTreeMap::new();

    for handler in app.handlers() {
        let server = resolve_server(handler.server.as_deref(), &servers);
        let on_servers = server
            .map(|name| vec![Reference::new(format!("#/servers/{name}"))])
            .unwrap_or_default();
        let tags = server
            .and_then(|name| servers.get(name))
            .map(|server| vec![Tag::new(server.protocol.clone())])
            .unwrap_or_default();

        // The reply contributes its channel and its message before the `receive` operation
        // points at them, and it takes no `send` operation of its own: what answers a request
        // is the request operation's `reply`.
        let reply = handler
            .outgoing
            .iter()
            .find(|outgoing| outgoing.kind == OutgoingKind::Answer)
            .map(|outgoing| {
                let refs = add_outgoing(outgoing, &on_servers, &mut channels, &mut messages);
                OperationReply {
                    address: outgoing.reply_address_location.map(ReplyAddress::new),
                    channel: refs.channel_ref(),
                    messages: vec![refs.message_ref()],
                }
            });

        add_receive(
            handler,
            reply,
            &on_servers,
            &tags,
            &mut channels,
            &mut operations,
            &mut messages,
        );

        // One `send` operation per remaining declared outgoing message: every Out slot
        // dictionary entry, the dead-letter destination of a `dead_letter(..)` declaration, and
        // the destination a `.to(name)` gave the registration's retry copies.
        // A channel several messages flow through lists them all; the message component is
        // shared with any handler that receives it.
        for outgoing in &handler.outgoing {
            if outgoing.kind == OutgoingKind::Answer {
                continue;
            }
            let refs = add_outgoing(outgoing, &on_servers, &mut channels, &mut messages);
            operations.insert(
                send_operation_id(&operations, handler.name.as_ref(), &refs.channel),
                Operation {
                    action: "send".to_owned(),
                    channel: refs.channel_ref(),
                    messages: vec![refs.message_ref()],
                    description: None,
                    reply: None,
                    tags: tags.clone(),
                    retry: None,
                    bindings: outgoing.bindings.operation.clone(),
                },
            );
        }
    }

    let default_content_type = agreed_content_type(&messages);

    Spec {
        asyncapi: ASYNCAPI_VERSION.to_owned(),
        id: app.info().id.clone(),
        info,
        servers,
        default_content_type,
        channels,
        operations,
        components: Components {
            messages,
            security_schemes,
        },
    }
}

/// The media type every message in the document names, for the root `defaultContentType`.
///
/// A reader applies the root value to the whole document, and `AsyncAPI` gives it to each message
/// that names no media type of its own. So it stands only while every message names the same one:
/// a second format, or a message no codec reads, would be misdescribed by it.
fn agreed_content_type(messages: &BTreeMap<String, MessageObject>) -> Option<String> {
    let mut agreed: Option<&str> = None;
    for message in messages.values() {
        let content_type = message.content_type.as_deref()?;
        match agreed {
            None => agreed = Some(content_type),
            Some(existing) if existing == content_type => {}
            Some(_) => return None,
        }
    }
    agreed.map(str::to_owned)
}
