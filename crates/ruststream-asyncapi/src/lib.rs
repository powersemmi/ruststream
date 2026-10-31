//! The `AsyncAPI` 3.1 document generator of [RustStream](https://docs.rs/ruststream): it reads a
//! built service's handlers and metadata and renders the document and its HTML viewer.
//!
//! Services use it through the `ruststream` facade, as
//! [`ruststream::asyncapi`](https://docs.rs/ruststream/latest/ruststream/asyncapi/), whose overview
//! teaches it.

use std::collections::BTreeMap;

mod messages;
mod operations;
mod servers;
mod spec;
mod viewer;

pub use ruststream_core::bindings::{
    Binding, BindingError, Bindings, PublishBindings, SubscriptionBindings,
};
pub use spec::{
    Channel, Components, Info, MessageObject, Operation, OperationReply, Parameter, Reference,
    ReplyAddress, RetryExtension, Server, Spec,
};
pub use viewer::{ViewerOptions, render_viewer_html};

use ruststream_core::Tag;
use ruststream_runtime::OutgoingKind;
use ruststream_runtime_tokio::App;

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
///         b.include(accept)
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
