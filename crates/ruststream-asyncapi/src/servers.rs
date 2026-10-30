//! The servers of a document: the brokers a service connects to, and the security schemes they
//! reference.

use std::collections::BTreeMap;

use serde_json::Value;

use ruststream_runtime_tokio::App;

use super::spec::{Reference, Server};

/// Renders the service's registered servers, collecting the security schemes they reference into
/// the components map as it goes.
pub(super) fn build_servers<A: App>(
    app: &A,
) -> (BTreeMap<String, Server>, BTreeMap<String, Value>) {
    let mut security_schemes = BTreeMap::new();
    let servers = app
        .servers()
        .iter()
        .map(|(name, spec)| {
            let security = spec
                .security
                .iter()
                .enumerate()
                .map(|(index, scheme)| {
                    let key = if index == 0 {
                        name.clone()
                    } else {
                        format!("{name}-{index}")
                    };
                    security_schemes.insert(key.clone(), scheme.document_object());
                    Reference::new(format!("#/components/securitySchemes/{key}"))
                })
                .collect();
            (
                name.clone(),
                Server {
                    host: spec.host.clone(),
                    protocol: spec.protocol.clone(),
                    protocol_version: spec.protocol_version.clone(),
                    description: spec.description.clone(),
                    security,
                    bindings: spec.bindings.clone(),
                },
            )
        })
        .collect();
    (servers, security_schemes)
}

/// The server a registration's channels live on: the label the broker was registered under, or
/// the single server of a one-broker document.
///
/// A document with several servers and an unlabeled registration gets no answer, and the channel
/// then says nothing rather than claiming a server it cannot name.
pub(super) fn resolve_server<'a>(
    label: Option<&'a str>,
    servers: &'a BTreeMap<String, Server>,
) -> Option<&'a str> {
    match label {
        Some(label) if servers.contains_key(label) => Some(label),
        _ if servers.len() == 1 => servers.keys().next().map(String::as_str),
        _ => None,
    }
}
