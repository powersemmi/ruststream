//! The servers of the AsyncAPI document: a broker registered under a label describes itself, a
//! server no broker of the process describes is declared with a `ServerSpec`, and the spec
//! carries how clients authenticate and the protocol's bindings. The tests check what the
//! document says.
//!
//! ```text
//! cargo test --example asyncapi_servers --features macros,memory,asyncapi
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::asyncapi::{Binding, BindingError, Bindings};
use ruststream::memory::prelude::*;
use ruststream::schemars::JsonSchema;
use ruststream::{SecurityScheme, ServerSpec};
use serde::{Deserialize, Serialize};

/// An order placed by a customer.
#[derive(Debug, Deserialize, JsonSchema)]
struct Order {
    id: u64,
}

#[subscriber("orders")]
async fn accept(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "accepted");
    HandlerOutcome::ack()
}

// --8<-- [start:server]
/// The cluster the service is deployed against, as its clients reach it.
fn production() -> ServerSpec {
    ServerSpec::new("kafka.example.com:9093", "kafka")
        .protocol_version("3.9")
        .description("The production cluster")
}
// --8<-- [end:server]

// --8<-- [start:security]
/// How clients authenticate: SCRAM over TLS, or a client certificate.
fn secured(server: ServerSpec) -> ServerSpec {
    server
        .security(SecurityScheme::scram_sha512().description("SASL over TLS"))
        .security(SecurityScheme::x509())
}
// --8<-- [end:security]

// --8<-- [start:bindings]
/// The Kafka server binding: where the cluster's schema registry is.
#[derive(Serialize)]
struct KafkaServer {
    #[serde(rename = "schemaRegistryUrl")]
    schema_registry_url: &'static str,
}

/// Adds the binding; a protocol the specification does not list fails here, at startup.
fn with_registry(server: ServerSpec) -> Result<ServerSpec, BindingError> {
    let registry = KafkaServer {
        schema_registry_url: "https://registry.example.com",
    };
    let binding = Binding::new("kafka", "0.5.0", &registry)?;
    Ok(server.bindings(Bindings::new().with(binding)))
}
// --8<-- [end:bindings]

// --8<-- [start:app]
fn app() -> Result<impl App, BindingError> {
    let production = with_registry(secured(production()))?;
    Ok(RustStream::new(AppInfo::new("orders", "0.1.0"))
        // Declared: no broker of this process describes the cluster.
        .server("production", production)
        // Described by the broker itself, under its label.
        .with_broker_labeled("in-process", MemoryBroker::new(), |b| {
            b.include(accept);
        }))
}
// --8<-- [end:app]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::asyncapi::build_spec;
    use serde_json::{Value, json};

    use super::app;

    fn document() -> Result<Value, Box<dyn Error>> {
        Ok(serde_json::from_str(&build_spec(&app()?).to_json()?)?)
    }

    #[test]
    fn the_labeled_broker_is_a_server_its_channels_live_on() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(
            spec["servers"]["in-process"],
            json!({ "protocol": "memory" })
        );
        assert_eq!(
            spec["channels"]["orders"]["servers"],
            json!([{ "$ref": "#/servers/in-process" }])
        );
        assert_eq!(
            spec["operations"]["receive_orders"]["tags"],
            json!([{ "name": "memory" }])
        );
        Ok(())
    }

    #[test]
    fn the_declared_server_says_how_clients_reach_it() -> Result<(), Box<dyn Error>> {
        let spec = document()?;
        let production = &spec["servers"]["production"];

        assert_eq!(production["host"], "kafka.example.com:9093");
        assert_eq!(production["protocol"], "kafka");
        assert_eq!(production["protocolVersion"], "3.9");
        assert_eq!(production["description"], "The production cluster");
        Ok(())
    }

    #[test]
    fn each_security_scheme_is_a_component_the_server_references() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(
            spec["servers"]["production"]["security"],
            json!([
                { "$ref": "#/components/securitySchemes/production" },
                { "$ref": "#/components/securitySchemes/production-1" },
            ])
        );
        assert_eq!(
            spec["components"]["securitySchemes"],
            json!({
                "production": { "type": "scramSha512", "description": "SASL over TLS" },
                "production-1": { "type": "X509" },
            })
        );
        Ok(())
    }

    #[test]
    fn the_binding_carries_its_version() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(
            spec["servers"]["production"]["bindings"],
            json!({
                "kafka": {
                    "schemaRegistryUrl": "https://registry.example.com",
                    "bindingVersion": "0.5.0",
                },
            })
        );
        Ok(())
    }
}
