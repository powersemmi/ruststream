//! Content types in the AsyncAPI document: each message names the media type of the codec that
//! reads it, and the root `defaultContentType` appears only when every message names the same
//! one. The tests check what the document says.
//!
//! ```text
//! cargo test --example asyncapi_content_type --features macros,memory,asyncapi,cbor
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::codec::CborCodec;
use ruststream::memory::prelude::*;
use ruststream::schemars::JsonSchema;
use serde::Deserialize;

/// An order placed by a customer.
#[derive(Debug, Deserialize, JsonSchema)]
struct Order {
    id: u64,
}

/// A reading a sensor sends in CBOR.
#[derive(Debug, Deserialize, JsonSchema)]
struct Reading {
    sensor: u32,
    value: f64,
}

#[subscriber("orders")]
async fn accept(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, "accepted");
    HandlerOutcome::ack()
}

#[subscriber("readings")]
async fn record(reading: &Reading) -> HandlerOutcome {
    tracing::info!(reading.sensor, reading.value, "recorded");
    HandlerOutcome::ack()
}

// --8<-- [start:app]
fn app() -> impl App {
    RustStream::new(AppInfo::new("plant", "0.1.0"))
        // The default codec: `Order` is `application/json`.
        .with_broker(MemoryBroker::new(), |b| {
            b.include(accept);
        })
        // The scope's codec: `Reading` is `application/cbor`.
        .with_broker_codec(MemoryBroker::new(), CborCodec, |b| {
            b.include(record);
        })
}
// --8<-- [end:app]

/// The same service with JSON alone, so every message names one media type.
fn json_only() -> impl App {
    RustStream::new(AppInfo::new("plant", "0.1.0")).with_broker(MemoryBroker::new(), |b| {
        b.include(accept);
    })
}

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::asyncapi::build_spec;
    use ruststream::runtime::App;
    use serde_json::Value;

    use super::{app, json_only};

    fn document(app: &impl App) -> Result<Value, Box<dyn Error>> {
        Ok(serde_json::from_str(&build_spec(app).to_json()?)?)
    }

    #[test]
    fn each_message_names_the_media_type_of_its_codec() -> Result<(), Box<dyn Error>> {
        let spec = document(&app())?;
        let messages = &spec["components"]["messages"];

        assert_eq!(messages["Order"]["contentType"], "application/json");
        assert_eq!(messages["Reading"]["contentType"], "application/cbor");
        // Two formats: a document-wide default would misdescribe one of them.
        assert!(spec["defaultContentType"].is_null());
        Ok(())
    }

    #[test]
    fn one_format_is_the_documents_default() -> Result<(), Box<dyn Error>> {
        let spec = document(&json_only())?;

        assert_eq!(spec["defaultContentType"], "application/json");
        Ok(())
    }
}
