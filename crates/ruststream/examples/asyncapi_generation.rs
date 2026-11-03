//! Generating a service's AsyncAPI document: `build_spec` reads it from the app value, it
//! serializes to JSON and YAML, and `render_viewer_html` renders a page that shows it. The tests
//! check what the document says.
//!
//! ```text
//! cargo test --example asyncapi_generation --features macros,memory,asyncapi
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use std::error::Error;
use std::fs;
use std::path::Path;

use ruststream::asyncapi::{Spec, ViewerOptions, build_spec, render_viewer_html};
use ruststream::memory::prelude::*;
use ruststream::schemars::JsonSchema;
use serde::Deserialize;

/// An order placed by a customer.
#[derive(Debug, Deserialize, JsonSchema)]
struct Order {
    id: u64,
    item: String,
}

// --8<-- [start:spec]
// --8<-- [start:describe]
/// Accepts every order placed in the shop.
#[subscriber("orders")]
async fn accept(order: &Order) -> HandlerOutcome {
    tracing::info!(order.id, item = %order.item, "accepted");
    HandlerOutcome::ack()
}
// --8<-- [end:describe]

fn app() -> impl App {
    RustStream::new(AppInfo::new("orders", "0.1.0"))
        .with_broker(MemoryBroker::new(), |b| b.include(accept))
}

/// The document of the service, read from the same app value `main` runs.
fn document() -> Spec {
    build_spec(&app())
}
// --8<-- [end:spec]

// --8<-- [start:formats]
/// Writes the document next to the service's code, in both formats AsyncAPI tools read.
fn write_document(dir: &Path) -> Result<(), Box<dyn Error>> {
    let spec = build_spec(&app());
    fs::write(dir.join("asyncapi.json"), spec.to_json()?)?;
    fs::write(dir.join("asyncapi.yaml"), spec.to_yaml()?)?;
    Ok(())
}
// --8<-- [end:formats]

// --8<-- [start:viewer]
/// The page the service serves at `/docs`: it renders the document served at `/asyncapi.json`.
fn docs_page() -> String {
    let options = ViewerOptions::default().title("Orders events");
    render_viewer_html("/asyncapi.json", &options)
}
// --8<-- [end:viewer]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::{env, fs, process};

    use serde_json::{Value, json};

    use super::{docs_page, document, write_document};

    #[test]
    fn the_subscriber_becomes_a_channel_an_operation_and_a_message() -> Result<(), Box<dyn Error>> {
        let spec: Value = serde_json::from_str(&document().to_json()?)?;

        assert_eq!(spec["asyncapi"], "3.1.0");
        assert_eq!(
            spec["info"],
            json!({ "title": "orders", "version": "0.1.0" })
        );
        assert_eq!(spec["channels"]["orders"]["address"], "orders");
        let operation = &spec["operations"]["receive_orders"];
        assert_eq!(operation["action"], "receive");
        assert_eq!(operation["channel"]["$ref"], "#/channels/orders");
        assert_eq!(
            operation["description"],
            "Accepts every order placed in the shop."
        );
        let order = &spec["components"]["messages"]["Order"];
        assert_eq!(order["description"], "An order placed by a customer.");
        assert_eq!(order["contentType"], "application/json");
        assert_eq!(order["payload"]["required"], json!(["id", "item"]));
        Ok(())
    }

    #[test]
    fn the_document_is_written_as_json_and_yaml() -> Result<(), Box<dyn Error>> {
        let dir = env::temp_dir().join(format!("asyncapi-generation-{}", process::id()));
        fs::create_dir_all(&dir)?;
        write_document(&dir)?;

        let json: Value = serde_json::from_str(&fs::read_to_string(dir.join("asyncapi.json"))?)?;
        let yaml = fs::read_to_string(dir.join("asyncapi.yaml"))?;
        fs::remove_dir_all(&dir)?;

        assert_eq!(json["asyncapi"], "3.1.0");
        assert!(yaml.starts_with("asyncapi: 3.1.0\n"), "{yaml}");
        assert!(yaml.contains("receive_orders:"), "{yaml}");
        Ok(())
    }

    #[test]
    fn the_viewer_page_renders_the_served_document() {
        let page = docs_page();

        assert!(page.contains("<title>Orders events</title>"));
        assert!(page.contains("url: \"/asyncapi.json\""));
        assert!(
            page.contains("https://cdn.jsdelivr.net/npm/@asyncapi/react-component@3.1.8/browser/standalone/index.js")
        );
    }
}
