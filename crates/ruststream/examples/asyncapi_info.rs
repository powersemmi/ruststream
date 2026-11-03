//! The `info` section of the AsyncAPI document and the service's id: what `AppInfo` carries
//! reaches the document as written. None of it involves a macro, so the file serves both forms.
//! The tests check what the document says.
//!
//! ```text
//! cargo test --example asyncapi_info --no-default-features --features asyncapi
//! ```

// A plain build compiles the service; the tests that use it exist only in the test build.
#![cfg_attr(not(test), allow(dead_code, unused_imports))]

use ruststream::runtime::{App, AppInfo, RustStream};
use ruststream::{AppId, AppIdError, Contact, ExternalDocs, License, Tag};

// --8<-- [start:info]
/// What the service is, who owns it, and under what terms it runs.
fn info() -> AppInfo {
    AppInfo::new("payments", "1.4.0")
        .description("Everything the payment domain publishes.")
        .terms_of_service("https://example.com/terms")
        .contact(contact())
        .license(license())
        .tag(tag())
        .external_docs(external_docs())
}
// --8<-- [end:info]

// --8<-- [start:contact]
/// Who answers for the service; every field is optional.
fn contact() -> Contact {
    Contact::new()
        .name("Payments team")
        .url("https://example.com/teams/payments")
        .email("payments@example.com")
}
// --8<-- [end:contact]

// --8<-- [start:license]
/// The license, by its SPDX identifier and where its text is.
fn license() -> License {
    License::new("Apache-2.0").url("https://spdx.org/licenses/Apache-2.0.html")
}
// --8<-- [end:license]

// --8<-- [start:tag]
/// The domain the service belongs to among its neighbours.
fn tag() -> Tag {
    Tag::new("payments")
        .description("Everything that moves money")
        .external_docs(ExternalDocs::new("https://example.com/domains/payments"))
}
// --8<-- [end:tag]

// --8<-- [start:external_docs]
/// Where the prose about the service lives.
fn external_docs() -> ExternalDocs {
    ExternalDocs::new("https://example.com/runbooks/payments").description("The runbook")
}
// --8<-- [end:external_docs]

// --8<-- [start:app_id]
/// The id comes from the deployment's configuration, so a value that is not a URI stops the
/// build of the app.
fn app(id: &str) -> Result<impl App, AppIdError> {
    let id: AppId = id.parse()?;
    Ok(RustStream::new(info().id(id)))
}
// --8<-- [end:app_id]

// The scenarios are the tests below; the command in the header runs them.
fn main() {}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use ruststream::AppIdError;
    use ruststream::asyncapi::build_spec;
    use serde_json::{Value, json};

    use super::app;

    fn document() -> Result<Value, Box<dyn Error>> {
        Ok(serde_json::from_str(
            &build_spec(&app("urn:example:payments")?).to_json()?,
        )?)
    }

    #[test]
    fn the_info_section_carries_what_app_info_holds() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(
            spec["info"],
            json!({
                "title": "payments",
                "version": "1.4.0",
                "description": "Everything the payment domain publishes.",
                "termsOfService": "https://example.com/terms",
                "contact": {
                    "name": "Payments team",
                    "url": "https://example.com/teams/payments",
                    "email": "payments@example.com",
                },
                "license": {
                    "name": "Apache-2.0",
                    "url": "https://spdx.org/licenses/Apache-2.0.html",
                },
                "tags": [{
                    "name": "payments",
                    "description": "Everything that moves money",
                    "externalDocs": { "url": "https://example.com/domains/payments" },
                }],
                "externalDocs": {
                    "url": "https://example.com/runbooks/payments",
                    "description": "The runbook",
                },
            })
        );
        Ok(())
    }

    #[test]
    fn the_id_is_the_documents_root_id() -> Result<(), Box<dyn Error>> {
        let spec = document()?;

        assert_eq!(spec["id"], "urn:example:payments");
        Ok(())
    }

    #[test]
    fn an_id_that_is_not_a_uri_stops_the_app() {
        assert!(matches!(app("payments"), Err(AppIdError::NoScheme)));
        assert!(matches!(
            app("1st:payments"),
            Err(AppIdError::MalformedScheme)
        ));
    }
}
