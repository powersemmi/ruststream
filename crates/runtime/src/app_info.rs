//! [`AppInfo`]: the service metadata every runner reports to the `AsyncAPI` generator.

use alloc::string::String;
use alloc::vec::Vec;

use crate::{AppId, Contact, ExternalDocs, License, Tag};

/// Service-level metadata, surfaced to the `AsyncAPI` generator as the spec `Info` object.
///
/// The title and the version are what a service must state; everything else is optional and
/// reaches the document only when it is filled in.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::{App, AppInfo, RustStream};
/// use ruststream::{Contact, License, Tag};
///
/// fn app() -> impl App {
///     let info = AppInfo::new("payments", "1.4.0")
///         .description("everything the payment domain publishes")
///         .contact(Contact::new().email("payments@example.com"))
///         .license(License::new("Apache-2.0"))
///         .tag(Tag::new("payments"));
///     RustStream::new(info)
/// }
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct AppInfo {
    /// Human-readable service title.
    pub title: String,
    /// Service version string.
    pub version: String,
    /// Optional longer description.
    pub description: Option<String>,
    /// The service's own identifier, a URI. Emitted as the document's root `id`.
    pub id: Option<AppId>,
    /// Where the terms of service are published.
    pub terms_of_service: Option<String>,
    /// Who to contact about the service.
    pub contact: Contact,
    /// The licence the service is published under.
    pub license: Option<License>,
    /// Labels grouping the service among its neighbours.
    pub tags: Vec<Tag>,
    /// Where the prose about this service lives.
    pub external_docs: Option<ExternalDocs>,
}

impl AppInfo {
    /// Creates info with a title and version and nothing else.
    #[must_use]
    pub fn new(title: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            title: title.into(),
            version: version.into(),
            description: None,
            id: None,
            terms_of_service: None,
            contact: Contact::new(),
            license: None,
            tags: Vec::new(),
            external_docs: None,
        }
    }

    /// Sets the description.
    #[must_use]
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Sets the service's own identifier.
    ///
    /// The identifier is a URI, which [`AppId`] checks on construction, so the document cannot
    /// carry a title where a reader expects an identifier.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    /// use ruststream::{AppId, AppIdError};
    ///
    /// /// The id comes from the deployment's configuration, so a malformed one fails the build of
    /// /// the app.
    /// fn app(id: &str) -> Result<impl App, AppIdError> {
    ///     let info = AppInfo::new("payments", "1.4.0").id(id.parse::<AppId>()?);
    ///     Ok(RustStream::new(info))
    /// }
    /// ```
    #[must_use]
    pub fn id(mut self, id: AppId) -> Self {
        self.id = Some(id);
        self
    }

    /// Sets where the terms of service are published.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    ///
    /// fn app() -> impl App {
    ///     let info =
    ///         AppInfo::new("payments", "1.4.0").terms_of_service("https://example.com/terms");
    ///     RustStream::new(info)
    /// }
    /// ```
    #[must_use]
    pub fn terms_of_service(mut self, url: impl Into<String>) -> Self {
        self.terms_of_service = Some(url.into());
        self
    }

    /// Sets who to contact about the service.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::Contact;
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    ///
    /// fn app() -> impl App {
    ///     let info =
    ///         AppInfo::new("payments", "1.4.0").contact(Contact::new().name("Payments team"));
    ///     RustStream::new(info)
    /// }
    /// ```
    #[must_use]
    pub fn contact(mut self, contact: Contact) -> Self {
        self.contact = contact;
        self
    }

    /// Sets the licence the service is published under.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::License;
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    ///
    /// fn app() -> impl App {
    ///     let info = AppInfo::new("payments", "1.4.0").license(License::new("MIT"));
    ///     RustStream::new(info)
    /// }
    /// ```
    #[must_use]
    pub fn license(mut self, license: License) -> Self {
        self.license = Some(license);
        self
    }

    /// Adds one label to the service. Call repeatedly for several.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::Tag;
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    ///
    /// fn app() -> impl App {
    ///     let info = AppInfo::new("payments", "1.4.0")
    ///         .tag(Tag::new("payments"))
    ///         .tag(Tag::new("public"));
    ///     RustStream::new(info)
    /// }
    /// ```
    #[must_use]
    pub fn tag(mut self, tag: Tag) -> Self {
        self.tags.push(tag);
        self
    }

    /// Sets where the prose about this service lives.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::ExternalDocs;
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    ///
    /// fn app() -> impl App {
    ///     let info = AppInfo::new("payments", "1.4.0")
    ///         .external_docs(ExternalDocs::new("https://example.com/payments"));
    ///     RustStream::new(info)
    /// }
    /// ```
    #[must_use]
    pub fn external_docs(mut self, docs: ExternalDocs) -> Self {
        self.external_docs = Some(docs);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_builder_keeps_every_field_it_is_given() {
        let info = AppInfo::new("payments", "1.4.0")
            .description("the payment domain")
            .terms_of_service("https://example.com/terms")
            .contact(Contact::new().name("Payments team"))
            .license(License::new("MIT"))
            .tag(Tag::new("payments"))
            .external_docs(ExternalDocs::new("https://example.com/payments"));

        assert_eq!(info.title, "payments");
        assert_eq!(info.version, "1.4.0");
        assert_eq!(info.description.as_deref(), Some("the payment domain"));
        assert_eq!(
            info.terms_of_service.as_deref(),
            Some("https://example.com/terms")
        );
        assert!(!info.contact.is_empty());
        assert!(info.license.is_some());
        assert_eq!(info.tags.len(), 1);
        assert!(info.external_docs.is_some());
    }

    #[test]
    fn a_new_info_states_only_the_title_and_the_version() {
        let info = AppInfo::new("svc", "0.1.0");

        assert!(info.description.is_none() && info.id.is_none() && info.license.is_none());
        assert!(info.contact.is_empty() && info.tags.is_empty());
    }
}
