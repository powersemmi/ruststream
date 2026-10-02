//! The descriptive values a service and its brokers put in the generated document.
//!
//! [`Contact`], [`License`], [`Tag`] and [`ExternalDocs`] are the `AsyncAPI` objects of the same
//! names: who owns the service, under what licence it runs, how its operations group, and where
//! the prose lives. [`AppId`] is the service's own identifier.
//!
//! They live outside the `asyncapi` feature because [`AppInfo`](crate::runtime::AppInfo) and
//! [`ServerSpec`](crate::ServerSpec) carry them whether or not a document is ever generated.

use std::fmt;
use std::str::FromStr;

use serde::Serialize;
use thiserror::Error;

/// Who to contact about the service.
///
/// Every field is optional; an empty contact is omitted from the document.
///
/// # Examples
///
/// ```
/// use ruststream::Contact;
/// use ruststream::runtime::{App, AppInfo, RustStream};
///
/// fn app() -> impl App {
///     let info = AppInfo::new("payments", "1.4.0").contact(
///         Contact::new()
///             .name("Payments team")
///             .email("payments@example.com"),
///     );
///     RustStream::new(info)
/// }
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct Contact {
    /// The name of the person or team.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// A URL pointing at the contact information.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    /// An e-mail address.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
}

impl Contact {
    /// An empty contact.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::Contact;
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    ///
    /// fn app() -> impl App {
    ///     // Every field is optional, so a contact states only what the team publishes.
    ///     let info = AppInfo::new("payments", "1.4.0")
    ///         .contact(Contact::new().url("https://example.com/teams/payments"));
    ///     RustStream::new(info)
    /// }
    /// ```
    #[must_use]
    pub const fn new() -> Self {
        Self {
            name: None,
            url: None,
            email: None,
        }
    }

    /// Sets the name of the person or team.
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
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Sets the contact URL.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::Contact;
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    ///
    /// fn app() -> impl App {
    ///     let info = AppInfo::new("payments", "1.4.0").contact(
    ///         Contact::new()
    ///             .name("Payments team")
    ///             .url("https://example.com/support"),
    ///     );
    ///     RustStream::new(info)
    /// }
    /// ```
    #[must_use]
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }

    /// Sets the e-mail address.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::Contact;
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    ///
    /// fn app() -> impl App {
    ///     let info = AppInfo::new("payments", "1.4.0")
    ///         .contact(Contact::new().email("payments@example.com"));
    ///     RustStream::new(info)
    /// }
    /// ```
    #[must_use]
    pub fn email(mut self, email: impl Into<String>) -> Self {
        self.email = Some(email.into());
        self
    }

    /// True when nothing was filled in, so the document leaves the object out.
    #[cfg(feature = "asyncapi")]
    #[must_use]
    pub(crate) const fn is_empty(&self) -> bool {
        self.name.is_none() && self.url.is_none() && self.email.is_none()
    }
}

/// The licence the service is published under.
///
/// # Examples
///
/// ```
/// use ruststream::License;
/// use ruststream::runtime::{App, AppInfo, RustStream};
///
/// fn app() -> impl App {
///     let info = AppInfo::new("payments", "1.4.0")
///         .license(License::new("Apache-2.0").url("https://spdx.org/licenses/Apache-2.0.html"));
///     RustStream::new(info)
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct License {
    /// The licence name, an SPDX identifier by convention.
    pub name: String,
    /// A URL pointing at the licence text.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
}

impl License {
    /// A licence named `name` with no URL.
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
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            url: None,
        }
    }

    /// Sets the URL of the licence text.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::License;
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    ///
    /// fn app() -> impl App {
    ///     let info = AppInfo::new("payments", "1.4.0")
    ///         .license(License::new("MIT").url("https://spdx.org/licenses/MIT.html"));
    ///     RustStream::new(info)
    /// }
    /// ```
    #[must_use]
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = Some(url.into());
        self
    }
}

/// A pointer at documentation kept outside the generated document.
///
/// # Examples
///
/// ```
/// use ruststream::ExternalDocs;
/// use ruststream::runtime::{App, AppInfo, RustStream};
///
/// fn app() -> impl App {
///     let info = AppInfo::new("payments", "1.4.0").external_docs(
///         ExternalDocs::new("https://example.com/payments").description("the payment flow"),
///     );
///     RustStream::new(info)
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct ExternalDocs {
    /// Where the documentation lives.
    pub url: String,
    /// What the reader finds there.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
}

impl ExternalDocs {
    /// Documentation at `url`, undescribed.
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
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            description: None,
        }
    }

    /// Sets what the reader finds there.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::ExternalDocs;
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    ///
    /// fn app() -> impl App {
    ///     let info = AppInfo::new("payments", "1.4.0").external_docs(
    ///         ExternalDocs::new("https://example.com/runbooks/payments")
    ///             .description("the runbook"),
    ///     );
    ///     RustStream::new(info)
    /// }
    /// ```
    #[must_use]
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
}

/// A group label: on the service, on a server, on an operation.
///
/// # Examples
///
/// ```
/// use ruststream::Tag;
/// use ruststream::runtime::{App, AppInfo, RustStream};
///
/// fn app() -> impl App {
///     let info = AppInfo::new("payments", "1.4.0")
///         .tag(Tag::new("payments").description("everything the payment domain emits"));
///     RustStream::new(info)
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[non_exhaustive]
pub struct Tag {
    /// The label itself.
    pub name: String,
    /// What the label means.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Where the label is documented.
    #[serde(rename = "externalDocs", skip_serializing_if = "Option::is_none")]
    pub external_docs: Option<ExternalDocs>,
}

impl Tag {
    /// A bare label.
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
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: None,
            external_docs: None,
        }
    }

    /// Sets what the label means.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::Tag;
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    ///
    /// fn app() -> impl App {
    ///     let info = AppInfo::new("payments", "1.4.0")
    ///         .tag(Tag::new("payments").description("the payment domain"));
    ///     RustStream::new(info)
    /// }
    /// ```
    #[must_use]
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Sets where the label is documented.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::runtime::{App, AppInfo, RustStream};
    /// use ruststream::{ExternalDocs, Tag};
    ///
    /// fn app() -> impl App {
    ///     let info = AppInfo::new("payments", "1.4.0").tag(
    ///         Tag::new("payments")
    ///             .external_docs(ExternalDocs::new("https://example.com/payments")),
    ///     );
    ///     RustStream::new(info)
    /// }
    /// ```
    #[must_use]
    pub fn external_docs(mut self, docs: ExternalDocs) -> Self {
        self.external_docs = Some(docs);
        self
    }
}

/// The service's own identifier, which `AsyncAPI` requires to be a URI.
///
/// Parsed on construction, so a value that reaches the document is a URI and not a title someone
/// typed into the wrong builder. The check is the shape a URI scheme has to have: a letter,
/// then letters, digits, `+`, `-` and `.`, then a colon. What follows the colon is the scheme's
/// business, not this crate's.
///
/// # Examples
///
/// ```
/// use ruststream::runtime::{App, AppInfo, RustStream};
/// use ruststream::{AppId, AppIdError};
///
/// /// A bare title is refused: `"payments".parse::<AppId>()` fails, a URI does not.
/// fn app() -> Result<impl App, AppIdError> {
///     let id: AppId = "urn:example:payments".parse()?;
///     Ok(RustStream::new(AppInfo::new("payments", "1.4.0").id(id)))
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize)]
#[serde(transparent)]
pub struct AppId(String);

impl AppId {
    /// The identifier as written.
    ///
    /// # Examples
    ///
    /// ```
    /// use ruststream::AppId;
    /// use ruststream::runtime::AppInfo;
    ///
    /// /// The key the deployment tooling registers the service under: its id, or its title.
    /// fn registry_key(info: &AppInfo) -> &str {
    ///     info.id.as_ref().map_or(info.title.as_str(), AppId::as_str)
    /// }
    /// ```
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for AppId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl FromStr for AppId {
    type Err = AppIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (scheme, rest) = value.split_once(':').ok_or(AppIdError::NoScheme)?;
        let mut characters = scheme.chars();
        let first_is_letter = characters.next().is_some_and(|c| c.is_ascii_alphabetic());
        let tail_is_legal =
            characters.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.'));
        if !first_is_letter || !tail_is_legal {
            return Err(AppIdError::MalformedScheme);
        }
        let _ = rest;
        Ok(Self(value.to_owned()))
    }
}

impl TryFrom<&str> for AppId {
    type Error = AppIdError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl TryFrom<String> for AppId {
    type Error = AppIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.as_str().parse()
    }
}

/// Why a string is not an [`AppId`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum AppIdError {
    /// The value carries no `scheme:` prefix at all.
    #[error("an application id is a URI and needs a scheme, as in `urn:example:orders`")]
    NoScheme,
    /// The part before the colon is not a URI scheme.
    #[error(
        "a URI scheme starts with a letter and continues with letters, digits, `+`, `-` or `.`"
    )]
    MalformedScheme,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_uri_scheme_is_what_separates_an_id_from_a_title() {
        assert!("urn:example:orders".parse::<AppId>().is_ok());
        assert!("https://example.com/orders".parse::<AppId>().is_ok());
        assert!("a+b-c.d:rest".parse::<AppId>().is_ok());

        assert_eq!("orders".parse::<AppId>(), Err(AppIdError::NoScheme));
        assert_eq!("1http:x".parse::<AppId>(), Err(AppIdError::MalformedScheme));
        assert_eq!(":x".parse::<AppId>(), Err(AppIdError::MalformedScheme));
        assert_eq!("ht tp:x".parse::<AppId>(), Err(AppIdError::MalformedScheme));
    }

    #[cfg(feature = "asyncapi")]
    #[test]
    fn an_empty_contact_is_left_out_of_the_document() {
        assert!(Contact::new().is_empty());
        assert!(!Contact::new().name("Ops").is_empty());
    }
}
