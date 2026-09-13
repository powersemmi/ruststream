//! The self-contained viewer page a service can serve next to its document.

/// Renders a self-contained HTML page that displays `spec_url` using the `AsyncAPI` React component.
///
/// The component and its styles load from a CDN (jsDelivr) by default; override
/// [`cdn_base`](ViewerOptions::cdn_base) to pin a version or self-host for offline / locked-down
/// deployments. Serve the returned HTML from your own HTTP stack alongside the spec document.
///
/// # Examples
///
/// ```
/// use ruststream::asyncapi::{render_viewer_html, ViewerOptions};
///
/// let html = render_viewer_html("/asyncapi.json", &ViewerOptions::default());
/// assert!(html.contains("/asyncapi.json"));
/// ```
#[must_use]
pub fn render_viewer_html(spec_url: &str, opts: &ViewerOptions<'_>) -> String {
    let title = opts.title;
    let cdn = opts.cdn_base.trim_end_matches('/');
    let spec = spec_url.replace('"', "&quot;");
    format!(
        "<!DOCTYPE html>\n\
<html lang=\"en\">\n\
<head>\n\
  <meta charset=\"utf-8\" />\n\
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\" />\n\
  <title>{title}</title>\n\
  <link rel=\"stylesheet\" href=\"{cdn}/styles/default.min.css\" />\n\
</head>\n\
<body>\n\
  <div id=\"asyncapi\"></div>\n\
  <script src=\"{cdn}/browser/standalone/index.js\"></script>\n\
  <script>\n\
    AsyncApiStandalone.render(\n\
      {{ schema: {{ url: \"{spec}\" }}, config: {{ show: {{ sidebar: true }} }} }},\n\
      document.getElementById(\"asyncapi\"),\n\
    );\n\
  </script>\n\
</body>\n\
</html>\n"
    )
}

/// Options for [`render_viewer_html`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ViewerOptions<'a> {
    /// The HTML page title.
    pub title: &'a str,
    /// Base URL the `AsyncAPI` React assets load from (no trailing slash required).
    pub cdn_base: &'a str,
}

impl<'a> ViewerOptions<'a> {
    /// Sets the HTML page title.
    #[must_use]
    pub const fn with_title(mut self, title: &'a str) -> Self {
        self.title = title;
        self
    }

    /// Sets the base URL the `AsyncAPI` React assets load from.
    #[must_use]
    pub const fn with_cdn_base(mut self, cdn_base: &'a str) -> Self {
        self.cdn_base = cdn_base;
        self
    }
}

impl Default for ViewerOptions<'_> {
    fn default() -> Self {
        Self {
            title: "AsyncAPI",
            cdn_base: "https://cdn.jsdelivr.net/npm/@asyncapi/react-component@3.1.8",
        }
    }
}
