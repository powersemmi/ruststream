//! The runtime the generated code targets, chosen once per crate by the facade's runtime feature,
//! and the spellings that differ between the two.

use syn::punctuated::Punctuated;
use syn::{Path, PathSegment, Token, TypeParamBound};

/// The runtime a service runs its app on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Runtime {
    /// The multi-thread runner: the bodies and the publishers they hold are `Send`.
    Tokio,
    /// The embedded runner on embassy: every delivery is polled on the app's one task, so
    /// nothing is asked to be `Send`.
    Embassy,
    /// The compio runner: one single-threaded runtime per core, so nothing is asked to be
    /// `Send` and the publisher capabilities are named as the service wrote them.
    Compio,
}

impl Runtime {
    /// The bounds a capability written on an `Out` parameter becomes: as written on tokio; on
    /// embassy a plain `Publisher` is named through the facade (`::ruststream::Publisher`), so
    /// the generated code does not depend on what the service imported.
    pub(crate) fn capability(
        self,
        bounds: &Punctuated<TypeParamBound, Token![+]>,
    ) -> Punctuated<TypeParamBound, Token![+]> {
        match self {
            Self::Tokio | Self::Compio => bounds.clone(),
            Self::Embassy => bounds.iter().map(through_facade).collect(),
        }
    }
}

/// The publisher capabilities named through the facade, by the name a service writes.
// Only the plain publish is named: the transactional and request capabilities keep the bounds as
// written, so they mount where their wire implements them.
const FACADE_CAPABILITIES: [&str; 1] = ["Publisher"];

/// One bound with a core publisher capability named through the facade, keeping the arguments it
/// was written with (`Publisher<Options = Qos>`). Any other bound stays as written.
fn through_facade(bound: &TypeParamBound) -> TypeParamBound {
    let TypeParamBound::Trait(written) = bound else {
        return bound.clone();
    };
    // A parsed path has a last segment; one naming no capability stays as written.
    let Some(last) = written
        .path
        .segments
        .last()
        .filter(|last| FACADE_CAPABILITIES.iter().any(|name| last.ident == name))
    else {
        return bound.clone();
    };
    let mut named = written.clone();
    let mut segments: Punctuated<PathSegment, Token![::]> = Punctuated::new();
    segments.push(syn::Ident::new("ruststream", last.ident.span()).into());
    segments.push(PathSegment {
        ident: last.ident.clone(),
        arguments: last.arguments.clone(),
    });
    named.path = Path {
        leading_colon: Some(Token![::](last.ident.span())),
        segments,
    };
    TypeParamBound::Trait(named)
}

#[cfg(test)]
mod tests {
    use quote::ToTokens;

    use super::*;

    fn mapped(runtime: Runtime, bounds: &Punctuated<TypeParamBound, Token![+]>) -> String {
        runtime.capability(bounds).to_token_stream().to_string()
    }

    #[test]
    fn a_publisher_capability_is_named_through_the_facade_on_embassy_and_other_bounds_stay() {
        let bounds: Punctuated<TypeParamBound, Token![+]> =
            syn::parse_quote!(ruststream::Publisher<Options = Qos> + Audited + 'static);

        assert_eq!(
            mapped(Runtime::Embassy, &bounds),
            ":: ruststream :: Publisher < Options = Qos > + Audited + 'static"
        );
    }

    #[test]
    fn the_tokio_runtime_keeps_the_bounds_as_written() {
        let bounds: Punctuated<TypeParamBound, Token![+]> = syn::parse_quote!(Publisher + 'static);

        assert_eq!(mapped(Runtime::Tokio, &bounds), "Publisher + 'static");
    }
}
