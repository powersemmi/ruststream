//! The runtime the generated code targets, chosen once per crate by the facade's runtime feature,
//! and the spellings that differ between the two.

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::punctuated::Punctuated;
use syn::{Path, PathSegment, Token, TypeParamBound};

/// The runtime a service runs its app on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Runtime {
    /// The multi-thread runner: the bodies and the publishers they hold are `Send`.
    Tokio,
    /// The embedded runner on embassy: every delivery is polled on the app's one task, so the
    /// local forms carry no `Send`.
    Embassy,
}

impl Runtime {
    /// The runtime the facade's feature selected. A build that names neither (the macros on
    /// their own) keeps the tokio forms.
    pub(crate) const SELECTED: Self = if cfg!(feature = "embassy") {
        Self::Embassy
    } else {
        Self::Tokio
    };

    /// The bounds a capability written on an `Out` parameter becomes: as written on tokio; on
    /// embassy each publisher capability of the core maps to its local twin, so
    /// `Out<impl Publisher>` asks for a `LocalPublisher`.
    pub(crate) fn capability(
        self,
        bounds: &Punctuated<TypeParamBound, Token![+]>,
    ) -> Punctuated<TypeParamBound, Token![+]> {
        match self {
            Self::Tokio => bounds.clone(),
            Self::Embassy => bounds.iter().map(local_bound).collect(),
        }
    }
}

/// The publisher capabilities of the core that have a local twin, by the name a service writes.
const LOCAL_TWINS: [(&str, &str); 4] = [
    ("Publisher", "LocalPublisher"),
    ("TransactionalPublisher", "LocalTransactionalPublisher"),
    ("RequestReply", "LocalRequestReply"),
    ("OwnedTransactions", "LocalOwnedTransactions"),
];

/// One bound with a core publisher capability replaced by its local twin, named through the
/// facade and keeping the arguments it was written with (`Publisher<Options = Qos>`). Any other
/// bound stays as written.
fn local_bound(bound: &TypeParamBound) -> TypeParamBound {
    let TypeParamBound::Trait(written) = bound else {
        return bound.clone();
    };
    // A parsed path has a last segment; one naming no capability stays as written.
    let Some((last, twin)) = written.path.segments.last().and_then(|last| {
        LOCAL_TWINS
            .iter()
            .find(|(core, _)| last.ident == core)
            .map(|(_, twin)| (last, twin))
    }) else {
        return bound.clone();
    };
    let mut local = written.clone();
    let mut segments: Punctuated<PathSegment, Token![::]> = Punctuated::new();
    segments.push(syn::Ident::new("ruststream", last.ident.span()).into());
    segments.push(PathSegment {
        ident: syn::Ident::new(twin, last.ident.span()),
        arguments: last.arguments.clone(),
    });
    local.path = Path {
        leading_colon: Some(Token![::](last.ident.span())),
        segments,
    };
    TypeParamBound::Trait(local)
}

#[cfg(test)]
mod tests {
    use quote::ToTokens;

    use super::*;

    fn mapped(runtime: Runtime, bounds: &Punctuated<TypeParamBound, Token![+]>) -> String {
        runtime.capability(bounds).to_token_stream().to_string()
    }

    #[test]
    fn a_publisher_capability_maps_to_its_local_twin_on_embassy_and_other_bounds_stay() {
        let bounds: Punctuated<TypeParamBound, Token![+]> =
            syn::parse_quote!(ruststream::Publisher<Options = Qos> + Audited + 'static);

        assert_eq!(
            mapped(Runtime::Embassy, &bounds),
            ":: ruststream :: LocalPublisher < Options = Qos > + Audited + 'static"
        );
    }

    #[test]
    fn every_publisher_capability_has_its_twin() {
        let bounds: Punctuated<TypeParamBound, Token![+]> = syn::parse_quote!(
            TransactionalPublisher + RequestReply + OwnedTransactions + LocalPublisher
        );

        assert_eq!(
            mapped(Runtime::Embassy, &bounds),
            ":: ruststream :: LocalTransactionalPublisher + :: ruststream :: LocalRequestReply \
             + :: ruststream :: LocalOwnedTransactions + LocalPublisher"
        );
    }

    #[test]
    fn the_tokio_runtime_keeps_the_bounds_as_written() {
        let bounds: Punctuated<TypeParamBound, Token![+]> = syn::parse_quote!(Publisher + 'static);

        assert_eq!(mapped(Runtime::Tokio, &bounds), "Publisher + 'static");
    }
}
