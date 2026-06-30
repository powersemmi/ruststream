//! Expansion of `#[derive(FromState)]`: generates a `FromContext` impl for each field of an
//! application-state struct, so handlers can take the field types as extractor arguments without a
//! hand-written impl per field.

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Field, Fields};

pub(crate) fn expand(input: &DeriveInput) -> syn::Result<TokenStream2> {
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "#[derive(FromState)] does not support generic state types",
        ));
    }
    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            input,
            "#[derive(FromState)] supports only structs with named fields",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(syn::Error::new_spanned(
            &data.fields,
            "#[derive(FromState)] supports only structs with named fields",
        ));
    };

    let name = &input.ident;
    let mut seen: Vec<(String, &Field)> = Vec::new();
    let mut impls = Vec::new();
    for field in &fields.named {
        if field_skipped(field)? {
            continue;
        }
        let ident = field.ident.as_ref().expect("named field");
        let ty = &field.ty;

        // Two fields of the same type would generate conflicting impls: extraction by type would be
        // ambiguous, so reject it with a clear span instead.
        let key = quote!(#ty).to_string();
        if seen.iter().any(|(seen_key, _)| *seen_key == key) {
            return Err(syn::Error::new(
                ty.span(),
                "two fields share a type, so extraction by type is ambiguous; mark one \
                 `#[from_state(skip)]`",
            ));
        }
        seen.push((key, field));

        impls.push(quote! {
            #[automatically_derived]
            impl<__RsCtx> ::ruststream::runtime::FromContext<__RsCtx, #name> for #ty {
                type Rejection = ::ruststream::runtime::HandlerResult;
                fn from_context(
                    __rs_ctx: &mut ::ruststream::runtime::Context<'_, __RsCtx, #name>,
                ) -> impl ::core::future::Future<
                    Output = ::core::result::Result<Self, ::ruststream::runtime::HandlerResult>,
                > + ::core::marker::Send {
                    let __rs_value = ::core::clone::Clone::clone(&__rs_ctx.state().#ident);
                    async move { ::core::result::Result::Ok(__rs_value) }
                }
            }
        });
    }

    if impls.is_empty() {
        return Err(syn::Error::new_spanned(
            input,
            "#[derive(FromState)] generated no extractors: every field is `#[from_state(skip)]`",
        ));
    }
    Ok(quote!(#(#impls)*))
}

/// Whether a field opts out with `#[from_state(skip)]` (used for fields whose type is foreign - the
/// orphan rule forbids generating an impl for it - or that are plain configuration, not a
/// dependency).
fn field_skipped(field: &Field) -> syn::Result<bool> {
    let mut skip = false;
    for attr in &field.attrs {
        if attr.path().is_ident("from_state") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("skip") {
                    skip = true;
                    Ok(())
                } else {
                    Err(meta.error("unknown `#[from_state]` option; expected `skip`"))
                }
            })?;
        }
    }
    Ok(skip)
}
