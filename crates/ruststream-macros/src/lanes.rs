//! The lane derives: `Deserialized` (a self-deserializing input) and `Serialized` (a
//! self-serialized reply).
//!
//! Both are sugar over a pair of short public-trait impls (see the core traits' rustdoc for the
//! hand-written form); the derives cover the obvious shapes - a newtype or single-field struct
//! over the bytes.

use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{ToTokens, quote};
use syn::{Data, DeriveInput, Error, Fields, GenericParam, Lifetime, Type};

/// The one field of a lane-derive struct: its accessor tokens (`0` or the name) and its type.
fn single_field(input: &DeriveInput, derive: &str) -> syn::Result<(TokenStream2, Type)> {
    let Data::Struct(data) = &input.data else {
        return Err(Error::new_spanned(
            &input.ident,
            format!("{derive} is derived on a struct with exactly one field"),
        ));
    };
    let mut fields = match &data.fields {
        Fields::Named(named) => named.named.iter(),
        Fields::Unnamed(unnamed) => unnamed.unnamed.iter(),
        Fields::Unit => {
            return Err(Error::new_spanned(
                &input.ident,
                format!("{derive} is derived on a struct with exactly one field, not a unit one"),
            ));
        }
    };
    let field = fields.next().ok_or_else(|| {
        Error::new_spanned(
            &input.ident,
            format!("{derive} is derived on a struct with exactly one field"),
        )
    })?;
    if let Some(extra) = fields.next() {
        return Err(Error::new_spanned(
            extra,
            format!(
                "{derive} is derived on a struct with exactly one field; implement the trait by \
                 hand for a larger shape (see its rustdoc)"
            ),
        ));
    }
    let accessor = field
        .ident
        .as_ref()
        .map_or_else(|| quote!(0), ToTokens::to_token_stream);
    Ok((accessor, field.ty.clone()))
}

/// True when `ty` is syntactically a `&[u8]` reference (any lifetime).
fn is_byte_slice_ref(ty: &Type) -> bool {
    let Type::Reference(reference) = ty else {
        return false;
    };
    if reference.mutability.is_some() {
        return false;
    }
    let Type::Slice(slice) = &*reference.elem else {
        return false;
    };
    matches!(&*slice.elem, Type::Path(path) if path.qself.is_none() && path.path.is_ident("u8"))
}

pub(crate) fn derive_deserialized(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let name = &input.ident;
    let (accessor, field_ty) = single_field(input, "Deserialized")?;
    if !is_byte_slice_ref(&field_ty) {
        return Err(Error::new_spanned(
            &field_ty,
            "#[derive(Deserialized)] covers a struct over the payload view `&'a [u8]`; a \
             validating or converting construction implements `Deserialized` (and the `Input` \
             spelling) by hand - see the trait's rustdoc",
        ));
    }
    let mut lifetimes = input
        .generics
        .params
        .iter()
        .filter_map(|param| match param {
            GenericParam::Lifetime(lifetime) => Some(lifetime),
            _ => None,
        });
    if lifetimes.next().is_none() || lifetimes.next().is_some() {
        return Err(Error::new_spanned(
            &input.generics,
            "#[derive(Deserialized)] expects exactly one lifetime parameter (the payload's), \
             and no type parameters",
        ));
    }
    if input
        .generics
        .params
        .iter()
        .any(|param| !matches!(param, GenericParam::Lifetime(_)))
    {
        return Err(Error::new_spanned(
            &input.generics,
            "#[derive(Deserialized)] takes no type or const parameters; implement the traits by \
             hand for a generic shape",
        ));
    }
    // A distinct name so the GAT's lifetime can never shadow the struct's own.
    let out = Lifetime::new("'__rs_out", Span::call_site());
    Ok(quote! {
        impl ::ruststream::runtime::Deserialized for #name<'_> {
            type Output<#out> = #name<#out>;
            type Error = ::core::convert::Infallible;

            fn from_payload(
                payload: &[u8],
            ) -> ::core::result::Result<#name<'_>, Self::Error> {
                ::core::result::Result::Ok(#name { #accessor: payload })
            }
        }

        impl ::ruststream::runtime::Input for #name<'_> {
            type Axis = ::ruststream::runtime::SoloDeserialized<#name<'static>>;
        }
    })
}

pub(crate) fn derive_serialized(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let name = &input.ident;
    let (accessor, _field_ty) = single_field(input, "Serialized")?;
    if !input.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &input.generics,
            "#[derive(Serialized)] covers an owned buffer type with no generic parameters; \
             implement `Serialized` (and `ReplyShape`) by hand for a generic shape - see the \
             trait's rustdoc",
        ));
    }
    Ok(quote! {
        impl ::ruststream::runtime::Serialized for #name {
            fn bytes(&self) -> &[u8] {
                &self.#accessor
            }
        }

        impl ::ruststream::runtime::ReplyShape for #name {
            type Body = Self;
            type Headers = ();
            type Wire = ::ruststream::runtime::SerializedReply;
        }
    })
}
