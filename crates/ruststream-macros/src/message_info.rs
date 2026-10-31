//! Expansion of `#[derive(MessageInfo)]`: the type's name and doc comment as the metadata the
//! generated document reads, and its optional typed header contract.

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Attribute, DeriveInput, Meta, Type};

use crate::parse::{derive_generics, doc_description};

pub(crate) fn expand(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let name = &input.ident;
    let name_str = name.to_string();
    let description = doc_description(&input.attrs);
    let contract = message_headers_contract(&input.attrs)?.map_or_else(
        || quote!(::ruststream::NoHeaders),
        |headers| quote!(::ruststream::WithHeaders<#headers>),
    );
    let generics = derive_generics(&input.generics);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics ::ruststream::MessageInfo for #name #ty_generics #where_clause {
            const NAME: &'static str = #name_str;
            const DESCRIPTION: ::core::option::Option<&'static str> = #description;
        }

        impl #impl_generics ::ruststream::MessageHeaders for #name #ty_generics #where_clause {
            type Contract = #contract;
        }
    })
}

/// Parses the optional `#[message(headers(Type))]` helper attribute of `#[derive(MessageInfo)]`.
fn message_headers_contract(attrs: &[Attribute]) -> syn::Result<Option<Type>> {
    let mut found: Option<Type> = None;
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("message")) {
        let meta: Meta = attr.parse_args()?;
        let Meta::List(list) = &meta else {
            return Err(syn::Error::new_spanned(
                meta,
                "expected `headers(<Type>)` inside #[message(..)]",
            ));
        };
        if !list.path.is_ident("headers") {
            return Err(syn::Error::new_spanned(
                &list.path,
                "expected `headers(<Type>)` inside #[message(..)]",
            ));
        }
        let headers: Type = list.parse_args()?;
        if found.replace(headers).is_some() {
            return Err(syn::Error::new_spanned(
                attr,
                "duplicate `headers(..)`: a message declares one header contract",
            ));
        }
    }
    Ok(found)
}
