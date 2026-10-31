//! Expansion of the slot derives: `#[derive(OutSlot)]`, the marker an `Out` parameter names with
//! the messages it publishes, and `#[derive(OutMessages)]`, a declared set of message models.

use proc_macro2::TokenStream as TokenStream2;
use quote::{ToTokens, quote};
use syn::punctuated::Punctuated;
use syn::{Attribute, DeriveInput, Ident, Token, Type};

pub(crate) fn expand_out_slot(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let syn::Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "OutSlot is derived on a unit struct",
        ));
    };
    if !matches!(data.fields, syn::Fields::Unit) {
        return Err(syn::Error::new_spanned(
            &data.fields,
            "an OutSlot marker is a unit struct: it identifies the slot and carries no data",
        ));
    }
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "an OutSlot marker takes no generic parameters",
        ));
    }
    let dictionary = publishes_dictionary(&input.attrs)?;
    let name = &input.ident;
    let name_str = name.to_string();

    let outgoing = slot_outgoing_metadata(name, &dictionary);
    // The list is the whole membership: what the document reports as leaving the slot is exactly
    // what the publish builder admits.
    let memberships = dictionary.iter().map(|ty| {
        quote! {
            impl ::ruststream::runtime::PublishedThrough<#name> for #ty {}
        }
    });
    let destination = slot_destination(&dictionary);

    Ok(quote! {
        impl ::ruststream::runtime::OutSlot for #name {
            const NAME: &'static str = #name_str;

            type Destination = #destination;

            #outgoing
        }

        #(#memberships)*
    })
}

/// What the marker offers a transform, folded out of its `#[publishes(..)]` list: the list as
/// types, nested right and closed with `()`, projected through `ListOffer`. A marker with no list
/// promises nothing, because it admits every declared message, so it offers `Reads`.
fn slot_destination(dictionary: &[Type]) -> TokenStream2 {
    if dictionary.is_empty() {
        return quote!(::ruststream::runtime::Reads);
    }
    let list = dictionary
        .iter()
        .rev()
        .fold(quote!(()), |tail, ty| quote!((#ty, #tail)));
    quote!(<#list as ::ruststream::runtime::ListOffer>::Offer)
}

/// The marker's `outgoing()` override, reporting its `#[publishes(..)]` list as the document's
/// outgoing messages: each listed type contributes what its own `#[derive(Outgoing)]` declares.
/// An empty list keeps the trait's own default (nothing declared).
fn slot_outgoing_metadata(name: &Ident, dictionary: &[Type]) -> TokenStream2 {
    if dictionary.is_empty() {
        return quote!();
    }
    let entries = dictionary.iter().map(|ty| {
        quote! {
            __rs_entries.extend(
                <#ty as ::ruststream::runtime::OutMessages<#name>>::outgoing(),
            );
        }
    });
    quote! {
        fn outgoing() -> ::ruststream::__private::Vec<::ruststream::runtime::OutgoingMessageMetadata> {
            let mut __rs_entries = ::ruststream::__private::Vec::new();
            #(#entries)*
            __rs_entries
        }
    }
}

pub(crate) fn expand_out_messages(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let syn::Data::Enum(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "OutMessages is derived on an enum whose variants each wrap one message model",
        ));
    };
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "an OutMessages set takes no generic parameters",
        ));
    }
    let models = out_messages_models(data)?;
    let name = &input.ident;
    let memberships = models.iter().enumerate().map(|(index, model)| {
        quote! {
            impl ::ruststream::runtime::ContainsMessage<
                #model,
                ::ruststream::runtime::SlotPos<#index>,
            > for #name
            {
            }
        }
    });
    // Each model already declares itself as a one-element set; the enum's contribution is the
    // concatenation, so a model's destination is described in exactly one place.
    let entries = models.iter().map(|model| {
        quote! {
            __rs_entries.extend(
                <#model as ::ruststream::runtime::OutMessages<__RsM>>::outgoing(),
            );
        }
    });
    Ok(quote! {
        #(#memberships)*

        impl<__RsM: ::ruststream::runtime::OutSlot> ::ruststream::runtime::OutMessages<__RsM>
            for #name
        where
            #(#models: ::ruststream::runtime::OutMessages<__RsM>,)*
        {
            fn outgoing()
                -> ::ruststream::__private::Vec<::ruststream::runtime::OutgoingMessageMetadata>
            {
                let mut __rs_entries = ::ruststream::__private::Vec::new();
                #(#entries)*
                __rs_entries
            }
        }
    })
}

/// The set enum's models, one per variant: each variant wraps exactly one type, and a
/// duplicate model is rejected (its membership index inference would be ambiguous).
fn out_messages_models(data: &syn::DataEnum) -> syn::Result<Vec<&Type>> {
    let mut models: Vec<&Type> = Vec::new();
    for variant in &data.variants {
        let syn::Fields::Unnamed(fields) = &variant.fields else {
            return Err(syn::Error::new_spanned(
                variant,
                "an OutMessages variant wraps exactly one message model: `Variant(Model)`",
            ));
        };
        if fields.unnamed.len() != 1 {
            return Err(syn::Error::new_spanned(
                fields,
                "an OutMessages variant wraps exactly one message model: `Variant(Model)`",
            ));
        }
        let model = &fields.unnamed[0].ty;
        let rendered = model.to_token_stream().to_string();
        if models
            .iter()
            .any(|existing| existing.to_token_stream().to_string() == rendered)
        {
            return Err(syn::Error::new_spanned(
                model,
                "this model is already in the set: a duplicate would make the membership \
                 index inference ambiguous",
            ));
        }
        models.push(model);
    }
    Ok(models)
}

/// Parses every `#[publishes(Type, ..)]` attribute into one list, rejecting a type listed twice
/// (a second entry would say nothing the first does not).
fn publishes_dictionary(attrs: &[Attribute]) -> syn::Result<Vec<Type>> {
    let mut dictionary: Vec<Type> = Vec::new();
    for attr in attrs
        .iter()
        .filter(|attr| attr.path().is_ident("publishes"))
    {
        let entries = attr.parse_args_with(Punctuated::<Type, Token![,]>::parse_terminated)?;
        for ty in entries {
            let name = ty.to_token_stream().to_string();
            if dictionary
                .iter()
                .any(|existing| existing.to_token_stream().to_string() == name)
            {
                return Err(syn::Error::new_spanned(
                    &ty,
                    "this type is already listed on the slot: a type appears once",
                ));
            }
            dictionary.push(ty);
        }
    }
    Ok(dictionary)
}
