//! `#[derive(Outgoing)]`: everything a message type declares about being sent.

use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{ToTokens, format_ident, quote};
use syn::punctuated::Punctuated;
use syn::{Attribute, DeriveInput, Expr, Ident, LitStr, Meta, Token, Type};

use crate::parse::doc_description;

/// The parsed `#[outgoing(..)]` attribute: the declared destination and the header contract.
struct OutgoingArgs {
    name: Option<LitStr>,
    headers: Option<Type>,
}

/// The declared name, split into what the generated code needs from it.
struct Template {
    /// The literal parts around the placeholders, in order (one more than the placeholders).
    literals: Vec<String>,
    /// The placeholder names, in order.
    placeholders: Vec<Ident>,
}

impl Template {
    /// The `format!` string that renders the address: the literal parts with `{}` between them.
    fn format_string(&self) -> String {
        self.literals.join("{}")
    }
}

pub(crate) fn expand(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let args = parse_args(&input.attrs)?;
    let name = &input.ident;
    let name_str = name.to_string();
    let description = doc_description(&input.attrs);
    let vis = &input.vis;
    let contract = args.headers.as_ref().map_or_else(
        || quote!(::ruststream::NoHeaders),
        |headers| quote!(::ruststream::WithHeaders<#headers>),
    );
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let template = args.name.as_ref().map(parse_template).transpose()?;
    let address = args
        .name
        .as_ref()
        .map_or_else(|| quote!(""), |literal| quote!(#literal));
    let (form, parameters, template_items) = match &template {
        None => (quote!(::ruststream::CallerName), quote!(&[]), quote!()),
        Some(template) if template.placeholders.is_empty() => {
            (quote!(::ruststream::FixedName), quote!(&[]), quote!())
        }
        Some(template) => {
            let names = template
                .placeholders
                .iter()
                .map(|placeholder| LitStr::new(&placeholder.to_string(), placeholder.span()));
            let items = template_builder(name, vis, template, &input.generics);
            (
                quote!(::ruststream::NameTemplate),
                quote!(&[#(#names),*]),
                items,
            )
        }
    };

    let entries = declared_entries(template.is_some(), name, &ty_generics, &parameters);

    // The OutMessages impl adds its own marker type parameter; pushing it onto a clone of the
    // generics (after any lifetimes) keeps the emitted parameter order legal.
    let mut set_generics = input.generics.clone();
    set_generics
        .params
        .push(syn::parse_quote!(__RsM: ::ruststream::runtime::OutSlot));
    let (set_impl_generics, _, set_where_clause) = set_generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics ::ruststream::OutgoingDestination for #name #ty_generics
        #where_clause
        {
            type Form = #form;
            const ADDRESS: &'static str = #address;
            const PARAMETERS: &'static [&'static str] = #parameters;
        }

        impl #impl_generics ::ruststream::Message for #name #ty_generics #where_clause {
            const NAME: &'static str = #name_str;
            const DESCRIPTION: ::core::option::Option<&'static str> = #description;
        }

        impl #impl_generics ::ruststream::MessageHeaders for #name #ty_generics #where_clause {
            type Contract = #contract;
        }

        // The type as a one-element declared message set: `Out<.., Marker, TheType>`.
        impl #impl_generics
            ::ruststream::runtime::ContainsMessage<#name #ty_generics, ::ruststream::runtime::SlotPos<0>>
            for #name #ty_generics #where_clause
        {
        }

        impl #set_impl_generics ::ruststream::runtime::OutMessages<__RsM> for #name #ty_generics
        #set_where_clause
        {
            fn outgoing()
                -> ::std::vec::Vec<::ruststream::runtime::OutgoingMessageMetadata>
            {
                #entries
            }
        }

        #template_items
    })
}

/// The type's own contribution to the generated document.
///
/// A type declaring no destination says nothing about where it goes, so it contributes no
/// channel; the other two forms declare theirs, the templated one with its parameters.
fn declared_entries(
    declares: bool,
    name: &Ident,
    ty_generics: &syn::TypeGenerics<'_>,
    parameters: &TokenStream2,
) -> TokenStream2 {
    if !declares {
        return quote!(::std::vec::Vec::new());
    }
    quote! {
        ::std::vec![
            ::ruststream::runtime::OutgoingMessageMetadata::new(
                <#name #ty_generics as ::ruststream::OutgoingDestination>::ADDRESS,
                ::core::any::type_name::<#name #ty_generics>(),
            )
            .with_parameters(#parameters)
            .with_message_name({
                #[allow(unused_imports)]
                use ::ruststream::__private::NoMessageProbe as _;
                ::ruststream::__private::Probe::<#name #ty_generics>::new().message_name()
            })
            .with_message_description({
                #[allow(unused_imports)]
                use ::ruststream::__private::NoMessageProbe as _;
                ::ruststream::__private::Probe::<#name #ty_generics>::new().message_description()
            })
            .with_payload_schema({
                #[allow(unused_imports)]
                use ::ruststream::__private::NoSchemaProbe as _;
                ::ruststream::__private::Probe::<#name #ty_generics>::new().schema_json()
            })
            .with_headers_schema({
                #[allow(unused_imports)]
                use ::ruststream::__private::NoHeadersSchemaProbe as _;
                ::ruststream::__private::Probe::<#name #ty_generics>::new().headers_schema_json()
            }),
        ]
    }
}

/// Parses `#[outgoing(name = "..", headers = Type)]`, in any order, at most one of each.
fn parse_args(attrs: &[Attribute]) -> syn::Result<OutgoingArgs> {
    let mut args = OutgoingArgs {
        name: None,
        headers: None,
    };
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("outgoing")) {
        let metas = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
        for meta in metas {
            let Meta::NameValue(pair) = &meta else {
                return Err(syn::Error::new_spanned(
                    &meta,
                    "every #[outgoing(..)] parameter is `key = value`: `name = \"orders.done\"`, \
                     `headers = OrderMeta`",
                ));
            };
            if pair.path.is_ident("name") {
                let literal = string_literal(&pair.value)?;
                if args.name.replace(literal).is_some() {
                    return Err(syn::Error::new_spanned(
                        &meta,
                        "duplicate `name`: a message declares one destination",
                    ));
                }
            } else if pair.path.is_ident("headers") {
                let headers: Type = syn::parse2(pair.value.to_token_stream())?;
                if args.headers.replace(headers).is_some() {
                    return Err(syn::Error::new_spanned(
                        &meta,
                        "duplicate `headers`: a message declares one header contract",
                    ));
                }
            } else {
                return Err(syn::Error::new_spanned(
                    &pair.path,
                    "unknown #[outgoing(..)] parameter: expected `name` or `headers`",
                ));
            }
        }
    }
    Ok(args)
}

/// The `name = ..` value: a string literal, since the placeholders are read at compile time.
fn string_literal(value: &Expr) -> syn::Result<LitStr> {
    if let Expr::Lit(literal) = value
        && let syn::Lit::Str(string) = &literal.lit
    {
        return Ok(string.clone());
    }
    Err(syn::Error::new_spanned(
        value,
        "`name` is a string literal: the destination (and its `{placeholder}` segments) is read \
         at compile time",
    ))
}

/// Splits a declared name into its literal parts and its `{placeholder}` segments.
fn parse_template(literal: &LitStr) -> syn::Result<Template> {
    let declared = literal.value();
    let span = literal.span();
    let mut literals = Vec::new();
    let mut placeholders: Vec<Ident> = Vec::new();
    let mut current = String::new();
    let mut rest = declared.as_str();
    while let Some(open) = rest.find('{') {
        current.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        let Some(close) = after.find('}') else {
            return Err(syn::Error::new(
                span,
                "unclosed `{` in the declared name: every placeholder reads `{segment}`",
            ));
        };
        let placeholder = &after[..close];
        if placeholder.is_empty() {
            return Err(syn::Error::new(
                span,
                "empty placeholder in the declared name: name the segment, as in `{tenant}`",
            ));
        }
        let ident = syn::parse_str::<Ident>(placeholder).map_err(|_| {
            syn::Error::new(
                span,
                format!(
                    "`{placeholder}` is not a valid segment name: a placeholder becomes the \
                     setter that binds it, so it has to be an identifier"
                ),
            )
        })?;
        if placeholders.contains(&ident) {
            return Err(syn::Error::new(
                span,
                format!(
                    "`{placeholder}` appears twice in the declared name: one setter cannot bind \
                     two segments"
                ),
            ));
        }
        placeholders.push(ident);
        literals.push(std::mem::take(&mut current));
        rest = &after[close + 1..];
    }
    if rest.contains('}') {
        return Err(syn::Error::new(
            span,
            "unmatched `}` in the declared name: every placeholder reads `{segment}`",
        ));
    }
    current.push_str(rest);
    literals.push(current);
    Ok(Template {
        literals,
        placeholders,
    })
}

/// The generated address builder of a templated name: one setter per placeholder, and a publish
/// terminal that exists only once every one of them is bound.
fn template_builder(
    name: &Ident,
    vis: &syn::Visibility,
    template: &Template,
    generics: &syn::Generics,
) -> TokenStream2 {
    let module = format_ident!("__ruststream_{}_address", to_snake_case(&name.to_string()));
    let markers: Vec<Ident> = template
        .placeholders
        .iter()
        .map(|placeholder| Ident::new(&to_pascal_case(&placeholder.to_string()), Span::call_site()))
        .collect();
    let fields: Vec<Ident> = (0..template.placeholders.len())
        .map(|index| format_ident!("segment_{index}"))
        .collect();
    let states: Vec<Ident> = (0..template.placeholders.len())
        .map(|index| format_ident!("S{index}"))
        .collect();

    let marker_items = markers
        .iter()
        .zip(&template.placeholders)
        .map(|(marker, placeholder)| {
            let doc = format!(
                "The `{placeholder}` segment of the declared name, named in the compile error \
                 of a publish that never bound it."
            );
            quote! {
                #[doc = #doc]
                #[derive(Debug, Clone, Copy)]
                pub struct #marker;
            }
        });

    let defaults = markers
        .iter()
        .zip(&states)
        .map(|(marker, state)| quote!(#state = ::ruststream::runtime::MissingSegment<#marker>));
    let start_fields = fields
        .iter()
        .map(|field| quote!(#field: ::ruststream::runtime::MissingSegment::new()));
    let struct_fields = fields
        .iter()
        .zip(&states)
        .map(|(field, state)| quote!(#field: #state));

    let setters = template
        .placeholders
        .iter()
        .enumerate()
        .map(|(index, placeholder)| setter(index, placeholder, &markers, &fields, &states));

    let bound = fields.iter().map(|_| quote!(::std::string::String));
    let format_string = template.format_string();
    let format_args = fields.iter().map(|field| quote!(self.#field));
    let (_, ty_generics, _) = generics.split_for_impl();
    let mut template_generics = generics.clone();
    template_generics.params.push(syn::parse_quote!(__RsCont));
    let (template_impl_generics, _, template_where_clause) = template_generics.split_for_impl();
    let module_doc = format!(
        "The address builder of [`{name}`](super::{name}): one setter per placeholder of its \
         declared name."
    );

    quote! {
        #[doc = #module_doc]
        #[doc(hidden)]
        #vis mod #module {
            #(#marker_items)*

            /// One publish whose templated address is being bound, segment by segment.
            ///
            /// The publish terminal appears once every placeholder is bound; until then the
            /// unbound ones ride in this type, so the compile error names them.
            #[must_use = "an address builder does nothing until publish() is awaited"]
            pub struct To<Cont #(, #defaults)*> {
                pub(super) cont: Cont,
                #(pub(super) #struct_fields,)*
            }

            impl<Cont> To<Cont> {
                /// Opens the address builder over the publish it finishes.
                pub(super) fn start(cont: Cont) -> Self {
                    Self { cont #(, #start_fields)* }
                }
            }

            #(#setters)*

            impl<Cont: ::ruststream::runtime::PublishAt> To<Cont #(, #bound)*> {
                /// Renders the declared name with the bound segments and publishes to it.
                ///
                /// # Errors
                ///
                /// Returns [`PublishError`](::ruststream::runtime::PublishError) when the
                /// payload cannot be encoded, the typed headers cannot be serialized, or the
                /// broker rejects the message.
                pub async fn publish(
                    self,
                ) -> ::core::result::Result<
                    (),
                    ::ruststream::runtime::PublishError<
                        <Cont as ::ruststream::runtime::PublishAt>::Error,
                    >,
                > {
                    // A templated address is rendered per publish: that allocation is what
                    // computing an address at run time costs, and the fixed form avoids it.
                    let address = ::std::format!(#format_string #(, #format_args)*);
                    ::ruststream::runtime::PublishAt::publish_at(self.cont, &address).await
                }
            }
        }

        impl #template_impl_generics ::ruststream::runtime::TemplateAddress<__RsCont>
            for #name #ty_generics
        #template_where_clause
        {
            type Builder = #module::To<__RsCont>;

            fn begin(cont: __RsCont) -> Self::Builder {
                #module::To::start(cont)
            }
        }
    }
}

/// One placeholder's setter: it exists while that segment is unbound, and leaves the rendered
/// value in its place.
fn setter(
    index: usize,
    placeholder: &Ident,
    markers: &[Ident],
    fields: &[Ident],
    states: &[Ident],
) -> TokenStream2 {
    let marker = &markers[index];
    let before: Vec<TokenStream2> = states
        .iter()
        .enumerate()
        .map(|(position, state)| {
            if position == index {
                quote!(::ruststream::runtime::MissingSegment<#marker>)
            } else {
                quote!(#state)
            }
        })
        .collect();
    let after: Vec<TokenStream2> = states
        .iter()
        .enumerate()
        .map(|(position, state)| {
            if position == index {
                quote!(::std::string::String)
            } else {
                quote!(#state)
            }
        })
        .collect();
    let open = states
        .iter()
        .enumerate()
        .filter(|(position, _)| *position != index)
        .map(|(_, state)| quote!(#state));
    let moved = fields.iter().enumerate().map(|(position, field)| {
        if position == index {
            quote!(#field: ::std::string::ToString::to_string(&value))
        } else {
            quote!(#field: self.#field)
        }
    });
    let doc = format!("Binds the `{placeholder}` segment of the declared name.");
    quote! {
        impl<Cont #(, #open)*> To<Cont #(, #before)*> {
            #[doc = #doc]
            pub fn #placeholder(
                self,
                value: impl ::core::fmt::Display,
            ) -> To<Cont #(, #after)*> {
                To { cont: self.cont #(, #moved)* }
            }
        }
    }
}

/// `OrderPlaced` -> `order_placed`, for the generated module name.
fn to_snake_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    for (index, ch) in name.char_indices() {
        if ch.is_ascii_uppercase() {
            if index != 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

/// `tenant_id` -> `TenantId`, for the generated segment marker.
fn to_pascal_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut upper = true;
    for ch in name.chars() {
        if ch == '_' {
            upper = true;
        } else if upper {
            out.extend(ch.to_uppercase());
            upper = false;
        } else {
            out.push(ch);
        }
    }
    out
}
