//! `#[derive(InMessages)]`: a declared set of message models a handler receives, with the rule
//! that picks the variant of one delivery.

use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{ToTokens, format_ident, quote, quote_spanned};
use syn::punctuated::Punctuated;
use syn::spanned::Spanned;
use syn::{
    Attribute, Data, DeriveInput, Expr, ExprLit, ExprPath, GenericArgument, Ident, Lit, LitByteStr,
    LitStr, Meta, PathArguments, Token, Type, Variant,
};

/// How a set picks the variant of one delivery.
enum Rule {
    /// The variants are tried in the order of the enum; the first that decodes wins.
    LeftToRight,
    /// The named function reads the delivery and returns the kind to decode.
    Discriminate(ExprPath),
    /// The named type reads the decode codec and the delivery and returns the kind to decode.
    DiscriminateWithCodec(ExprPath),
    /// The named header carries one declared value per variant.
    Header(LitStr),
}

/// One variant of the set: the model it wraps, and the header value it declares under the
/// header rule.
struct Model<'a> {
    variant: &'a Variant,
    ty: &'a Type,
    value: Option<LitStr>,
}

impl Model<'_> {
    fn ident(&self) -> &Ident {
        &self.variant.ident
    }

    fn name(&self) -> LitStr {
        LitStr::new(&self.variant.ident.to_string(), self.variant.ident.span())
    }
}

pub(crate) fn expand(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let Data::Enum(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "InMessages is derived on an enum whose variants each wrap one message model",
        ));
    };
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "an InMessages set takes no generic parameters",
        ));
    }
    if data.variants.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "an InMessages set names at least one model: `Variant(Model)`",
        ));
    }
    let rule = parse_rule(&input.attrs)?;
    let models = parse_models(data, &rule)?;

    let name = &input.ident;
    let name_str = LitStr::new(&name.to_string(), name.span());
    let vis = &input.vis;
    let kind = format_ident!("{name}Kind");
    let kind_doc =
        format!("The kinds of [`{name}`]: one per variant, what a discriminator of the set names.");
    // The kind enum's variants take the call site's span: a kind nobody constructs (a left to
    // right set names none) is not reported as a dead variant of the set itself.
    let kinds = models
        .iter()
        .map(|model| Ident::new(&model.ident().to_string(), Span::call_site()));
    // One membership impl per variant, spanned at it: a model named twice, through an alias
    // the spelling check cannot see through, is two impls of one trait for one type.
    let memberships = models.iter().map(|model| {
        let ty = model.ty;
        quote_spanned! {model.variant.span()=>
            impl ::ruststream::runtime::VariantOf<#name> for #ty {}
        }
    });
    let decode = decode_body(name, &name_str, &kind, &rule, &models);
    let entries = models.iter().map(|model| entry(name, &rule, model));

    // The declaration marker is spanned at the enum's name, so a `MessageInfo` derived beside
    // this one is reported there as the conflict it is.
    let declaration = quote_spanned! {name.span()=>
        impl ::ruststream::runtime::MessageDeclaration for #name {}
    };

    // A codec-discriminated set decodes only under the codecs its discriminator accepts, so its
    // decode is written per codec and the set implements the codec-parameterized trait alone.
    let set_impl = if let Rule::DiscriminateWithCodec(discriminator) = &rule {
        quote! {
            impl<__RsCodec> ::ruststream::runtime::DecodeSet<__RsCodec> for #name
            where
                __RsCodec: ::ruststream::codec::Codec,
                #discriminator: ::ruststream::runtime::CodecDiscriminator<#kind, __RsCodec>,
            {
                fn decode<__RsMsg: ::ruststream::IncomingMessage>(
                    codec: &__RsCodec,
                    msg: &__RsMsg,
                ) -> ::core::result::Result<Self, ::ruststream::codec::CodecError> {
                    #decode
                }

                fn incoming() -> ::ruststream::__private::Vec<::ruststream::runtime::IncomingMessageMetadata> {
                    ::ruststream::__private::vec![#(#entries),*]
                }
            }
        }
    } else {
        quote! {
            impl ::ruststream::runtime::InMessages for #name {
                type Kind = #kind;

                fn decode<__RsCodec, __RsMsg>(
                    codec: &__RsCodec,
                    msg: &__RsMsg,
                ) -> ::core::result::Result<Self, ::ruststream::codec::CodecError>
                where
                    __RsCodec: ::ruststream::codec::Codec,
                    __RsMsg: ::ruststream::IncomingMessage,
                {
                    #decode
                }

                fn incoming() -> ::ruststream::__private::Vec<::ruststream::runtime::IncomingMessageMetadata> {
                    ::ruststream::__private::vec![#(#entries),*]
                }
            }
        }
    };

    Ok(quote! {
        #[doc = #kind_doc]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        #[allow(dead_code)]
        #vis enum #kind {
            #(#kinds),*
        }

        impl ::ruststream::runtime::Input for #name {
            type Axis = ::ruststream::runtime::SoloSet<#name>;
        }

        #declaration

        #(#memberships)*

        #set_impl
    })
}

/// The body of `InMessages::decode` under the set's rule.
fn decode_body(
    name: &Ident,
    name_str: &LitStr,
    kind: &Ident,
    rule: &Rule,
    models: &[Model<'_>],
) -> TokenStream2 {
    let errors = quote!(::ruststream::__private::in_messages);
    match rule {
        Rule::LeftToRight => {
            let slots: Vec<Ident> = (0..models.len())
                .map(|index| format_ident!("__rs_failed_{index}"))
                .collect();
            let tries = models.iter().zip(&slots).map(|(model, slot)| {
                let (ty, ident, label) = (model.ty, model.ident(), model.name());
                // Spanned at the variant, so a model that cannot be one is reported there.
                quote_spanned! {model.variant.span()=>
                    let #slot = match <#ty as ::ruststream::runtime::InVariant<#name>>::decode_variant(
                        codec,
                        payload,
                    ) {
                        ::core::result::Result::Ok(model) => {
                            return ::core::result::Result::Ok(#name::#ident(model));
                        }
                        ::core::result::Result::Err(err) => (#label, err),
                    };
                }
            });
            quote! {
                let payload = ::ruststream::IncomingMessage::payload(msg);
                #(#tries)*
                ::core::result::Result::Err(#errors::every_variant_failed(
                    #name_str,
                    [#(#slots),*],
                ))
            }
        }
        Rule::Discriminate(path) => {
            discriminated(name, name_str, kind, models, &quote!(#path(msg)))
        }
        Rule::DiscriminateWithCodec(discriminator) => {
            let chosen = quote! {
                <#discriminator as ::ruststream::runtime::CodecDiscriminator<
                    #kind,
                    __RsCodec,
                >>::kind(codec, msg)
            };
            discriminated(name, name_str, kind, models, &chosen)
        }
        Rule::Header(header) => {
            let arms = models.iter().map(|model| {
                let decode = decode_arm(name, model);
                let value = model
                    .value
                    .as_ref()
                    .map(|value| LitByteStr::new(value.value().as_bytes(), value.span()));
                quote!(#value => #decode,)
            });
            let values = models.iter().map(|model| &model.value);
            quote! {
                let payload = ::ruststream::IncomingMessage::payload(msg);
                let ::core::option::Option::Some(seen) = ::ruststream::HeaderMap::get(
                    ::ruststream::IncomingMessage::headers(msg),
                    #header,
                ) else {
                    return ::core::result::Result::Err(#errors::header_missing(#name_str, #header));
                };
                match seen {
                    #(#arms)*
                    other => ::core::result::Result::Err(#errors::header_unknown(
                        #name_str,
                        #header,
                        other,
                        &[#(#values),*],
                    )),
                }
            }
        }
    }
}

/// The decode of a set whose discriminator returns the kind: `chosen` is the call that asks it.
fn discriminated(
    name: &Ident,
    name_str: &LitStr,
    kind: &Ident,
    models: &[Model<'_>],
    chosen: &TokenStream2,
) -> TokenStream2 {
    let errors = quote!(::ruststream::__private::in_messages);
    let arms = models.iter().map(|model| {
        let (ident, decode) = (model.ident(), decode_arm(name, model));
        quote!(::core::option::Option::Some(#kind::#ident) => #decode,)
    });
    quote! {
        let payload = ::ruststream::IncomingMessage::payload(msg);
        match #chosen {
            #(#arms)*
            ::core::option::Option::None => {
                ::core::result::Result::Err(#errors::no_variant_chosen(#name_str))
            }
        }
    }
}

/// One variant's decode, as the arm of a rule that already picked it.
fn decode_arm(name: &Ident, model: &Model<'_>) -> TokenStream2 {
    let (ty, ident) = (model.ty, model.ident());
    quote_spanned! {model.variant.span()=>
        ::core::result::Result::map(
            <#ty as ::ruststream::runtime::InVariant<#name>>::decode_variant(codec, payload),
            #name::#ident,
        )
    }
}

/// One variant's entry in the generated document: the model's own name, description and
/// schemas, its lane, and the header value that names it under the header rule.
fn entry(name: &Ident, rule: &Rule, model: &Model<'_>) -> TokenStream2 {
    let ty = model.ty;
    let discriminator = match (rule, &model.value) {
        (Rule::Header(header), Some(value)) => quote! {
            .with_discriminator(::ruststream::runtime::HeaderDiscriminator::new(#header, #value))
        },
        _ => quote!(),
    };
    quote_spanned! {model.variant.span()=>
        <#ty as ::ruststream::runtime::InVariant<#name>>::describe()
            .with_payload_schema({
                #[allow(unused_imports)]
                use ::ruststream::__private::NoSchemaProbe as _;
                ::ruststream::__private::Probe::<#ty>::new().schema_json()
            })
            .with_headers_schema({
                #[allow(unused_imports)]
                use ::ruststream::__private::NoHeadersSchemaProbe as _;
                ::ruststream::__private::Probe::<#ty>::new().headers_schema_json()
            })
            #discriminator
    }
}

/// Reads every `#[in_messages(..)]` on the enum into the set's rule.
fn parse_rule(attrs: &[Attribute]) -> syn::Result<Rule> {
    let mut discriminate: Option<ExprPath> = None;
    let mut with_codec: Option<ExprPath> = None;
    let mut header: Option<LitStr> = None;
    for attr in attrs
        .iter()
        .filter(|attr| attr.path().is_ident("in_messages"))
    {
        let metas = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
        for meta in metas {
            let Meta::NameValue(pair) = &meta else {
                return Err(syn::Error::new_spanned(
                    &meta,
                    "every #[in_messages(..)] parameter is `key = value`: `discriminate = \
                     by_event_header`, `discriminate_with_codec = ByRecord`, `header = \"event\"`",
                ));
            };
            if pair.path.is_ident("discriminate") {
                let Expr::Path(path) = &pair.value else {
                    return Err(syn::Error::new_spanned(
                        &pair.value,
                        "`discriminate` names a function that reads the delivery and returns \
                         the kind: `discriminate = by_event_header`",
                    ));
                };
                if discriminate.replace(path.clone()).is_some() {
                    return Err(syn::Error::new_spanned(
                        &meta,
                        "duplicate `discriminate`: a set picks its variant by one rule",
                    ));
                }
            } else if pair.path.is_ident("discriminate_with_codec") {
                let Expr::Path(path) = &pair.value else {
                    return Err(syn::Error::new_spanned(
                        &pair.value,
                        "`discriminate_with_codec` names a type implementing `CodecDiscriminator`, \
                         which reads the codec and the delivery: `discriminate_with_codec = \
                         ByRecord`",
                    ));
                };
                if with_codec.replace(path.clone()).is_some() {
                    return Err(syn::Error::new_spanned(
                        &meta,
                        "duplicate `discriminate_with_codec`: a set picks its variant by one rule",
                    ));
                }
            } else if pair.path.is_ident("header") {
                let literal = string_literal(&pair.value, "header")?;
                if header.replace(literal).is_some() {
                    return Err(syn::Error::new_spanned(
                        &meta,
                        "duplicate `header`: a set reads its variant from one header",
                    ));
                }
            } else if pair.path.is_ident("value") {
                return Err(syn::Error::new_spanned(
                    &pair.path,
                    "`value` goes on a variant, under `header = \"..\"` on the enum: \
                     `#[in_messages(value = \"created\")]`",
                ));
            } else {
                return Err(syn::Error::new_spanned(
                    &pair.path,
                    "unknown #[in_messages(..)] parameter: expected `discriminate`, \
                     `discriminate_with_codec` or `header`",
                ));
            }
        }
    }
    match (discriminate, with_codec, header) {
        (Some(_), Some(discriminator), _) => Err(syn::Error::new_spanned(
            discriminator,
            "`discriminate` and `discriminate_with_codec` together: a set picks its variant by \
             one rule",
        )),
        (Some(_), None, Some(header)) => Err(syn::Error::new(
            header.span(),
            "`discriminate` and `header` together: a set picks its variant by one rule",
        )),
        (None, Some(_), Some(header)) => Err(syn::Error::new(
            header.span(),
            "`discriminate_with_codec` and `header` together: a set picks its variant by one rule",
        )),
        (Some(path), None, None) => Ok(Rule::Discriminate(path)),
        (None, Some(discriminator), None) => Ok(Rule::DiscriminateWithCodec(discriminator)),
        (None, None, Some(header)) => Ok(Rule::Header(header)),
        (None, None, None) => Ok(Rule::LeftToRight),
    }
}

/// The set's models, one per variant: each variant wraps exactly one type, a model appears
/// once, and under the header rule every variant declares its value, each value once.
fn parse_models<'a>(data: &'a syn::DataEnum, rule: &Rule) -> syn::Result<Vec<Model<'a>>> {
    let mut models: Vec<Model<'a>> = Vec::new();
    for variant in &data.variants {
        let syn::Fields::Unnamed(fields) = &variant.fields else {
            return Err(syn::Error::new_spanned(
                variant,
                "an InMessages variant wraps exactly one message model: `Variant(Model)`",
            ));
        };
        if fields.unnamed.len() != 1 {
            return Err(syn::Error::new_spanned(
                fields,
                "an InMessages variant wraps exactly one message model: `Variant(Model)`",
            ));
        }
        let ty = &fields.unnamed[0].ty;
        if borrows(ty) {
            return Err(syn::Error::new_spanned(
                ty,
                "a variant owns its model past the delivery: a type over a lifetime borrows the \
                 payload, so it cannot be one",
            ));
        }
        let rendered = ty.to_token_stream().to_string();
        if models
            .iter()
            .any(|model| model.ty.to_token_stream().to_string() == rendered)
        {
            return Err(syn::Error::new_spanned(
                ty,
                "this model is already in the set: a model appears once, so one variant \
                 receives it",
            ));
        }
        let value = parse_value(variant, rule, &models)?;
        models.push(Model { variant, ty, value });
    }
    Ok(models)
}

/// Whether a model type names a lifetime: a reference, or a path with a lifetime argument
/// (`Frame<'static>`). What a hand-written alias hides, the trait bound still rejects.
fn borrows(ty: &Type) -> bool {
    match ty {
        Type::Reference(_) => true,
        Type::Path(path) => path.path.segments.iter().any(|segment| {
            if let PathArguments::AngleBracketed(arguments) = &segment.arguments {
                arguments
                    .args
                    .iter()
                    .any(|arg| matches!(arg, GenericArgument::Lifetime(_)))
            } else {
                false
            }
        }),
        _ => false,
    }
}

/// The `#[in_messages(value = "..")]` of one variant, demanded under the header rule and
/// rejected under every other.
fn parse_value(
    variant: &Variant,
    rule: &Rule,
    earlier: &[Model<'_>],
) -> syn::Result<Option<LitStr>> {
    let mut value: Option<LitStr> = None;
    for attr in variant
        .attrs
        .iter()
        .filter(|attr| attr.path().is_ident("in_messages"))
    {
        let metas = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
        for meta in metas {
            let Meta::NameValue(pair) = &meta else {
                return Err(syn::Error::new_spanned(
                    &meta,
                    "a variant's #[in_messages(..)] takes `value = \"..\"`: the header value \
                     that names it",
                ));
            };
            if !pair.path.is_ident("value") {
                return Err(syn::Error::new_spanned(
                    &pair.path,
                    "unknown #[in_messages(..)] parameter on a variant: expected `value`",
                ));
            }
            let literal = string_literal(&pair.value, "value")?;
            if value.replace(literal).is_some() {
                return Err(syn::Error::new_spanned(
                    &meta,
                    "duplicate `value`: a variant is named by one header value",
                ));
            }
        }
    }
    let Rule::Header(header) = rule else {
        if let Some(value) = value {
            return Err(syn::Error::new(
                value.span(),
                "`value` needs `#[in_messages(header = \"..\")]` on the enum, which names the \
                 header the value is read from",
            ));
        }
        return Ok(None);
    };
    let Some(value) = value else {
        return Err(syn::Error::new_spanned(
            variant,
            format!(
                "the set reads its variant from the `{}` header: `#[in_messages(value = \
                 \"..\")]` names this variant's value",
                header.value()
            ),
        ));
    };
    if let Some(taken) = earlier.iter().find(|model| {
        model
            .value
            .as_ref()
            .is_some_and(|v| v.value() == value.value())
    }) {
        return Err(syn::Error::new(
            value.span(),
            format!(
                "`{}` is already the value of `{}`: a value names one variant",
                value.value(),
                taken.ident()
            ),
        ));
    }
    Ok(Some(value))
}

/// A `key = ".."` value: a string literal, since the derive reads it at compile time.
fn string_literal(value: &Expr, key: &str) -> syn::Result<LitStr> {
    if let Expr::Lit(ExprLit {
        lit: Lit::Str(string),
        ..
    }) = value
    {
        return Ok(string.clone());
    }
    Err(syn::Error::new_spanned(
        value,
        format!("`{key}` is a string literal: the derive reads it at compile time"),
    ))
}

#[cfg(test)]
mod tests {
    use super::{Rule, expand, parse_models, parse_rule};
    use syn::DeriveInput;

    fn rejection(input: &DeriveInput) -> String {
        expand(input)
            .expect_err("the derive rejects the input")
            .to_string()
    }

    #[test]
    fn a_set_without_an_attribute_tries_its_variants_left_to_right() {
        let input: DeriveInput = syn::parse_quote!(
            enum OrderEvent {
                Created(OrderCreated),
                Cancelled(OrderCancelled),
            }
        );
        assert!(matches!(parse_rule(&input.attrs), Ok(Rule::LeftToRight)));
        let expanded = expand(&input).expect("a plain set expands").to_string();
        assert!(expanded.contains("enum OrderEventKind"));
        assert!(expanded.contains("every_variant_failed"));
        // One membership impl per variant: a model named twice, through an alias or not, is
        // two impls of one trait, which the compiler rejects.
        assert_eq!(expanded.matches("VariantOf < OrderEvent >").count(), 2);
    }

    #[test]
    fn the_function_rule_names_a_path() {
        let input: DeriveInput = syn::parse_quote!(
            #[in_messages(discriminate = by_event_header)]
            enum OrderEvent {
                Created(OrderCreated),
            }
        );
        assert!(matches!(
            parse_rule(&input.attrs),
            Ok(Rule::Discriminate(_))
        ));
        let expanded = expand(&input)
            .expect("a discriminated set expands")
            .to_string();
        assert!(expanded.contains("by_event_header (msg)"));
        assert!(expanded.contains("no_variant_chosen"));
    }

    #[test]
    fn the_codec_rule_names_a_discriminator_type() {
        let input: DeriveInput = syn::parse_quote!(
            #[in_messages(discriminate_with_codec = registry::ByRecord)]
            enum OrderEvent {
                Created(OrderCreated),
            }
        );
        assert!(matches!(
            parse_rule(&input.attrs),
            Ok(Rule::DiscriminateWithCodec(_))
        ));
        let expanded = expand(&input)
            .expect("a codec-discriminated set expands")
            .to_string();
        assert!(expanded.contains(
            "registry :: ByRecord : :: ruststream :: runtime :: CodecDiscriminator < \
             OrderEventKind , __RsCodec >"
        ));
        assert!(expanded.contains("no_variant_chosen"));
        // Its decode is written per codec, so the set implements only the codec-parameterized
        // trait.
        assert!(!expanded.contains("runtime :: InMessages for"));
        assert!(expanded.contains("DecodeSet < __RsCodec > for OrderEvent"));
    }

    #[test]
    fn the_header_rule_reads_one_value_per_variant() {
        let input: DeriveInput = syn::parse_quote!(
            #[in_messages(header = "event")]
            enum OrderEvent {
                #[in_messages(value = "created")]
                Created(OrderCreated),
                #[in_messages(value = "cancelled")]
                Cancelled(OrderCancelled),
            }
        );
        let rule = parse_rule(&input.attrs).expect("the header rule parses");
        assert!(matches!(&rule, Rule::Header(header) if header.value() == "event"));
        let syn::Data::Enum(data) = &input.data else {
            unreachable!()
        };
        let models = parse_models(data, &rule).expect("every variant names its value");
        let values: Vec<String> = models
            .iter()
            .map(|model| {
                model
                    .value
                    .as_ref()
                    .map(syn::LitStr::value)
                    .unwrap_or_default()
            })
            .collect();
        assert_eq!(values, ["created", "cancelled"]);
        let expanded = expand(&input).expect("a header set expands").to_string();
        assert!(expanded.contains("header_missing"));
        assert!(expanded.contains("with_discriminator"));
    }

    #[test]
    fn a_rule_is_one_of_the_two() {
        let input: DeriveInput = syn::parse_quote!(
            #[in_messages(discriminate = pick, header = "event")]
            enum OrderEvent {
                #[in_messages(value = "created")]
                Created(OrderCreated),
            }
        );
        assert!(rejection(&input).contains("`discriminate` and `header` together"));
    }

    #[test]
    fn the_codec_rule_excludes_the_other_two() {
        let with_function: DeriveInput = syn::parse_quote!(
            #[in_messages(discriminate = pick, discriminate_with_codec = ByRecord)]
            enum OrderEvent {
                Created(OrderCreated),
            }
        );
        assert!(
            rejection(&with_function)
                .contains("`discriminate` and `discriminate_with_codec` together")
        );
        let with_header: DeriveInput = syn::parse_quote!(
            #[in_messages(discriminate_with_codec = ByRecord, header = "event")]
            enum OrderEvent {
                #[in_messages(value = "created")]
                Created(OrderCreated),
            }
        );
        assert!(
            rejection(&with_header).contains("`discriminate_with_codec` and `header` together")
        );
    }

    #[test]
    fn a_value_needs_the_header_rule() {
        let input: DeriveInput = syn::parse_quote!(
            enum OrderEvent {
                #[in_messages(value = "created")]
                Created(OrderCreated),
            }
        );
        assert!(rejection(&input).contains("`value` needs `#[in_messages(header = \"..\")]`"));
    }

    #[test]
    fn the_header_rule_demands_a_value_on_every_variant() {
        let input: DeriveInput = syn::parse_quote!(
            #[in_messages(header = "event")]
            enum OrderEvent {
                #[in_messages(value = "created")]
                Created(OrderCreated),
                Cancelled(OrderCancelled),
            }
        );
        assert!(rejection(&input).contains("names this variant's value"));
    }

    #[test]
    fn a_value_names_one_variant() {
        let input: DeriveInput = syn::parse_quote!(
            #[in_messages(header = "event")]
            enum OrderEvent {
                #[in_messages(value = "created")]
                Created(OrderCreated),
                #[in_messages(value = "created")]
                Cancelled(OrderCancelled),
            }
        );
        assert!(rejection(&input).contains("`created` is already the value of `Created`"));
    }

    #[test]
    fn a_model_appears_once() {
        let input: DeriveInput = syn::parse_quote!(
            enum OrderEvent {
                Created(OrderCreated),
                Again(OrderCreated),
            }
        );
        assert!(rejection(&input).contains("this model is already in the set"));
    }

    #[test]
    fn a_variant_owns_its_model() {
        let view: DeriveInput = syn::parse_quote!(
            enum OrderEvent {
                Raw(Frame<'static>),
            }
        );
        assert!(rejection(&view).contains("a variant owns its model"));
        let reference: DeriveInput = syn::parse_quote!(
            enum OrderEvent {
                Raw(&'static [u8]),
            }
        );
        assert!(rejection(&reference).contains("a variant owns its model"));
        let array: DeriveInput = syn::parse_quote!(
            enum OrderEvent {
                Raw([u8; 4]),
            }
        );
        assert!(expand(&array).is_ok(), "an array model owns its bytes");
    }

    #[test]
    fn a_variant_wraps_exactly_one_model() {
        let unit: DeriveInput = syn::parse_quote!(
            enum OrderEvent {
                Created,
            }
        );
        assert!(rejection(&unit).contains("wraps exactly one message model"));
        let pair: DeriveInput = syn::parse_quote!(
            enum OrderEvent {
                Created(OrderCreated, OrderCancelled),
            }
        );
        assert!(rejection(&pair).contains("wraps exactly one message model"));
    }

    #[test]
    fn the_shape_is_a_non_empty_enum_without_generics() {
        let structure: DeriveInput = syn::parse_quote!(
            struct OrderEvent(OrderCreated);
        );
        assert!(rejection(&structure).contains("derived on an enum"));
        let generic: DeriveInput = syn::parse_quote!(
            enum OrderEvent<T> {
                Created(T),
            }
        );
        assert!(rejection(&generic).contains("no generic parameters"));
        let empty: DeriveInput = syn::parse_quote!(
            enum OrderEvent {}
        );
        assert!(rejection(&empty).contains("names at least one model"));
    }

    #[test]
    fn the_attribute_vocabulary_is_closed() {
        let unknown: DeriveInput = syn::parse_quote!(
            #[in_messages(tag = "event")]
            enum OrderEvent {
                Created(OrderCreated),
            }
        );
        assert!(rejection(&unknown).contains("unknown #[in_messages(..)] parameter"));
        let misplaced: DeriveInput = syn::parse_quote!(
            #[in_messages(value = "created")]
            enum OrderEvent {
                Created(OrderCreated),
            }
        );
        assert!(rejection(&misplaced).contains("`value` goes on a variant"));
        let bare: DeriveInput = syn::parse_quote!(
            #[in_messages(header)]
            enum OrderEvent {
                Created(OrderCreated),
            }
        );
        assert!(rejection(&bare).contains("is `key = value`"));
        let not_a_path: DeriveInput = syn::parse_quote!(
            #[in_messages(discriminate = "pick")]
            enum OrderEvent {
                Created(OrderCreated),
            }
        );
        assert!(rejection(&not_a_path).contains("`discriminate` names a function"));
        let not_a_type: DeriveInput = syn::parse_quote!(
            #[in_messages(discriminate_with_codec = "ByRecord")]
            enum OrderEvent {
                Created(OrderCreated),
            }
        );
        assert!(rejection(&not_a_type).contains("`discriminate_with_codec` names a type"));
        let not_a_literal: DeriveInput = syn::parse_quote!(
            #[in_messages(header = EVENT)]
            enum OrderEvent {
                Created(OrderCreated),
            }
        );
        assert!(rejection(&not_a_literal).contains("`header` is a string literal"));
        let on_variant: DeriveInput = syn::parse_quote!(
            #[in_messages(header = "event")]
            enum OrderEvent {
                #[in_messages(tag = "created")]
                Created(OrderCreated),
            }
        );
        assert!(rejection(&on_variant).contains("on a variant: expected `value`"));
        let bare_on_variant: DeriveInput = syn::parse_quote!(
            #[in_messages(header = "event")]
            enum OrderEvent {
                #[in_messages(value)]
                Created(OrderCreated),
            }
        );
        assert!(rejection(&bare_on_variant).contains("takes `value = \"..\"`"));
    }

    #[test]
    fn a_malformed_attribute_is_refused() {
        let on_enum: DeriveInput = syn::parse_quote!(
            #[in_messages(+)]
            enum OrderEvent {
                Created(OrderCreated),
            }
        );
        let on_variant: DeriveInput = syn::parse_quote!(
            #[in_messages(header = "event")]
            enum OrderEvent {
                #[in_messages(+)]
                Created(OrderCreated),
            }
        );
        let value_not_a_literal: DeriveInput = syn::parse_quote!(
            #[in_messages(header = "event")]
            enum OrderEvent {
                #[in_messages(value = CREATED)]
                Created(OrderCreated),
            }
        );

        for input in [on_enum, on_variant, value_not_a_literal] {
            assert!(expand(&input).is_err());
        }
    }

    #[test]
    fn each_key_is_given_once() {
        let twice: DeriveInput = syn::parse_quote!(
            #[in_messages(header = "event", header = "kind")]
            enum OrderEvent {
                #[in_messages(value = "created")]
                Created(OrderCreated),
            }
        );
        assert!(rejection(&twice).contains("duplicate `header`"));
        let twice: DeriveInput = syn::parse_quote!(
            #[in_messages(discriminate = pick)]
            #[in_messages(discriminate = pick)]
            enum OrderEvent {
                Created(OrderCreated),
            }
        );
        assert!(rejection(&twice).contains("duplicate `discriminate`"));
        let twice: DeriveInput = syn::parse_quote!(
            #[in_messages(discriminate_with_codec = ByRecord)]
            #[in_messages(discriminate_with_codec = ByRecord)]
            enum OrderEvent {
                Created(OrderCreated),
            }
        );
        assert!(rejection(&twice).contains("duplicate `discriminate_with_codec`"));
        let twice: DeriveInput = syn::parse_quote!(
            #[in_messages(header = "event")]
            enum OrderEvent {
                #[in_messages(value = "created", value = "placed")]
                Created(OrderCreated),
            }
        );
        assert!(rejection(&twice).contains("duplicate `value`"));
    }
}
