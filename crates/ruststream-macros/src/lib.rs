//! Procedural macros for [RustStream](https://github.com/powersemmi/ruststream).
//!
//! Re-exported from the `ruststream` crate under the `macros` feature; depend on that rather than
//! on this crate directly.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{
    Attribute, DeriveInput, Expr, ExprLit, FnArg, Ident, ItemFn, Lit, LitStr, Meta, PatType,
    ReturnType, Token, Type, parse_macro_input,
};

/// Arguments to `#[subscriber(..)]`: the subscribe topic and an optional bare `publish` flag.
struct SubscriberArgs {
    topic: LitStr,
    publish: bool,
}

impl Parse for SubscriberArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let topic: LitStr = input.parse()?;
        let mut publish = false;
        if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            let keyword: Ident = input.parse()?;
            if keyword != "publish" {
                return Err(syn::Error::new(keyword.span(), "expected `publish`"));
            }
            publish = true;
        }
        Ok(Self { topic, publish })
    }
}

/// Turns an `async fn` handler into a mountable subscriber definition.
///
/// ```ignore
/// /// Processes incoming orders.
/// #[subscriber("orders")]
/// async fn handle(order: &Order) -> HandlerResult { HandlerResult::Ack }
/// // later: broker_scope.include(handle, JsonCodec);
///
/// // reply form: the return value is encoded and published through the `Publication` passed at
/// // wiring time (it carries the destination topic and reply codec).
/// #[subscriber("requests", publish)]
/// async fn reply(req: &Request) -> Response { /* ... */ }
/// // later: broker_scope.include_publishing(reply, JsonCodec, publication);
/// ```
///
/// Without `publish(..)` the handler returns any `IntoHandlerResult` (a `HandlerResult`, `()`, or
/// `Result<_, E>`). With `publish(..)` it returns the reply value to publish.
#[proc_macro_attribute]
pub fn subscriber(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as SubscriberArgs);
    let func = parse_macro_input!(item as ItemFn);
    expand(&args, &func).unwrap_or_else(|err| err.to_compile_error().into())
}

fn expand(args: &SubscriberArgs, func: &ItemFn) -> syn::Result<TokenStream> {
    let vis = &func.vis;
    let name = &func.sig.ident;
    let block = &func.block;

    let first = func.sig.inputs.first().ok_or_else(|| {
        syn::Error::new_spanned(
            &func.sig,
            "a #[subscriber] handler must take exactly one message parameter",
        )
    })?;
    let FnArg::Typed(PatType { pat, ty, .. }) = first else {
        return Err(syn::Error::new_spanned(
            first,
            "a #[subscriber] handler cannot take `self`",
        ));
    };
    let Type::Reference(reference) = &**ty else {
        return Err(syn::Error::new_spanned(
            ty,
            "the message parameter must be a reference `&T`",
        ));
    };
    let input_ty = &reference.elem;
    let description = doc_description(&func.attrs);
    let topic = &args.topic;

    // Captures the input type's JSON Schema for AsyncAPI when it implements `JsonSchema` (and the
    // `asyncapi` feature is on), via the autoref-specialization probe; `None` otherwise. The
    // concrete input type makes the trait selection resolve at the call site.
    let input_schema = quote! {
        fn input_schema(&self) -> ::core::option::Option<::std::string::String> {
            #[allow(unused_imports)]
            use ::ruststream::__private::NoSchemaProbe as _;
            ::ruststream::__private::Probe::<#input_ty>::new().schema_json()
        }
    };

    // Optional second handler parameter: the per-delivery `&mut Context`. If the user declares it,
    // bind it to their name; otherwise generate an ignored binding.
    let ctx_param = if let Some(FnArg::Typed(PatType { pat, .. })) = func.sig.inputs.get(1) {
        quote!(#pat)
    } else {
        quote!(_ctx)
    };

    let body = if args.publish {
        let reply_ty = match &func.sig.output {
            ReturnType::Type(_, ty) => &**ty,
            ReturnType::Default => {
                return Err(syn::Error::new_spanned(
                    &func.sig,
                    "a publishing handler must return the reply value",
                ));
            }
        };
        quote! {
            #[allow(non_camel_case_types)]
            #vis struct #name;

            impl ::ruststream::runtime::PublishingDef for #name {
                type Input = #input_ty;
                type Reply = #reply_ty;

                fn subscribe_channel(&self) -> &str { #topic }

                fn description(&self) -> ::core::option::Option<&str> {
                    #description
                }

                #input_schema

                fn call(
                    &self,
                    #pat: &#input_ty,
                ) -> impl ::core::future::Future<Output = #reply_ty> + ::core::marker::Send {
                    async move #block
                }
            }
        }
    } else {
        quote! {
            #[derive(Clone, Copy)]
            #[allow(non_camel_case_types)]
            #vis struct #name;

            impl ::ruststream::runtime::Handler<#input_ty> for #name {
                async fn handle(
                    &self,
                    #pat: &#input_ty,
                    #ctx_param: &mut ::ruststream::runtime::Context<'_>,
                ) -> ::ruststream::runtime::HandlerResult {
                    ::ruststream::runtime::IntoHandlerResult::into_handler_result(
                        (async move #block).await,
                    )
                }
            }

            impl ::ruststream::runtime::SubscriberDef for #name {
                type Input = #input_ty;
                type Handler = Self;

                fn channel(&self) -> &str { #topic }

                fn description(&self) -> ::core::option::Option<&str> {
                    #description
                }

                #input_schema

                fn into_handler(self) -> Self { self }
            }
        }
    };

    Ok(body.into())
}

/// Derives [`Message`](../ruststream/trait.Message.html) metadata: the type name and its doc
/// comment.
///
/// ```ignore
/// /// An order placed by a customer.
/// #[derive(Message)]
/// struct Order { id: u32 }
/// // Order::NAME == "Order", Order::DESCRIPTION == Some("An order placed by a customer.")
/// ```
#[proc_macro_derive(Message)]
pub fn derive_message(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let name = &input.ident;
    let name_str = name.to_string();
    let description = doc_description(&input.attrs);
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    quote! {
        impl #impl_generics ::ruststream::Message for #name #ty_generics #where_clause {
            const NAME: &'static str = #name_str;
            const DESCRIPTION: ::core::option::Option<&'static str> = #description;
        }
    }
    .into()
}

/// Collects doc-comment lines from `attrs` into a single description literal, or `None`.
fn doc_description(attrs: &[Attribute]) -> TokenStream2 {
    let lines: Vec<String> = attrs
        .iter()
        .filter(|attr| attr.path().is_ident("doc"))
        .filter_map(|attr| match &attr.meta {
            Meta::NameValue(nv) => match &nv.value {
                Expr::Lit(ExprLit {
                    lit: Lit::Str(text),
                    ..
                }) => Some(text.value().trim().to_owned()),
                _ => None,
            },
            _ => None,
        })
        .collect();

    if lines.is_empty() {
        quote!(::core::option::Option::None)
    } else {
        let joined = lines.join("\n");
        quote!(::core::option::Option::Some(#joined))
    }
}
