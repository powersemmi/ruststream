//! Procedural macros for [RustStream](https://github.com/powersemmi/ruststream).
//!
//! Re-exported from the `ruststream` crate under the `macros` feature; depend on that rather than
//! on this crate directly.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{
    Attribute, DeriveInput, Expr, ExprCall, ExprLit, ExprMethodCall, ExprPath, ExprStruct, FnArg,
    Ident, ItemFn, Lit, LitStr, Meta, PatType, Path, ReturnType, Token, Type, TypePath,
    parenthesized, parse_macro_input,
};

/// Arguments to `#[subscriber(..)]`: the subscription source (a string literal name, or a
/// descriptor constructor `Type::new(..)` / `Type { .. }`) and an optional `publish("topic")`
/// clause naming the reply destination.
struct SubscriberArgs {
    source: Expr,
    publish: Option<LitStr>,
}

impl Parse for SubscriberArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let source: Expr = input.parse()?;
        let mut publish = None;
        if input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            let keyword: Ident = input.parse()?;
            if keyword != "publish" {
                return Err(syn::Error::new(
                    keyword.span(),
                    "expected `publish(\"reply-topic\")`",
                ));
            }
            let content;
            parenthesized!(content in input);
            publish = Some(content.parse()?);
        }
        Ok(Self { source, publish })
    }
}

/// Derives the subscription `Source` type and a constructor expression from the macro argument.
///
/// A string literal `"orders"` becomes `(Name, Name::new("orders"))`; a constructor expression
/// `RedisStream::new(..)` or `RedisStream { .. }` becomes `(RedisStream, <the expr verbatim>)` by
/// pulling the type out of the call/struct path. A builder chain
/// `SubscribeOptions::new(..).jetstream(..)` is followed down its receivers to that base
/// constructor, so fluent options that return `Self` can be written inline. Free functions
/// (`redis::stream(..)`) are still rejected - their result type is not visible in the tokens.
fn source_tokens(expr: &Expr) -> syn::Result<(TokenStream2, TokenStream2)> {
    if let Expr::Lit(ExprLit {
        lit: Lit::Str(name),
        ..
    }) = expr
    {
        return Ok((
            quote!(::ruststream::Name),
            quote!(::ruststream::Name::new(#name)),
        ));
    }

    let ty = source_type(expr)?;
    Ok((quote!(#ty), quote!(#expr)))
}

/// Recovers the source type from a constructor expression, following a builder chain's receivers
/// down to the base `Type::new(..)` / `Type { .. }`. Methods in the chain are assumed to return
/// `Self`; a builder that returns a different type produces a type-mismatch the user can see and
/// fix. Free functions and other shapes are rejected (their type is not visible in the tokens).
fn source_type(expr: &Expr) -> syn::Result<Type> {
    match expr {
        Expr::Call(ExprCall { func, .. }) => match &**func {
            Expr::Path(ExprPath {
                path, qself: None, ..
            }) => type_from_constructor_path(path),
            _ => Err(unsupported_source(expr)),
        },
        Expr::Struct(ExprStruct { path, .. }) => Ok(Type::Path(TypePath {
            qself: None,
            path: path.clone(),
        })),
        Expr::MethodCall(ExprMethodCall { receiver, .. }) => source_type(receiver),
        _ => Err(unsupported_source(expr)),
    }
}

/// Builds the type from a constructor path by dropping the final segment (`Type::new` -> `Type`).
fn type_from_constructor_path(path: &Path) -> syn::Result<Type> {
    let n = path.segments.len();
    if n < 2 {
        return Err(syn::Error::new_spanned(
            path,
            "expected `Type::new(..)`: the path must name a type and an associated constructor",
        ));
    }
    let segments = path.segments.iter().take(n - 1).cloned().collect();
    Ok(Type::Path(TypePath {
        qself: None,
        path: Path {
            leading_colon: path.leading_colon,
            segments,
        },
    }))
}

fn unsupported_source(expr: &Expr) -> syn::Error {
    syn::Error::new_spanned(
        expr,
        "expected a string literal name, `Type::new(..)`, `Type { .. }`, or a builder chain on \
         one of those - a free function does not expose its type to the macro",
    )
}

/// Turns an `async fn` handler into a mountable subscriber definition.
///
/// ```ignore
/// /// Processes incoming orders.
/// #[subscriber("orders")]
/// async fn handle(order: &Order) -> HandlerResult { HandlerResult::Ack }
/// // later: broker_scope.include(handle, JsonCodec);
///
/// // reply form: the return value is encoded and published to "responses" through the
/// // TypedPublisher (broker + reply codec) passed at wiring time.
/// #[subscriber("requests", publish("responses"))]
/// async fn reply(req: &Request) -> Response { /* ... */ }
/// // later: broker_scope.include_publishing(reply, JsonCodec, typed_publisher);
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

/// Generates a `main` entry point for a `RustStream` service.
///
/// Place it on a synchronous, argument-free function that builds and returns a `RustStream`
/// application. The expansion keeps the function and adds a `main` that hands it to
/// `ruststream::runtime::cli::run_main`, producing a binary that understands the `run` and
/// `asyncapi gen` commands with no hand-written runtime boilerplate.
///
/// ```ignore
/// #[ruststream::app]
/// fn app() -> RustStream {
///     RustStream::new(AppInfo::new("svc", "0.1.0")).register_broker(MemoryBroker::new())
/// }
/// ```
#[proc_macro_attribute]
pub fn app(attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);
    expand_app(&attr.into(), &func).unwrap_or_else(|err| err.to_compile_error().into())
}

fn expand_app(attr: &TokenStream2, func: &ItemFn) -> syn::Result<TokenStream> {
    if !attr.is_empty() {
        return Err(syn::Error::new_spanned(
            attr,
            "#[ruststream::app] takes no arguments",
        ));
    }
    if let Some(asyncness) = func.sig.asyncness {
        return Err(syn::Error::new_spanned(
            asyncness,
            "#[ruststream::app] requires a synchronous builder returning `RustStream`",
        ));
    }
    if !func.sig.inputs.is_empty() {
        return Err(syn::Error::new_spanned(
            &func.sig.inputs,
            "#[ruststream::app] builder must take no arguments",
        ));
    }
    let name = &func.sig.ident;
    Ok(quote! {
        #func

        fn main() -> ::std::process::ExitCode {
            ::ruststream::runtime::cli::run_main(#name)
        }
    }
    .into())
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
    let (source_ty, source_expr) = source_tokens(&args.source)?;

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

    let body = if let Some(reply_topic) = &args.publish {
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
                type Source = #source_ty;

                fn source(&self) -> Self::Source { #source_expr }
                fn reply_name(&self) -> &str { #reply_topic }

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
                type Source = #source_ty;

                fn source(&self) -> Self::Source { #source_expr }

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
