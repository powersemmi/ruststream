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
/// descriptor constructor `Type::new(..)` / `Type { .. }`), optionally wrapped in `batch(..)`
/// to consume whole batches, and an optional `publish("topic")` clause naming the reply
/// destination.
struct SubscriberArgs {
    source: Expr,
    batch: bool,
    publish: Option<LitStr>,
}

impl Parse for SubscriberArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut source: Expr = input.parse()?;
        // `batch(<source>)` is a marker around the usual source argument, not a constructor:
        // unwrap it and remember the form. A real constructor is never a bare one-segment call
        // (free functions are rejected by `source_tokens`), so this cannot misfire.
        let mut batch = false;
        if let Expr::Call(call) = &source {
            if let Expr::Path(ExprPath {
                path, qself: None, ..
            }) = &*call.func
            {
                if path.is_ident("batch") {
                    if call.args.len() != 1 {
                        return Err(syn::Error::new_spanned(
                            call,
                            "batch(..) takes exactly one source argument",
                        ));
                    }
                    batch = true;
                    source = call.args[0].clone();
                }
            }
        }
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
        Ok(Self {
            source,
            batch,
            publish,
        })
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

/// If `ty` is syntactically `Result<Reply, HandlerResult>` (under any path prefix, e.g.
/// `std::result::Result` / `ruststream::runtime::HandlerResult`), returns the reply type.
///
/// The check is token-based: a type alias hiding the `Result` is not recognized and is treated as
/// a plain reply type, which then fails to compile with a `Serialize` error the user can act on.
fn publish_result_reply(ty: &Type) -> Option<&Type> {
    let Type::Path(TypePath { qself: None, path }) = ty else {
        return None;
    };
    let last = path.segments.last()?;
    if last.ident != "Result" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &last.arguments else {
        return None;
    };
    let mut args = args.args.iter();
    let (Some(syn::GenericArgument::Type(ok)), Some(syn::GenericArgument::Type(err)), None) =
        (args.next(), args.next(), args.next())
    else {
        return None;
    };
    let Type::Path(TypePath {
        qself: None,
        path: err_path,
    }) = err
    else {
        return None;
    };
    (err_path.segments.last()?.ident == "HandlerResult").then_some(ok)
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
/// // later: broker_scope.include(handle);
///
/// // reply form: the return value is encoded and published to "responses" through the
/// // TypedPublisher (broker + reply codec) passed at wiring time.
/// #[subscriber("requests", publish("responses"))]
/// async fn reply(req: &Request) -> Response { /* ... */ }
/// // later: broker_scope.include_publishing(reply, typed_publisher);
///
/// // reply form with explicit ack control: `Ok` publishes the reply, `Err` skips it and the
/// // dispatcher acts on the returned HandlerResult.
/// #[subscriber("requests", publish("responses"))]
/// async fn confirm(req: &Request) -> Result<Response, HandlerResult> { /* ... */ }
///
/// // batch form: the handler takes the whole decoded batch as a slice; the source's
/// // subscriber must implement BatchSubscriber. Mounted with include_batch.
/// #[subscriber(batch("orders"))]
/// async fn bill(orders: &[Order]) -> HandlerResult { /* settles the whole batch */ }
/// ```
///
/// Without `publish(..)` the handler returns any `IntoHandlerResult` (a `HandlerResult`, `()`, or
/// `Result<_, E>`). With `publish(..)` it returns the reply value to publish, or
/// `Result<Reply, HandlerResult>` to control acknowledgement: `Err(result)` publishes nothing and
/// returns `result` to the dispatcher. The `Result` form is detected syntactically, so spell it
/// out in the signature (a type alias is treated as a plain reply type).
///
/// Wrapping the source in `batch(..)` switches the definition to a `BatchDef`: the handler takes
/// `&[T]` and runs once per batch pulled from the broker's `BatchSubscriber` (use the `Buffered`
/// adapter for brokers without native batching). `batch(..)` cannot be combined with
/// `publish(..)`. The source type is recovered from the constructor path, so a generic source
/// spells its parameters: `batch(Buffered::<Name>::new(Name::new("orders")))`.
///
/// In both forms the handler may declare an optional second parameter, the per-delivery
/// `&mut Context`, to read app state or publish manually.
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

/// The pieces of the handler shared by both expansion forms, extracted from the signature.
struct HandlerParts<'a> {
    vis: &'a syn::Visibility,
    name: &'a Ident,
    block: &'a syn::Block,
    pat: &'a syn::Pat,
    input_ty: &'a Type,
    description: TokenStream2,
    source_ty: TokenStream2,
    source_expr: TokenStream2,
    input_schema: TokenStream2,
    message_meta: TokenStream2,
    ctx_param: TokenStream2,
}

fn handler_parts<'a>(args: &SubscriberArgs, func: &'a ItemFn) -> syn::Result<HandlerParts<'a>> {
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
    // In the batch(..) form the parameter is the whole batch `&[T]`; the def's `Input` is the
    // element type either way.
    let input_ty = if args.batch {
        match &*reference.elem {
            Type::Slice(slice) => &*slice.elem,
            other => {
                return Err(syn::Error::new_spanned(
                    other,
                    "a batch handler takes the whole batch as a slice: `&[T]`",
                ));
            }
        }
    } else {
        if matches!(&*reference.elem, Type::Slice(_)) {
            return Err(syn::Error::new_spanned(
                &reference.elem,
                "a slice parameter needs the batch source form: #[subscriber(batch(..))]",
            ));
        }
        &*reference.elem
    };
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

    // Captures the input type's `Message` name / description when it implements that trait, via
    // the same autoref-specialization probe; `None` otherwise.
    let message_meta = quote! {
        fn message_name(&self) -> ::core::option::Option<&'static str> {
            #[allow(unused_imports)]
            use ::ruststream::__private::NoMessageProbe as _;
            ::ruststream::__private::Probe::<#input_ty>::new().message_name()
        }

        fn message_description(&self) -> ::core::option::Option<&'static str> {
            #[allow(unused_imports)]
            use ::ruststream::__private::NoMessageProbe as _;
            ::ruststream::__private::Probe::<#input_ty>::new().message_description()
        }
    };

    // Optional second handler parameter: the per-delivery `&mut Context`. If the user declares it,
    // bind it to their name; otherwise generate an ignored binding.
    let ctx_param = if let Some(FnArg::Typed(PatType { pat, .. })) = func.sig.inputs.get(1) {
        quote!(#pat)
    } else {
        quote!(_ctx)
    };

    Ok(HandlerParts {
        vis: &func.vis,
        name: &func.sig.ident,
        block: &func.block,
        pat,
        input_ty,
        description,
        source_ty,
        source_expr,
        input_schema,
        message_meta,
        ctx_param,
    })
}

fn expand(args: &SubscriberArgs, func: &ItemFn) -> syn::Result<TokenStream> {
    let parts = handler_parts(args, func)?;
    let body = if args.batch {
        if let Some(publish) = &args.publish {
            return Err(syn::Error::new(
                publish.span(),
                "batch(..) cannot be combined with publish(..)",
            ));
        }
        expand_batch(&parts)
    } else if let Some(reply_topic) = &args.publish {
        expand_publishing(&parts, func, reply_topic)?
    } else {
        expand_subscribing(&parts)
    };
    Ok(body.into())
}

fn expand_batch(parts: &HandlerParts<'_>) -> TokenStream2 {
    let HandlerParts {
        vis,
        name,
        block,
        pat,
        input_ty,
        description,
        source_ty,
        source_expr,
        input_schema,
        message_meta,
        ctx_param,
    } = parts;

    quote! {
            #[derive(Clone, Copy)]
            #[allow(non_camel_case_types)]
            #vis struct #name;

            impl ::ruststream::runtime::SliceHandler<#input_ty> for #name {
                async fn handle_slice(
                    &self,
                    #pat: &[#input_ty],
                    #ctx_param: &mut ::ruststream::runtime::Context<'_>,
                ) -> ::ruststream::runtime::HandlerResult {
                    ::ruststream::runtime::IntoHandlerResult::into_handler_result(
                        (async move #block).await,
                    )
                }
            }

            impl ::ruststream::runtime::BatchDef for #name {
                type Input = #input_ty;
                type Handler = Self;
                type Source = #source_ty;

                fn source(&self) -> Self::Source { #source_expr }

                fn description(&self) -> ::core::option::Option<&str> {
                    #description
                }

                #input_schema

                #message_meta

                fn into_handler(self) -> Self { self }
            }
    }
}

fn expand_publishing(
    parts: &HandlerParts<'_>,
    func: &ItemFn,
    reply_topic: &LitStr,
) -> syn::Result<TokenStream2> {
    let HandlerParts {
        vis,
        name,
        block,
        pat,
        input_ty,
        description,
        source_ty,
        source_expr,
        input_schema,
        message_meta,
        ctx_param,
    } = parts;

    let declared_ty = match &func.sig.output {
        ReturnType::Type(_, ty) => &**ty,
        ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                &func.sig,
                "a publishing handler must return the reply value",
            ));
        }
    };
    // `-> Result<Reply, HandlerResult>` lets the handler skip the publish: `Err(result)` is
    // returned to the dispatcher as-is. A plain `-> Reply` is wrapped in `Ok` here. The check
    // is syntactic, so a type alias hiding the `Result` is treated as a plain reply type.
    let (reply_ty, call_body) = match publish_result_reply(declared_ty) {
        Some(reply_ty) => (reply_ty, quote!((async move #block).await)),
        None => (
            declared_ty,
            quote!(::core::result::Result::Ok((async move #block).await)),
        ),
    };
    Ok(quote! {
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

            #message_meta

            async fn call(
                &self,
                #pat: &#input_ty,
                #ctx_param: &mut ::ruststream::runtime::Context<'_>,
            ) -> ::core::result::Result<#reply_ty, ::ruststream::runtime::HandlerResult> {
                #call_body
            }
        }
    })
}

fn expand_subscribing(parts: &HandlerParts<'_>) -> TokenStream2 {
    let HandlerParts {
        vis,
        name,
        block,
        pat,
        input_ty,
        description,
        source_ty,
        source_expr,
        input_schema,
        message_meta,
        ctx_param,
    } = parts;

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

                #message_meta

                fn into_handler(self) -> Self { self }
            }
    }
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
