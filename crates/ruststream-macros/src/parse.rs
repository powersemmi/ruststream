//! Parsing of `#[subscriber(..)]` arguments and syntactic inspection of the handler input:
//! recovering the source type from a constructor expression, seeing through `Result` / `Vec`
//! return shapes, and collecting doc comments.

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{
    Attribute, Expr, ExprCall, ExprLit, ExprMethodCall, ExprPath, ExprStruct, Ident, Lit, LitStr,
    Meta, Path, Token, Type, TypePath, parenthesized,
};

/// Arguments to `#[subscriber(..)]`: the subscription source (a string literal name, or a
/// descriptor constructor `Type::new(..)` / `Type { .. }`), optionally wrapped in `batch(..)`
/// to consume whole batches, plus optional `publish("topic")` (the reply destination) and
/// `workers(n[, by_key])` (the dispatch concurrency) clauses, in any order.
pub(crate) struct SubscriberArgs {
    pub(crate) source: Expr,
    pub(crate) batch: bool,
    pub(crate) publish: Option<LitStr>,
    pub(crate) workers: Option<WorkersArg>,
    pub(crate) on_failure: Option<FailureArg>,
}

pub(crate) struct WorkersArg {
    pub(crate) count: syn::LitInt,
    pub(crate) by_key: Option<Ident>,
}

/// The `on_failure(panic = .., decode = ..)` clause. Each key is optional; an omitted key keeps the
/// runtime default (a panic fails fast, a decode failure drops).
pub(crate) struct FailureArg {
    pub(crate) panic: Option<FailurePolicyArg>,
    pub(crate) decode: Option<FailurePolicyArg>,
}

/// One failure policy value: `fail_fast`, `drop`, `retry`, `retry_after(<duration>)`, or `skip`.
pub(crate) enum FailurePolicyArg {
    FailFast,
    Drop,
    Retry,
    RetryAfter(Expr),
    Skip,
}

impl Parse for FailurePolicyArg {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let ident: Ident = input.parse()?;
        match ident.to_string().as_str() {
            "fail_fast" => Ok(Self::FailFast),
            "drop" => Ok(Self::Drop),
            "retry" => Ok(Self::Retry),
            "skip" => Ok(Self::Skip),
            "retry_after" => {
                let content;
                parenthesized!(content in input);
                Ok(Self::RetryAfter(content.parse()?))
            }
            _ => Err(syn::Error::new(
                ident.span(),
                "expected `fail_fast`, `drop`, `retry`, `retry_after(<duration>)`, or `skip`",
            )),
        }
    }
}

impl Parse for FailureArg {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut panic = None;
        let mut decode = None;
        while !input.is_empty() {
            let key: Ident = input.parse()?;
            input.parse::<Token![=]>()?;
            let value: FailurePolicyArg = input.parse()?;
            if key == "panic" {
                if panic.is_some() {
                    return Err(syn::Error::new(
                        key.span(),
                        "duplicate `panic` in on_failure(..)",
                    ));
                }
                panic = Some(value);
            } else if key == "decode" {
                if decode.is_some() {
                    return Err(syn::Error::new(
                        key.span(),
                        "duplicate `decode` in on_failure(..)",
                    ));
                }
                decode = Some(value);
            } else {
                return Err(syn::Error::new(
                    key.span(),
                    "expected `panic = ..` or `decode = ..`",
                ));
            }
            if input.peek(Token![,]) {
                input.parse::<Token![,]>()?;
            } else {
                break;
            }
        }
        if panic.is_none() && decode.is_none() {
            return Err(syn::Error::new(
                input.span(),
                "on_failure(..) needs at least one of `panic = ..` or `decode = ..`",
            ));
        }
        Ok(Self { panic, decode })
    }
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
        let mut workers = None;
        let mut on_failure = None;
        while input.peek(Token![,]) {
            input.parse::<Token![,]>()?;
            let keyword: Ident = input.parse()?;
            if keyword == "on_failure" {
                if on_failure.is_some() {
                    return Err(syn::Error::new(keyword.span(), "duplicate on_failure(..)"));
                }
                let content;
                parenthesized!(content in input);
                on_failure = Some(content.parse()?);
            } else if keyword == "publish" {
                if publish.is_some() {
                    return Err(syn::Error::new(keyword.span(), "duplicate publish(..)"));
                }
                let content;
                parenthesized!(content in input);
                publish = Some(content.parse()?);
            } else if keyword == "workers" {
                if workers.is_some() {
                    return Err(syn::Error::new(keyword.span(), "duplicate workers(..)"));
                }
                let content;
                parenthesized!(content in input);
                let count: syn::LitInt = content.parse()?;
                let mut by_key = None;
                if content.peek(Token![,]) {
                    content.parse::<Token![,]>()?;
                    let marker: Ident = content.parse()?;
                    if marker != "by_key" {
                        return Err(syn::Error::new(
                            marker.span(),
                            "expected `by_key`: workers(n) or workers(n, by_key)",
                        ));
                    }
                    by_key = Some(marker);
                }
                workers = Some(WorkersArg { count, by_key });
            } else {
                return Err(syn::Error::new(
                    keyword.span(),
                    "expected `publish(\"reply-topic\")`, `workers(n[, by_key])`, or \
                     `on_failure(panic = .., decode = ..)`",
                ));
            }
        }
        Ok(Self {
            source,
            batch,
            publish,
            workers,
            on_failure,
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
pub(crate) fn source_tokens(expr: &Expr) -> syn::Result<(TokenStream2, TokenStream2)> {
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
pub(crate) fn publish_result_reply(ty: &Type) -> Option<&Type> {
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

/// If `ty` is syntactically `Vec<Reply>` (under any path prefix), returns the element type.
pub(crate) fn vec_element(ty: &Type) -> Option<&Type> {
    let Type::Path(TypePath { qself: None, path }) = ty else {
        return None;
    };
    let last = path.segments.last()?;
    if last.ident != "Vec" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &last.arguments else {
        return None;
    };
    let mut args = args.args.iter();
    let (Some(syn::GenericArgument::Type(elem)), None) = (args.next(), args.next()) else {
        return None;
    };
    Some(elem)
}

fn unsupported_source(expr: &Expr) -> syn::Error {
    syn::Error::new_spanned(
        expr,
        "expected a string literal name, `Type::new(..)`, `Type { .. }`, or a builder chain on \
         one of those - a free function does not expose its type to the macro",
    )
}

/// Collects doc-comment lines from `attrs` into a single description literal, or `None`.
pub(crate) fn doc_description(attrs: &[Attribute]) -> TokenStream2 {
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
