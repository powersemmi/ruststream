//! Parsing of `#[subscriber(..)]` arguments and syntactic inspection of the handler input:
//! recovering the source type from a constructor expression, seeing through `Result` / `Vec`
//! return shapes, and collecting doc comments.

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{
    Attribute, Error, Expr, ExprCall, ExprLit, ExprMethodCall, ExprPath, ExprStruct, Ident, Lit,
    Meta, Path, Token, Type, TypePath, parenthesized, token,
};

/// Arguments to `#[subscriber(..)]`: the subscription source (a string literal name, or a
/// descriptor constructor `Type::new(..)` / `Type { .. }`), optionally wrapped in `batch(..)`
/// to consume whole batches, plus optional `publish("topic")` (the encoded reply destination),
/// `publish_raw("topic")` (the reply is published as raw bytes), `workers(n[, by_key])` (the
/// dispatch concurrency), `start_at(<position>)` (the subscription opens at that position),
/// and `raw` (the handler takes the payload bytes, undecoded) clauses, in any order.
pub(crate) struct SubscriberArgs {
    pub(crate) source: SourceArg,
    pub(crate) batch: bool,
    /// The `publish(..)` destination: a string literal, or a `&'static str` constant.
    pub(crate) publish: Option<Expr>,
    /// The `publish_raw(..)` destination (the reply bytes go out unencoded): a string literal,
    /// or a `&'static str` constant.
    pub(crate) publish_raw: Option<Expr>,
    pub(crate) workers: Option<WorkersArg>,
    pub(crate) on_failure: Option<FailureArg>,
    /// The `start_at(<position>)` clause: a broker position constructor the subscription is
    /// sought to before the first delivery.
    pub(crate) start_at: Option<Expr>,
    /// The `raw` flag keyword, kept as the parsed [`Ident`] so combination errors can point at it.
    pub(crate) raw: Option<Ident>,
}

/// The subscription the attribute fixes. The kind is always fixed here (the definition and the
/// broker extension traits bind on it); what may be deferred is the value that fills it.
pub(crate) enum SourceArg {
    /// `#[subscriber]`: the by-name source, its value left to the mount site.
    OpenName,
    /// `#[subscriber(RedisStream)]`: a named kind, its value left to the mount site.
    OpenKind(Type),
    /// `#[subscriber("orders")]` / `#[subscriber(RedisStream::new("orders").group("w"))]`: the
    /// value is fixed here too.
    Fixed(Expr),
}

/// The clause keywords, so a leading one is not mistaken for a source expression: they parse as
/// expressions too (`workers(4)` is a call, `raw` a path).
const CLAUSES: &[&str] = &[
    "raw",
    "on_failure",
    "publish",
    "publish_raw",
    "start_at",
    "workers",
];

/// True when the next tokens open a clause rather than a source. A clause keyword stands alone,
/// is followed by a comma, or opens a parenthesized argument list; anything else with the same
/// name (a `raw::Frames` path) is a source.
fn peeks_clause(input: ParseStream) -> bool {
    let fork = input.fork();
    let Ok(keyword) = fork.parse::<Ident>() else {
        return false;
    };
    if !CLAUSES.contains(&keyword.to_string().as_str()) {
        return false;
    }
    fork.is_empty() || fork.peek(Token![,]) || fork.peek(token::Paren)
}

pub(crate) struct WorkersArg {
    /// The worker count: an integer literal (zero rejected at expansion), or any `usize`
    /// expression - a constant, a static, a function call (zero rejected at registration).
    pub(crate) count: Expr,
    pub(crate) by_key: Option<Ident>,
}

/// The `on_failure(panic = .., decode = ..)` clause. Each key is optional; an omitted key keeps
/// the runtime default (a panic fails fast, a decode failure drops). The `decode` policy covers
/// both the payload codec and a `FromHeaders` contract - one materialization policy.
pub(crate) struct FailureArg {
    pub(crate) panic: Option<FailurePolicyArg>,
    pub(crate) decode: Option<FailurePolicyArg>,
}

/// One failure policy value: `fail_fast`, `drop`, `retry`, `retry_after(<duration>)`, `skip`,
/// or any expression evaluating to a `FailurePolicy` (a shared constant, a function call).
pub(crate) enum FailurePolicyArg {
    FailFast,
    Drop,
    Retry,
    RetryAfter(Expr),
    Skip,
    Value(Expr),
}

impl Parse for FailurePolicyArg {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let expr: Expr = input.parse()?;
        // The keyword vocabulary parses as expressions too (`drop` is a path,
        // `retry_after(..)` a call); recognize it first, and let anything else pass through as
        // a `FailurePolicy` value.
        if let Expr::Path(ExprPath {
            path, qself: None, ..
        }) = &expr
            && let Some(keyword) = path.get_ident()
        {
            let name = keyword.to_string();
            match name.as_str() {
                "fail_fast" => return Ok(Self::FailFast),
                "drop" => return Ok(Self::Drop),
                "retry" => return Ok(Self::Retry),
                "skip" => return Ok(Self::Skip),
                _ => {}
            }
            // A bare lowercase name outside the vocabulary is almost certainly a keyword typo,
            // not a constant (constants are SCREAMING_CASE by convention); rejecting it here
            // keeps the vocabulary error instead of an unresolved-name one.
            if name.chars().all(|c| c.is_ascii_lowercase() || c == '_') {
                return Err(Error::new(
                    keyword.span(),
                    "expected `fail_fast`, `drop`, `retry`, `retry_after(<duration>)`, `skip`, \
                     or a `FailurePolicy` expression (a constant, a function call)",
                ));
            }
        }
        if let Expr::Call(call) = &expr
            && let Expr::Path(ExprPath {
                path, qself: None, ..
            }) = &*call.func
            && path.is_ident("retry_after")
        {
            if call.args.len() != 1 {
                return Err(Error::new_spanned(
                    call,
                    "retry_after(..) takes exactly one duration argument",
                ));
            }
            return Ok(Self::RetryAfter(call.args[0].clone()));
        }
        Ok(Self::Value(expr))
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
                    return Err(Error::new(
                        key.span(),
                        "duplicate `panic` in on_failure(..)",
                    ));
                }
                panic = Some(value);
            } else if key == "decode" {
                if decode.is_some() {
                    return Err(Error::new(
                        key.span(),
                        "duplicate `decode` in on_failure(..)",
                    ));
                }
                decode = Some(value);
            } else {
                return Err(Error::new(
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
            return Err(Error::new(
                input.span(),
                "on_failure(..) needs at least one of `panic = ..` or `decode = ..`",
            ));
        }
        Ok(Self { panic, decode })
    }
}

impl Parse for SubscriberArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let (source, batch, named_here) = parse_source(input)?;
        let mut publish = None;
        let mut publish_raw = None;
        let mut workers = None;
        let mut on_failure = None;
        let mut start_at = None;
        let mut raw = None;
        let mut need_comma = named_here;
        while !input.is_empty() {
            if need_comma {
                input.parse::<Token![,]>()?;
                if input.is_empty() {
                    break;
                }
            }
            need_comma = true;
            let keyword: Ident = input.parse()?;
            if keyword == "raw" {
                if raw.is_some() {
                    return Err(Error::new(keyword.span(), "duplicate `raw`"));
                }
                raw = Some(keyword);
            } else if keyword == "on_failure" {
                if on_failure.is_some() {
                    return Err(Error::new(keyword.span(), "duplicate on_failure(..)"));
                }
                let content;
                parenthesized!(content in input);
                on_failure = Some(content.parse()?);
            } else if keyword == "publish" {
                if publish.is_some() {
                    return Err(Error::new(keyword.span(), "duplicate publish(..)"));
                }
                let content;
                parenthesized!(content in input);
                publish = Some(content.parse()?);
            } else if keyword == "publish_raw" {
                if publish_raw.is_some() {
                    return Err(Error::new(keyword.span(), "duplicate publish_raw(..)"));
                }
                let content;
                parenthesized!(content in input);
                publish_raw = Some(content.parse()?);
            } else if keyword == "start_at" {
                if start_at.is_some() {
                    return Err(Error::new(keyword.span(), "duplicate start_at(..)"));
                }
                let content;
                parenthesized!(content in input);
                if content.is_empty() {
                    return Err(Error::new(
                        keyword.span(),
                        "start_at(..) needs a position constructor; without the clause the \
                         subscription simply opens at the broker's default",
                    ));
                }
                start_at = Some(content.parse()?);
            } else if keyword == "workers" {
                if workers.is_some() {
                    return Err(Error::new(keyword.span(), "duplicate workers(..)"));
                }
                let content;
                parenthesized!(content in input);
                workers = Some(parse_workers(&content)?);
            } else {
                return Err(Error::new(
                    keyword.span(),
                    "expected `publish(\"reply-topic\")`, `publish_raw(\"reply-topic\")`, \
                     `workers(n[, by_key])`, `on_failure(panic = .., decode = ..)`, \
                     `start_at(<position>)`, or `raw`",
                ));
            }
        }
        Ok(Self {
            source,
            batch,
            publish,
            publish_raw,
            workers,
            on_failure,
            start_at,
            raw,
        })
    }
}

/// Parses the leading source argument, if the attribute opens on one. Reports the subscription,
/// whether the retiring `batch(..)` wrapper was there, and whether anything was consumed (which
/// decides if the first clause needs a separating comma).
///
/// An empty attribute, or one opening on a clause, leaves the subscription unnamed: it is the
/// by-name source with its value left out, the shortest of the source forms.
fn parse_source(input: ParseStream) -> syn::Result<(SourceArg, bool, bool)> {
    if input.is_empty() || peeks_clause(input) {
        return Ok((SourceArg::OpenName, false, false));
    }
    let mut batch = false;
    let mut expr: Expr = input.parse()?;
    // `batch(<source>)` is a marker around the usual source argument, not a constructor: unwrap
    // it and remember the form. A real constructor is never a bare one-segment call (free
    // functions are rejected by `source_tokens`), so this cannot misfire.
    if let Expr::Call(call) = &expr
        && let Expr::Path(ExprPath {
            path, qself: None, ..
        }) = &*call.func
        && path.is_ident("batch")
    {
        if call.args.len() != 1 {
            return Err(Error::new_spanned(
                call,
                "batch(..) takes exactly one source argument",
            ));
        }
        batch = true;
        expr = call.args[0].clone();
    }
    Ok((source_arg(expr), batch, true))
}

/// Classifies a parsed source expression: a bare type path names the kind and leaves the value
/// to the mount site, anything else (a string literal, a constructor, a builder chain) fixes it
/// here.
fn source_arg(expr: Expr) -> SourceArg {
    match expr {
        Expr::Path(ExprPath {
            attrs,
            qself: None,
            path,
        }) if attrs.is_empty() => SourceArg::OpenKind(Type::Path(TypePath {
            attrs: Vec::new(),
            qself: None,
            path,
        })),
        other => SourceArg::Fixed(other),
    }
}

/// Parses the inside of a `workers(..)` clause: the count, optionally followed by `by_key`.
fn parse_workers(content: ParseStream) -> syn::Result<WorkersArg> {
    let count: Expr = content.parse()?;
    let mut by_key = None;
    if content.peek(Token![,]) {
        content.parse::<Token![,]>()?;
        let marker: Ident = content.parse()?;
        if marker != "by_key" {
            return Err(Error::new(
                marker.span(),
                "expected `by_key`: workers(n) or workers(n, by_key)",
            ));
        }
        by_key = Some(marker);
    }
    Ok(WorkersArg { count, by_key })
}

/// Derives the subscription `Source` type and a constructor expression from the macro argument.
///
/// A string literal `"orders"` becomes `(Name, Name::new("orders"))`; a constructor expression
/// `RedisStream::new(..)` or `RedisStream { .. }` becomes `(RedisStream, <the expr verbatim>)` by
/// pulling the type out of the call/struct path. A builder chain
/// `SubscribeOptions::new(..).jetstream(..)` is followed down its receivers to that base
/// constructor, so fluent options that return `Self` can be written inline. Free functions
/// (`redis::stream(..)`) are still rejected - their result type is not visible in the tokens.
///
/// The two open forms carry the kind alone: they become `Unnamed<Kind>`, which is no
/// subscription source until the mount site names it.
pub(crate) fn source_tokens(source: &SourceArg) -> syn::Result<(TokenStream2, TokenStream2)> {
    let kind = match source {
        SourceArg::OpenName => quote!(::ruststream::Name),
        SourceArg::OpenKind(ty) => quote!(#ty),
        SourceArg::Fixed(expr) => {
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
            return Ok((quote!(#ty), quote!(#expr)));
        }
    };
    Ok((
        quote!(::ruststream::Unnamed<#kind>),
        quote!(::ruststream::Unnamed::<#kind>::new()),
    ))
}

/// Derives the position type from a `start_at(..)` argument, the same way [`source_tokens`]
/// recovers the source type: the constructor path (`MemoryPosition::start()`,
/// `KafkaPosition::latest()`, a builder chain on one) names the type.
pub(crate) fn position_type(expr: &Expr) -> syn::Result<Type> {
    source_type(expr).map_err(|_| {
        Error::new_spanned(
            expr,
            "expected a position constructor `Type::latest()` / `Type::new(..)` / `Type { .. }`, \
             or a builder chain on one - a free function does not expose its type to the macro",
        )
    })
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
            attrs: Vec::new(),
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
        return Err(Error::new_spanned(
            path,
            "expected `Type::new(..)`: the path must name a type and an associated constructor",
        ));
    }
    let segments = path.segments.iter().take(n - 1).cloned().collect();
    Ok(Type::Path(TypePath {
        attrs: Vec::new(),
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
    let Type::Path(TypePath {
        qself: None, path, ..
    }) = ty
    else {
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
        ..
    }) = err
    else {
        return None;
    };
    (err_path.segments.last()?.ident == "HandlerResult").then_some(ok)
}

/// If `ty` is syntactically `Vec<Reply>` (under any path prefix), returns the element type.
pub(crate) fn vec_element(ty: &Type) -> Option<&Type> {
    let Type::Path(TypePath {
        qself: None, path, ..
    }) = ty
    else {
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

fn unsupported_source(expr: &Expr) -> Error {
    Error::new_spanned(
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
