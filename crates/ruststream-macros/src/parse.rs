//! Parsing of `#[subscriber(..)]` arguments and syntactic inspection of the handler input:
//! recovering the source type from a constructor expression, seeing through `Result` / `Vec`
//! return shapes, and collecting doc comments.

use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::{
    Attribute, Error, Expr, ExprCall, ExprLit, ExprMethodCall, ExprPath, ExprStruct, Generics,
    Ident, Lit, Meta, Path, Token, Type, TypePath, parenthesized, token,
};

/// Arguments to `#[subscriber(..)]`: the subscription source (a string literal name, or a
/// descriptor constructor `Type::new(..)` / `Type { .. }`), plus optional `reply` /
/// `reply("topic")` (the return value is published; the reply type decides its wire and,
/// where it declares one, its destination), `workers(n[, by_key])` (the dispatch concurrency),
/// and `start_at(<position>)` (the subscription opens at that position) clauses, in any order.
/// The subscription form (single, batch) is never spelled here: it is inferred from the payload
/// parameter's type.
pub(crate) struct SubscriberArgs {
    pub(crate) source: SourceArg,
    pub(crate) reply: Option<ReplyArg>,
    pub(crate) workers: Option<WorkersArg>,
    pub(crate) on_failure: Option<FailureArg>,
    /// The `start_at(<position>)` clause: a broker position constructor the subscription is
    /// sought to before the first delivery.
    pub(crate) start_at: Option<Expr>,
}

/// The `reply` clause: the reply is published either way, and the clause says where.
pub(crate) enum ReplyArg {
    /// `reply`: the reply type's own `#[outgoing(name = "..")]` declaration is the
    /// destination.
    Declared,
    /// `reply("topic")`: the default destination for a reply type that declares none. A
    /// string literal, or a `&'static str` constant.
    Default(Expr),
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
/// expressions too (`workers(4)` is a call).
// The retired `publish` and `publish_raw` stay in the peek list, so a leading one meets the list
// of accepted clauses instead of parsing as a source expression.
const CLAUSES: &[&str] = &[
    "on_failure",
    "publish",
    "publish_raw",
    "reply",
    "start_at",
    "threads",
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

/// The concurrency clause: `workers(n[, by_key])` on the app's runtime, or `threads(n[, by_key])`
/// on dedicated threads. Both fill the one concurrency position.
pub(crate) struct WorkersArg {
    /// The clause keyword, `workers` or `threads`.
    pub(crate) keyword: Ident,
    /// The worker count: an integer literal (zero rejected at expansion), or any `usize`
    /// expression - a constant, a static, a function call (zero rejected at registration).
    pub(crate) count: Expr,
    pub(crate) by_key: Option<Ident>,
}

impl WorkersArg {
    /// Whether the clause asks for dedicated threads.
    pub(crate) fn dedicated(&self) -> bool {
        self.keyword == "threads"
    }
}

/// The `on_failure(panic = .., decode = ..)` clause. Each key is optional; an omitted key keeps
/// the runtime default (a panic fails fast, a decode failure drops). The `decode` policy covers
/// both the payload codec and a `Headers` contract - one materialization policy.
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
        let (source, named_here) = parse_source(input)?;
        let mut reply = None;
        let mut workers = None;
        let mut on_failure = None;
        let mut start_at = None;
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
            if keyword == "on_failure" {
                if on_failure.is_some() {
                    return Err(Error::new(keyword.span(), "duplicate on_failure(..)"));
                }
                let content;
                parenthesized!(content in input);
                on_failure = Some(content.parse()?);
            } else if keyword == "reply" {
                if reply.is_some() {
                    return Err(Error::new(keyword.span(), "duplicate reply clause"));
                }
                reply = Some(parse_reply(input, &keyword)?);
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
            } else if keyword == "workers" || keyword == "threads" {
                if let Some(WorkersArg { keyword: first, .. }) = &workers {
                    let message = if *first == keyword {
                        format!("duplicate {keyword}(..)")
                    } else {
                        "workers(..) and threads(..) fill the same concurrency position: name one \
                         of them"
                            .to_owned()
                    };
                    return Err(Error::new(keyword.span(), message));
                }
                let content;
                parenthesized!(content in input);
                workers = Some(parse_workers(keyword, &content)?);
            } else {
                return Err(Error::new(
                    keyword.span(),
                    "expected `reply` / `reply(\"reply-topic\")`, `workers(n[, by_key])`, \
                     `threads(n[, by_key])`, `on_failure(panic = .., decode = ..)`, or \
                     `start_at(<position>)`",
                ));
            }
        }
        Ok(Self {
            source,
            reply,
            workers,
            on_failure,
            start_at,
        })
    }
}

/// Parses a `reply` clause: bare, or with the default destination in parentheses.
fn parse_reply(input: ParseStream, keyword: &Ident) -> syn::Result<ReplyArg> {
    if !input.peek(token::Paren) {
        return Ok(ReplyArg::Declared);
    }
    let content;
    parenthesized!(content in input);
    if content.is_empty() {
        return Err(Error::new(
            keyword.span(),
            "reply() names nothing: write `reply` to send the reply where its type declares, \
             or `reply(\"dest\")` to name the destination of a reply type that declares none",
        ));
    }
    Ok(ReplyArg::Default(content.parse()?))
}

/// Parses the leading source argument, if the attribute opens on one. Reports the subscription
/// and whether anything was consumed (which decides if the first clause needs a separating
/// comma).
///
/// An empty attribute, or one opening on a clause, leaves the subscription unnamed: it is the
/// by-name source with its value left out, the shortest of the source forms.
fn parse_source(input: ParseStream) -> syn::Result<(SourceArg, bool)> {
    if input.is_empty() || peeks_clause(input) {
        return Ok((SourceArg::OpenName, false));
    }
    let expr: Expr = input.parse()?;
    Ok((source_arg(expr), true))
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

/// Parses the inside of a `workers(..)` or `threads(..)` clause: the count, optionally followed
/// by `by_key`.
fn parse_workers(keyword: Ident, content: ParseStream) -> syn::Result<WorkersArg> {
    let count: Expr = content.parse()?;
    let by_key = if content.peek(Token![,]) {
        content.parse::<Token![,]>()?;
        let marker: Ident = content.parse()?;
        if marker != "by_key" {
            return Err(Error::new(
                marker.span(),
                format!("expected `by_key`: {keyword}(n) or {keyword}(n, by_key)"),
            ));
        }
        Some(marker)
    } else {
        None
    };
    Ok(WorkersArg {
        keyword,
        count,
        by_key,
    })
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
            "expected `Type::new(..)`: the path must name a type and an associated constructor - \
             a free function does not expose its type to the macro",
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

/// If `ty` is syntactically `Result<Reply, HandlerOutcome>` (under any path prefix, e.g.
/// `std::result::Result` / `ruststream::runtime::HandlerOutcome`), returns the reply type.
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
    (err_path.segments.last()?.ident == "HandlerOutcome").then_some(ok)
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

/// The generics a derive writes its impls with: the input's own, each lifetime parameter spanned
/// at the derive.
///
/// An impl header names a lifetime once, `impl<'a> Trait for Type<'a>`, and `single_use_lifetimes`
/// reports that against the user's type while the tokens carry the user's span. Spanned at the
/// derive, the header is the macro's own code; the lifetime still resolves where the user wrote it.
pub(crate) fn derive_generics(generics: &Generics) -> Generics {
    let mut generics = generics.clone();
    for param in generics.lifetimes_mut() {
        param.lifetime.set_span(Span::call_site());
    }
    generics
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

#[cfg(test)]
mod tests {
    use syn::parse_quote;

    use super::*;

    fn refusal(tokens: TokenStream2) -> String {
        let parsed = syn::parse2::<SubscriberArgs>(tokens);
        parsed
            .err()
            .expect("these arguments are refused")
            .to_string()
    }

    fn source_refusal(expr: &Expr) -> String {
        let source = source_tokens(&SourceArg::Fixed(expr.clone()));
        source.expect_err("this source is refused").to_string()
    }

    #[test]
    fn a_retry_after_policy_takes_one_duration() {
        assert_eq!(
            refusal(quote!("orders", on_failure(decode = retry_after(a, b)))),
            "retry_after(..) takes exactly one duration argument"
        );
    }

    #[test]
    fn each_failure_kind_is_named_once() {
        assert_eq!(
            refusal(quote!("orders", on_failure(panic = drop, panic = skip))),
            "duplicate `panic` in on_failure(..)"
        );
        assert_eq!(
            refusal(quote!("orders", on_failure(decode = drop, decode = skip))),
            "duplicate `decode` in on_failure(..)"
        );
    }

    #[test]
    fn a_failure_clause_names_panic_or_decode() {
        assert_eq!(
            refusal(quote!("orders", on_failure(timeout = drop))),
            "expected `panic = ..` or `decode = ..`"
        );
        assert_eq!(
            refusal(quote!("orders", on_failure())),
            "on_failure(..) needs at least one of `panic = ..` or `decode = ..`"
        );
    }

    #[test]
    fn each_clause_is_written_once() {
        assert_eq!(
            refusal(quote!(
                "orders",
                on_failure(decode = drop),
                on_failure(decode = skip)
            )),
            "duplicate on_failure(..)"
        );
        assert_eq!(
            refusal(quote!(
                "orders",
                start_at(Position::start()),
                start_at(Position::start())
            )),
            "duplicate start_at(..)"
        );
        assert_eq!(
            refusal(quote!("orders", workers(2), workers(3))),
            "duplicate workers(..)"
        );
    }

    #[test]
    fn an_empty_reply_clause_names_nothing() {
        assert!(
            refusal(quote!("orders", reply())).starts_with("reply() names nothing"),
            "the refusal of `reply()`"
        );
    }

    #[test]
    fn malformed_arguments_are_refused_where_the_parser_stops() {
        let malformed = [
            quote!(+),
            quote!("orders" reply),
            quote!("orders", 5),
            quote!("orders", on_failure(+ = drop)),
            quote!("orders", on_failure(1 = drop)),
            quote!("orders", on_failure(decode drop)),
            quote!("orders", start_at(+)),
            quote!("orders", reply(+)),
            quote!("orders", workers(+)),
            quote!("orders", workers(2, 3)),
        ];

        for tokens in malformed {
            assert!(syn::parse2::<SubscriberArgs>(tokens).is_err());
        }
    }

    #[test]
    fn every_policy_keyword_names_its_policy() {
        let args: SubscriberArgs = syn::parse2(quote!(
            "orders",
            on_failure(panic = retry, decode = fail_fast)
        ))
        .expect("the keywords parse");
        let failure = args.on_failure.expect("the clause is kept");

        assert!(matches!(failure.panic, Some(FailurePolicyArg::Retry)));
        assert!(matches!(failure.decode, Some(FailurePolicyArg::FailFast)));
    }

    #[test]
    fn a_trailing_comma_closes_the_arguments() {
        let args: SubscriberArgs =
            syn::parse2(quote!("orders", reply,)).expect("a trailing comma parses");

        assert!(matches!(args.reply, Some(ReplyArg::Declared)));
    }

    #[test]
    fn a_source_whose_type_the_tokens_do_not_show_is_refused() {
        let expected = "expected a string literal name, `Type::new(..)`, `Type { .. }`, or a \
                        builder chain on one of those - a free function does not expose its \
                        type to the macro";

        assert_eq!(source_refusal(&parse_quote!((make)("orders"))), expected);
        assert_eq!(source_refusal(&parse_quote!(SOURCES[0])), expected);
    }

    #[test]
    fn only_a_result_with_a_handler_outcome_error_carries_a_reply() {
        let carried: Type = parse_quote!(Result<Receipt, HandlerOutcome>);
        assert!(publish_result_reply(&carried).is_some());

        let plain: [Type; 4] = [
            parse_quote!(&'static Receipt),
            parse_quote!(Result),
            parse_quote!(Result<Receipt>),
            parse_quote!(Result<Receipt, &'static HandlerOutcome>),
        ];
        for ty in plain {
            assert!(publish_result_reply(&ty).is_none(), "a plain reply type");
        }
    }

    #[test]
    fn only_a_vec_of_one_type_is_a_batch_of_replies() {
        let batch: Type = parse_quote!(Vec<Receipt>);
        assert!(vec_element(&batch).is_some());

        let plain: [Type; 3] = [
            parse_quote!(&'static [Receipt]),
            parse_quote!(Vec),
            parse_quote!(Vec<Receipt, Global>),
        ];
        for ty in plain {
            assert!(vec_element(&ty).is_none(), "no batch of replies");
        }
    }

    #[test]
    fn only_written_doc_lines_describe_the_item() {
        let attrs: Vec<Attribute> = vec![
            parse_quote!(#[doc = " Settles an order."]),
            parse_quote!(#[doc = include_str!("order.md")]),
            parse_quote!(#[doc(hidden)]),
        ];

        assert_eq!(
            doc_description(&attrs).to_string(),
            quote!(::core::option::Option::Some("Settles an order.")).to_string()
        );
    }
}
