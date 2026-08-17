//! Expansion of the `#[subscriber]` forms: the handler signature is dissected into
//! [`HandlerParts`], then one of the definition impls (plain, publishing, injected, batch,
//! batch publishing) is generated around the original function body; the input axis (typed vs
//! raw bytes) is a flag on the plain, injected, and publishing expansions, not a form.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{ToTokens, quote};
use syn::{Error, Expr, FnArg, Ident, ItemFn, Pat, PatType, ReturnType, Type, TypePath};

use crate::parse::{
    FailurePolicyArg, SubscriberArgs, WorkersArg, doc_description, position_type,
    publish_result_reply, source_tokens, vec_element,
};

pub(crate) fn subscriber(args: &SubscriberArgs, func: &ItemFn) -> syn::Result<TokenStream> {
    reject_raw_combinations(args, func)?;
    let parts = handler_parts(args, func)?;
    let body = if args.raw.is_some() {
        // The remaining combinations are already rejected above; publish_raw(..) selects the
        // byte reply form, otherwise raw is the plain form (injected when the signature
        // carries Out / Seek parameters - the input axis makes raw compose with them).
        match &args.publish_raw {
            Some(reply_topic) => expand_publishing(&parts, func, reply_topic, true, true)?,
            None if !parts.outs.is_empty() || parts.seek.is_some() => expand_injected(&parts, true),
            None => expand_subscribing(&parts, true),
        }
    } else if let Some(reply_topic) = &args.publish_raw {
        expand_publishing(&parts, func, reply_topic, true, false)?
    } else {
        match (&args.batch, &args.publish) {
            (true, Some(reply_topic)) => expand_batch_publishing(&parts, func, reply_topic)?,
            (true, None) if !parts.outs.is_empty() || parts.seek.is_some() => {
                expand_batch_injected(&parts, func)
            }
            (true, None) => expand_batch(&parts, func),
            (false, Some(reply_topic)) => {
                expand_publishing(&parts, func, reply_topic, false, false)?
            }
            (false, None) if !parts.outs.is_empty() || parts.seek.is_some() => {
                expand_injected(&parts, false)
            }
            (false, None) => expand_subscribing(&parts, false),
        }
    };
    Ok(body.into())
}

/// Rejects the argument combinations the byte-level forms do not take: `raw` with batches (one
/// delivery's bytes only) or the decode failure policy (there is no decode step), an encoded
/// `publish(..)` reply off raw bytes (the reply of a raw handler is bytes - `publish_raw`), both
/// reply clauses at once, and a batch with a byte reply.
fn reject_raw_combinations(args: &SubscriberArgs, func: &ItemFn) -> syn::Result<()> {
    if let (Some(_), Some(publish_raw)) = (&args.publish, &args.publish_raw) {
        return Err(Error::new_spanned(
            publish_raw,
            "publish(..) and publish_raw(..) are mutually exclusive: one reply, one destination",
        ));
    }
    if let Some(publish_raw) = &args.publish_raw
        && args.batch
    {
        return Err(Error::new_spanned(
            publish_raw,
            "publish_raw(..) is not supported together with batch(..) yet; publish per message \
             or use the encoded batch reply form",
        ));
    }
    let Some(raw) = &args.raw else {
        return Ok(());
    };
    if args.publish.is_some() {
        return Err(Error::new(
            raw.span(),
            "the reply of a raw handler is bytes and is never encoded; use \
             publish_raw(\"dest\") instead of publish(..)",
        ));
    }
    if args.batch {
        return Err(Error::new(
            raw.span(),
            "raw is not supported together with batch(..); a raw handler takes one delivery's \
             payload as `&[u8]`",
        ));
    }
    // With a FromHeaders parameter the decode policy still has a job on a raw handler: it
    // settles a header contract that fails to parse.
    let has_from_headers = func
        .sig
        .inputs
        .iter()
        .any(|arg| matches!(arg, FnArg::Typed(pt) if from_headers_ty(&pt.ty).is_some()));
    if let Some(failure) = &args.on_failure
        && failure.decode.is_some()
        && !has_from_headers
    {
        return Err(Error::new(
            raw.span(),
            "on_failure(decode = ..) does not apply to raw: the payload is not decoded and \
             there is no FromHeaders parameter; keep only on_failure(panic = ..)",
        ));
    }
    Ok(())
}

/// The pieces of the handler shared by every expansion form, extracted from the signature.
struct HandlerParts<'a> {
    vis: &'a syn::Visibility,
    name: &'a Ident,
    block: &'a syn::Block,
    pat: &'a Pat,
    input_ty: &'a Type,
    description: TokenStream2,
    source_ty: TokenStream2,
    source_expr: TokenStream2,
    input_schema: TokenStream2,
    message_meta: TokenStream2,
    ctx_param: TokenStream2,
    ctx_ty: TokenStream2,
    state_ty: Option<TokenStream2>,
    extractors: Vec<(&'a Pat, &'a Type)>,
    outs: Vec<OutParam<'a>>,
    seek: Option<(&'a Pat, &'a Type)>,
    workers_method: TokenStream2,
    failure_method: TokenStream2,
    /// The `FailurePolicy` value applied to `FromHeaders` extraction (the `decode` key).
    headers_policy: TokenStream2,
    /// The `headers_schema()` def-method override lifted from the first `FromHeaders<T>`
    /// parameter's contract type, or empty when the handler takes none.
    headers_schema: TokenStream2,
}

/// The per-delivery context type the handler named in its `ctx: &mut Context<'_, C>` parameter,
/// projected from the first `Ctx<K>` extractor's key when there is no ctx parameter, or `()`
/// when neither names one. Threaded into the single-subscriber `SubscriberDef::Context` so a
/// macro handler can read broker fields by key or take them as parameters.
fn context_type(func: &ItemFn) -> TokenStream2 {
    let Some(FnArg::Typed(PatType { ty, .. })) = func.sig.inputs.get(1) else {
        return quote!(());
    };
    // The second parameter is the context only when it is `&mut Context<..>`; otherwise it is an
    // extractor, so the handler named no context type explicitly.
    if !is_context_param(ty) {
        return inferred_context_type(func);
    }
    // Dig the second generic argument (after the lifetime) out of `&mut Context<'_, C>`.
    if let Type::Reference(reference) = &**ty
        && let Type::Path(path) = &*reference.elem
        && let Some(segment) = path.path.segments.last()
        && let syn::PathArguments::AngleBracketed(args) = &segment.arguments
    {
        for arg in &args.args {
            if let syn::GenericArgument::Type(context_ty) = arg {
                return quote!(#context_ty);
            }
        }
    }
    quote!(())
}

/// The context type projected from the first `Ctx<K>` extractor parameter's key, for handlers
/// without a `&mut Context` parameter. Emitted as `<K as ContextField>::Context`, so the
/// compiler resolves the type; further `Ctx` keys are checked against it by the extractor
/// bounds. Purely syntactic (the last path segment `Ctx` with exactly one type argument): a
/// type alias hides the shape and falls back to `()`.
fn inferred_context_type(func: &ItemFn) -> TokenStream2 {
    for arg in func.sig.inputs.iter().skip(1) {
        if let FnArg::Typed(PatType { ty, .. }) = arg
            && let Some(key) = ctx_extractor_key(ty)
        {
            return quote!(<#key as ::ruststream::ContextField>::Context);
        }
    }
    quote!(())
}

/// One `Out<impl Bounds[, Marker[, Set]]>` handler parameter: the binding pattern, the
/// capability bounds the publisher generic carries, the slot marker type (the implicit
/// `DefaultSlot` when the parameter names none), and the declared message set of the optional
/// third position.
struct OutParam<'a> {
    pat: &'a Pat,
    bounds: &'a syn::punctuated::Punctuated<syn::TypeParamBound, syn::Token![+]>,
    marker: TokenStream2,
    /// The declared message set; `None` (also written `()`) leaves the slot unrestricted.
    bodies: Option<BodyDecl<'a>>,
}

/// The declared message set of an `Out` parameter's third position.
enum BodyDecl<'a> {
    /// A tuple listing the types inline.
    List(Vec<&'a Type>),
    /// A set-defining type: a `#[derive(Message)]` type (itself) or a `#[derive(OutMessages)]`
    /// enum (its variants' models).
    Set(&'a Type),
}

/// The type arguments of an `Out<..>`-shaped parameter type, when the type has that shape.
/// Purely syntactic (the last path segment `Out` with one or two type arguments), like the
/// `Ctx<K>` probe below.
fn out_param_args(ty: &Type) -> Option<Vec<&Type>> {
    let Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != "Out" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    let types: Vec<&Type> = args
        .args
        .iter()
        .filter_map(|arg| match arg {
            syn::GenericArgument::Type(ty) => Some(ty),
            _ => None,
        })
        .collect();
    if types.is_empty() || types.len() > 3 {
        return None;
    }
    Some(types)
}

/// The seeker type `K` of a `Seek<K>`-shaped parameter type, when the type has that shape.
/// Purely syntactic (the last path segment `Seek` with exactly one type argument), like the
/// `Out<P>` probe above.
fn seek_param_type(ty: &Type) -> Option<&Type> {
    let Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != "Seek" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    let mut types = args.args.iter().filter_map(|arg| match arg {
        syn::GenericArgument::Type(ty) => Some(ty),
        _ => None,
    });
    let seeker = types.next()?;
    if types.next().is_some() {
        return None;
    }
    Some(seeker)
}

/// The contract type `T` of a `FromHeaders<T>`-shaped parameter type, when the type has that
/// shape. Purely syntactic (the last path segment `FromHeaders` with exactly one type argument),
/// like the `Ctx<K>` probe below.
fn from_headers_ty(ty: &Type) -> Option<&Type> {
    let Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != "FromHeaders" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    let mut types = args.args.iter().filter_map(|arg| match arg {
        syn::GenericArgument::Type(ty) => Some(ty),
        _ => None,
    });
    let contract = types.next()?;
    if types.next().is_some() {
        return None;
    }
    Some(contract)
}

/// The key type `K` of a `Ctx<K>`-shaped parameter type, when the type has that shape.
fn ctx_extractor_key(ty: &Type) -> Option<&Type> {
    let Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != "Ctx" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    let mut types = args.args.iter().filter_map(|arg| match arg {
        syn::GenericArgument::Type(ty) => Some(ty),
        _ => None,
    });
    let key = types.next()?;
    if types.next().is_some() {
        return None;
    }
    Some(key)
}

/// The application-state type the handler named as the third generic of its
/// `ctx: &mut Context<'_, C, St>` parameter, or `None` when it named none (only `Context` or
/// `Context<'_, C>`). When present, the handler is bound to that state type; when absent, the
/// generated [`Handler`] impl is generic over the state, so the handler mounts on an app with any
/// state.
fn state_type(func: &ItemFn) -> Option<TokenStream2> {
    let FnArg::Typed(PatType { ty, .. }) = func.sig.inputs.get(1)? else {
        return None;
    };
    // Only a `&mut Context<..>` second parameter names a state; a second extractor parameter does
    // not.
    if !is_context_param(ty) {
        return None;
    }
    // Collect the type arguments of `&mut Context<'_, C, St>` (skipping the lifetime); the second is
    // the state type.
    if let Type::Reference(reference) = &**ty
        && let Type::Path(path) = &*reference.elem
        && let Some(segment) = path.path.segments.last()
        && let syn::PathArguments::AngleBracketed(args) = &segment.arguments
    {
        let mut types = args.args.iter().filter_map(|arg| match arg {
            syn::GenericArgument::Type(ty) => Some(ty),
            _ => None,
        });
        let _context_ty = types.next();
        if let Some(state_ty) = types.next() {
            return Some(quote!(#state_ty));
        }
    }
    None
}

/// True when a handler parameter is the optional per-delivery `&mut Context<..>` (matched by the
/// path's last segment being `Context`), as opposed to an extractor parameter.
fn is_context_param(ty: &Type) -> bool {
    if let Type::Reference(reference) = ty
        && reference.mutability.is_some()
        && let Type::Path(path) = &*reference.elem
        && let Some(segment) = path.path.segments.last()
    {
        return segment.ident == "Context";
    }
    false
}

/// The handler's extractor parameters: every parameter after the message that is not the optional
/// `&mut Context`. Each is resolved through [`FromContext`] before the body runs. `ctx_present`
/// reports whether the second parameter is the context (and so is skipped here).
fn collect_extractors(func: &ItemFn, ctx_present: bool) -> syn::Result<Vec<(&Pat, &Type)>> {
    let start = if ctx_present { 2 } else { 1 };
    let mut extractors = Vec::new();
    for arg in func.sig.inputs.iter().skip(start) {
        let FnArg::Typed(PatType { pat, ty, .. }) = arg else {
            return Err(Error::new_spanned(
                arg,
                "a #[subscriber] handler cannot take `self`",
            ));
        };
        if is_context_param(ty) {
            return Err(Error::new_spanned(
                ty,
                "the `&mut Context` parameter must come immediately after the message, before any \
                 extractor parameters",
            ));
        }
        extractors.push((&**pat, &**ty));
    }
    Ok(extractors)
}

/// The predicates binding each extractor type to [`FromContext`] for the handler's context `C`
/// and state `S`. Added to the generated call impl so a state-specific extractor compiles
/// without forcing the handler to name a `&mut Context`.
fn extractor_preds(
    extractors: &[(&Pat, &Type)],
    ctx_ty: &TokenStream2,
    state: &TokenStream2,
) -> Vec<TokenStream2> {
    extractors
        .iter()
        .map(|(_, ty)| quote!(#ty: ::ruststream::runtime::FromContext<#ctx_ty, #state>))
        .collect()
}

/// Renders a predicate list as a `where` clause, or nothing when the list is empty.
fn where_clause(preds: &[TokenStream2]) -> TokenStream2 {
    if preds.is_empty() {
        return quote!();
    }
    quote!(where #(#preds),*)
}

/// [`extractor_preds`] rendered as a full `where` clause, for the expansions without slot
/// bounds of their own.
fn extractor_where(
    extractors: &[(&Pat, &Type)],
    ctx_ty: &TokenStream2,
    state: &TokenStream2,
) -> TokenStream2 {
    where_clause(&extractor_preds(extractors, ctx_ty, state))
}

/// The `let` bindings that resolve each extractor from the context before the body runs. A failed
/// extraction runs `reject` (a `return` settling the delivery by the rejection's `HandlerResult`).
///
/// A `FromHeaders<T>` parameter takes the policy-aware path instead of the generic
/// [`FromContext`] call, so the subscriber's `on_failure(decode = ..)` policy (interpolated as
/// `headers_policy`) applies to the header contract exactly like it applies to the payload codec.
fn extractor_prelude(
    extractors: &[(&Pat, &Type)],
    ctx_param: &TokenStream2,
    ctx_ty: &TokenStream2,
    state: &TokenStream2,
    headers_policy: &TokenStream2,
    reject: &TokenStream2,
) -> TokenStream2 {
    let binds = extractors.iter().map(|(pat, ty)| {
        if from_headers_ty(ty).is_some() {
            return quote! {
                let #pat = match <#ty>::extract(&mut *#ctx_param, #headers_policy) {
                    ::core::result::Result::Ok(__rs_value) => __rs_value,
                    ::core::result::Result::Err(__rs_err) => { #reject; }
                };
            };
        }
        quote! {
            let #pat = match <#ty as ::ruststream::runtime::FromContext<#ctx_ty, #state>>::from_context(
                &mut *#ctx_param,
            )
            .await
            {
                ::core::result::Result::Ok(__rs_value) => __rs_value,
                ::core::result::Result::Err(__rs_err) => { #reject; }
            };
        }
    });
    quote!(#(#binds)*)
}

/// Renders the `on_failure(..)` clause as an override of the def's defaulted `failure_policies`
/// method, or nothing when the clause is absent. Only the keys named in the clause are set; the
/// rest keep the runtime defaults.
fn failure_method(args: &SubscriberArgs) -> TokenStream2 {
    let Some(failure) = &args.on_failure else {
        return quote!();
    };
    let panic = failure
        .panic
        .as_ref()
        .map(failure_policy_tokens)
        .map(|policy| quote!(.with_panic(#policy)));
    let decode = failure
        .decode
        .as_ref()
        .map(failure_policy_tokens)
        .map(|policy| quote!(.with_decode(#policy)));
    quote! {
        fn failure_policies(&self) -> ::ruststream::runtime::FailurePolicies {
            ::ruststream::runtime::FailurePolicies::default() #panic #decode
        }
    }
}

/// Renders the def's `headers_schema()` override, rejecting `FromHeaders` on batch forms (a
/// header contract is per-delivery; a batch spans many deliveries with as many header maps).
///
/// The schema source: a `FromHeaders<T>` parameter wins (its contract is what the runtime
/// actually enforces); otherwise the input type's `#[message(headers(..))]` contract, when it
/// declares one. Both use the same autoref-specialization probes as the payload schema. A raw
/// handler has no typed input to carry a contract, so without a `FromHeaders` parameter it
/// emits nothing.
fn headers_schema_method(
    args: &SubscriberArgs,
    extractors: &[(&Pat, &Type)],
    input_ty: &Type,
) -> syn::Result<TokenStream2> {
    if args.batch
        && let Some((_, ty)) = extractors
            .iter()
            .find(|(_, ty)| from_headers_ty(ty).is_some())
    {
        return Err(Error::new_spanned(
            ty,
            "FromHeaders is per-delivery and does not apply to batch(..) forms: a batch spans \
             many deliveries, each with its own headers",
        ));
    }
    let method = extractors
        .iter()
        .find_map(|(_, ty)| from_headers_ty(ty))
        .map_or_else(
            || {
                if args.raw.is_some() {
                    return quote!();
                }
                quote! {
                    fn headers_schema(&self) -> ::core::option::Option<::std::string::String> {
                        #[allow(unused_imports)]
                        use ::ruststream::__private::NoHeadersSchemaProbe as _;
                        ::ruststream::__private::Probe::<#input_ty>::new().headers_schema_json()
                    }
                }
            },
            |contract| {
                quote! {
                    fn headers_schema(&self) -> ::core::option::Option<::std::string::String> {
                        #[allow(unused_imports)]
                        use ::ruststream::__private::NoSchemaProbe as _;
                        ::ruststream::__private::Probe::<#contract>::new().schema_json()
                    }
                }
            },
        );
    Ok(method)
}

/// The failure policy applied to `FromHeaders` extraction, as a `FailurePolicy` value: the
/// `on_failure(decode = ..)` key (one materialization policy covers the payload codec and the
/// header contract), or the runtime default (drop) when the clause omits it. Interpolated into
/// the generated extraction call - the handler body has no access to the def's
/// `failure_policies()`.
fn headers_policy_tokens(args: &SubscriberArgs) -> TokenStream2 {
    args.on_failure
        .as_ref()
        .and_then(|failure| failure.decode.as_ref())
        .map_or_else(
            || quote!(::ruststream::runtime::FailurePolicy::Drop),
            failure_policy_tokens,
        )
}

/// Renders the def's `outgoing()` override: the reply message (typed probes on the reply type,
/// or a bare bytes entry for `publish_raw`) plus each `Out` parameter's declarations - a
/// declared message set (its channels read off the dictionary consts), or the whole
/// `#[publishes(..)]` dictionary for a byte-level slot. Empty when the handler declares no
/// outgoing messages, keeping the trait default.
fn outgoing_method(
    reply: Option<(&Expr, Option<TokenStream2>)>,
    outs: &[OutParam<'_>],
) -> TokenStream2 {
    if reply.is_none() && outs.is_empty() {
        return quote!();
    }
    let reply_entry = reply.map(|(topic, reply_ty)| {
        reply_ty.map_or_else(
            // A publish_raw reply is bytes: no schema, no Message metadata to probe. The
            // explicit &'static str binding keeps a wrongly-typed destination expression a
            // plain type error instead of a trait-bound failure inside the metadata builder.
            || {
                quote! {
                    __rs_outgoing.push(::ruststream::runtime::OutgoingMessageMetadata::new(
                        { let __rs_channel: &'static str = #topic; __rs_channel },
                        "bytes",
                    ));
                }
            },
            |reply_ty| outgoing_entry(&quote!(#topic), &reply_ty),
        )
    });
    let slots = outs.iter().map(|out| {
        let marker = &out.marker;
        match &out.bodies {
            // Unrestricted: the honest declaration is the marker's whole dictionary.
            None => quote! {
                __rs_outgoing.extend(<#marker as ::ruststream::runtime::OutSlot>::outgoing());
            },
            Some(BodyDecl::List(bodies)) => {
                let entries = bodies.iter().map(|body| {
                    let channel = quote! {
                        <#body as ::ruststream::runtime::OutMessage<#marker>>::CHANNEL
                    };
                    outgoing_entry(&channel, &quote!(#body))
                });
                quote!(#(#entries)*)
            }
            Some(BodyDecl::Set(set)) => quote! {
                __rs_outgoing.extend(
                    <#set as ::ruststream::runtime::OutMessages<#marker>>::outgoing(),
                );
            },
        }
    });
    quote! {
        fn outgoing(&self)
            -> ::std::vec::Vec<::ruststream::runtime::OutgoingMessageMetadata>
        {
            let mut __rs_outgoing = ::std::vec::Vec::new();
            #reply_entry
            #(#slots)*
            __rs_outgoing
        }
    }
}

/// The reply-form pieces shared by both publishing expansions: the def's `outgoing()` override
/// and its `reply_name()` body.
fn reply_pieces(
    reply_topic: &Expr,
    reply_ty: Option<TokenStream2>,
    outs: &[OutParam<'_>],
) -> (TokenStream2, TokenStream2) {
    (
        outgoing_method(Some((reply_topic, reply_ty)), outs),
        reply_name_body(reply_topic),
    )
}

/// Renders the body of the def's `reply_name()`: a string literal passes through (zero cost);
/// any other expression is evaluated once into a process-wide `LazyLock` - `reply_name()` sits
/// on the per-delivery path, and an arbitrary destination expression (a function call) must
/// not run per message.
fn reply_name_body(reply_topic: &Expr) -> TokenStream2 {
    if matches!(
        reply_topic,
        Expr::Lit(syn::ExprLit {
            lit: syn::Lit::Str(_),
            ..
        })
    ) {
        return quote!(#reply_topic);
    }
    quote! {
        {
            static __RS_REPLY_NAME: ::std::sync::LazyLock<&'static str> =
                ::std::sync::LazyLock::new(|| #reply_topic);
            *__RS_REPLY_NAME
        }
    }
}

/// One `outgoing()` entry: the message type's metadata probed at the call site (schema,
/// `Message` name / description, headers contract), published to `channel`. The explicit
/// &'static str binding keeps a wrongly-typed destination expression a plain type error
/// instead of a trait-bound failure inside the metadata builder.
fn outgoing_entry(channel: &TokenStream2, message_ty: &TokenStream2) -> TokenStream2 {
    quote! {
        __rs_outgoing.push(
            ::ruststream::runtime::OutgoingMessageMetadata::new(
                { let __rs_channel: &'static str = #channel; __rs_channel },
                ::core::any::type_name::<#message_ty>(),
            )
            .with_message_name({
                #[allow(unused_imports)]
                use ::ruststream::__private::NoMessageProbe as _;
                ::ruststream::__private::Probe::<#message_ty>::new().message_name()
            })
            .with_message_description({
                #[allow(unused_imports)]
                use ::ruststream::__private::NoMessageProbe as _;
                ::ruststream::__private::Probe::<#message_ty>::new().message_description()
            })
            .with_payload_schema({
                #[allow(unused_imports)]
                use ::ruststream::__private::NoSchemaProbe as _;
                ::ruststream::__private::Probe::<#message_ty>::new().schema_json()
            })
            .with_headers_schema({
                #[allow(unused_imports)]
                use ::ruststream::__private::NoHeadersSchemaProbe as _;
                ::ruststream::__private::Probe::<#message_ty>::new().headers_schema_json()
            }),
        );
    }
}

/// Renders one [`FailurePolicyArg`] as the matching `FailurePolicy` value.
fn failure_policy_tokens(policy: &FailurePolicyArg) -> TokenStream2 {
    match policy {
        FailurePolicyArg::FailFast => quote!(::ruststream::runtime::FailurePolicy::FailFast),
        FailurePolicyArg::Drop => quote!(::ruststream::runtime::FailurePolicy::Drop),
        FailurePolicyArg::Retry => quote!(::ruststream::runtime::FailurePolicy::Retry),
        FailurePolicyArg::RetryAfter(expr) => {
            quote!(::ruststream::runtime::FailurePolicy::RetryAfter(#expr))
        }
        FailurePolicyArg::Skip => quote!(::ruststream::runtime::FailurePolicy::Skip),
        // A shared constant / any FailurePolicy expression passes through verbatim.
        FailurePolicyArg::Value(expr) => quote!(#expr),
    }
}

/// Renders the `workers(..)` clause as an override of the def's defaulted `workers` method, or
/// nothing when the clause is absent.
fn workers_method(args: &SubscriberArgs, handler: &Ident) -> syn::Result<TokenStream2> {
    let Some(WorkersArg { count, by_key }) = &args.workers else {
        return Ok(quote!());
    };
    let count = workers_count(count, handler)?;
    if let Some(marker) = by_key {
        if args.batch {
            return Err(Error::new(
                marker.span(),
                "by_key lanes order single messages per key; they do not apply to batch(..) \
                 forms",
            ));
        }
        return Ok(quote! {
            fn workers(&self) -> ::ruststream::runtime::Workers {
                ::ruststream::runtime::Workers::keyed(#count)
            }
        });
    }
    Ok(quote! {
        fn workers(&self) -> ::ruststream::runtime::Workers {
            ::ruststream::runtime::Workers::pool(#count)
        }
    })
}

/// Lowers the `workers(..)` count to a `NonZeroUsize`: an integer literal is checked at
/// expansion (zero is a compile error) and lowered panic-free; any other `usize` expression -
/// a constant, a static, a function call - is not knowable here, so zero surfaces as a
/// registration-time panic naming the clause (the startup rung: the value is external input to
/// the macro).
fn workers_count(count: &Expr, handler: &Ident) -> syn::Result<TokenStream2> {
    if let Expr::Lit(syn::ExprLit {
        lit: syn::Lit::Int(literal),
        ..
    }) = count
    {
        if literal.base10_parse::<usize>()? == 0 {
            return Err(Error::new(
                literal.span(),
                "workers(0) is not a policy; the minimum is 1",
            ));
        }
        // The literal is checked above, so the None arm is unreachable; MIN keeps the
        // lowering panic-free.
        return Ok(quote! {
            match ::core::num::NonZeroUsize::new(#literal) {
                ::core::option::Option::Some(count) => count,
                ::core::option::Option::None => ::core::num::NonZeroUsize::MIN,
            }
        });
    }
    let misconfigured = format!(
        "workers(..) on subscriber `{handler}` needs a non-zero count; the configured value is 0",
    );
    Ok(quote! {
        ::core::num::NonZeroUsize::new(#count).expect(#misconfigured)
    })
}

/// Splits the `Out<..>` parameters out of the extractor list, in signature order.
///
/// Each must carry its capability as `impl Trait` (the concrete publisher type is inferred at
/// the include site) and a distinct marker; a single parameter may omit the marker and binds
/// the implicit `DefaultSlot`.
fn split_outs<'a>(extractors: &mut Vec<(&'a Pat, &'a Type)>) -> syn::Result<Vec<OutParam<'a>>> {
    let mut outs = Vec::new();
    let mut kept = Vec::new();
    for (pat, ty) in extractors.drain(..) {
        let Some(args) = out_param_args(ty) else {
            kept.push((pat, ty));
            continue;
        };
        let Type::ImplTrait(capability) = args[0] else {
            return Err(Error::new_spanned(
                args[0],
                "an Out parameter names the capability it needs, not a publisher type: write \
                 `Out<impl Publisher>` (or a capability like `impl OwnedTransactions`); the \
                 concrete publisher is inferred from the policy attached at the include site",
            ));
        };
        let marker = args.get(1).map_or_else(
            || quote!(::ruststream::runtime::DefaultSlot),
            |marker| quote!(#marker),
        );
        let bodies = match args.get(2) {
            Some(body) => body_decl(body)?,
            None => None,
        };
        outs.push(OutParam {
            pat,
            bounds: &capability.bounds,
            marker,
            bodies,
        });
    }
    *extractors = kept;
    if outs.len() > 3 {
        return Err(Error::new_spanned(
            outs[3].pat,
            "a #[subscriber] handler takes at most three Out parameters",
        ));
    }
    for (index, out) in outs.iter().enumerate() {
        let name = out.marker.to_string();
        if outs
            .iter()
            .skip(index + 1)
            .any(|other| other.marker.to_string() == name)
        {
            return Err(Error::new_spanned(
                out.pat,
                "every Out parameter needs its own slot marker: two parameters bind the same \
                 slot (name each with `Out<impl Publisher, MyMarker>` and derive the markers \
                 with #[derive(OutSlot)])",
            ));
        }
    }
    Ok(outs)
}

/// The declared message set of an `Out` parameter's third position: a tuple lists types
/// inline (`()` = unrestricted, like an absent position), a bare type defines its own set.
/// Rejects a duplicate list entry (its `publish_typed` index inference would be ambiguous).
fn body_decl(body: &Type) -> syn::Result<Option<BodyDecl<'_>>> {
    let Type::Tuple(tuple) = body else {
        // A bare type defines its own set: a #[derive(Message)] type declares itself, a
        // #[derive(OutMessages)] enum declares its variants' models.
        return Ok(Some(BodyDecl::Set(body)));
    };
    let bodies: Vec<&Type> = tuple.elems.iter().collect();
    // `()` spells the default out: the slot stays unrestricted.
    if bodies.is_empty() {
        return Ok(None);
    }
    if bodies.len() > 4 {
        return Err(Error::new_spanned(
            bodies[4],
            "an Out parameter lists at most four message types inline; declare a \
             #[derive(OutMessages)] set enum for a larger one",
        ));
    }
    for (index, body) in bodies.iter().enumerate() {
        let name = body.to_token_stream().to_string();
        if bodies
            .iter()
            .skip(index + 1)
            .any(|other| other.to_token_stream().to_string() == name)
        {
            return Err(Error::new_spanned(
                body,
                "this type is already in the Out parameter's message list",
            ));
        }
    }
    Ok(Some(BodyDecl::List(bodies)))
}

/// Splits the (at most one) `Seek<K>` parameter out of the extractor list, rejecting a
/// duplicate.
fn split_seek<'a>(
    extractors: &mut Vec<(&'a Pat, &'a Type)>,
) -> syn::Result<Option<(&'a Pat, &'a Type)>> {
    let mut seek = None;
    extractors.retain(|(pat, ty)| {
        if let Some(seeker_ty) = seek_param_type(ty) {
            // Only the first Seek parameter is kept; a duplicate is rejected below.
            if seek.is_none() {
                seek = Some((*pat, seeker_ty));
                return false;
            }
        }
        true
    });
    if let Some((_, dup)) = extractors
        .iter()
        .find(|(_, ty)| seek_param_type(ty).is_some())
    {
        return Err(Error::new_spanned(
            dup,
            "a #[subscriber] handler takes at most one Seek parameter",
        ));
    }
    Ok(seek)
}

/// Resolves the message parameter's referent into the def's input type per form: batch unwraps
/// the `&[T]` slice to its element, raw accepts only `&[u8]` (its type is never emitted), and the
/// plain form takes `&T`. Each misuse gets an error naming the fix.
fn input_type<'a>(
    args: &SubscriberArgs,
    reference: &'a syn::TypeReference,
) -> syn::Result<&'a Type> {
    if args.batch {
        return match &*reference.elem {
            Type::Slice(slice) => Ok(&slice.elem),
            other => Err(Error::new_spanned(
                other,
                "a batch handler takes the whole batch as a slice: `&[T]`",
            )),
        };
    }
    if args.raw.is_some() {
        return match &*reference.elem {
            elem if is_u8_slice(elem) => Ok(elem),
            other => Err(Error::new_spanned(
                other,
                "a raw subscriber receives the payload bytes: make the message parameter \
                 `&[u8]`, or drop `raw` to decode into a typed value",
            )),
        };
    }
    if matches!(&*reference.elem, Type::Slice(_)) {
        return Err(Error::new_spanned(
            &reference.elem,
            "a slice parameter needs the batch source form: #[subscriber(batch(..))]; for the \
             undecoded payload bytes use #[subscriber(.., raw)]",
        ));
    }
    Ok(&reference.elem)
}

/// True when `ty` is syntactically `[u8]`, the only message parameter shape the raw form takes.
fn is_u8_slice(ty: &Type) -> bool {
    if let Type::Slice(slice) = ty
        && let Type::Path(TypePath {
            qself: None, path, ..
        }) = &*slice.elem
    {
        return path.is_ident("u8");
    }
    false
}

fn handler_parts<'a>(args: &SubscriberArgs, func: &'a ItemFn) -> syn::Result<HandlerParts<'a>> {
    let first = func.sig.inputs.first().ok_or_else(|| {
        Error::new_spanned(
            &func.sig,
            "a #[subscriber] handler must take exactly one message parameter",
        )
    })?;
    let FnArg::Typed(PatType { pat, ty, .. }) = first else {
        return Err(Error::new_spanned(
            first,
            "a #[subscriber] handler cannot take `self`",
        ));
    };
    let Type::Reference(reference) = &**ty else {
        return Err(Error::new_spanned(
            ty,
            "the message parameter must be a reference `&T`",
        ));
    };
    let input_ty = input_type(args, reference)?;
    let description = doc_description(&func.attrs);
    let (source_ty, source_expr) = source_tokens(&args.source)?;
    // `start_at(<position>)` wraps the source in the core `StartAt` decorator, so the
    // subscription is sought to the position before the first delivery. Orthogonal to the
    // definition form: every form carries a `Source`.
    let (source_ty, source_expr) = match &args.start_at {
        Some(position) => {
            let position_ty = position_type(position)?;
            (
                quote!(::ruststream::StartAt<#source_ty, #position_ty>),
                quote!(::ruststream::StartAt::new(#source_expr, #position)),
            )
        }
        None => (source_ty, source_expr),
    };

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
    // bind it to their name; otherwise generate an ignored binding. Any later by-value parameter is
    // an extractor, resolved through `FromContext` before the body.
    let ctx_arg = func
        .sig
        .inputs
        .get(1)
        .filter(|arg| matches!(arg, FnArg::Typed(pt) if is_context_param(&pt.ty)));
    let ctx_param = if let Some(FnArg::Typed(PatType { pat, .. })) = ctx_arg {
        quote!(#pat)
    } else {
        quote!(_ctx)
    };
    let mut extractors = collect_extractors(func, ctx_arg.is_some())?;
    let outs = split_outs(&mut extractors)?;
    let seek = split_seek(&mut extractors)?;
    let ctx_ty = context_type(func);
    let state_ty = state_type(func);

    let headers_schema = headers_schema_method(args, &extractors, input_ty)?;
    let headers_policy = headers_policy_tokens(args);

    let workers_method = workers_method(args, &func.sig.ident)?;
    let failure_method = failure_method(args);

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
        ctx_ty,
        state_ty,
        extractors,
        outs,
        seek,
        workers_method,
        failure_method,
        headers_policy,
        headers_schema,
    })
}

/// Splits a batch publishing handler's declared return type into the reply element type and the
/// body that yields `Result<Vec<Reply>, HandlerResult>`. `-> Result<Vec<Reply>, HandlerResult>` is
/// passed through; a plain `-> Vec<Reply>` is wrapped in `Ok`. Both checks are syntactic, like the
/// single-message publish form: a type alias is not seen through.
fn batch_reply_body<'a>(
    func: &'a ItemFn,
    block: &syn::Block,
) -> syn::Result<(&'a Type, TokenStream2)> {
    let declared_ty = match &func.sig.output {
        ReturnType::Type(_, ty) => &**ty,
        ReturnType::Default => {
            return Err(Error::new_spanned(
                &func.sig,
                "a batch publishing handler must return the replies: Vec<Reply>, or \
                 Result<Vec<Reply>, HandlerResult>",
            ));
        }
    };
    if let Some(ok_ty) = publish_result_reply(declared_ty) {
        let Some(elem) = vec_element(ok_ty) else {
            return Err(Error::new_spanned(
                ok_ty,
                "a batch publishing handler replies with a Vec: Result<Vec<Reply>, HandlerResult>",
            ));
        };
        Ok((elem, quote!((async move #block).await)))
    } else {
        let Some(elem) = vec_element(declared_ty) else {
            return Err(Error::new_spanned(
                declared_ty,
                "a batch publishing handler returns the replies: Vec<Reply>, or \
                 Result<Vec<Reply>, HandlerResult>",
            ));
        };
        Ok((
            elem,
            quote!(::core::result::Result::Ok((async move #block).await)),
        ))
    }
}

fn expand_batch_publishing(
    parts: &HandlerParts<'_>,
    func: &ItemFn,
    reply_topic: &Expr,
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
        ctx_ty: _,
        state_ty,
        extractors,
        outs,
        seek,
        workers_method,
        failure_method,
        headers_policy,
        headers_schema,
    } = parts;

    let (reply_elem, call_body) = batch_reply_body(func, block)?;
    let (outgoing, reply_name_body) = reply_pieces(reply_topic, Some(quote!(#reply_elem)), outs);
    // The injection tuple is shared with the other forms; an Out parameter selects the
    // two-attachment builder (`.out(marker, ..)` next to `.publisher(..)`).
    let form = if outs.is_empty() {
        quote!(::ruststream::runtime::forms::BatchPublishing)
    } else {
        quote!(::ruststream::runtime::forms::BatchPublishingOut)
    };
    let SlotScaffold {
        def_target,
        def_generics,
        generics: out_generics,
        scaffold: scaffold_items,
        out_bounds,
    } = slot_scaffold(vis, name, outs, seek.is_some());
    let (injection_tys, injection_bindings) = injection_pieces(outs, &out_generics, *seek);

    // Like the single-message publishing form: the handler implements `BatchPublishingCall` only
    // for its named state (mounts on a matching app), or generically when it names none (mounts on
    // any app). The metadata-only `BatchPublishingDef` is unconditional.
    let (state_generic, state_in_ctx) = state_pieces(state_ty.as_ref());
    // The batch context is always `()`; extractors resolve against it.
    let unit_ctx = quote!(());
    let def_where = where_clause(&out_bounds);
    let mut call_preds = extractor_preds(extractors, &unit_ctx, &state_in_ctx);
    call_preds.extend(out_bounds.iter().cloned());
    let call_where = where_clause(&call_preds);
    let prelude = extractor_prelude(
        extractors,
        ctx_param,
        &unit_ctx,
        &state_in_ctx,
        headers_policy,
        &quote!(
            return ::core::result::Result::Err(::core::convert::Into::<
                ::ruststream::runtime::HandlerResult,
            >::into(__rs_err),)
        ),
    );
    Ok(quote! {
        #[allow(non_camel_case_types)]
        #vis struct #name;

        impl ::ruststream::runtime::IncludeDef for #name {
            type Form = #form;
        }

        #scaffold_items

        impl<#def_generics> ::ruststream::runtime::BatchPublishingDef for #def_target
        #def_where
        {
            type Input = ::ruststream::runtime::Decoded<#input_ty>;
            type Injections = (#(#injection_tys,)*);
            type Reply = #reply_elem;
            type Source = #source_ty;

            fn source(&self) -> Self::Source { #source_expr }
            fn reply_name(&self) -> &str { #reply_name_body }

            #workers_method

            #failure_method

            fn description(&self) -> ::core::option::Option<&str> {
                #description
            }

            #input_schema

            #headers_schema

            #message_meta

            #outgoing
        }

        impl<#state_generic #def_generics>
            ::ruststream::runtime::BatchPublishingCall<#state_in_ctx> for #def_target
            #call_where
        {
            async fn call(
                &self,
                #pat: &[#input_ty],
                __rs_inj: &Self::Injections,
                #ctx_param: &mut ::ruststream::runtime::Context<'_, (), #state_in_ctx>,
            ) -> ::core::result::Result<
                ::std::vec::Vec<#reply_elem>,
                ::ruststream::runtime::HandlerResult,
            > {
                #prelude
                #injection_bindings
                #call_body
            }
        }
    })
}

fn expand_batch(parts: &HandlerParts<'_>, func: &ItemFn) -> TokenStream2 {
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
        ctx_ty: _,
        state_ty,
        extractors,
        outs: _,
        seek: _,
        workers_method,
        failure_method,
        headers_policy,
        headers_schema,
    } = parts;

    // Pin the body's type to the declared return type before the `IntoBatchResult` conversion:
    // the trait has several impls, so an open-ended tail like `.collect()` cannot infer through
    // the conversion alone.
    let outcome_ty = match &func.sig.output {
        ReturnType::Type(_, ty) => quote!(#ty),
        ReturnType::Default => quote!(()),
    };

    // As for `expand_subscribing`: a batch handler that names a state type is bound to it, one that
    // names none is generic over the state, so it mounts on an app with any state type.
    let (impl_generics, state_in_ctx) = match &state_ty {
        Some(state_ty) => (quote!(), quote!(#state_ty)),
        None => (
            quote!(<__RsState: ::core::marker::Send + ::core::marker::Sync>),
            quote!(__RsState),
        ),
    };
    // The batch context is always `()`; extractors resolve against it.
    let unit_ctx = quote!(());
    let where_clause = extractor_where(extractors, &unit_ctx, &state_in_ctx);
    let prelude = extractor_prelude(
        extractors,
        ctx_param,
        &unit_ctx,
        &state_in_ctx,
        headers_policy,
        &quote!(
            return ::ruststream::runtime::IntoBatchResult::into_batch_result(
                ::core::convert::Into::<::ruststream::runtime::HandlerResult>::into(__rs_err),
            )
        ),
    );

    quote! {
            #[derive(Clone, Copy)]
            #[allow(non_camel_case_types)]
            #vis struct #name;

            impl #impl_generics
                ::ruststream::runtime::SliceHandler<#input_ty, #state_in_ctx> for #name
                #where_clause
            {
                async fn handle_slice(
                    &self,
                    #pat: &[#input_ty],
                    #ctx_param: &mut ::ruststream::runtime::Context<'_, (), #state_in_ctx>,
                ) -> ::ruststream::runtime::BatchResult {
                    #prelude
                    let outcome: #outcome_ty = (async move #block).await;
                    ::ruststream::runtime::IntoBatchResult::into_batch_result(outcome)
                }
            }

            impl ::ruststream::runtime::IncludeDef for #name {
                type Form = ::ruststream::runtime::forms::Batch;
            }

            impl ::ruststream::runtime::BatchDef for #name {
                type Input = ::ruststream::runtime::Decoded<#input_ty>;
                type Handler = Self;
                type Source = #source_ty;

                fn source(&self) -> Self::Source { #source_expr }

                #workers_method

            #failure_method

                fn description(&self) -> ::core::option::Option<&str> {
                    #description
                }

                #input_schema

            #headers_schema

                #message_meta

                fn into_handler(self) -> Self { self }
            }
    }
}

/// The injected batch form: `batch(..)` with Out / Seek parameters. Mirrors `expand_injected`
/// at the batch shape (slice input, `IntoBatchResult` conversion, unit context).
fn expand_batch_injected(parts: &HandlerParts<'_>, func: &ItemFn) -> TokenStream2 {
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
        ctx_ty: _,
        state_ty,
        extractors,
        outs,
        seek,
        workers_method,
        failure_method,
        headers_policy,
        headers_schema,
    } = parts;

    // The injection tuple is shared with the single-message form; only the form token differs
    // (the batch mount arms drive batches, not a per-message stream).
    let form = if outs.is_empty() {
        quote!(::ruststream::runtime::forms::BatchSeek)
    } else {
        quote!(::ruststream::runtime::forms::BatchOut)
    };
    let outgoing = outgoing_method(None, outs);
    let SlotScaffold {
        def_target,
        def_generics,
        generics: out_generics,
        scaffold: scaffold_items,
        out_bounds,
    } = slot_scaffold(vis, name, outs, seek.is_some());
    let (injection_tys, injection_bindings) = injection_pieces(outs, &out_generics, *seek);

    // As for `expand_batch`: pin the body's type before the `IntoBatchResult` conversion.
    let outcome_ty = match &func.sig.output {
        ReturnType::Type(_, ty) => quote!(#ty),
        ReturnType::Default => quote!(()),
    };
    let (state_generic, state_in_ctx) = state_pieces(state_ty.as_ref());
    // The batch context is always `()`; extractors resolve against it.
    let unit_ctx = quote!(());
    let def_where = where_clause(&out_bounds);
    let mut call_preds = extractor_preds(extractors, &unit_ctx, &state_in_ctx);
    call_preds.extend(out_bounds.iter().cloned());
    let call_where = where_clause(&call_preds);
    let prelude = extractor_prelude(
        extractors,
        ctx_param,
        &unit_ctx,
        &state_in_ctx,
        headers_policy,
        &quote!(
            return ::ruststream::runtime::IntoBatchResult::into_batch_result(
                ::core::convert::Into::<::ruststream::runtime::HandlerResult>::into(__rs_err),
            )
        ),
    );

    quote! {
        #[derive(Clone, Copy)]
        #[allow(non_camel_case_types)]
        #vis struct #name;

        impl ::ruststream::runtime::IncludeDef for #name {
            type Form = #form;
        }

        #scaffold_items

        impl<#def_generics> ::ruststream::runtime::BatchInjectDef for #def_target
        #def_where
        {
            type Input = ::ruststream::runtime::Decoded<#input_ty>;
            type Source = #source_ty;
            type Injections = (#(#injection_tys,)*);

            fn source(&self) -> Self::Source { #source_expr }

            #workers_method

            #failure_method

            fn description(&self) -> ::core::option::Option<&str> {
                #description
            }

            #input_schema

            #headers_schema

            #message_meta

            #outgoing
        }

        impl<#state_generic #def_generics>
            ::ruststream::runtime::BatchInjectCall<#state_in_ctx> for #def_target
            #call_where
        {
            async fn call(
                &self,
                #pat: &[#input_ty],
                __rs_inj: &Self::Injections,
                #ctx_param: &mut ::ruststream::runtime::Context<'_, (), #state_in_ctx>,
            ) -> ::ruststream::runtime::BatchResult {
                #prelude
                #injection_bindings
                let outcome: #outcome_ty = (async move #block).await;
                ::ruststream::runtime::IntoBatchResult::into_batch_result(outcome)
            }
        }
    }
}

/// The include form token of the reply-publishing expansion. The injection tuple is shared
/// with the plain forms; an Out parameter additionally selects the slot-taking builder (which
/// grows the `.out(..)` attachments next to `.publisher(..)`).
fn publishing_form(bare: bool, has_outs: bool) -> TokenStream2 {
    match (bare, has_outs) {
        (false, false) => quote!(::ruststream::runtime::forms::Publishing),
        (false, true) => quote!(::ruststream::runtime::forms::PublishingOut),
        (true, false) => quote!(::ruststream::runtime::forms::RawReply),
        (true, true) => quote!(::ruststream::runtime::forms::RawReplyOut),
    }
}

/// The reply-publishing form. `bare` marks the `publish_raw` (byte reply) variant - the same
/// definition and machinery, with the form token selecting the bare-policy default commit
/// instead of the typed-codec one - and `raw_input` selects the byte input kind (the handler
/// borrows the payload as `&[u8]`).
fn expand_publishing(
    parts: &HandlerParts<'_>,
    func: &ItemFn,
    reply_topic: &Expr,
    bare: bool,
    raw_input: bool,
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
        ctx_ty,
        state_ty,
        extractors,
        outs,
        seek,
        workers_method,
        failure_method,
        headers_policy,
        headers_schema,
    } = parts;

    let (reply_ty, call_body) = publishing_reply(func, block, bare)?;
    let (outgoing, reply_name_body) =
        reply_pieces(reply_topic, (!bare).then(|| quote!(#reply_ty)), outs);
    let form = publishing_form(bare, !outs.is_empty());
    let SlotScaffold {
        def_target,
        def_generics,
        generics: out_generics,
        scaffold: scaffold_items,
        out_bounds,
    } = slot_scaffold(vis, name, outs, seek.is_some());
    let (injection_tys, injection_bindings) = injection_pieces(outs, &out_generics, *seek);
    let (input_kind, input_param, input_schema, message_meta) =
        input_pieces(input_ty, input_schema, message_meta, raw_input);

    // As for `expand_subscribing`: a publishing handler that names a state type implements
    // `PublishingCall` only for that state (mounts on a matching app); one that names none is
    // generic over the state (mounts on any app). The metadata-only `PublishingDef` is unconditional.
    let (state_generic, state_in_ctx) = state_pieces(state_ty.as_ref());
    let def_where = where_clause(&out_bounds);
    let mut call_preds = extractor_preds(extractors, ctx_ty, &state_in_ctx);
    call_preds.extend(out_bounds.iter().cloned());
    let call_where = where_clause(&call_preds);
    let prelude = extractor_prelude(
        extractors,
        ctx_param,
        ctx_ty,
        &state_in_ctx,
        headers_policy,
        &quote!(
            return ::core::result::Result::Err(::core::convert::Into::<
                ::ruststream::runtime::HandlerResult,
            >::into(__rs_err),)
        ),
    );
    Ok(quote! {
        #[allow(non_camel_case_types)]
        #vis struct #name;

        impl ::ruststream::runtime::IncludeDef for #name {
            type Form = #form;
        }

        #scaffold_items

        impl<#def_generics> ::ruststream::runtime::PublishingDef for #def_target
        #def_where
        {
            type Input = #input_kind;
            type Injections = (#(#injection_tys,)*);
            type Reply = #reply_ty;
            type Context = #ctx_ty;
            type Source = #source_ty;

            fn source(&self) -> Self::Source { #source_expr }
            fn reply_name(&self) -> &str { #reply_name_body }

            #workers_method

            #failure_method

            fn description(&self) -> ::core::option::Option<&str> {
                #description
            }

            #input_schema

            #headers_schema

            #message_meta

            #outgoing
        }

        impl<#state_generic #def_generics>
            ::ruststream::runtime::PublishingCall<#state_in_ctx> for #def_target
            #call_where
        {
            async fn call(
                &self,
                #pat: #input_param,
                __rs_inj: &Self::Injections,
                #ctx_param: &mut ::ruststream::runtime::Context<'_, #ctx_ty, #state_in_ctx>,
            ) -> ::core::result::Result<#reply_ty, ::ruststream::runtime::HandlerResult> {
                #prelude
                #injection_bindings
                #call_body
            }
        }
    })
}

/// Resolves a publishing handler's reply type and call body from its return type.
/// `-> Result<Reply, HandlerResult>` lets the handler skip the publish: `Err(result)` is
/// returned to the dispatcher as-is. A plain `-> Reply` is wrapped in `Ok` here. The check
/// is syntactic, so a type alias hiding the `Result` is treated as a plain reply type.
fn publishing_reply<'a>(
    func: &'a ItemFn,
    block: &syn::Block,
    bare: bool,
) -> syn::Result<(&'a Type, TokenStream2)> {
    let declared_ty = match &func.sig.output {
        ReturnType::Type(_, ty) => &**ty,
        ReturnType::Default => {
            return Err(Error::new_spanned(
                &func.sig,
                if bare {
                    "a publish_raw handler must return the reply bytes: Vec<u8>, or \
                     Result<Vec<u8>, HandlerResult>"
                } else {
                    "a publishing handler must return the reply value"
                },
            ));
        }
    };
    Ok(match publish_result_reply(declared_ty) {
        Some(reply_ty) => (reply_ty, quote!((async move #block).await)),
        None => (
            declared_ty,
            quote!(::core::result::Result::Ok((async move #block).await)),
        ),
    })
}

/// The input-axis pieces of a definition form: the input kind, the concrete parameter type
/// the generated call binds, and the schema / message metadata. A typed parameter decodes
/// into an owned value the handler borrows; a raw one borrows the payload itself and carries
/// no schema or message metadata.
fn input_pieces(
    input_ty: &Type,
    input_schema: &TokenStream2,
    message_meta: &TokenStream2,
    raw: bool,
) -> (TokenStream2, TokenStream2, TokenStream2, TokenStream2) {
    if raw {
        (
            quote!(::ruststream::runtime::RawBytes),
            quote!(&[u8]),
            quote!(),
            quote!(),
        )
    } else {
        (
            quote!(::ruststream::runtime::Decoded<#input_ty>),
            quote!(&#input_ty),
            input_schema.clone(),
            message_meta.clone(),
        )
    }
}

/// Everything the slot-carrying expansions share: where the definition traits land, the impls
/// tying the user-visible unit struct to the hidden publisher-generic definition, and the
/// pieces the def / call impls interpolate.
struct SlotScaffold {
    /// The definition impls' target: the unit struct itself (no Out parameters) or the hidden
    /// generic struct applied to its publisher generics.
    def_target: TokenStream2,
    /// The publisher generic parameters of the definition impls (empty without Out parameters).
    def_generics: TokenStream2,
    /// The hidden generic idents per Out parameter, in signature order.
    generics: Vec<SlotGenerics>,
    /// The hidden generic struct plus the unit struct's `HasSlots` / `BindSlots` impls.
    scaffold: TokenStream2,
    /// The `__RsOutN: <bounds> + 'static` predicates for the hidden definition's impls.
    out_bounds: Vec<TokenStream2>,
}

/// The hidden generic idents of one Out parameter: its publisher (`__RsOutN`) and the scope
/// codec its typed publishes encode with (`__RsOutCodecN`).
struct SlotGenerics {
    publisher: Ident,
    codec: Ident,
}

/// The where-clause predicates of the hidden definition's impls, per Out parameter: the
/// publisher generic carries the user's capability bounds, the codec generic the scope codec's,
/// and a declared message set adds its dictionary membership (fully concrete predicates, so a
/// type outside the dictionary fails right at the handler).
fn slot_bounds(outs: &[OutParam<'_>], generics: &[SlotGenerics]) -> Vec<TokenStream2> {
    let mut out_bounds: Vec<TokenStream2> = Vec::new();
    for (out, slot) in outs.iter().zip(generics) {
        let publisher = &slot.publisher;
        let codec = &slot.codec;
        let bounds = out.bounds;
        // The dispatch machinery shares the injected value across worker tasks, so
        // Send + Sync are structural, not optional; adding them here keeps broker-defined
        // capability bounds (which need not imply them) as ergonomic as the core ones.
        out_bounds.push(
            quote!(#publisher: #bounds + ::core::marker::Send + ::core::marker::Sync + 'static),
        );
        out_bounds.push(quote! {
            #codec: ::ruststream::codec::Codec
                + ::core::marker::Send
                + ::core::marker::Sync
                + 'static
        });
        let marker = &out.marker;
        match &out.bodies {
            Some(BodyDecl::List(bodies)) => {
                for body in bodies {
                    out_bounds.push(quote!(#body: ::ruststream::runtime::OutMessage<#marker>));
                }
            }
            Some(BodyDecl::Set(set)) => {
                out_bounds.push(quote!(#set: ::ruststream::runtime::OutMessages<#marker>));
            }
            None => {}
        }
    }
    out_bounds
}

/// Builds the [`SlotScaffold`]: without Out parameters the definition stays on the unit struct
/// and every slot piece is empty; with them, the unit struct the user passes to `include` keeps
/// marker metadata (`HasSlots`) plus the source-to-definition instantiation (`BindSlots`), and
/// the definition traits land on a hidden struct generic over the slot publishers, so the
/// concrete types are inferred from the include-site attachments.
fn slot_scaffold(
    vis: &syn::Visibility,
    name: &Ident,
    outs: &[OutParam<'_>],
    has_seek: bool,
) -> SlotScaffold {
    if outs.is_empty() {
        return SlotScaffold {
            def_target: quote!(#name),
            def_generics: quote!(),
            generics: Vec::new(),
            scaffold: quote!(),
            out_bounds: Vec::new(),
        };
    }
    let hidden_ident = Ident::new(&format!("__RsOutDef_{name}"), name.span());
    let generics: Vec<SlotGenerics> = (0..outs.len())
        .map(|index| SlotGenerics {
            publisher: Ident::new(&format!("__RsOut{index}"), name.span()),
            codec: Ident::new(&format!("__RsOutCodec{index}"), name.span()),
        })
        .collect();
    let all_generics: Vec<&Ident> = generics
        .iter()
        .flat_map(|slot| [&slot.publisher, &slot.codec])
        .collect();
    let markers: Vec<&TokenStream2> = outs.iter().map(|out| &out.marker).collect();
    let sources: Vec<Ident> = (0..outs.len())
        .map(|index| Ident::new(&format!("__RsSrc{index}"), name.span()))
        .collect();
    let source_values: Vec<Ident> = (0..outs.len())
        .map(|index| Ident::new(&format!("__rs_src{index}"), name.span()))
        .collect();
    let out_bounds = slot_bounds(outs, &generics);
    // Per parameter: the paired slot publisher and the scope codec.
    let bound_args: Vec<TokenStream2> = outs
        .iter()
        .zip(&sources)
        .flat_map(|(out, source)| {
            let marker = &out.marker;
            [
                quote! {
                    ::ruststream::runtime::SlotPublisher<
                        <#source as ::ruststream::PublishPolicy<__RsC>>::Live,
                        #marker,
                    >
                },
                quote!(__RsCodec),
            ]
        })
        .collect();
    // A trailing Seek parameter resolves off the subscription itself, so its extra is a unit.
    let seek_extra = has_seek.then(|| quote!((),));
    let scaffold = quote! {
        #[doc(hidden)]
        #[allow(non_camel_case_types)]
        #vis struct #hidden_ident<#(#all_generics),*>(
            ::core::marker::PhantomData<fn() -> (#(#all_generics,)*)>,
        );

        impl ::ruststream::runtime::HasSlots for #name {
            type Markers = (#(#markers,)*);
        }

        impl<__RsC, __RsCodec, #(#sources),*>
            ::ruststream::runtime::BindSlots<__RsC, (#((#sources, __RsCodec),)*)> for #name
        where
            __RsC: ::ruststream::ConnectedBroker,
            #(#sources: ::ruststream::PublishPolicy<__RsC>,)*
        {
            type Bound = #hidden_ident<#(#bound_args,)*>;
            type Extra = (#((#sources, __RsCodec),)* #seek_extra);

            fn bind(self, sources: (#((#sources, __RsCodec),)*)) -> (Self::Bound, Self::Extra) {
                let (#(#source_values,)*) = sources;
                (
                    #hidden_ident(::core::marker::PhantomData),
                    (#(#source_values,)* #seek_extra),
                )
            }
        }
    };
    SlotScaffold {
        def_target: quote!(#hidden_ident<#(#all_generics),*>),
        def_generics: quote!(#(#all_generics),*),
        generics,
        scaffold,
        out_bounds,
    }
}

/// Collects the canonical injection tuple (Out parameters in signature order, then Seek), and
/// the `let` bindings resolving each user pattern from the resolved tuple. An `Out(x)` /
/// `Seek(x)`-shaped pattern binds the injected value itself; any other pattern binds a
/// reference to the whole marker struct. `out_generics` supplies the publisher generic per Out
/// parameter (empty when the handler has none).
fn injection_pieces(
    outs: &[OutParam<'_>],
    out_generics: &[SlotGenerics],
    seek: Option<(&Pat, &Type)>,
) -> (Vec<TokenStream2>, TokenStream2) {
    let mut injection_tys = Vec::new();
    let mut bindings = Vec::new();
    for (index, (out, slot)) in outs.iter().zip(out_generics).enumerate() {
        let marker = &out.marker;
        let publisher = &slot.publisher;
        let codec = &slot.codec;
        let body = match &out.bodies {
            Some(BodyDecl::List(bodies)) => quote!((#(#bodies,)*)),
            Some(BodyDecl::Set(set)) => quote!(#set),
            None => quote!(()),
        };
        injection_tys.push(quote! {
            ::ruststream::runtime::Out<#publisher, #marker, #body, #codec>
        });
        bindings.push(injection_binding(out.pat, index));
    }
    if let Some((seek_pat, seeker_ty)) = seek {
        injection_tys.push(quote!(::ruststream::runtime::Seek<#seeker_ty>));
        bindings.push(injection_binding(seek_pat, outs.len()));
    }
    (injection_tys, quote!(#(#bindings)*))
}

/// One injected parameter's binding: a single-element tuple-struct pattern (`Out(x)`,
/// `Seek(x)`) binds the wrapped value by reference, through the user's own path (so their
/// import stays used) with a rest pattern absorbing the marker field; any other pattern binds
/// the whole element.
fn injection_binding(pat: &Pat, index: usize) -> TokenStream2 {
    let index = syn::Index::from(index);
    if let Pat::TupleStruct(tuple) = pat
        && tuple.elems.len() == 1
    {
        let path = &tuple.path;
        let inner = &tuple.elems[0];
        return quote!(let #path(#inner, ..) = &__rs_inj.#index;);
    }
    quote!(let #pat = &__rs_inj.#index;)
}

/// The state generic of a call impl and the state type used in its `Context`: a handler that
/// names a state type is bound to it (no generic), one that names none is generic over the
/// state, so it mounts on an app with any state type. The generic ends with a trailing comma
/// so it composes with the slot generics.
fn state_pieces(state_ty: Option<&TokenStream2>) -> (TokenStream2, TokenStream2) {
    match state_ty {
        Some(state_ty) => (quote!(), quote!(#state_ty)),
        None => (
            quote!(__RsState: ::core::marker::Send + ::core::marker::Sync,),
            quote!(__RsState),
        ),
    }
}

/// The form token of an injected definition: an Out parameter needs publisher attachments at
/// the include site, so it selects the builder form; injections resolved off the subscription
/// alone mount eagerly.
fn injected_form(has_out: bool) -> TokenStream2 {
    if has_out {
        quote!(::ruststream::runtime::forms::Out)
    } else {
        quote!(::ruststream::runtime::forms::Seek)
    }
}

/// The startup-injection form: `Out` / `Seek` parameters travel as one tuple resolved by the
/// runtime after the subscription opens, so any combination shares this single expansion.
/// `raw` selects the byte input kind (the handler borrows the payload as `&[u8]`).
///
/// A Seek-only handler keeps the definition on the unit struct itself; Out parameters move the
/// definition onto the hidden publisher-generic struct, connected through [`slot_scaffold`].
fn expand_injected(parts: &HandlerParts<'_>, raw: bool) -> TokenStream2 {
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
        ctx_ty,
        state_ty,
        extractors,
        outs,
        seek,
        workers_method,
        failure_method,
        headers_policy,
        headers_schema,
    } = parts;

    let form = injected_form(!outs.is_empty());
    let SlotScaffold {
        def_target,
        def_generics,
        generics: out_generics,
        scaffold: scaffold_items,
        out_bounds,
    } = slot_scaffold(vis, name, outs, seek.is_some());
    let (injection_tys, injection_bindings) = injection_pieces(outs, &out_generics, *seek);
    let (input_kind, input_param, input_schema, message_meta) =
        input_pieces(input_ty, input_schema, message_meta, raw);
    let outgoing = outgoing_method(None, outs);

    let (state_generic, state_in_ctx) = state_pieces(state_ty.as_ref());
    let def_where = where_clause(&out_bounds);
    let mut call_preds = extractor_preds(extractors, ctx_ty, &state_in_ctx);
    call_preds.extend(out_bounds.iter().cloned());
    let call_where = where_clause(&call_preds);
    let prelude = extractor_prelude(
        extractors,
        ctx_param,
        ctx_ty,
        &state_in_ctx,
        headers_policy,
        &quote!(
            return ::ruststream::runtime::IntoSettle::into_settle(::core::convert::Into::<
                ::ruststream::runtime::HandlerResult,
            >::into(__rs_err),)
        ),
    );

    quote! {
        #[derive(Clone, Copy)]
        #[allow(non_camel_case_types)]
        #vis struct #name;

        impl ::ruststream::runtime::IncludeDef for #name {
            type Form = #form;
        }

        #scaffold_items

        impl<#def_generics> ::ruststream::runtime::InjectDef for #def_target
        #def_where
        {
            type Input = #input_kind;
            type Context = #ctx_ty;
            type Source = #source_ty;
            type Injections = (#(#injection_tys,)*);

            fn source(&self) -> Self::Source { #source_expr }

            #workers_method

            #failure_method

            fn description(&self) -> ::core::option::Option<&str> {
                #description
            }

            #input_schema

            #headers_schema

            #message_meta

            #outgoing
        }

        impl<#state_generic #def_generics>
            ::ruststream::runtime::InjectCall<#state_in_ctx> for #def_target
            #call_where
        {
            async fn call(
                &self,
                #pat: #input_param,
                __rs_inj: &Self::Injections,
                #ctx_param: &mut ::ruststream::runtime::Context<'_, #ctx_ty, #state_in_ctx>,
            ) -> ::ruststream::runtime::Settle {
                #prelude
                #injection_bindings
                ::ruststream::runtime::IntoSettle::into_settle(
                    (async move #block).await,
                )
            }
        }
    }
}

fn expand_subscribing(parts: &HandlerParts<'_>, raw: bool) -> TokenStream2 {
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
        ctx_ty,
        state_ty,
        extractors,
        outs: _,
        seek: _,
        workers_method,
        failure_method,
        headers_policy,
        headers_schema,
    } = parts;

    let (input_kind, input_param, input_schema, message_meta) =
        input_pieces(input_ty, input_schema, message_meta, raw);
    // The handler runs over the input kind's borrowed target: `Handler<T>` for a typed
    // parameter, `Handler<[u8]>` for a raw one - the mount adapter lends it the payload itself.
    let input_target = if raw { quote!([u8]) } else { quote!(#input_ty) };
    let form = if raw {
        quote!(::ruststream::runtime::forms::RawSubscribing)
    } else {
        quote!(::ruststream::runtime::forms::Subscribing)
    };

    // A handler that names a state type is bound to it; one that does not is generic over the
    // state, so it mounts on an app with any state type. Either shape satisfies the mount-site
    // `Handler<Target, Context, St>` bound, the former only for its `St`, the latter for every
    // `St`.
    let (impl_generics, state_in_ctx) = match &state_ty {
        Some(state_ty) => (quote!(), quote!(#state_ty)),
        None => (
            quote!(<__RsState: ::core::marker::Send + ::core::marker::Sync>),
            quote!(__RsState),
        ),
    };
    let where_clause = extractor_where(extractors, ctx_ty, &state_in_ctx);
    let prelude = extractor_prelude(
        extractors,
        ctx_param,
        ctx_ty,
        &state_in_ctx,
        headers_policy,
        &quote!(
            return ::ruststream::runtime::IntoSettle::into_settle(::core::convert::Into::<
                ::ruststream::runtime::HandlerResult,
            >::into(__rs_err),)
        ),
    );

    quote! {
            #[derive(Clone, Copy)]
            #[allow(non_camel_case_types)]
            #vis struct #name;

            impl #impl_generics
                ::ruststream::runtime::Handler<#input_target, #ctx_ty, #state_in_ctx> for #name
                #where_clause
            {
                async fn handle(
                    &self,
                    #pat: #input_param,
                    #ctx_param: &mut ::ruststream::runtime::Context<'_, #ctx_ty, #state_in_ctx>,
                ) -> ::ruststream::runtime::Settle {
                    #prelude
                    ::ruststream::runtime::IntoSettle::into_settle(
                        (async move #block).await,
                    )
                }
            }

            impl ::ruststream::runtime::IncludeDef for #name {
                type Form = #form;
            }

            impl ::ruststream::runtime::SubscriberDef for #name {
                type Input = #input_kind;
                type Context = #ctx_ty;
                type Handler = Self;
                type Source = #source_ty;

                fn source(&self) -> Self::Source { #source_expr }

                #workers_method

            #failure_method

                fn description(&self) -> ::core::option::Option<&str> {
                    #description
                }

                #input_schema

            #headers_schema

                #message_meta

                fn into_handler(self) -> Self { self }
            }
    }
}
