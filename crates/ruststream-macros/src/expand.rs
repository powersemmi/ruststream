//! Expansion of the `#[subscriber]` forms: the handler signature is dissected into
//! [`HandlerParts`], then one of the definition impls (plain, publishing, injected, batch,
//! batch publishing) is generated around the original function body; the input axis (typed vs
//! raw bytes) is a flag on the plain, injected, and publishing expansions, not a form.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Error, FnArg, Ident, ItemFn, LitStr, Pat, PatType, ReturnType, Type, TypePath};

use crate::parse::{
    FailurePolicyArg, SubscriberArgs, WorkersArg, doc_description, position_type,
    publish_result_reply, source_tokens, vec_element,
};

pub(crate) fn subscriber(args: &SubscriberArgs, func: &ItemFn) -> syn::Result<TokenStream> {
    reject_raw_combinations(args)?;
    let parts = handler_parts(args, func)?;
    let body = if args.raw.is_some() {
        // The remaining combinations are already rejected above; publish_raw(..) selects the
        // byte reply form, otherwise raw is the plain form (injected when the signature
        // carries Out / Seek parameters - the input axis makes raw compose with them).
        match &args.publish_raw {
            Some(reply_topic) => expand_publishing(&parts, func, reply_topic, true, true)?,
            None if parts.out.is_some() || parts.seek.is_some() => expand_injected(&parts, true),
            None => expand_subscribing(&parts, true),
        }
    } else if let Some(reply_topic) = &args.publish_raw {
        expand_publishing(&parts, func, reply_topic, true, false)?
    } else {
        match (&args.batch, &args.publish) {
            (true, Some(reply_topic)) => expand_batch_publishing(&parts, func, reply_topic)?,
            (true, None) => expand_batch(&parts, func),
            (false, Some(reply_topic)) => {
                expand_publishing(&parts, func, reply_topic, false, false)?
            }
            (false, None) if parts.out.is_some() || parts.seek.is_some() => {
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
fn reject_raw_combinations(args: &SubscriberArgs) -> syn::Result<()> {
    if let (Some(_), Some(publish_raw)) = (&args.publish, &args.publish_raw) {
        return Err(Error::new(
            publish_raw.span(),
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
    if let Some(failure) = &args.on_failure
        && failure.decode.is_some()
    {
        return Err(Error::new(
            raw.span(),
            "on_failure(decode = ..) does not apply to raw: the payload is not decoded; keep \
             only on_failure(panic = ..)",
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
    out: Option<(&'a Pat, &'a Type)>,
    seek: Option<(&'a Pat, &'a Type)>,
    workers_method: TokenStream2,
    failure_method: TokenStream2,
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

/// The publisher type `P` of an `Out<P>`-shaped parameter type, when the type has that shape.
/// Purely syntactic (the last path segment `Out` with exactly one type argument), like the
/// `Ctx<K>` probe below.
fn out_param_type(ty: &Type) -> Option<&Type> {
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
    let mut types = args.args.iter().filter_map(|arg| match arg {
        syn::GenericArgument::Type(ty) => Some(ty),
        _ => None,
    });
    let publisher = types.next()?;
    if types.next().is_some() {
        return None;
    }
    Some(publisher)
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

/// The `where` predicates binding each extractor type to [`FromContext`] for the handler's context
/// `C` and state `S`, or nothing when there are no extractors. Added to the generated call impl so a
/// state-specific extractor compiles without forcing the handler to name a `&mut Context`.
fn extractor_where(
    extractors: &[(&Pat, &Type)],
    ctx_ty: &TokenStream2,
    state: &TokenStream2,
) -> TokenStream2 {
    if extractors.is_empty() {
        return quote!();
    }
    let preds = extractors
        .iter()
        .map(|(_, ty)| quote!(#ty: ::ruststream::runtime::FromContext<#ctx_ty, #state>));
    quote!(where #(#preds),*)
}

/// The `let` bindings that resolve each extractor from the context before the body runs. A failed
/// extraction runs `reject` (a `return` settling the delivery by the rejection's `HandlerResult`).
fn extractor_prelude(
    extractors: &[(&Pat, &Type)],
    ctx_param: &TokenStream2,
    ctx_ty: &TokenStream2,
    state: &TokenStream2,
    reject: &TokenStream2,
) -> TokenStream2 {
    let binds = extractors.iter().map(|(pat, ty)| {
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
    }
}

/// Renders the `workers(..)` clause as an override of the def's defaulted `workers` method, or
/// nothing when the clause is absent.
fn workers_method(args: &SubscriberArgs) -> syn::Result<TokenStream2> {
    let Some(WorkersArg { count, by_key }) = &args.workers else {
        return Ok(quote!());
    };
    if count.base10_parse::<usize>()? == 0 {
        return Err(Error::new(
            count.span(),
            "workers(0) is not a policy; the minimum is 1",
        ));
    }
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
                // The macro rejects workers(0) at expansion, so the None arm is unreachable;
                // MIN keeps the lowering panic-free.
                ::ruststream::runtime::Workers::keyed(
                    match ::core::num::NonZeroUsize::new(#count) {
                        ::core::option::Option::Some(count) => count,
                        ::core::option::Option::None => ::core::num::NonZeroUsize::MIN,
                    },
                )
            }
        });
    }
    Ok(quote! {
        fn workers(&self) -> ::ruststream::runtime::Workers {
            // The macro rejects workers(0) at expansion, so the None arm is unreachable;
            // MIN keeps the lowering panic-free.
            ::ruststream::runtime::Workers::pool(
                match ::core::num::NonZeroUsize::new(#count) {
                    ::core::option::Option::Some(count) => count,
                    ::core::option::Option::None => ::core::num::NonZeroUsize::MIN,
                },
            )
        }
    })
}

/// Splits the (at most one) `Out<P>` parameter out of the extractor list, rejecting a
/// duplicate and the unsupported form combinations.
fn split_out<'a>(
    args: &SubscriberArgs,
    func: &ItemFn,
    extractors: &mut Vec<(&'a Pat, &'a Type)>,
) -> syn::Result<Option<(&'a Pat, &'a Type)>> {
    let mut out = None;
    extractors.retain(|(pat, ty)| {
        if let Some(publisher_ty) = out_param_type(ty) {
            // Only the first Out parameter is kept; a duplicate is rejected below.
            if out.is_none() {
                out = Some((*pat, publisher_ty));
                return false;
            }
        }
        true
    });
    if let Some((_, dup)) = extractors
        .iter()
        .find(|(_, ty)| out_param_type(ty).is_some())
    {
        return Err(Error::new_spanned(
            dup,
            "a #[subscriber] handler takes at most one Out parameter",
        ));
    }
    if out.is_some() && (args.batch || args.publish.is_some() || args.publish_raw.is_some()) {
        return Err(Error::new_spanned(
            &func.sig,
            "an Out parameter is not supported together with batch(..), publish(..) or \
             publish_raw(..) yet; use the plain subscriber form",
        ));
    }
    Ok(out)
}

/// Splits the (at most one) `Seek<K>` parameter out of the extractor list, rejecting a
/// duplicate and the unsupported form combinations.
fn split_seek<'a>(
    args: &SubscriberArgs,
    func: &ItemFn,
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
    if seek.is_some() && (args.batch || args.publish.is_some() || args.publish_raw.is_some()) {
        return Err(Error::new_spanned(
            &func.sig,
            "a Seek parameter is not supported together with batch(..), publish(..) or \
             publish_raw(..) yet; use the plain subscriber form",
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
    let out = split_out(args, func, &mut extractors)?;
    let seek = split_seek(args, func, &mut extractors)?;
    let ctx_ty = context_type(func);
    let state_ty = state_type(func);

    let workers_method = workers_method(args)?;
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
        out,
        seek,
        workers_method,
        failure_method,
    })
}

/// Splits a batch publishing handler's declared return type into the reply element type and the
/// body that yields `Result<Vec<Reply>, HandlerResult>`. `-> Result<Vec<Reply>, HandlerResult>` is
/// passed through; a plain `-> Vec<Reply>` is wrapped in `Ok`. Both checks are syntactic, like the
/// single-message publish form: a type alias is not seen through.
fn batch_reply_body<'a>(
    declared_ty: &'a Type,
    block: &syn::Block,
) -> syn::Result<(&'a Type, TokenStream2)> {
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
        ctx_ty: _,
        state_ty,
        extractors,
        out: _,
        seek: _,
        workers_method,
        failure_method,
    } = parts;

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
    let (reply_elem, call_body) = batch_reply_body(declared_ty, block)?;

    // Like the single-message publishing form: the handler implements `BatchPublishingCall` only
    // for its named state (mounts on a matching app), or generically when it names none (mounts on
    // any app). The metadata-only `BatchPublishingDef` is unconditional.
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
            type Form = ::ruststream::runtime::forms::BatchPublishing;
        }

        impl ::ruststream::runtime::BatchPublishingDef for #name {
            type Input = ::ruststream::runtime::Decoded<#input_ty>;
            type Reply = #reply_elem;
            type Source = #source_ty;

            fn source(&self) -> Self::Source { #source_expr }
            fn reply_name(&self) -> &str { #reply_topic }

            #workers_method

            #failure_method

            fn description(&self) -> ::core::option::Option<&str> {
                #description
            }

            #input_schema

            #message_meta
        }

        impl #impl_generics
            ::ruststream::runtime::BatchPublishingCall<#state_in_ctx> for #name
            #where_clause
        {
            async fn call(
                &self,
                #pat: &[#input_ty],
                #ctx_param: &mut ::ruststream::runtime::Context<'_, (), #state_in_ctx>,
            ) -> ::core::result::Result<
                ::std::vec::Vec<#reply_elem>,
                ::ruststream::runtime::HandlerResult,
            > {
                #prelude
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
        out: _,
        seek: _,
        workers_method,
        failure_method,
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

                #message_meta

                fn into_handler(self) -> Self { self }
            }
    }
}

/// The reply-publishing form. `bare` marks the `publish_raw` (byte reply) variant - the same
/// definition and machinery, with the form token selecting the bare-policy default commit
/// instead of the typed-codec one - and `raw_input` selects the byte input kind (the handler
/// borrows the payload as `&[u8]`).
fn expand_publishing(
    parts: &HandlerParts<'_>,
    func: &ItemFn,
    reply_topic: &LitStr,
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
        out: _,
        seek: _,
        workers_method,
        failure_method,
    } = parts;

    let (reply_ty, call_body) = publishing_reply(func, block, bare)?;
    let form = if bare {
        quote!(::ruststream::runtime::forms::RawReply)
    } else {
        quote!(::ruststream::runtime::forms::Publishing)
    };
    let (input_kind, input_param, input_schema, message_meta) =
        input_pieces(input_ty, input_schema, message_meta, raw_input);

    // As for `expand_subscribing`: a publishing handler that names a state type implements
    // `PublishingCall` only for that state (mounts on a matching app); one that names none is
    // generic over the state (mounts on any app). The metadata-only `PublishingDef` is unconditional.
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

        impl ::ruststream::runtime::PublishingDef for #name {
            type Input = #input_kind;
            type Reply = #reply_ty;
            type Context = #ctx_ty;
            type Source = #source_ty;

            fn source(&self) -> Self::Source { #source_expr }
            fn reply_name(&self) -> &str { #reply_topic }

            #workers_method

            #failure_method

            fn description(&self) -> ::core::option::Option<&str> {
                #description
            }

            #input_schema

            #message_meta
        }

        impl #impl_generics
            ::ruststream::runtime::PublishingCall<#state_in_ctx> for #name
            #where_clause
        {
            async fn call(
                &self,
                #pat: #input_param,
                #ctx_param: &mut ::ruststream::runtime::Context<'_, #ctx_ty, #state_in_ctx>,
            ) -> ::core::result::Result<#reply_ty, ::ruststream::runtime::HandlerResult> {
                #prelude
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

/// Collects the canonical injection tuple (Out first, then Seek), the matching binding
/// patterns for the generated call (the user's parameter patterns are already
/// `Out(..)` / `Seek(..)`-shaped, so they destructure the tuple elements directly), and the
/// form token: an Out parameter needs a publisher attachment at the include site, so it
/// selects the builder form; injections resolved off the subscription alone mount eagerly.
fn injection_pieces(
    out: Option<(&Pat, &Type)>,
    seek: Option<(&Pat, &Type)>,
) -> (Vec<TokenStream2>, Vec<TokenStream2>, TokenStream2) {
    let mut injection_tys = Vec::new();
    let mut injection_pats = Vec::new();
    if let Some((out_pat, out_ty)) = out {
        injection_tys.push(quote!(::ruststream::runtime::Out<#out_ty>));
        injection_pats.push(quote!(#out_pat));
    }
    if let Some((seek_pat, seeker_ty)) = seek {
        injection_tys.push(quote!(::ruststream::runtime::Seek<#seeker_ty>));
        injection_pats.push(quote!(#seek_pat));
    }
    let form = if out.is_some() {
        quote!(::ruststream::runtime::forms::Out)
    } else {
        quote!(::ruststream::runtime::forms::Seek)
    };
    (injection_tys, injection_pats, form)
}

/// The startup-injection form: `Out` / `Seek` parameters travel as one tuple resolved by the
/// runtime after the subscription opens, so any combination shares this single expansion.
/// `raw` selects the byte input kind (the handler borrows the payload as `&[u8]`).
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
        out,
        seek,
        workers_method,
        failure_method,
    } = parts;

    let (injection_tys, injection_pats, form) = injection_pieces(*out, *seek);
    let (input_kind, input_param, input_schema, message_meta) =
        input_pieces(input_ty, input_schema, message_meta, raw);

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

        impl ::ruststream::runtime::InjectDef for #name {
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

            #message_meta
        }

        impl #impl_generics
            ::ruststream::runtime::InjectCall<#state_in_ctx> for #name
            #where_clause
        {
            async fn call(
                &self,
                #pat: #input_param,
                __rs_inj: &Self::Injections,
                #ctx_param: &mut ::ruststream::runtime::Context<'_, #ctx_ty, #state_in_ctx>,
            ) -> ::ruststream::runtime::Settle {
                #prelude
                let (#(#injection_pats,)*) = __rs_inj;
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
        out: _,
        seek: _,
        workers_method,
        failure_method,
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

                #message_meta

                fn into_handler(self) -> Self { self }
            }
    }
}
