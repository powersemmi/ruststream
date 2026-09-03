//! Expansion of the `#[subscriber]` forms: the handler signature is dissected into
//! [`HandlerParts`], then one `impl Handle` plus the value-definition wiring is generated
//! around the original function body ([`unified`]).

mod unified;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::{ToTokens, quote};
use syn::{Error, Expr, FnArg, Ident, ItemFn, Pat, PatType, ReturnType, Type, TypePath};

use crate::parse::{
    FailurePolicyArg, SubscriberArgs, WorkersArg, doc_description, position_type, source_tokens,
    vec_element,
};

pub(crate) fn subscriber(args: &SubscriberArgs, func: &ItemFn) -> syn::Result<TokenStream> {
    let parts = handler_parts(args, func)?;
    Ok(unified::expand(args, &parts, func)?.into())
}

/// The pieces of the handler shared by every expansion form, extracted from the signature.
struct HandlerParts<'a> {
    vis: &'a syn::Visibility,
    name: &'a Ident,
    block: &'a syn::Block,
    pat: &'a Pat,
    input_ty: &'a Type,
    /// The `(H, P)` arguments of a `Message<H, P>`-shaped input, when the input has that shape:
    /// the core decodes the payload and the header contract in one stage.
    pair: Option<(&'a Type, &'a Type)>,
    description: TokenStream2,
    source_expr: TokenStream2,
    ctx_param: TokenStream2,
    ctx_ty: TokenStream2,
    state_ty: Option<TokenStream2>,
    extractors: Vec<(&'a Pat, &'a Type)>,
    outs: Vec<OutParam<'a>>,
    /// What the handler consumes per invocation, inferred from its message parameter.
    shape: Shape,
    /// The builder calls the attribute's settings expand into, chained onto
    /// `SubscriberBuilder::new(..)` inside the generated `Declared::declare`.
    settings_chain: TokenStream2,
    /// The source type the declared builder ends up over: the attribute's source, wrapped in
    /// `StartAt<_, _>` when the attribute names a start position.
    settings_source_ty: TokenStream2,
    /// Which settings the attribute fixed, as the builder's `(workers, failures, position)`
    /// state tuple.
    settings_state_ty: TokenStream2,
}

/// The per-delivery context type the handler named in its `ctx: &mut Context<'_, C>` parameter,
/// projected from the first `Ctx<K>` extractor's key when there is no ctx parameter, or `()`
/// when neither names one. Threaded into the single-subscriber definition's context axis so a
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

/// One `Out<impl Bounds[, Marker[, Set]]>` handler parameter: the binding pattern, the written
/// parameter type, the capability bounds the publisher generic carries, the slot marker type
/// (the implicit `DefaultSlot` when the parameter names none), and the declared message set of
/// the optional third position.
struct OutParam<'a> {
    pat: &'a Pat,
    /// The parameter type exactly as written, for the signature witness the unified emission
    /// keeps the user's imports used with.
    ty: &'a Type,
    bounds: &'a syn::punctuated::Punctuated<syn::TypeParamBound, syn::Token![+]>,
    marker: TokenStream2,
    /// The declared message set; `None` (also written `()`) leaves the slot unrestricted.
    bodies: Option<BodyDecl<'a>>,
}

/// The declared message set of an `Out` parameter's third position.
enum BodyDecl<'a> {
    /// A tuple listing the types inline.
    List(Vec<&'a Type>),
    /// A set-defining type: a `#[derive(Outgoing)]` type (itself) or a `#[derive(OutMessages)]`
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

/// The contract type `T` of a `Headers<T>`-shaped parameter type, when the type has that
/// shape. Purely syntactic (the last path segment `Headers` with exactly one type argument),
/// like the `Ctx<K>` probe below.
fn from_headers_ty(ty: &Type) -> Option<&Type> {
    let Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != "Headers" {
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

/// The `(H, P)` arguments of a `Message<H, P>`-shaped type, when the type has that shape.
/// Purely syntactic (the last path segment `Message` with exactly two type arguments), like the
/// `Ctx<K>` probe below: an alias hiding the pair reads as a plain payload type and fails on
/// its missing `Deserialize` impl instead.
fn message_pair_args(ty: &Type) -> Option<(&Type, &Type)> {
    let Type::Path(path) = ty else {
        return None;
    };
    let segment = path.path.segments.last()?;
    if segment.ident != "Message" {
        return None;
    }
    let syn::PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    let mut types = args.args.iter().filter_map(|arg| match arg {
        syn::GenericArgument::Type(ty) => Some(ty),
        _ => None,
    });
    let headers = types.next()?;
    let payload = types.next()?;
    if types.next().is_some() {
        return None;
    }
    Some((headers, payload))
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
/// generated `Handle` impl is generic over the state, so the handler mounts on an app with any
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
/// `&mut Context`. Each is resolved through `FromContext` before the body runs. `ctx_present`
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

/// The predicates binding each extractor type to `FromContext` for the handler's context `C`
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

/// The `let` bindings that resolve each extractor from the context before the body runs. A failed
/// extraction runs `reject` (a `return` settling the delivery by the rejection's
/// `HandlerOutcome`).
///
/// A `Headers<T>` parameter takes the policy-aware path instead of the generic
/// `FromContext` call, and reads the policy off the delivery context rather than off the
/// attribute: the effective materialization policy is the one the mount resolved, whether it was
/// named in `on_failure(..)` or on the builder.
fn extractor_prelude(
    extractors: &[(&Pat, &Type)],
    ctx_param: &TokenStream2,
    ctx_ty: &TokenStream2,
    state: &TokenStream2,
    reject: &TokenStream2,
) -> TokenStream2 {
    let binds = extractors.iter().map(|(pat, ty)| {
        if from_headers_ty(ty).is_some() {
            return quote! {
                let #pat = {
                    // Read before the mutable borrow the extraction takes.
                    let __rs_policy = #ctx_param.decode_policy();
                    match <#ty>::extract(&mut *#ctx_param, __rs_policy) {
                        ::core::result::Result::Ok(__rs_value) => __rs_value,
                        ::core::result::Result::Err(__rs_err) => { #reject; }
                    }
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

/// Renders the `on_failure(..)` clause as the builder step it expands into, or nothing when the
/// clause is absent. Only the keys named in the clause are set; the rest keep the runtime
/// defaults.
fn failure_step(args: &SubscriberArgs) -> TokenStream2 {
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
        .on_failure(::ruststream::runtime::FailurePolicies::default() #panic #decode)
    }
}

/// Rejects a `Headers<T>` parameter on the batch forms: a header contract is per-delivery,
/// and a batch pairs each element with its own contract through the `Message<H, P>` input
/// instead.
fn reject_batch_headers(shape: Shape, extractors: &[(&Pat, &Type)]) -> syn::Result<()> {
    if shape == Shape::Batch
        && let Some((_, ty)) = extractors
            .iter()
            .find(|(_, ty)| from_headers_ty(ty).is_some())
    {
        return Err(Error::new_spanned(
            ty,
            "headers are per-delivery: a batch pairs each element with its contract - take the \
             page as `&[Message<H, T>]` and read `element.headers` (`Headers<..>` extraction \
             stays on the single-message forms)",
        ));
    }
    Ok(())
}

/// The expression capturing the headers schema for the unified emission: the `Headers<T>`
/// extractor's contract when one is declared, the pair input's own contract (`Message<H, P>`
/// documents `H`), else the input type's declared header contract probe.
fn headers_schema_expr(
    extractors: &[(&Pat, &Type)],
    input_static: &TokenStream2,
    pair: Option<(&Type, &Type)>,
) -> TokenStream2 {
    if let Some((headers, _)) = pair {
        return quote! {{
            #[allow(unused_imports)]
            use ::ruststream::__private::NoSchemaProbe as _;
            ::ruststream::__private::Probe::<#headers>::new().schema_json()
        }};
    }
    extractors
        .iter()
        .find_map(|(_, ty)| from_headers_ty(ty))
        .map_or_else(
            || {
                quote! {{
                    #[allow(unused_imports)]
                    use ::ruststream::__private::NoHeadersSchemaProbe as _;
                    ::ruststream::__private::Probe::<#input_static>::new().headers_schema_json()
                }}
            },
            |contract| {
                quote! {{
                    #[allow(unused_imports)]
                    use ::ruststream::__private::NoSchemaProbe as _;
                    ::ruststream::__private::Probe::<#contract>::new().schema_json()
                }}
            },
        )
}

/// One `outgoing()` entry: the message type's metadata probed at the call site (schema,
/// `MessageInfo` name / description, headers contract), published to `channel`. The explicit
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
            })
            .with_serialized({
                #[allow(unused_imports)]
                use ::ruststream::__private::NoSerializedProbe as _;
                ::ruststream::__private::Probe::<#message_ty>::new().serialized_wire()
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

/// Renders the `workers(..)` clause as the builder step it expands into, or nothing when the
/// clause is absent.
fn workers_step(args: &SubscriberArgs, handler: &Ident, shape: Shape) -> syn::Result<TokenStream2> {
    let Some(WorkersArg { count, by_key }) = &args.workers else {
        return Ok(quote!());
    };
    let count = workers_count(count, handler)?;
    if let Some(marker) = by_key {
        if shape == Shape::Batch {
            return Err(Error::new(
                marker.span(),
                "by_key lanes order single messages per key; they do not apply to a batch \
                 handler",
            ));
        }
        return Ok(quote!(.workers_by_key(#count)));
    }
    Ok(quote!(.workers(#count)))
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
/// the implicit `DefaultSlot`. The bound lands on the wired live value, so what a body writes is
/// the broker capability vocabulary the include site's policy is checked against.
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
            ty,
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
/// Rejects a duplicate list entry (its membership index inference would be ambiguous).
fn body_decl(body: &Type) -> syn::Result<Option<BodyDecl<'_>>> {
    let Type::Tuple(tuple) = body else {
        // A bare type defines its own set: a #[derive(Outgoing)] type declares itself, a
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

/// What the handler consumes per invocation, read off its message parameter - the attribute
/// carries no form clause: `&T` is one message, `&[T]` a whole page of them. Which lane the
/// element rides (the codec, or its own `Deserialized` construction) is the type's own
/// business, so the shape stops at the slice question.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Shape {
    Single,
    Batch,
}

/// Resolves the message parameter's referent into the handler's shape and the element type. The
/// mapping is purely syntactic, over the type's own tokens: an alias hiding a slice reads as a
/// single message. A bare byte slice is rejected with the derive that names the lane.
fn resolve_shape(reference: &syn::TypeReference) -> syn::Result<(Shape, &Type)> {
    let elem = &*reference.elem;
    if is_u8_slice(elem) {
        return Err(Error::new_spanned(
            elem,
            "a payload's bytes take a named type: #[derive(Deserialized)] on a newtype over \
             the payload view (struct Frame<'a>(&'a [u8]);) and take `&Frame<'_>`",
        ));
    }
    match elem {
        Type::Slice(slice) => {
            if is_u8_slice(&slice.elem)
                || matches!(&*slice.elem, Type::Reference(r) if is_u8_slice(&r.elem))
            {
                return Err(Error::new_spanned(
                    &slice.elem,
                    "a page of payloads takes a named element type: #[derive(Deserialized)] on \
                     a newtype over the payload view (struct Frame<'a>(&'a [u8]);) and take \
                     `&[Frame<'_>]`",
                ));
            }
            Ok((Shape::Batch, &slice.elem))
        }
        other => Ok((Shape::Single, other)),
    }
}

/// True when `ty` is syntactically `[u8]`, the byte-slice spelling the input rule rejects in
/// favor of a named `Deserialized` type.
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

/// Rewrites every elided lifetime (`'_`) in `ty` to `lifetime`, reporting whether any was
/// found. How a lifetime-carrying input (a `Deserialized` view) is spelled at each position:
/// the emitted impl introduces its own lifetime for the `In` axis, and the definition's
/// lifetime-free projections take the `'static` representative.
fn subst_elided_lifetime(ty: &Type, lifetime: &str) -> (Type, bool) {
    use syn::visit_mut::VisitMut;

    struct Subst {
        lifetime: syn::Lifetime,
        found: bool,
    }

    impl VisitMut for Subst {
        fn visit_lifetime_mut(&mut self, node: &mut syn::Lifetime) {
            if node.ident == "_" {
                *node = self.lifetime.clone();
                self.found = true;
            }
        }
    }

    let mut subst = Subst {
        lifetime: syn::Lifetime::new(lifetime, proc_macro2::Span::call_site()),
        found: false,
    };
    let mut rewritten = ty.clone();
    subst.visit_type_mut(&mut rewritten);
    (rewritten, subst.found)
}

/// Dissects the handler's first parameter into the message pattern, the shape, the input type,
/// and the optional `Message<H, P>` pair arguments.
fn message_param(func: &ItemFn) -> syn::Result<MessageParam<'_>> {
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
    let (shape, input_ty) = resolve_shape(reference)?;
    Ok((pat, shape, input_ty, message_pair_args(input_ty)))
}

/// What [`message_param`] dissects: the pattern, the shape, the input type, and the pair
/// arguments when the input is `Message<H, P>`-shaped.
type MessageParam<'a> = (&'a Pat, Shape, &'a Type, Option<(&'a Type, &'a Type)>);

fn handler_parts<'a>(args: &SubscriberArgs, func: &'a ItemFn) -> syn::Result<HandlerParts<'a>> {
    let (pat, shape, input_ty, pair) = message_param(func)?;
    let description = doc_description(&func.attrs);
    let (source_ty, source_expr) = source_tokens(&args.source)?;
    // `start_at(<position>)` decorates the source with the core `StartAt` wrapper, so the
    // subscription is sought to the position before the first delivery. It is a builder step
    // like the rest, which is why it changes the declared builder's source type rather than the
    // definition's own.
    let (settings_source_ty, start_at_step, position_state) = match &args.start_at {
        Some(position) => {
            let position_ty = position_type(position)?;
            (
                quote!(::ruststream::StartAt<#source_ty, #position_ty>),
                quote!(.start_at(#position)),
                quote!(::ruststream::runtime::Fixed),
            )
        }
        None => (
            source_ty.clone(),
            quote!(),
            quote!(::ruststream::runtime::Open),
        ),
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
    let ctx_ty = context_type(func);
    let state_ty = state_type(func);

    reject_batch_headers(shape, &extractors)?;

    // The attribute's settings are the builder calls a user would write at the mount site, and
    // the settings state records which of them are no longer open there. `start_at` comes last:
    // it wraps the source the earlier steps left alone.
    let workers_step = workers_step(args, &func.sig.ident, shape)?;
    let failure_step = failure_step(args);
    let workers_state = state_marker(args.workers.is_some());
    let failure_state = state_marker(args.on_failure.is_some());
    let settings_chain = quote!(#workers_step #failure_step #start_at_step);
    let settings_state_ty = quote!((#workers_state, #failure_state, #position_state));

    Ok(HandlerParts {
        vis: &func.vis,
        name: &func.sig.ident,
        block: &func.block,
        pat,
        input_ty,
        pair,
        description,
        source_expr,
        ctx_param,
        ctx_ty,
        state_ty,
        extractors,
        outs,
        shape,
        settings_chain,
        settings_source_ty,
        settings_state_ty,
    })
}

/// The settings-state marker of one setting: fixed when the attribute named it, open otherwise.
fn state_marker(fixed: bool) -> TokenStream2 {
    if fixed {
        quote!(::ruststream::runtime::Fixed)
    } else {
        quote!(::ruststream::runtime::Open)
    }
}

/// Splits a batch publishing handler's declared return type into the reply element type and the
/// body that yields `Result<Vec<Reply>, HandlerOutcome>`. `-> Result<Vec<Reply>, HandlerOutcome>`
/// is passed through; a plain `-> Vec<Reply>` is wrapped in `Ok`. Both checks are syntactic, like
/// the single-message publish form: a type alias is not seen through.
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
                 Result<Vec<Reply>, HandlerOutcome>",
            ));
        }
    };
    if let Some(ok_ty) = crate::parse::publish_result_reply(declared_ty) {
        let Some(elem) = vec_element(ok_ty) else {
            return Err(Error::new_spanned(
                ok_ty,
                "a batch publishing handler replies with a Vec: Result<Vec<Reply>, HandlerOutcome>",
            ));
        };
        Ok((elem, quote!((async move #block).await)))
    } else {
        let Some(elem) = vec_element(declared_ty) else {
            return Err(Error::new_spanned(
                declared_ty,
                "a batch publishing handler returns the replies: Vec<Reply>, or \
                 Result<Vec<Reply>, HandlerOutcome>",
            ));
        };
        Ok((
            elem,
            quote!(::core::result::Result::Ok((async move #block).await)),
        ))
    }
}

/// Resolves a publishing handler's reply type and call body from its return type.
/// `-> Result<Reply, HandlerOutcome>` lets the handler skip the publish: `Err(result)` is
/// returned to the dispatcher as-is. A plain `-> Reply` is wrapped in `Ok` here. The check
/// is syntactic, so a type alias hiding the `Result` is treated as a plain reply type.
fn publishing_reply<'a>(
    func: &'a ItemFn,
    block: &syn::Block,
) -> syn::Result<(&'a Type, TokenStream2)> {
    let declared_ty = match &func.sig.output {
        ReturnType::Type(_, ty) => &**ty,
        ReturnType::Default => {
            return Err(Error::new_spanned(
                &func.sig,
                "a publishing handler must return the reply value",
            ));
        }
    };
    Ok(match crate::parse::publish_result_reply(declared_ty) {
        Some(reply_ty) => (reply_ty, quote!((async move #block).await)),
        None => (
            declared_ty,
            quote!(::core::result::Result::Ok((async move #block).await)),
        ),
    })
}
