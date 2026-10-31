//! The handler's parameters, read off its signature: the message parameter and the shape it
//! gives the handler, the optional `&mut Context`, the extractors, and the `Out` slots.

use proc_macro2::TokenStream as TokenStream2;
use quote::{ToTokens, quote};
use syn::punctuated::Punctuated;
use syn::{Error, FnArg, ItemFn, Pat, PatType, Type, TypePath};

/// One `Out<impl Bounds[, Marker[, Set]]>` handler parameter: the binding pattern, the written
/// parameter type, the capability bounds the publisher generic carries, the slot marker type
/// (the implicit `DefaultSlot` when the parameter names none), and the declared message set of
/// the optional third position.
pub(super) struct OutParam<'a> {
    pub(super) pat: &'a Pat,
    /// The parameter type exactly as written, for the signature witness the unified emission
    /// keeps the user's imports used with.
    pub(super) ty: &'a Type,
    pub(super) bounds: &'a Punctuated<syn::TypeParamBound, syn::Token![+]>,
    pub(super) marker: TokenStream2,
    /// The declared message set; `None` (also written `()`) leaves the slot unrestricted.
    pub(super) bodies: Option<BodyDecl<'a>>,
}

/// The declared message set of an `Out` parameter's third position.
pub(super) enum BodyDecl<'a> {
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
pub(super) fn from_headers_ty(ty: &Type) -> Option<&Type> {
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

/// Whether a `Ctx` key is the app runtime's handle, by the last segment of its path.
pub(super) fn is_main_runtime_key(key: &Type) -> bool {
    matches!(key, Type::Path(path) if path.path.segments.last().is_some_and(|segment| segment.ident == "MainRuntime"))
}

/// The key type `K` of a `Ctx<K>`-shaped parameter type, when the type has that shape.
pub(super) fn ctx_extractor_key(ty: &Type) -> Option<&Type> {
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
pub(super) fn state_type(func: &ItemFn) -> Option<TokenStream2> {
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
pub(super) fn is_context_param(ty: &Type) -> bool {
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
pub(super) fn collect_extractors(
    func: &ItemFn,
    ctx_present: bool,
) -> syn::Result<Vec<(&Pat, &Type)>> {
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

/// Splits the `Out<..>` parameters out of the extractor list, in signature order.
///
/// Each must carry its capability as `impl Trait` (the concrete publisher type is inferred at
/// the include site) and a distinct marker; a single parameter may omit the marker and binds
/// the implicit `DefaultSlot`. The bound lands on the wired live value, so what a body writes is
/// the broker capability vocabulary the include site's policy is checked against.
pub(super) fn split_outs<'a>(
    extractors: &mut Vec<(&'a Pat, &'a Type)>,
) -> syn::Result<Vec<OutParam<'a>>> {
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
/// carries no form clause: `&T` is one message, `&[T]` a whole batch of them. Which lane the
/// element rides (the codec, or its own `Deserialized` construction) is the type's own
/// business, so the shape stops at the slice question.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Shape {
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
                    "a batch of payloads takes a named element type: #[derive(Deserialized)] on \
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
pub(super) fn subst_elided_lifetime(ty: &Type, lifetime: &str) -> (Type, bool) {
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
pub(super) fn message_param(func: &ItemFn) -> syn::Result<MessageParam<'_>> {
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
pub(super) type MessageParam<'a> = (&'a Pat, Shape, &'a Type, Option<(&'a Type, &'a Type)>);

#[cfg(test)]
mod tests {
    use syn::parse_quote;

    use super::*;

    fn refusal<T>(result: syn::Result<T>) -> String {
        match result {
            Ok(_) => panic!("this signature is refused"),
            Err(err) => err.to_string(),
        }
    }

    /// The extractors of `func` with the `Out` slots taken out, or why the slots are refused.
    fn outs_of(func: &ItemFn) -> syn::Result<usize> {
        let mut extractors = collect_extractors(func, false)?;
        split_outs(&mut extractors).map(|outs| outs.len())
    }

    #[test]
    fn the_message_parameter_is_one_reference() {
        let none: ItemFn = parse_quote!(
            async fn on_tick() {}
        );
        let receiver: ItemFn = parse_quote!(
            async fn on_order(&self) {}
        );
        let owned: ItemFn = parse_quote!(
            async fn on_order(order: Order) {}
        );

        assert_eq!(
            refusal(message_param(&none)),
            "a #[subscriber] handler must take exactly one message parameter"
        );
        assert_eq!(
            refusal(message_param(&receiver)),
            "a #[subscriber] handler cannot take `self`"
        );
        assert_eq!(
            refusal(message_param(&owned)),
            "the message parameter must be a reference `&T`"
        );
    }

    #[test]
    fn the_context_comes_right_after_the_message() {
        let late: ItemFn = parse_quote!(
            async fn on_order(order: &Order, State(db): State<Db>, ctx: &mut Context<'_>) {}
        );

        assert_eq!(
            refusal(collect_extractors(&late, false)),
            "the `&mut Context` parameter must come immediately after the message, before any \
             extractor parameters"
        );
    }

    #[test]
    fn a_handler_takes_at_most_three_slots() {
        let four: ItemFn = parse_quote!(
            async fn on_order(
                order: &Order,
                Out(a): Out<impl Publisher, A>,
                Out(b): Out<impl Publisher, B>,
                Out(c): Out<impl Publisher, C>,
                Out(d): Out<impl Publisher, D>,
            ) {
            }
        );

        assert_eq!(
            refusal(outs_of(&four)),
            "a #[subscriber] handler takes at most three Out parameters"
        );
    }

    #[test]
    fn a_slot_lists_at_most_four_distinct_types_inline() {
        let five: ItemFn = parse_quote!(
            async fn on_order(order: &Order, Out(out): Out<impl Publisher, Sent, (A, B, C, D, E)>) {
            }
        );
        let twice: ItemFn = parse_quote!(
            async fn on_order(order: &Order, Out(out): Out<impl Publisher, Sent, (A, B, A)>) {}
        );

        assert_eq!(
            refusal(outs_of(&five)),
            "an Out parameter lists at most four message types inline; declare a \
             #[derive(OutMessages)] set enum for a larger one"
        );
        assert_eq!(
            refusal(outs_of(&twice)),
            "this type is already in the Out parameter's message list"
        );
    }

    #[test]
    fn a_parameter_that_only_looks_like_a_slot_is_an_extractor() {
        let lookalikes: ItemFn = parse_quote!(
            async fn on_order(
                order: &Order,
                outs: &Out,
                bare: Out,
                lifetime: Out<'static>,
                four: Out<impl Publisher, A, (), D>,
            ) {
            }
        );

        let mut extractors =
            collect_extractors(&lookalikes, false).expect("every parameter is an extractor");
        let outs = split_outs(&mut extractors).expect("no slot is refused");

        assert!(outs.is_empty(), "slots taken out of look-alike parameters");
        assert_eq!(extractors.len(), 4, "the look-alikes stay extractors");
    }

    #[test]
    fn only_a_headers_of_one_contract_reads_the_headers() {
        let contract: Type = parse_quote!(Headers<Meta>);
        assert!(from_headers_ty(&contract).is_some());

        let plain: [Type; 4] = [
            parse_quote!(&Headers<Meta>),
            parse_quote!(Headers),
            parse_quote!(Headers<'static>),
            parse_quote!(Headers<Meta, Other>),
        ];
        for ty in plain {
            assert!(
                from_headers_ty(&ty).is_none(),
                "`{}` reads no header contract",
                quote!(#ty)
            );
        }
    }

    #[test]
    fn only_a_message_of_two_types_is_a_pair() {
        let pair: Type = parse_quote!(Message<Meta, Order>);
        assert!(message_pair_args(&pair).is_some());

        let plain: [Type; 4] = [
            parse_quote!([Message<Meta, Order>]),
            parse_quote!(Message),
            parse_quote!(Message<Order>),
            parse_quote!(Message<Meta, Order, Extra>),
        ];
        for ty in plain {
            assert!(
                message_pair_args(&ty).is_none(),
                "`{}` is a plain payload type",
                quote!(#ty)
            );
        }
    }

    #[test]
    fn only_a_ctx_of_one_key_reads_a_field() {
        let field: Type = parse_quote!(Ctx<Partition>);
        assert!(ctx_extractor_key(&field).is_some());

        let plain: [Type; 4] = [
            parse_quote!(&Ctx<Partition>),
            parse_quote!(Ctx),
            parse_quote!(Ctx<'static>),
            parse_quote!(Ctx<Partition, Offset>),
        ];
        for ty in plain {
            assert!(
                ctx_extractor_key(&ty).is_none(),
                "`{}` reads no context field",
                quote!(#ty)
            );
        }
    }

    #[test]
    fn a_second_parameter_that_is_no_context_names_no_state() {
        let extractor: ItemFn = parse_quote!(
            async fn on_order(order: &Order, State(db): State<Db>) {}
        );
        let stateless: ItemFn = parse_quote!(
            async fn on_order(order: &Order, ctx: &mut Context<'_>) {}
        );
        let stateful: ItemFn = parse_quote!(
            async fn on_order(order: &Order, ctx: &mut Context<'_, (), Db>) {}
        );

        assert!(state_type(&extractor).is_none());
        assert!(state_type(&stateless).is_none());
        assert_eq!(
            state_type(&stateful).map(|state| state.to_string()),
            Some(quote!(Db).to_string())
        );
    }
}
