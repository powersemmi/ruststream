//! The unified emission: one `impl Handle` around the user's body, and the value-definition
//! wiring in place of the definition-trait impls.
//!
//! The attribute stays sugar over the manual path's public vocabulary: the generated `Declared`
//! builds the same sealed definition the `subscriber(..) .. .build()` chain produces (with the
//! probe-captured metadata riding it as data), and every mount, include-site builder and
//! settings step is the runtime's own. A handler with `Out` parameters keeps the unit struct as
//! the include-site value and instantiates the arena-carrying definition in `BindSlots::bind`,
//! so the concrete publisher types are still inferred from the attached policies.

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Ident, ItemFn, Pat};

use crate::parse::{ReplyArg, SubscriberArgs};
use crate::runtime::Runtime;

use super::{
    BodyDecl, HandlerParts, OutParam, Shape, batch_reply_body, extractor_preds, extractor_prelude,
    headers_schema_expr, outgoing_entry, publishing_reply, subst_elided_lifetime, where_clause,
};

/// The `Body` position of one `Out` parameter's arena entry: the declared message set as a
/// type, or the unrestricted `()`.
fn body_ty(bodies: Option<&BodyDecl<'_>>) -> TokenStream2 {
    match bodies {
        None => quote!(()),
        Some(BodyDecl::List(bodies)) => quote!((#(#bodies,)*)),
        Some(BodyDecl::Set(set)) => quote!(#set),
    }
}

/// The reply clause of one handler, with the body already normalized to the `Result` shape.
/// Which wire the reply travels (the reply codec, or its own bytes) is the reply type's
/// business, so the plan carries no route.
enum ReplyPlan<'a> {
    /// No reply: the body settles and nothing is published.
    None,
    /// A reply (`reply` / `reply("dest")`): the returned value publishes where its type
    /// declares, or at the clause's default, on the wire its type selects.
    Reply {
        dest: &'a ReplyArg,
        ty: TokenStream2,
        body: TokenStream2,
    },
}

impl ReplyPlan<'_> {
    fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    /// The channel one reply entry documents: the destination the mount site and the reply
    /// type resolve between them, not the clause's literal, so the document reports where the
    /// reply actually goes.
    fn channel(dest: &ReplyArg, ty: &TokenStream2) -> TokenStream2 {
        let resolved = Self::resolved(dest, ty);
        quote!(#resolved.name())
    }

    /// The resolved destination the definition carries: the name replies go to, plus the
    /// clause's name when the reply type's declaration overrides it (reported at startup).
    fn resolved(dest: &ReplyArg, ty: &TokenStream2) -> TokenStream2 {
        match dest {
            ReplyArg::Declared => {
                quote!(::ruststream::runtime::declared_reply_destination::<#ty>())
            }
            ReplyArg::Default(topic) => {
                quote!(::ruststream::runtime::reply_destination::<#ty>(#topic))
            }
        }
    }

    /// The `R` axis of the emitted `Handle` impl: the reply type one-by-one, a `Vec` of it per
    /// batch, `()` without a reply.
    fn r_tokens(&self, batched: bool) -> TokenStream2 {
        match self {
            Self::None => quote!(()),
            Self::Reply { ty, .. } => {
                if batched {
                    quote!(::ruststream::__private::Vec<#ty>)
                } else {
                    quote!(#ty)
                }
            }
        }
    }
}

/// Expands one unified-path handler.
pub(super) fn expand(
    args: &SubscriberArgs,
    parts: &HandlerParts<'_>,
    func: &ItemFn,
) -> syn::Result<TokenStream2> {
    let HandlerParts {
        vis,
        name,
        block,
        pat,
        input_ty,
        ctx_param,
        state_ty,
        extractors,
        outs,
        shape,
        ..
    } = parts;
    let shape = *shape;
    let batched = shape == Shape::Batch;

    let InputAxis {
        in_ty,
        axis,
        input_arg,
        lifetime,
        input_binding,
    } = input_axis(shape, input_ty, pat);

    let reply = reply_plan(args, func, block, batched)?;
    let r_tokens = reply.r_tokens(batched);
    let _ = in_ty;

    // The handler's named or `Ctx`-projected context type, on the solo and batch forms alike: a
    // batch context is subscription-scoped data (see `BuildBatchContext` in the core crate).
    let ctx_ty = parts.ctx_ty.clone();
    // The state is whatever the app holds: the runner that shares it across threads asks
    // `Send + Sync` of it where it mounts.
    let (state_param, state_in_ctx) = state_ty.as_ref().map_or_else(
        || (quote!(__RsState,), quote!(__RsState)),
        |state_ty| (quote!(), quote!(#state_ty)),
    );

    let VerdictPieces {
        verdict_ty,
        batch_len,
        reject,
        glue,
    } = verdict_pieces(func, block, &reply, batched);

    let prelude = extractor_prelude(extractors, ctx_param, &ctx_ty, &state_in_ctx, &reject);

    // The runtime-neutral expansion: the body stays a function, and each runner states what it
    // needs of the function's future where it mounts it.
    let arena = NeutralArena::of(outs, name);
    let params = &arena.params;
    let bindings = &arena.bindings;
    let outs_param = if outs.is_empty() {
        quote!(__rs_outs: &())
    } else {
        let o_ty = &arena.o_ty;
        quote!(__rs_outs: &#o_ty)
    };
    let mut preds = extractor_preds(extractors, &ctx_ty, &state_in_ctx);
    preds.extend(arena.bounds.iter().cloned());
    let body_where = where_clause(&preds);
    let body_fn = quote! {
        #[allow(
            clippy::future_not_send,
            clippy::unused_async,
            clippy::needless_pass_by_ref_mut,
            clippy::trivially_copy_pass_by_ref,
            clippy::used_underscore_binding,
        )]
        #vis async fn #name<#lifetime #(#params,)* #state_param>(
            _: ::ruststream::runtime::BodyTag<#name>,
            #input_arg,
            #outs_param,
            #ctx_param: &mut ::ruststream::runtime::Context<'_, #ctx_ty, #state_in_ctx>,
        ) -> #verdict_ty
        #body_where
        {
            // A slot's capability is the local family's on the body's side; its methods are
            // reachable under the names the service imported.
            #[allow(unused_imports)]
            use ::ruststream::LocalPublisher as _;
            #input_binding
            #batch_len
            #prelude
            #(#bindings)*
            #glue
        }
    };

    let docs_expr = probed_docs_expr(parts, &reply);
    let wiring = body_wiring(parts, &reply, &docs_expr, &axis, &r_tokens, &ctx_ty);
    Ok(quote! {
        #[allow(non_camel_case_types)]
        #[doc(hidden)]
        #vis struct #name {}

        #body_fn

        #wiring
    })
}

/// The input-axis pieces of one handler: the `In` spelling of the `Handle` impl, its
/// lifetime-free axis marker (projected off the type through `Input`, which is what makes the
/// lane the type's own business), the emitted input parameter (with the user's pattern in
/// place where the types match exactly), the impl's lifetime intro, and the rebinding that
/// hands the user's declared pattern the input where they do not.
struct InputAxis {
    in_ty: TokenStream2,
    axis: TokenStream2,
    input_arg: TokenStream2,
    lifetime: TokenStream2,
    input_binding: TokenStream2,
}

fn input_axis(shape: Shape, input_ty: &syn::Type, pat: &Pat) -> InputAxis {
    // A `Deserialized` view's elided lifetime (`&Frame<'_>`) becomes the impl's own lifetime in
    // the `In` position and the `'static` representative in the lifetime-free projections.
    let (in_elem, borrows) = subst_elided_lifetime(input_ty, "'__rs");
    let (static_elem, _) = subst_elided_lifetime(input_ty, "'static");
    let lifetime = if borrows { quote!('__rs,) } else { quote!() };
    match shape {
        // The declared `&T` parameter is the trait's own input type, so the user's pattern
        // stays in parameter position when nothing was rewritten, and no rebinding exists to
        // lint about; a rewritten (borrowing) input rebinds off a named parameter. A
        // `_`-prefixed pattern makes that rebinding look effect-free to the lint, which is
        // exactly what it is.
        Shape::Single => InputAxis {
            in_ty: quote!(#in_elem),
            axis: quote!(<#static_elem as ::ruststream::runtime::Input>::Axis),
            input_arg: if borrows {
                quote!(__rs_input: &#in_elem)
            } else {
                quote!(#pat: &#in_elem)
            },
            lifetime,
            input_binding: if borrows {
                quote! {
                    #[allow(clippy::no_effect_underscore_binding)]
                    let #pat = __rs_input;
                }
            } else {
                quote!()
            },
        },
        // The batch rebinds off a named parameter so the batch length stays reachable whatever
        // the user's pattern is.
        Shape::Batch => InputAxis {
            in_ty: quote!([#in_elem]),
            axis: quote!(<[#static_elem] as ::ruststream::runtime::Input>::Axis),
            input_arg: quote!(__rs_input: &[#in_elem]),
            lifetime,
            input_binding: quote! {
                #[allow(clippy::no_effect_underscore_binding)]
                let #pat = __rs_input;
            },
        },
    }
}

/// Resolves the attribute's reply clause against the signature, normalizing the body to the
/// `Result` shape the verdict lowers from.
fn reply_plan<'a>(
    args: &'a SubscriberArgs,
    func: &ItemFn,
    block: &syn::Block,
    batched: bool,
) -> syn::Result<ReplyPlan<'a>> {
    let Some(dest) = &args.reply else {
        return Ok(ReplyPlan::None);
    };
    Ok(if batched {
        let (elem, body) = batch_reply_body(func, block)?;
        ReplyPlan::Reply {
            dest,
            ty: quote!(#elem),
            body,
        }
    } else {
        let (ty, body) = publishing_reply(func, block)?;
        ReplyPlan::Reply {
            dest,
            ty: quote!(#ty),
            body,
        }
    })
}

/// The declaration of one handler over the body function it wraps: the settings builder
/// `subscriber(..)` also produces, generic over the body so that each runner mounts the same
/// declaration. With slots the builder wraps the body itself, and the slot binding instantiates
/// the sealed definition once the policies are known, for either runner.
fn body_wiring(
    parts: &HandlerParts<'_>,
    reply: &ReplyPlan<'_>,
    docs_expr: &TokenStream2,
    axis: &TokenStream2,
    r_tokens: &TokenStream2,
    ctx_ty: &TokenStream2,
) -> TokenStream2 {
    let HandlerParts {
        name,
        source_expr,
        outs,
        settings_chain,
        settings_source_ty,
        settings_state_ty,
        ..
    } = parts;
    let form = form_token(axis, r_tokens, reply, !outs.is_empty());
    let doc_state = quote!(::ruststream::runtime::Probed);
    let sealed_ty = |o: &TokenStream2| {
        let plain = quote! {
            ::ruststream::runtime::HandleValue<#axis, #r_tokens, #o, #ctx_ty, __RsBody, #doc_state>
        };
        match reply {
            ReplyPlan::None => quote!(::ruststream::runtime::Sealed<#plain>),
            ReplyPlan::Reply { .. } => quote! {
                ::ruststream::runtime::Sealed<
                    ::ruststream::runtime::ReplyValue<#plain, ::ruststream::runtime::ResolvedDest>,
                >
            },
        }
    };
    let def_expr = match reply {
        ReplyPlan::None => quote!(::ruststream::runtime::probed_def(__rs_body, #docs_expr)),
        ReplyPlan::Reply { dest, ty, .. } => {
            let resolved = ReplyPlan::resolved(dest, ty);
            quote!(::ruststream::runtime::probed_reply_def(__rs_body, #docs_expr, #resolved))
        }
    };
    let (settings_def_ty, declared_expr) = if outs.is_empty() {
        (sealed_ty(&quote!(())), def_expr.clone())
    } else {
        (quote!(__RsBody), quote!(__rs_body))
    };
    let declared = quote! {
        impl<__RsBody> ::ruststream::runtime::DeclareBody<__RsBody> for #name {
            type Form = #form;
            type Settings = ::ruststream::runtime::SubscriberBuilder<
                #settings_def_ty,
                #settings_source_ty,
                #settings_state_ty,
            >;

            fn declare_body(__rs_body: __RsBody) -> Self::Settings {
                #[allow(unused_imports)]
                use ::ruststream::runtime::SubscriberSettings as _;
                ::ruststream::runtime::SubscriberBuilder::new(#declared_expr, #source_expr)
                    #settings_chain
            }
        }
    };
    if outs.is_empty() {
        return declared;
    }
    let declares_reply = (!reply.is_none()).then(|| {
        quote! {
            impl ::ruststream::runtime::DeclaresReply for #name {
                type Reply = #r_tokens;
                type Context = #ctx_ty;
            }
        }
    });
    let binding_impls = slot_binding_impls(
        name,
        outs,
        &sealed_ty,
        &def_expr,
        parts.shape == Shape::Batch,
    );
    quote! {
        #declared

        #declares_reply

        #binding_impls
    }
}

/// The slot-carrying wiring next to the declaration: the marker metadata and, for each runner,
/// the slot binding that builds the sealed arena definition around the body from the bound
/// policies. The two bindings differ only in the entry type each runner pairs; the body is the
/// same function in both.
fn slot_binding_impls(
    name: &Ident,
    outs: &[OutParam<'_>],
    sealed_ty: &dyn Fn(&TokenStream2) -> TokenStream2,
    def_expr: &TokenStream2,
    batched: bool,
) -> TokenStream2 {
    let caps_batches = batched.then(|| {
        quote! {
            impl ::ruststream::runtime::CapsBatches for #name {}
        }
    });
    let markers: Vec<&TokenStream2> = outs.iter().map(|out| &out.marker).collect();
    let policies = numbered_idents("__RsPolicy", outs.len(), name);
    let codecs = numbered_idents("__RsCodec", outs.len(), name);
    let pipelines = numbered_idents("__RsPipeline", outs.len(), name);
    let bindings = [Family::Tokio, Family::Local].map(|family| {
        let entries = bound_slot_entries(outs, &policies, &codecs, &pipelines, family);
        let bound_ty = sealed_ty(&quote!(::ruststream::runtime::Outs<(#(#entries,)*)>));
        bind_slots_impl(
            name, &policies, &codecs, &pipelines, &bound_ty, def_expr, family,
        )
    });
    let witness_tys: Vec<&syn::Type> = outs.iter().map(|out| out.ty).collect();
    quote! {
        #caps_batches

        impl ::ruststream::runtime::HasSlots for #name {
            type Markers = (#(#markers,)*);
        }

        impl ::ruststream::runtime::LocalHasSlots for #name {
            type Markers = (#(#markers,)*);
        }

        #(#bindings)*

        const _: () = {
            #[allow(dead_code)]
            fn __rs_signature_witness(#(_: #witness_tys),*) {}
        };
    }
}

/// The two families of slot entries: the multi-thread runner's and the embedded runner's.
#[derive(Clone, Copy)]
enum Family {
    Tokio,
    Local,
}

/// One runner's slot binding: given the policy, codec and pipeline of every slot, it builds the
/// sealed arena definition around the body and hands the sources back as the extra state.
fn bind_slots_impl(
    name: &Ident,
    policies: &[Ident],
    codecs: &[Ident],
    pipelines: &[Ident],
    bound_ty: &TokenStream2,
    def_expr: &TokenStream2,
    family: Family,
) -> TokenStream2 {
    let span = proc_macro2::Span::call_site();
    let (bind_slots, connected, policy, bind) = match family {
        Family::Tokio => (
            quote!(::ruststream::runtime::BindSlotsBody),
            quote!(::ruststream::ConnectedBroker),
            quote!(::ruststream::PublishPolicy),
            Ident::new("bind_body", span),
        ),
        Family::Local => (
            quote!(::ruststream::runtime::LocalBindSlotsBody),
            quote!(::ruststream::LocalConnectedBroker),
            quote!(::ruststream::LocalPublishPolicy),
            Ident::new("bind_local_body", span),
        ),
    };
    quote! {
        impl<__RsBody, __RsConn, #(#policies,)* #(#codecs,)* #(#pipelines,)*>
            #bind_slots<
                __RsConn,
                (#((#policies, #codecs, #pipelines),)*),
                __RsBody,
            >
            for #name
        where
            __RsConn: #connected,
            #(#policies: #policy<__RsConn>,)*
        {
            type Bound = #bound_ty;
            type Extra = (#((#policies, #codecs, #pipelines),)*);

            fn #bind(
                __rs_body: __RsBody,
                sources: (#((#policies, #codecs, #pipelines),)*),
            ) -> (Self::Bound, Self::Extra) {
                (#def_expr, sources)
            }
        }
    }
}

/// One generic parameter per slot, `{prefix}0`, `{prefix}1`, .., spanned at the handler's name.
fn numbered_idents(prefix: &str, count: usize, name: &Ident) -> Vec<Ident> {
    (0..count)
        .map(|index| Ident::new(&format!("{prefix}{index}"), name.span()))
        .collect()
}

/// The bound `Slot` type of every slot: its marker, the live publisher its policy pairs into, its
/// codec, its publish pipeline and the bodies it publishes.
fn bound_slot_entries(
    outs: &[OutParam<'_>],
    policies: &[Ident],
    codecs: &[Ident],
    pipelines: &[Ident],
    family: Family,
) -> Vec<TokenStream2> {
    let (slot, policy_trait) = match family {
        Family::Tokio => (
            quote!(::ruststream::runtime::Slot),
            quote!(::ruststream::PublishPolicy),
        ),
        Family::Local => (
            quote!(::ruststream::runtime::LocalSlot),
            quote!(::ruststream::LocalPublishPolicy),
        ),
    };
    outs.iter()
        .zip(policies.iter().zip(codecs.iter().zip(pipelines)))
        .map(|(out, (policy, (codec, pipeline)))| {
            let marker = &out.marker;
            let body = body_ty(out.bodies.as_ref());
            quote! {
                #slot<
                    #marker,
                    <#policy as #policy_trait<__RsConn>>::Live,
                    #codec,
                    #pipeline,
                    #body,
                >
            }
        })
        .collect()
}

/// The pieces of one handler's computed verdict: the emitted `handle` method's return type, the
/// batch-length capture, the extractor-rejection return, and the tail lowering the user's return
/// vocabulary into the fixed shape.
struct VerdictPieces {
    verdict_ty: TokenStream2,
    batch_len: TokenStream2,
    reject: TokenStream2,
    glue: TokenStream2,
}

/// The return type the handler declared, with an omitted one spelled `()`.
fn declared_output(func: &ItemFn) -> TokenStream2 {
    match &func.sig.output {
        syn::ReturnType::Type(_, ty) => quote!(#ty),
        syn::ReturnType::Default => quote!(()),
    }
}

/// Computes the verdict conversion of one handler: the fixed `Result` spelling per input family
/// and reply, with the user's whole return vocabulary lowered through the `IntoOutcome` /
/// batch-verdict seams.
fn verdict_pieces(
    func: &ItemFn,
    block: &syn::Block,
    reply: &ReplyPlan<'_>,
    batched: bool,
) -> VerdictPieces {
    let outcome = quote!(::ruststream::runtime::HandlerOutcome);
    // Pin the body's type to the declared return type before the `IntoOutcome` conversion: the
    // seam has several impls, so neither an open-ended tail like `.collect()` nor a bare
    // `Ok(())` (whose error type only the signature names) can infer through the conversion
    // alone. Ascribing it makes the body infer exactly as it does in a plain function.
    let outcome_ty = declared_output(func);
    if batched {
        let batch_len = quote!(let __rs_batch_len = __rs_input.len(););
        match reply {
            ReplyPlan::None => VerdictPieces {
                verdict_ty: quote!(::core::result::Result<(), ::ruststream::__private::Vec<#outcome>>),
                batch_len,
                reject: quote! {
                    return ::ruststream::runtime::batch_verdict(
                        ::core::convert::Into::<#outcome>::into(__rs_err),
                        __rs_batch_len,
                    )
                },
                glue: quote! {
                    let __rs_outcome: #outcome_ty = (async move #block).await;
                    ::ruststream::runtime::batch_verdict(__rs_outcome, __rs_batch_len)
                },
            },
            ReplyPlan::Reply { ty, body, .. } => VerdictPieces {
                verdict_ty: quote! {
                    ::core::result::Result<::ruststream::__private::Vec<#ty>, ::ruststream::__private::Vec<#outcome>>
                },
                batch_len,
                reject: quote! {
                    return ::core::result::Result::Err(::ruststream::runtime::uniform_batch(
                        ::core::convert::Into::<#outcome>::into(__rs_err),
                        __rs_batch_len,
                    ))
                },
                glue: quote! {
                    let __rs_replies: ::core::result::Result<::ruststream::__private::Vec<#ty>, #outcome> =
                        { #body };
                    __rs_replies.map_err(|__rs_uniform| {
                        ::ruststream::runtime::uniform_batch(__rs_uniform, __rs_batch_len)
                    })
                },
            },
        }
    } else {
        let reject = quote! {
            return ::core::result::Result::Err(
                ::core::convert::Into::<#outcome>::into(__rs_err),
            )
        };
        match reply {
            ReplyPlan::None => VerdictPieces {
                verdict_ty: quote!(::core::result::Result<(), #outcome>),
                batch_len: quote!(),
                reject,
                // `Err` carries the settlement whatever its status: an ack settles as an ack,
                // and the `Ok` arm stays the manual path's spelling.
                glue: quote! {
                    let __rs_outcome: #outcome_ty = (async move #block).await;
                    ::core::result::Result::Err(
                        ::ruststream::runtime::IntoOutcome::into_outcome(__rs_outcome),
                    )
                },
            },
            ReplyPlan::Reply { ty, body, .. } => VerdictPieces {
                verdict_ty: quote!(::core::result::Result<#ty, #outcome>),
                batch_len: quote!(),
                reject,
                glue: body.clone(),
            },
        }
    }
}

/// The mount form token of one unified handler, projected off the types: the input's axis
/// carries the eager and slot forms, and a reply routes by its own type's wire - so the
/// emission never decides a lane. The vocabulary is the same the include-site chains
/// (`.out(marker, policy)`, `.build()`) always dispatched on.
fn form_token(
    axis: &TokenStream2,
    r_tokens: &TokenStream2,
    reply: &ReplyPlan<'_>,
    has_outs: bool,
) -> TokenStream2 {
    let route = quote! {
        <#r_tokens as ::ruststream::runtime::ReplyRoute<
            <#axis as ::ruststream::runtime::Axis>::Family,
        >>
    };
    match (reply, has_outs) {
        (ReplyPlan::None, false) => quote!(<#axis as ::ruststream::runtime::Axis>::EagerForm),
        (ReplyPlan::None, true) => quote!(<#axis as ::ruststream::runtime::Axis>::SlotForm),
        (ReplyPlan::Reply { .. }, false) => quote!(#route::Form),
        (ReplyPlan::Reply { .. }, true) => quote!(#route::SlotForm),
    }
}

/// The arena of a runtime-neutral body: one generic entry per slot, bound by its marker, its
/// declared set and the capability the body named on the entry's wire; and the bindings that
/// pick each entry by marker.
struct NeutralArena {
    /// `Outs<(E0, E1, ..)>`.
    o_ty: TokenStream2,
    /// The entry parameters, in declaration order.
    params: Vec<Ident>,
    /// Their bounds.
    bounds: Vec<TokenStream2>,
    /// `let out = __rs_outs.slot(Marker);` per parameter.
    bindings: Vec<TokenStream2>,
}

impl NeutralArena {
    fn of(outs: &[OutParam<'_>], name: &Ident) -> Self {
        let mut params = Vec::new();
        let mut bounds = Vec::new();
        let mut bindings = Vec::new();
        for (index, out) in outs.iter().enumerate() {
            let entry = Ident::new(&format!("__RsSlot{index}"), name.span());
            let marker = &out.marker;
            // The local family's capability is the weaker one: every `Publisher` is a
            // `LocalPublisher`, and the multi-thread runner checks `Send` at its mount.
            let capability = Runtime::Embassy.capability(out.bounds);
            let body = body_ty(out.bodies.as_ref());
            bounds.push(quote! {
                #entry: ::ruststream::runtime::SlotEntry<#marker, #body, Wire: #capability>
            });
            // Every other capability also sits on the entry itself, which forwards it through
            // the slot (a broker grafts its own capability traits onto the entry types), so the
            // entry passes into positions that demand it. The plain publish is the entry's own
            // supertrait, and owned transactions open through the typed opener below.
            let on_entry: Vec<&syn::TypeParamBound> = out
                .bounds
                .iter()
                .filter(|bound| {
                    matches!(bound, syn::TypeParamBound::Trait(_))
                        && !capability_named(bound, &["Publisher", "OwnedTransactions"])
                })
                .collect();
            if !on_entry.is_empty() {
                bounds.push(quote!(#entry: #(#on_entry)+*));
            }
            if out.bounds.iter().any(|bound| {
                capability_named(bound, &["TransactionalPublisher", "OwnedTransactions"])
            }) {
                bounds.push(quote! {
                    #entry: ::ruststream::runtime::SlotTransactions<#marker, #body>
                });
            }
            bindings.push(arena_binding(out.pat, marker));
            params.push(entry);
        }
        Self {
            o_ty: quote!(::ruststream::runtime::Outs<(#(#params,)*)>),
            params,
            bounds,
            bindings,
        }
    }
}

/// Whether a capability bound names one of `names` by its last path segment.
fn capability_named(bound: &syn::TypeParamBound, names: &[&str]) -> bool {
    let syn::TypeParamBound::Trait(written) = bound else {
        return false;
    };
    written
        .path
        .segments
        .last()
        .is_some_and(|last| names.iter().any(|name| last.ident == name))
}

/// The body binding of one slot, unwrapping the user's `Out(..)` pattern.
fn arena_binding(pat: &Pat, marker: &TokenStream2) -> TokenStream2 {
    if let Pat::TupleStruct(tuple) = pat
        && tuple.elems.len() == 1
    {
        let inner = &tuple.elems[0];
        return quote!(let #inner = __rs_outs.slot(#marker););
    }
    quote!(let #pat = __rs_outs.slot(#marker);)
}

/// The probe-captured metadata expression of one unified handler: everything the definition
/// traits used to report through per-def method overrides, evaluated at the expansion site's
/// concrete types and carried into the sealed definition as data. The probes degrade per type
/// (a type without `JsonSchema` or `MessageInfo` contributes nothing), so a `Deserialized`
/// input and a `Serialized` reply are probed like any other type and report their names.
fn probed_docs_expr(parts: &HandlerParts<'_>, reply: &ReplyPlan<'_>) -> TokenStream2 {
    let HandlerParts {
        input_ty,
        pair,
        description,
        extractors,
        outs,
        ..
    } = parts;
    let none = quote!(::core::option::Option::None);
    // The pair input's payload half is what the schema and message metadata describe; its
    // contract half feeds the headers schema below. Probes take the lifetime-free
    // representative of a borrowing input.
    let payload_ty = pair.map_or(*input_ty, |(_, payload)| payload);
    let (payload_static, _) = subst_elided_lifetime(payload_ty, "'static");
    let payload_static = quote!(#payload_static);
    let (input_schema, message_name, message_description) = (
        quote! {{
            #[allow(unused_imports)]
            use ::ruststream::__private::NoSchemaProbe as _;
            ::ruststream::__private::Probe::<#payload_static>::new().schema_json()
        }},
        quote! {{
            #[allow(unused_imports)]
            use ::ruststream::__private::NoMessageProbe as _;
            ::ruststream::__private::Probe::<#payload_static>::new().message_name()
        }},
        quote! {{
            #[allow(unused_imports)]
            use ::ruststream::__private::NoMessageProbe as _;
            ::ruststream::__private::Probe::<#payload_static>::new().message_description()
        }},
    );
    let headers_schema = headers_schema_expr(extractors, &payload_static, *pair);

    // The reply entry (probed like the old `outgoing()` override) plus each slot's declared
    // set - the parameter's narrowed list when it names one (each member reporting itself
    // through `OutMessages`, which is also what rejects a non-set-defining type right here),
    // the marker's whole dictionary otherwise. A handler with no reply and no narrowed slot
    // leaves the capture empty and the sealed definition reports the markers' dictionaries
    // itself, which is the same declaration.
    let narrowed = outs.iter().any(|out| out.bodies.is_some());
    let outgoing = if reply.is_none() && !narrowed {
        none
    } else {
        let reply_entry = match reply {
            ReplyPlan::None => quote!(),
            ReplyPlan::Reply { dest, ty, .. } => outgoing_entry(&ReplyPlan::channel(dest, ty), ty),
        };
        let slots = outs.iter().map(|out| {
            let marker = &out.marker;
            match &out.bodies {
                None => quote! {
                    __rs_outgoing.extend(<#marker as ::ruststream::runtime::OutSlot>::outgoing());
                },
                Some(BodyDecl::List(bodies)) => {
                    let entries = bodies.iter().map(|body| {
                        quote! {
                            __rs_outgoing.extend(
                                <#body as ::ruststream::runtime::OutMessages<#marker>>::outgoing(),
                            );
                        }
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
            ::core::option::Option::Some({
                let mut __rs_outgoing = ::ruststream::__private::Vec::new();
                #reply_entry
                #(#slots)*
                __rs_outgoing
            })
        }
    };

    quote! {
        ::ruststream::runtime::ProbedDocs {
            description: #description,
            input_schema: #input_schema,
            headers_schema: #headers_schema,
            message_name: #message_name,
            message_description: #message_description,
            outgoing: #outgoing,
        }
    }
}
