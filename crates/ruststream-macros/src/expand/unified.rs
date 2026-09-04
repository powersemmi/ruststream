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
use syn::{Expr, Ident, ItemFn, Pat};

use crate::parse::SubscriberArgs;

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
    /// A reply (`publish("dest")`): the returned value publishes to the topic, on the wire its
    /// type selects.
    Publish {
        topic: &'a Expr,
        ty: TokenStream2,
        body: TokenStream2,
    },
}

impl ReplyPlan<'_> {
    fn is_none(&self) -> bool {
        matches!(self, Self::None)
    }

    /// The `R` axis of the emitted `Handle` impl: the reply type one-by-one, a `Vec` of it per
    /// batch, `()` without a reply.
    fn r_tokens(&self, batched: bool) -> TokenStream2 {
        match self {
            Self::None => quote!(()),
            Self::Publish { ty, .. } => {
                if batched {
                    quote!(::std::vec::Vec<#ty>)
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

    // The handler's named or `Ctx`-projected context type, on the solo and batch forms alike: a
    // batch context is subscription-scoped data (see `BuildBatchContext` in the core crate).
    let ctx_ty = parts.ctx_ty.clone();
    let (state_param, state_in_ctx, state_bound) = match state_ty {
        Some(state_ty) => (quote!(), quote!(#state_ty), None),
        None => (
            quote!(__RsState,),
            quote!(__RsState),
            Some(quote!(__RsState: ::core::marker::Send + ::core::marker::Sync)),
        ),
    };

    let arena = ArenaPieces::of(outs, name);
    let o_ty = &arena.o_ty;
    let outs_param = if outs.is_empty() {
        quote!(__rs_outs: &())
    } else {
        quote!(__rs_outs: &#o_ty)
    };

    let VerdictPieces {
        verdict_ty,
        batch_len,
        reject,
        glue,
    } = verdict_pieces(func, block, &reply, batched);

    let prelude = extractor_prelude(extractors, ctx_param, &ctx_ty, &state_in_ctx, &reject);
    let slot_bindings = &arena.bindings;

    let mut preds = extractor_preds(extractors, &ctx_ty, &state_in_ctx);
    preds.extend(arena.bounds.iter().cloned());
    preds.extend(state_bound);
    let handle_where = where_clause(&preds);
    let we_params = &arena.we_params;
    let handle_impl = quote! {
        impl<#lifetime #(#we_params,)* #state_param>
            ::ruststream::runtime::Handle<#in_ty, #r_tokens, #o_ty, #ctx_ty, #state_in_ctx>
            for #name
        #handle_where
        {
            async fn handle(
                &self,
                #input_arg,
                #outs_param,
                #ctx_param: &mut ::ruststream::runtime::Context<'_, #ctx_ty, #state_in_ctx>,
            ) -> #verdict_ty {
                #input_binding
                #batch_len
                #prelude
                #(#slot_bindings)*
                #glue
            }
        }
    };

    let docs_expr = probed_docs_expr(parts, &reply);

    let wiring = definition_wiring(parts, &reply, &docs_expr, &axis, &r_tokens, &ctx_ty);

    Ok(quote! {
        #[derive(Clone, Copy)]
        #[allow(non_camel_case_types)]
        #vis struct #name;

        #handle_impl

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
    let Some(topic) = &args.publish else {
        return Ok(ReplyPlan::None);
    };
    Ok(if batched {
        let (elem, body) = batch_reply_body(func, block)?;
        ReplyPlan::Publish {
            topic,
            ty: quote!(#elem),
            body,
        }
    } else {
        let (ty, body) = publishing_reply(func, block)?;
        ReplyPlan::Publish {
            topic,
            ty: quote!(#ty),
            body,
        }
    })
}

/// The `Declared` wiring of one unified handler: without slots the declaration wraps the sealed
/// value itself in the settings builder, exactly as `subscriber(..)` wraps it; with slots the
/// unit struct stays the include-site value and `BindSlots::bind` instantiates the sealed arena
/// definition once the policies are known.
fn definition_wiring(
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
            ::ruststream::runtime::HandleValue<#axis, #r_tokens, #o, #ctx_ty, #name, #doc_state>
        };
        match reply {
            ReplyPlan::None => quote!(::ruststream::runtime::Sealed<#plain>),
            ReplyPlan::Publish { .. } => quote! {
                ::ruststream::runtime::Sealed<::ruststream::runtime::ReplyValue<
                    #plain,
                    ::ruststream::runtime::NamedDest,
                >>
            },
        }
    };
    let def_expr = match reply {
        ReplyPlan::None => quote!(::ruststream::runtime::probed_def(self, #docs_expr)),
        ReplyPlan::Publish { topic, .. } => {
            quote!(::ruststream::runtime::probed_reply_def(self, #docs_expr, #topic))
        }
    };

    // The declaration: the settings-builder wrapper `subscriber(..)` also produces. Without
    // slots it wraps the sealed value itself; with slots it wraps the unit struct (the arena's
    // publisher types are only known once the policies bind).
    let (settings_def_ty, declared_expr) = if outs.is_empty() {
        (sealed_ty(&quote!(())), def_expr.clone())
    } else {
        (quote!(#name), quote!(self))
    };
    let declared = quote! {
        impl ::ruststream::runtime::Declared for #name {
            type Form = #form;
            type Settings = ::ruststream::runtime::SubscriberBuilder<
                #settings_def_ty,
                #settings_source_ty,
                #settings_state_ty,
            >;

            fn declare(self) -> Self::Settings {
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
    let binding_impls = slot_binding_impls(
        name,
        outs,
        &sealed_ty,
        &def_expr,
        parts.shape == Shape::Batch,
    );
    quote! {
        #declared

        #binding_impls
    }
}

/// The slot-carrying wiring next to the declaration: the marker metadata (`HasSlots`) and the
/// `BindSlots` instantiation that builds the sealed arena definition from the bound policies,
/// which the runtime's own definition impls then drive.
fn slot_binding_impls(
    name: &Ident,
    outs: &[OutParam<'_>],
    sealed_ty: &dyn Fn(&TokenStream2) -> TokenStream2,
    def_expr: &TokenStream2,
    batched: bool,
) -> TokenStream2 {
    // The include-site value is the unit struct until the slots bind, so the batch-size step is
    // declared on it directly; the size itself rides the settings builder either way. Only a
    // batch handler declares it, which is what keeps `.batch(..)` a compile error on a
    // single-message body with slots.
    let caps_batches = batched.then(|| {
        quote! {
            impl ::ruststream::runtime::CapsBatches for #name {}
        }
    });
    let markers: Vec<&TokenStream2> = outs.iter().map(|out| &out.marker).collect();
    let policies: Vec<Ident> = (0..outs.len())
        .map(|index| Ident::new(&format!("__RsPolicy{index}"), name.span()))
        .collect();
    let codecs: Vec<Ident> = (0..outs.len())
        .map(|index| Ident::new(&format!("__RsCodec{index}"), name.span()))
        .collect();
    // The slot's publish pipeline: what the mount site composed from its `.transform(..)` steps
    // and the app's own middleware. The definition stays generic over it, so one handler mounts
    // under any of them.
    let pipelines: Vec<Ident> = (0..outs.len())
        .map(|index| Ident::new(&format!("__RsPipeline{index}"), name.span()))
        .collect();
    let bound_entries: Vec<TokenStream2> = outs
        .iter()
        .zip(policies.iter().zip(codecs.iter().zip(&pipelines)))
        .map(|(out, (policy, (codec, pipeline)))| {
            let marker = &out.marker;
            let body = body_ty(out.bodies.as_ref());
            quote! {
                ::ruststream::runtime::Slot<
                    #marker,
                    <#policy as ::ruststream::PublishPolicy<__RsConn>>::Live,
                    #codec,
                    #pipeline,
                    #body,
                >
            }
        })
        .collect();
    let bound_ty = sealed_ty(&quote!(::ruststream::runtime::Outs<(#(#bound_entries,)*)>));
    let witness_tys: Vec<&syn::Type> = outs.iter().map(|out| out.ty).collect();
    quote! {
        #caps_batches

        impl ::ruststream::runtime::HasSlots for #name {
            type Markers = (#(#markers,)*);
        }

        impl<__RsConn, #(#policies,)* #(#codecs,)* #(#pipelines,)*>
            ::ruststream::runtime::BindSlots<
                __RsConn,
                (#((#policies, #codecs, #pipelines),)*),
            >
            for #name
        where
            __RsConn: ::ruststream::ConnectedBroker,
            #(#policies: ::ruststream::PublishPolicy<__RsConn>,)*
        {
            type Bound = #bound_ty;
            type Extra = (#((#policies, #codecs, #pipelines),)*);

            fn bind(
                self,
                sources: (#((#policies, #codecs, #pipelines),)*),
            ) -> (Self::Bound, Self::Extra) {
                (#def_expr, sources)
            }
        }

        // Keeps the signature's own vocabulary (the `Out` path, the markers, the declared
        // capabilities) used, the way the retired shell bindings did.
        const _: () = {
            #[allow(dead_code)]
            fn __rs_signature_witness(#(_: #witness_tys),*) {}
        };
    }
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
                verdict_ty: quote!(::core::result::Result<(), ::std::vec::Vec<#outcome>>),
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
            ReplyPlan::Publish { ty, body, .. } => VerdictPieces {
                verdict_ty: quote! {
                    ::core::result::Result<::std::vec::Vec<#ty>, ::std::vec::Vec<#outcome>>
                },
                batch_len,
                reject: quote! {
                    return ::core::result::Result::Err(::ruststream::runtime::uniform_batch(
                        ::core::convert::Into::<#outcome>::into(__rs_err),
                        __rs_batch_len,
                    ))
                },
                glue: quote! {
                    let __rs_replies: ::core::result::Result<::std::vec::Vec<#ty>, #outcome> =
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
            ReplyPlan::Publish { ty, body, .. } => VerdictPieces {
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
        (ReplyPlan::Publish { .. }, false) => quote!(#route::Form),
        (ReplyPlan::Publish { .. }, true) => quote!(#route::SlotForm),
    }
}

/// The arena pieces of a handler's `Out` parameters: the `O` axis, the per-slot generics with
/// their bounds, and the body bindings picking each entry by marker.
struct ArenaPieces {
    /// The `O` axis of the `Handle` impl: `()` without slots, `Outs<(Slot<..>,)>` with them.
    o_ty: TokenStream2,
    /// The per-slot `W` / `E` / pipeline generic parameters, in declaration order.
    we_params: Vec<Ident>,
    /// The bounds of those generics: the user's capability bounds on `W`, the codec's on `E`,
    /// and the publish path's on the pipeline.
    bounds: Vec<TokenStream2>,
    /// The body bindings: `let out = __rs_outs.get(Marker);` per parameter.
    bindings: Vec<TokenStream2>,
}

impl ArenaPieces {
    fn of(outs: &[OutParam<'_>], name: &Ident) -> Self {
        if outs.is_empty() {
            return Self {
                o_ty: quote!(()),
                we_params: Vec::new(),
                bounds: Vec::new(),
                bindings: Vec::new(),
            };
        }
        let mut entries = Vec::new();
        let mut we_params = Vec::new();
        let mut bounds = Vec::new();
        let mut bindings = Vec::new();
        for (index, out) in outs.iter().enumerate() {
            let wired = Ident::new(&format!("__RsOutW{index}"), name.span());
            let codec = Ident::new(&format!("__RsOutE{index}"), name.span());
            let pipeline = Ident::new(&format!("__RsOutP{index}"), name.span());
            let marker = &out.marker;
            let capability = out.bounds;
            let body = body_ty(out.bodies.as_ref());
            entries.push(
                quote!(::ruststream::runtime::Slot<#marker, #wired, #codec, #pipeline, #body>),
            );
            // The dispatch machinery shares the arena across worker tasks, so Send + Sync are
            // structural; the codec bound is what the entry's typed publishes encode with, and
            // the pipeline bound is the publish path the mount site composed for the slot.
            bounds.push(quote! {
                #wired: #capability + ::core::marker::Send + ::core::marker::Sync + 'static
            });
            bounds.push(quote! {
                #codec: ::ruststream::codec::Codec
                    + ::core::marker::Send
                    + ::core::marker::Sync
                    + 'static
            });
            bounds.push(quote! {
                #pipeline: ::ruststream::runtime::OutPipeline + 'static
            });
            bindings.push(arena_binding(out.pat, marker));
            we_params.push(wired);
            we_params.push(codec);
            we_params.push(pipeline);
        }
        Self {
            o_ty: quote!(::ruststream::runtime::Outs<(#(#entries,)*)>),
            we_params,
            bounds,
            bindings,
        }
    }
}

/// One `Out` parameter's body binding: an `Out(x)`-shaped pattern binds its inner name to the
/// arena entry; any other pattern binds the whole entry.
fn arena_binding(pat: &Pat, marker: &TokenStream2) -> TokenStream2 {
    if let Pat::TupleStruct(tuple) = pat
        && tuple.elems.len() == 1
    {
        let inner = &tuple.elems[0];
        return quote!(let #inner = __rs_outs.get(#marker););
    }
    quote!(let #pat = __rs_outs.get(#marker);)
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
    let (payload_static, _) = super::subst_elided_lifetime(payload_ty, "'static");
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
            ReplyPlan::Publish { topic, ty, .. } => outgoing_entry(&quote!(#topic), ty),
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
                let mut __rs_outgoing = ::std::vec::Vec::new();
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
