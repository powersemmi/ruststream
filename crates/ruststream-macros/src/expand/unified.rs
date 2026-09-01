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
    HandlerParts, OutParam, Shape, batch_reply_body, extractor_preds, extractor_prelude,
    headers_schema_expr, outgoing_entry, publishing_reply, where_clause,
};

/// The reply clause of one handler, with the body already normalized to the `Result` shape.
enum ReplyPlan<'a> {
    /// No reply: the body settles and nothing is published.
    None,
    /// An encoded reply (`publish("dest")`): the reply value serializes through the reply
    /// publisher's codec.
    Encoded {
        topic: &'a Expr,
        ty: TokenStream2,
        body: TokenStream2,
    },
    /// A bare byte reply (`publish_raw("dest")`): the returned bytes leave as they are.
    Bare {
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
    /// page, `()` without a reply.
    fn r_tokens(&self, paged: bool) -> TokenStream2 {
        match self {
            Self::None => quote!(()),
            Self::Encoded { ty, .. } | Self::Bare { ty, .. } => {
                if paged {
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
    let paged = shape == Shape::Batch;
    let raw = shape == Shape::Raw;

    // ------------------------------------------------------------------------- the input axis
    let InputAxis {
        in_ty,
        axis,
        input_arg,
        lifetime,
        input_binding,
    } = input_axis(shape, input_ty, pat);

    // ------------------------------------------------------------------------- the reply plan
    let reply = reply_plan(args, func, block, paged)?;
    let r_tokens = reply.r_tokens(paged);

    // -------------------------------------------------------------- the context and state axes
    // A batch definition's context is always `()` (a page spans many deliveries); the solo
    // forms carry the handler's named or `Ctx`-projected context type.
    let ctx_ty = if paged {
        quote!(())
    } else {
        parts.ctx_ty.clone()
    };
    let (state_param, state_in_ctx, state_bound) = match state_ty {
        Some(state_ty) => (quote!(), quote!(#state_ty), None),
        None => (
            quote!(__RsState,),
            quote!(__RsState),
            Some(quote!(__RsState: ::core::marker::Send + ::core::marker::Sync)),
        ),
    };

    // ------------------------------------------------------------------ the injections arena
    let arena = ArenaPieces::of(outs, name);
    let o_ty = &arena.o_ty;
    let outs_param = if outs.is_empty() {
        quote!(__rs_outs: &())
    } else {
        quote!(__rs_outs: &#o_ty)
    };

    // ------------------------------------------------------------------- the computed verdict
    let VerdictPieces {
        verdict_ty,
        page_len,
        reject,
        glue,
    } = verdict_pieces(func, block, &reply, paged);

    let prelude = extractor_prelude(extractors, ctx_param, &ctx_ty, &state_in_ctx, &reject);
    let slot_bindings = &arena.bindings;

    // ------------------------------------------------------------------------ the Handle impl
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
                #page_len
                #prelude
                #(#slot_bindings)*
                #glue
            }
        }
    };

    // -------------------------------------------------------------------- the captured docs
    let docs_expr = probed_docs_expr(parts, &reply, raw);

    // ------------------------------------------------------- the definition and its mounting
    let wiring = definition_wiring(parts, &reply, &docs_expr, &axis, &r_tokens, &ctx_ty, raw);

    Ok(quote! {
        #[derive(Clone, Copy)]
        #[allow(non_camel_case_types)]
        #vis struct #name;

        #handle_impl

        #wiring
    })
}

/// The input-axis pieces of one handler: the `In` spelling of the `Handle` impl, its
/// lifetime-free axis marker, the emitted input parameter (with the user's pattern in place
/// where the types match exactly), the impl's lifetime intro, and the rebinding that hands the
/// user's declared pattern the input where they do not.
struct InputAxis {
    in_ty: TokenStream2,
    axis: TokenStream2,
    input_arg: TokenStream2,
    lifetime: TokenStream2,
    input_binding: TokenStream2,
}

fn input_axis(shape: Shape, input_ty: &syn::Type, pat: &Pat) -> InputAxis {
    match shape {
        // The declared `&T` parameter is the trait's own input type, so the user's pattern
        // stays in parameter position and no rebinding exists to lint about.
        Shape::Single => InputAxis {
            in_ty: quote!(#input_ty),
            axis: quote!(::ruststream::runtime::Solo<#input_ty>),
            input_arg: quote!(#pat: &#input_ty),
            lifetime: quote!(),
            input_binding: quote!(),
        },
        // The wrapper derefs to `&[u8]`, so the user's declared `&[u8]` parameter rebinds by
        // coercion at zero cost; a `_`-prefixed pattern makes that rebinding look effect-free
        // to the lint, which is exactly what it is.
        Shape::Raw => InputAxis {
            in_ty: quote!(::ruststream::runtime::Payload<'__rs>),
            axis: quote!(::ruststream::runtime::SoloBytes),
            input_arg: quote!(__rs_input: &::ruststream::runtime::Payload<'__rs>),
            lifetime: quote!('__rs,),
            input_binding: quote! {
                #[allow(clippy::no_effect_underscore_binding)]
                let #pat: &[u8] = __rs_input;
            },
        },
        // The page rebinds off a named parameter so the page length stays reachable whatever
        // the user's pattern is.
        Shape::Batch => InputAxis {
            in_ty: quote!([#input_ty]),
            axis: quote!(::ruststream::runtime::Page<#input_ty>),
            input_arg: quote!(__rs_input: &[#input_ty]),
            lifetime: quote!(),
            input_binding: quote! {
                #[allow(clippy::no_effect_underscore_binding)]
                let #pat = __rs_input;
            },
        },
        // The raw batch shape keeps the legacy emission; the dispatcher never sends it here.
        Shape::RawBatch => unreachable!("the raw batch shape keeps the legacy emission"),
    }
}

/// Resolves the attribute's reply clause against the signature, normalizing the body to the
/// `Result` shape the verdict lowers from.
fn reply_plan<'a>(
    args: &'a SubscriberArgs,
    func: &ItemFn,
    block: &syn::Block,
    paged: bool,
) -> syn::Result<ReplyPlan<'a>> {
    if let Some(topic) = &args.publish_raw {
        let (ty, body) = publishing_reply(func, block, true)?;
        return Ok(ReplyPlan::Bare {
            topic,
            ty: quote!(#ty),
            body,
        });
    }
    let Some(topic) = &args.publish else {
        return Ok(ReplyPlan::None);
    };
    Ok(if paged {
        let (elem, body) = batch_reply_body(func, block)?;
        ReplyPlan::Encoded {
            topic,
            ty: quote!(#elem),
            body,
        }
    } else {
        let (ty, body) = publishing_reply(func, block, false)?;
        ReplyPlan::Encoded {
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
    raw: bool,
) -> TokenStream2 {
    let HandlerParts {
        name,
        source_expr,
        outs,
        shape,
        settings_chain,
        settings_source_ty,
        settings_state_ty,
        ..
    } = parts;
    let paged = *shape == Shape::Batch;
    let form = form_token(paged, raw, reply, !outs.is_empty());
    let doc_state = quote!(::ruststream::runtime::Probed);
    let sealed_ty = |o: &TokenStream2| {
        let plain = quote! {
            ::ruststream::runtime::HandleValue<#axis, #r_tokens, #o, #ctx_ty, #name, #doc_state>
        };
        match reply {
            ReplyPlan::None => quote!(::ruststream::runtime::Sealed<#plain>),
            ReplyPlan::Encoded { .. } => quote! {
                ::ruststream::runtime::Sealed<::ruststream::runtime::ReplyValue<
                    #plain,
                    ::ruststream::runtime::NamedDest,
                    ::ruststream::runtime::EncodedReply,
                    ::ruststream::runtime::DefaultReply,
                >>
            },
            ReplyPlan::Bare { .. } => quote! {
                ::ruststream::runtime::Sealed<::ruststream::runtime::ReplyValue<
                    #plain,
                    ::ruststream::runtime::NamedDest,
                    ::ruststream::runtime::BareReply,
                    ::ruststream::runtime::DefaultBareReply,
                >>
            },
        }
    };
    let def_expr = match reply {
        ReplyPlan::None => quote!(::ruststream::runtime::probed_def(self, #docs_expr)),
        ReplyPlan::Encoded { topic, .. } | ReplyPlan::Bare { topic, .. } => {
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
    let binding_impls = slot_binding_impls(name, outs, &sealed_ty, &def_expr);
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
) -> TokenStream2 {
    let markers: Vec<&TokenStream2> = outs.iter().map(|out| &out.marker).collect();
    let policies: Vec<Ident> = (0..outs.len())
        .map(|index| Ident::new(&format!("__RsPolicy{index}"), name.span()))
        .collect();
    let codecs: Vec<Ident> = (0..outs.len())
        .map(|index| Ident::new(&format!("__RsCodec{index}"), name.span()))
        .collect();
    let bound_entries: Vec<TokenStream2> = outs
        .iter()
        .zip(policies.iter().zip(&codecs))
        .map(|(out, (policy, codec))| {
            let marker = &out.marker;
            quote! {
                ::ruststream::runtime::Slot<
                    #marker,
                    <#policy as ::ruststream::PublishPolicy<__RsConn>>::Live,
                    #codec,
                >
            }
        })
        .collect();
    let bound_ty = sealed_ty(&quote!(::ruststream::runtime::Outs<(#(#bound_entries,)*)>));
    let witness_tys: Vec<&syn::Type> = outs.iter().map(|out| out.ty).collect();
    quote! {
        impl ::ruststream::runtime::HasSlots for #name {
            type Markers = (#(#markers,)*);
        }

        impl<__RsConn, #(#policies,)* #(#codecs,)*>
            ::ruststream::runtime::BindSlots<__RsConn, (#((#policies, #codecs),)*)>
            for #name
        where
            __RsConn: ::ruststream::ConnectedBroker,
            #(#policies: ::ruststream::PublishPolicy<__RsConn>,)*
        {
            type Bound = #bound_ty;
            type Extra = (#((#policies, #codecs),)*);

            fn bind(
                self,
                sources: (#((#policies, #codecs),)*),
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
/// page-length capture, the extractor-rejection return, and the tail lowering the user's return
/// vocabulary into the fixed shape.
struct VerdictPieces {
    verdict_ty: TokenStream2,
    page_len: TokenStream2,
    reject: TokenStream2,
    glue: TokenStream2,
}

/// Computes the verdict conversion of one handler: the fixed `Result` spelling per input family
/// and reply, with the user's whole return vocabulary lowered through the `IntoOutcome` /
/// page-verdict seams.
fn verdict_pieces(
    func: &ItemFn,
    block: &syn::Block,
    reply: &ReplyPlan<'_>,
    paged: bool,
) -> VerdictPieces {
    let outcome = quote!(::ruststream::runtime::HandlerOutcome);
    if paged {
        let page_len = quote!(let __rs_page_len = __rs_input.len(););
        match reply {
            ReplyPlan::None => {
                // Pin the body's type to the declared return type before the conversion: the
                // seam has several impls, so an open-ended tail like `.collect()` cannot infer
                // through the conversion alone.
                let outcome_ty = match &func.sig.output {
                    syn::ReturnType::Type(_, ty) => quote!(#ty),
                    syn::ReturnType::Default => quote!(()),
                };
                VerdictPieces {
                    verdict_ty: quote!(::core::result::Result<(), ::std::vec::Vec<#outcome>>),
                    page_len,
                    reject: quote! {
                        return ::ruststream::runtime::page_verdict(
                            ::core::convert::Into::<#outcome>::into(__rs_err),
                            __rs_page_len,
                        )
                    },
                    glue: quote! {
                        let __rs_outcome: #outcome_ty = (async move #block).await;
                        ::ruststream::runtime::page_verdict(__rs_outcome, __rs_page_len)
                    },
                }
            }
            ReplyPlan::Encoded { ty, body, .. } | ReplyPlan::Bare { ty, body, .. } => {
                VerdictPieces {
                    verdict_ty: quote! {
                        ::core::result::Result<::std::vec::Vec<#ty>, ::std::vec::Vec<#outcome>>
                    },
                    page_len,
                    reject: quote! {
                        return ::core::result::Result::Err(::ruststream::runtime::uniform_page(
                            ::core::convert::Into::<#outcome>::into(__rs_err),
                            __rs_page_len,
                        ))
                    },
                    glue: quote! {
                        let __rs_replies: ::core::result::Result<::std::vec::Vec<#ty>, #outcome> =
                            { #body };
                        __rs_replies.map_err(|__rs_uniform| {
                            ::ruststream::runtime::uniform_page(__rs_uniform, __rs_page_len)
                        })
                    },
                }
            }
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
                page_len: quote!(),
                reject,
                // `Err` carries the settlement whatever its status: an ack settles as an ack,
                // and the `Ok` arm stays the manual path's spelling.
                glue: quote! {
                    ::core::result::Result::Err(::ruststream::runtime::IntoOutcome::into_outcome(
                        (async move #block).await,
                    ))
                },
            },
            ReplyPlan::Encoded { ty, body, .. } | ReplyPlan::Bare { ty, body, .. } => {
                VerdictPieces {
                    verdict_ty: quote!(::core::result::Result<#ty, #outcome>),
                    page_len: quote!(),
                    reject,
                    glue: body.clone(),
                }
            }
        }
    }
}

/// The mount form token of one unified handler: the same vocabulary the old emission used, so
/// every include-site chain (`.publisher(..)`, `.out(marker, ..)`, `.build()`) keeps compiling
/// unchanged.
fn form_token(paged: bool, raw: bool, reply: &ReplyPlan<'_>, has_outs: bool) -> TokenStream2 {
    let forms = quote!(::ruststream::runtime::forms);
    match (paged, reply, has_outs) {
        (true, ReplyPlan::None, false) => quote!(#forms::Batch),
        (true, ReplyPlan::None, true) => quote!(#forms::BatchOut),
        (true, _, false) => quote!(#forms::BatchPublishing),
        (true, _, true) => quote!(#forms::BatchPublishingOut),
        (false, ReplyPlan::None, false) if raw => quote!(#forms::RawSubscribing),
        (false, ReplyPlan::None, false) => quote!(#forms::Subscribing),
        (false, ReplyPlan::None, true) => quote!(#forms::Out),
        (false, ReplyPlan::Bare { .. }, false) => quote!(#forms::RawReply),
        (false, ReplyPlan::Bare { .. }, true) => quote!(#forms::RawReplyOut),
        (false, ReplyPlan::Encoded { .. }, false) => quote!(#forms::Publishing),
        (false, ReplyPlan::Encoded { .. }, true) => quote!(#forms::PublishingOut),
    }
}

/// The arena pieces of a handler's `Out` parameters: the `O` axis, the per-slot generics with
/// their bounds, and the body bindings picking each entry by marker.
struct ArenaPieces {
    /// The `O` axis of the `Handle` impl: `()` without slots, `Outs<(Slot<..>,)>` with them.
    o_ty: TokenStream2,
    /// The per-slot `W` / `E` generic parameters, in declaration order.
    we_params: Vec<Ident>,
    /// The bounds of those generics: the user's capability bounds on `W`, the codec's on `E`.
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
            let marker = &out.marker;
            let capability = out.bounds;
            entries.push(quote!(::ruststream::runtime::Slot<#marker, #wired, #codec>));
            // The dispatch machinery shares the arena across worker tasks, so Send + Sync are
            // structural; the codec bound is what the entry's typed publishes encode with.
            bounds.push(quote! {
                #wired: #capability + ::core::marker::Send + ::core::marker::Sync + 'static
            });
            bounds.push(quote! {
                #codec: ::ruststream::codec::Codec
                    + ::core::marker::Send
                    + ::core::marker::Sync
                    + 'static
            });
            bindings.push(arena_binding(out.pat, marker));
            we_params.push(wired);
            we_params.push(codec);
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
/// concrete types and carried into the sealed definition as data.
fn probed_docs_expr(parts: &HandlerParts<'_>, reply: &ReplyPlan<'_>, raw: bool) -> TokenStream2 {
    let HandlerParts {
        input_ty,
        description,
        extractors,
        outs,
        shape,
        ..
    } = parts;
    let none = quote!(::core::option::Option::None);
    let (input_schema, message_name, message_description) = if raw {
        (none.clone(), none.clone(), none.clone())
    } else {
        (
            quote! {{
                #[allow(unused_imports)]
                use ::ruststream::__private::NoSchemaProbe as _;
                ::ruststream::__private::Probe::<#input_ty>::new().schema_json()
            }},
            quote! {{
                #[allow(unused_imports)]
                use ::ruststream::__private::NoMessageProbe as _;
                ::ruststream::__private::Probe::<#input_ty>::new().message_name()
            }},
            quote! {{
                #[allow(unused_imports)]
                use ::ruststream::__private::NoMessageProbe as _;
                ::ruststream::__private::Probe::<#input_ty>::new().message_description()
            }},
        )
    };
    let headers_schema = headers_schema_expr(*shape, extractors, input_ty);

    // The reply entry (probed like the old `outgoing()` override) plus each slot marker's whole
    // dictionary; a slot-only handler leaves the capture empty and the sealed definition
    // reports the markers' dictionaries itself, which is the same declaration.
    let outgoing = if reply.is_none() {
        none
    } else {
        let reply_entry = match reply {
            ReplyPlan::None => quote!(),
            ReplyPlan::Encoded { topic, ty, .. } => outgoing_entry(&quote!(#topic), ty),
            // A publish_raw reply is bytes: no schema, no MessageInfo metadata to probe. The
            // explicit &'static str binding keeps a wrongly-typed destination expression a
            // plain type error instead of a trait-bound failure inside the metadata builder.
            ReplyPlan::Bare { topic, .. } => quote! {
                __rs_outgoing.push(::ruststream::runtime::OutgoingMessageMetadata::new(
                    { let __rs_channel: &'static str = #topic; __rs_channel },
                    "bytes",
                ));
            },
        };
        let slots = outs.iter().map(|out| {
            let marker = &out.marker;
            quote! {
                __rs_outgoing.extend(<#marker as ::ruststream::runtime::OutSlot>::outgoing());
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
