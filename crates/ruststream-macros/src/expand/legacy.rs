//! The retained definition-trait emission, kept point-wise for the combinations the unified
//! value rails cannot express without losing a feature (see `uses_legacy` in the parent
//! module): `Seek(..)` parameters, an `Out` parameter's declared message set, the raw batch
//! shape, and a batch reading per-element header contracts.

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{Expr, Ident, ItemFn, Pat, ReturnType, Type};

use crate::parse::SubscriberArgs;

use super::{
    BodyDecl, HandlerParts, OutParam, Shape, batch_headers_param, batch_reply_body,
    extractor_preds, extractor_prelude, extractor_where, outgoing_entry, publishing_reply,
    where_clause,
};

/// Expands one legacy-path handler; the parent dispatcher guarantees the combination is one of
/// the preserved ones.
pub(super) fn expand(
    args: &SubscriberArgs,
    parts: &HandlerParts<'_>,
    func: &ItemFn,
) -> syn::Result<TokenStream2> {
    let injected = !parts.outs.is_empty() || parts.seek.is_some();
    Ok(match parts.shape {
        Shape::RawBatch => expand_raw_batch(parts, func),
        Shape::Batch => match &args.publish {
            Some(reply_topic) => expand_batch_publishing(parts, func, reply_topic)?,
            None if injected => expand_batch_injected(parts, func),
            None => expand_batch(parts, func),
        },
        // The input axis is a flag, not a form, so the byte input composes with every
        // single-message form.
        Shape::Single | Shape::Raw => {
            let raw = parts.shape == Shape::Raw;
            if let Some(reply_topic) = &args.publish_raw {
                expand_publishing(parts, func, reply_topic, true, raw)?
            } else if let Some(reply_topic) = &args.publish {
                expand_publishing(parts, func, reply_topic, false, raw)?
            } else if injected {
                expand_injected(parts, raw)
            } else {
                // A plain single-message handler always takes the unified path; only the
                // injection and reply combinations above reach the legacy one.
                unreachable!("the unified emission covers plain single-message handlers")
            }
        }
    })
}

/// The `Declared` impl every legacy expansion emits next to its definition: the mount form, and
/// the builder the attribute's settings expand into.
///
/// This is what makes the attribute sugar over the builder rather than a second implementation:
/// `#[subscriber("orders", workers(4))]` produces exactly the calls a user would write at the
/// mount site.
fn declaration(parts: &HandlerParts<'_>, form: &TokenStream2) -> TokenStream2 {
    let HandlerParts {
        name,
        source_expr,
        settings_chain,
        settings_source_ty,
        settings_state_ty,
        ..
    } = parts;
    quote! {
        impl ::ruststream::runtime::Declared for #name {
            type Form = #form;
            type Settings = ::ruststream::runtime::SubscriberBuilder<
                #name,
                #settings_source_ty,
                #settings_state_ty,
            >;

            fn declare(self) -> Self::Settings {
                #[allow(unused_imports)]
                use ::ruststream::runtime::SubscriberSettings as _;
                ::ruststream::runtime::SubscriberBuilder::new(self, #source_expr)
                    #settings_chain
            }
        }
    }
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
            // A publish_raw reply is bytes: no schema, no MessageInfo metadata to probe. The
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
            // Each listed type declares itself: a one-element set whose channel comes from the
            // type's own #[outgoing(name = ..)].
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
        headers_schema,
        ..
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
        &quote!(
            return ::core::result::Result::Err(::ruststream::runtime::BatchResult::Uniform(
                ::core::convert::Into::<::ruststream::runtime::HandlerOutcome>::into(__rs_err),
            ))
        ),
    );
    let declaration = declaration(parts, &form);
    Ok(quote! {
        #[allow(non_camel_case_types)]
        #vis struct #name;

        #declaration

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
                ::ruststream::runtime::BatchResult,
            > {
                #prelude
                #injection_bindings
                let __rs_replies: ::core::result::Result<
                    ::std::vec::Vec<#reply_elem>,
                    ::ruststream::runtime::HandlerOutcome,
                > = { #call_body };
                __rs_replies.map_err(::ruststream::runtime::BatchResult::Uniform)
            }
        }
    })
}

/// The pieces that distinguish a batch handler reading a per-element header contract from a plain
/// one: the handler trait, the extra argument carrying the contracts, the binding that rebuilds
/// the declared parameter, the include form, and the extra def impl.
fn batch_handler_shape(
    headers_param: Option<(&Pat, &Type, &Type)>,
    input_ty: &Type,
    state_in_ctx: &TokenStream2,
    name: &Ident,
) -> (
    TokenStream2,
    TokenStream2,
    TokenStream2,
    TokenStream2,
    TokenStream2,
) {
    match headers_param {
        Some((headers_pat, headers_ty, element_ty)) => (
            quote!(::ruststream::runtime::SliceHandlerWithHeaders<
                #input_ty,
                #element_ty,
                #state_in_ctx,
            >),
            quote!(__rs_headers: ::std::vec::Vec<#element_ty>,),
            quote!(let #headers_pat: #headers_ty = ::ruststream::runtime::Headers(__rs_headers);),
            quote!(::ruststream::runtime::forms::BatchWithHeaders),
            quote! {
                impl ::ruststream::runtime::BatchWithHeadersDef for #name {
                    type Headers = #element_ty;
                }
            },
        ),
        None => (
            quote!(::ruststream::runtime::SliceHandler<#input_ty, #state_in_ctx>),
            quote!(),
            quote!(),
            quote!(::ruststream::runtime::forms::Batch),
            quote!(),
        ),
    }
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
        headers_schema,
        ..
    } = parts;

    // Pin the body's type to the declared return type before the `IntoBatchResult` conversion:
    // the trait has several impls, so an open-ended tail like `.collect()` cannot infer through
    // the conversion alone.
    let outcome_ty = match &func.sig.output {
        ReturnType::Type(_, ty) => quote!(#ty),
        ReturnType::Default => quote!(()),
    };

    // A batch handler that names a state type is bound to it, one that names none is generic over
    // the state, so it mounts on an app with any state type.
    let (impl_generics, state_in_ctx) = match &state_ty {
        Some(state_ty) => (quote!(), quote!(#state_ty)),
        None => (
            quote!(<__RsState: ::core::marker::Send + ::core::marker::Sync>),
            quote!(__RsState),
        ),
    };
    // The batch context is always `()`; extractors resolve against it. A `Headers<Vec<H>>`
    // parameter is the exception: the decode adapter parses one contract per element next to the
    // payload, so it is bound from the handler's own argument instead of through `FromContext`.
    let unit_ctx = quote!(());
    let headers_param = batch_headers_param(extractors);
    let rest: Vec<(&Pat, &Type)> = extractors
        .iter()
        .filter(|(pat, _)| {
            headers_param.is_none_or(|(headers_pat, _, _)| !std::ptr::eq(*pat, headers_pat))
        })
        .copied()
        .collect();
    let extractors = &rest[..];
    let where_clause = extractor_where(extractors, &unit_ctx, &state_in_ctx);
    let prelude = extractor_prelude(
        extractors,
        ctx_param,
        &unit_ctx,
        &state_in_ctx,
        &quote!(
            return ::ruststream::runtime::IntoBatchResult::into_batch_result(
                ::core::convert::Into::<::ruststream::runtime::HandlerOutcome>::into(__rs_err),
            )
        ),
    );

    let (handler_trait, headers_arg, headers_binding, form, headers_def) =
        batch_handler_shape(headers_param, input_ty, &state_in_ctx, name);
    let declaration = declaration(parts, &form);

    quote! {
            #[derive(Clone, Copy)]
            #[allow(non_camel_case_types)]
            #vis struct #name;

            impl #impl_generics #handler_trait for #name
                #where_clause
            {
                async fn handle_slice(
                    &self,
                    #pat: &[#input_ty],
                    #headers_arg
                    #ctx_param: &mut ::ruststream::runtime::Context<'_, (), #state_in_ctx>,
                ) -> ::ruststream::runtime::BatchResult {
                    #prelude
                    #headers_binding
                    let outcome: #outcome_ty = (async move #block).await;
                    ::ruststream::runtime::IntoBatchResult::into_batch_result(outcome)
                }
            }

            #declaration

            #headers_def

            impl ::ruststream::runtime::BatchDef for #name {
                type Input = ::ruststream::runtime::Decoded<#input_ty>;
                type Handler = Self;
                type Source = #source_ty;

                fn source(&self) -> Self::Source { #source_expr }

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

/// The raw batch form: a handler taking `&[&[u8]]`.
///
/// The typed batch without the decode step, so nothing is probed for schemas or `MessageInfo`
/// metadata and no codec takes part; the payload slices are borrowed from the batch's own
/// messages, which the dispatcher holds for the duration of the call.
fn expand_raw_batch(parts: &HandlerParts<'_>, func: &ItemFn) -> TokenStream2 {
    let HandlerParts {
        vis,
        name,
        block,
        pat,
        description,
        source_ty,
        source_expr,
        ctx_param,
        state_ty,
        extractors,
        ..
    } = parts;

    // As for `expand_batch`: pin the body's type before the `IntoBatchResult` conversion.
    let outcome_ty = match &func.sig.output {
        ReturnType::Type(_, ty) => quote!(#ty),
        ReturnType::Default => quote!(()),
    };
    let (impl_generics, state_in_ctx) = match &state_ty {
        Some(state_ty) => (quote!(), quote!(#state_ty)),
        None => (
            quote!(<__RsState: ::core::marker::Send + ::core::marker::Sync>),
            quote!(__RsState),
        ),
    };
    let unit_ctx = quote!(());
    let where_clause = extractor_where(extractors, &unit_ctx, &state_in_ctx);
    let prelude = extractor_prelude(
        extractors,
        ctx_param,
        &unit_ctx,
        &state_in_ctx,
        &quote!(
            return ::ruststream::runtime::IntoBatchResult::into_batch_result(
                ::core::convert::Into::<::ruststream::runtime::HandlerOutcome>::into(__rs_err),
            )
        ),
    );
    let form = quote!(::ruststream::runtime::forms::RawBatch);
    let declaration = declaration(parts, &form);

    quote! {
        #[derive(Clone, Copy)]
        #[allow(non_camel_case_types)]
        #vis struct #name;

        impl #impl_generics ::ruststream::runtime::RawSliceHandler<#state_in_ctx> for #name
            #where_clause
        {
            async fn handle_slice(
                &self,
                #pat: &[&[u8]],
                #ctx_param: &mut ::ruststream::runtime::Context<'_, (), #state_in_ctx>,
            ) -> ::ruststream::runtime::BatchResult {
                #prelude
                let outcome: #outcome_ty = (async move #block).await;
                ::ruststream::runtime::IntoBatchResult::into_batch_result(outcome)
            }
        }

        #declaration

        impl ::ruststream::runtime::BatchDef for #name {
            type Input = ::ruststream::runtime::RawBytes;
            type Handler = Self;
            type Source = #source_ty;

            fn source(&self) -> Self::Source { #source_expr }

            fn description(&self) -> ::core::option::Option<&str> {
                #description
            }

            fn into_handler(self) -> Self { self }
        }
    }
}

/// The injected batch form: a slice handler with Out / Seek parameters. Mirrors
/// `expand_injected` at the batch shape (slice input, `IntoBatchResult` conversion, unit
/// context).
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
        headers_schema,
        ..
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
        &quote!(
            return ::ruststream::runtime::IntoBatchResult::into_batch_result(
                ::core::convert::Into::<::ruststream::runtime::HandlerOutcome>::into(__rs_err),
            )
        ),
    );
    let declaration = declaration(parts, &form);

    quote! {
        #[derive(Clone, Copy)]
        #[allow(non_camel_case_types)]
        #vis struct #name;

        #declaration

        #scaffold_items

        impl<#def_generics> ::ruststream::runtime::BatchInjectDef for #def_target
        #def_where
        {
            type Input = ::ruststream::runtime::Decoded<#input_ty>;
            type Source = #source_ty;
            type Injections = (#(#injection_tys,)*);

            fn source(&self) -> Self::Source { #source_expr }

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
        headers_schema,
        ..
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

    // A publishing handler that names a state type implements `PublishingCall` only for that
    // state (mounts on a matching app); one that names none is generic over the state (mounts on
    // any app). The metadata-only `PublishingDef` is unconditional.
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
        &quote!(
            return ::core::result::Result::Err(::core::convert::Into::<
                ::ruststream::runtime::HandlerOutcome,
            >::into(__rs_err),)
        ),
    );
    let declaration = declaration(parts, &form);
    Ok(quote! {
        #[allow(non_camel_case_types)]
        #vis struct #name;

        #declaration

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
            ) -> ::core::result::Result<#reply_ty, ::ruststream::runtime::HandlerOutcome> {
                #prelude
                #injection_bindings
                #call_body
            }
        }
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
        // The dispatch machinery shares the injected value across worker tasks, so Send + Sync
        // are structural: a broker-defined capability bound need not imply them.
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
                    out_bounds.push(quote!(#body: ::ruststream::runtime::OutMessages<#marker>));
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
        headers_schema,
        ..
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
        &quote!(
            return ::ruststream::runtime::IntoOutcome::into_outcome(::core::convert::Into::<
                ::ruststream::runtime::HandlerOutcome,
            >::into(__rs_err),)
        ),
    );
    let declaration = declaration(parts, &form);

    quote! {
        #[derive(Clone, Copy)]
        #[allow(non_camel_case_types)]
        #vis struct #name;

        #declaration

        #scaffold_items

        impl<#def_generics> ::ruststream::runtime::InjectDef for #def_target
        #def_where
        {
            type Input = #input_kind;
            type Context = #ctx_ty;
            type Source = #source_ty;
            type Injections = (#(#injection_tys,)*);

            fn source(&self) -> Self::Source { #source_expr }

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
            ) -> ::ruststream::runtime::HandlerOutcome {
                #prelude
                #injection_bindings
                ::ruststream::runtime::IntoOutcome::into_outcome(
                    (async move #block).await,
                )
            }
        }
    }
}
