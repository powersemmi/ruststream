//! Expansion of the `#[subscriber]` forms: the handler signature is dissected into
//! [`HandlerParts`], then one of the four definition impls (plain, publishing, batch, batch
//! publishing) is generated around the original function body.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{FnArg, Ident, ItemFn, LitStr, PatType, ReturnType, Type};

use crate::parse::{
    FailurePolicyArg, SubscriberArgs, WorkersArg, doc_description, publish_result_reply,
    source_tokens, vec_element,
};

pub(crate) fn subscriber(args: &SubscriberArgs, func: &ItemFn) -> syn::Result<TokenStream> {
    let parts = handler_parts(args, func)?;
    let body = match (&args.batch, &args.publish) {
        (true, Some(reply_topic)) => expand_batch_publishing(&parts, func, reply_topic)?,
        (true, None) => expand_batch(&parts, func),
        (false, Some(reply_topic)) => expand_publishing(&parts, func, reply_topic)?,
        (false, None) => expand_subscribing(&parts),
    };
    Ok(body.into())
}

/// The pieces of the handler shared by both expansion forms, extracted from the signature.
struct HandlerParts<'a> {
    vis: &'a syn::Visibility,
    name: &'a Ident,
    block: &'a syn::Block,
    pat: &'a syn::Pat,
    input_ty: &'a Type,
    description: TokenStream2,
    source_ty: TokenStream2,
    source_expr: TokenStream2,
    input_schema: TokenStream2,
    message_meta: TokenStream2,
    ctx_param: TokenStream2,
    workers_method: TokenStream2,
    failure_method: TokenStream2,
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
        return Err(syn::Error::new(
            count.span(),
            "workers(0) is not a policy; the minimum is 1",
        ));
    }
    if let Some(marker) = by_key {
        if args.batch {
            return Err(syn::Error::new(
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

fn handler_parts<'a>(args: &SubscriberArgs, func: &'a ItemFn) -> syn::Result<HandlerParts<'a>> {
    let first = func.sig.inputs.first().ok_or_else(|| {
        syn::Error::new_spanned(
            &func.sig,
            "a #[subscriber] handler must take exactly one message parameter",
        )
    })?;
    let FnArg::Typed(PatType { pat, ty, .. }) = first else {
        return Err(syn::Error::new_spanned(
            first,
            "a #[subscriber] handler cannot take `self`",
        ));
    };
    let Type::Reference(reference) = &**ty else {
        return Err(syn::Error::new_spanned(
            ty,
            "the message parameter must be a reference `&T`",
        ));
    };
    // In the batch(..) form the parameter is the whole batch `&[T]`; the def's `Input` is the
    // element type either way.
    let input_ty = if args.batch {
        match &*reference.elem {
            Type::Slice(slice) => &*slice.elem,
            other => {
                return Err(syn::Error::new_spanned(
                    other,
                    "a batch handler takes the whole batch as a slice: `&[T]`",
                ));
            }
        }
    } else {
        if matches!(&*reference.elem, Type::Slice(_)) {
            return Err(syn::Error::new_spanned(
                &reference.elem,
                "a slice parameter needs the batch source form: #[subscriber(batch(..))]",
            ));
        }
        &*reference.elem
    };
    let description = doc_description(&func.attrs);
    let (source_ty, source_expr) = source_tokens(&args.source)?;

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
    // bind it to their name; otherwise generate an ignored binding.
    let ctx_param = if let Some(FnArg::Typed(PatType { pat, .. })) = func.sig.inputs.get(1) {
        quote!(#pat)
    } else {
        quote!(_ctx)
    };

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
        workers_method,
        failure_method,
    })
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
        workers_method,
        failure_method,
    } = parts;

    let declared_ty = match &func.sig.output {
        ReturnType::Type(_, ty) => &**ty,
        ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                &func.sig,
                "a batch publishing handler must return the replies: Vec<Reply>, or \
                 Result<Vec<Reply>, HandlerResult>",
            ));
        }
    };
    // `-> Result<Vec<Reply>, HandlerResult>` lets the handler skip the publish; a plain
    // `-> Vec<Reply>` is wrapped in `Ok` here. Both checks are syntactic, like the
    // single-message publish form: a type alias is not seen through.
    let (reply_elem, call_body) = if let Some(ok_ty) = publish_result_reply(declared_ty) {
        let Some(elem) = vec_element(ok_ty) else {
            return Err(syn::Error::new_spanned(
                ok_ty,
                "a batch publishing handler replies with a Vec: \
                 Result<Vec<Reply>, HandlerResult>",
            ));
        };
        (elem, quote!((async move #block).await))
    } else {
        let Some(elem) = vec_element(declared_ty) else {
            return Err(syn::Error::new_spanned(
                declared_ty,
                "a batch publishing handler returns the replies: Vec<Reply>, or \
                 Result<Vec<Reply>, HandlerResult>",
            ));
        };
        (
            elem,
            quote!(::core::result::Result::Ok((async move #block).await)),
        )
    };
    Ok(quote! {
        #[allow(non_camel_case_types)]
        #vis struct #name;

        impl ::ruststream::runtime::BatchPublishingDef for #name {
            type Input = #input_ty;
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

            async fn call(
                &self,
                #pat: &[#input_ty],
                #ctx_param: &mut ::ruststream::runtime::Context<'_>,
            ) -> ::core::result::Result<
                ::std::vec::Vec<#reply_elem>,
                ::ruststream::runtime::HandlerResult,
            > {
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

    quote! {
            #[derive(Clone, Copy)]
            #[allow(non_camel_case_types)]
            #vis struct #name;

            impl ::ruststream::runtime::SliceHandler<#input_ty> for #name {
                async fn handle_slice(
                    &self,
                    #pat: &[#input_ty],
                    #ctx_param: &mut ::ruststream::runtime::Context<'_>,
                ) -> ::ruststream::runtime::BatchResult {
                    let outcome: #outcome_ty = (async move #block).await;
                    ::ruststream::runtime::IntoBatchResult::into_batch_result(outcome)
                }
            }

            impl ::ruststream::runtime::BatchDef for #name {
                type Input = #input_ty;
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

fn expand_publishing(
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
        workers_method,
        failure_method,
    } = parts;

    let declared_ty = match &func.sig.output {
        ReturnType::Type(_, ty) => &**ty,
        ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                &func.sig,
                "a publishing handler must return the reply value",
            ));
        }
    };
    // `-> Result<Reply, HandlerResult>` lets the handler skip the publish: `Err(result)` is
    // returned to the dispatcher as-is. A plain `-> Reply` is wrapped in `Ok` here. The check
    // is syntactic, so a type alias hiding the `Result` is treated as a plain reply type.
    let (reply_ty, call_body) = match publish_result_reply(declared_ty) {
        Some(reply_ty) => (reply_ty, quote!((async move #block).await)),
        None => (
            declared_ty,
            quote!(::core::result::Result::Ok((async move #block).await)),
        ),
    };
    Ok(quote! {
        #[allow(non_camel_case_types)]
        #vis struct #name;

        impl ::ruststream::runtime::PublishingDef for #name {
            type Input = #input_ty;
            type Reply = #reply_ty;
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

            async fn call(
                &self,
                #pat: &#input_ty,
                #ctx_param: &mut ::ruststream::runtime::Context<'_>,
            ) -> ::core::result::Result<#reply_ty, ::ruststream::runtime::HandlerResult> {
                #call_body
            }
        }
    })
}

fn expand_subscribing(parts: &HandlerParts<'_>) -> TokenStream2 {
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
        workers_method,
        failure_method,
    } = parts;

    quote! {
            #[derive(Clone, Copy)]
            #[allow(non_camel_case_types)]
            #vis struct #name;

            impl ::ruststream::runtime::Handler<#input_ty> for #name {
                async fn handle(
                    &self,
                    #pat: &#input_ty,
                    #ctx_param: &mut ::ruststream::runtime::Context<'_>,
                ) -> ::ruststream::runtime::Settle {
                    ::ruststream::runtime::IntoSettle::into_settle(
                        (async move #block).await,
                    )
                }
            }

            impl ::ruststream::runtime::SubscriberDef for #name {
                type Input = #input_ty;
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
