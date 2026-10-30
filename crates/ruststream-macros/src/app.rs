//! `#[ruststream::app]`: the entry point a service's app builder gets.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{ItemFn, LitInt, LitStr, Meta, Token};

/// The runtime arguments of `#[ruststream::app]`, as `#[tokio::main]` spells them.
pub(crate) struct AppRuntimeArgs {
    flavor: Option<LitStr>,
    worker_threads: Option<LitInt>,
}

impl Parse for AppRuntimeArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut args = Self {
            flavor: None,
            worker_threads: None,
        };
        let metas = Punctuated::<Meta, Token![,]>::parse_terminated(input)?;
        let mut pairs = Vec::with_capacity(metas.len());
        for meta in metas {
            let Meta::NameValue(pair) = meta else {
                return Err(syn::Error::new_spanned(
                    meta,
                    "#[ruststream::app] takes `flavor = \"current_thread\"` or \
                     `flavor = \"multi_thread\"`, and `worker_threads = n`",
                ));
            };
            pairs.push(pair);
        }
        for pair in pairs {
            let syn::Expr::Lit(syn::ExprLit { lit, .. }) = &pair.value else {
                return Err(syn::Error::new_spanned(
                    &pair.value,
                    "expected a literal, as `#[tokio::main]` takes",
                ));
            };
            if pair.path.is_ident("flavor") {
                let syn::Lit::Str(flavor) = lit else {
                    return Err(syn::Error::new_spanned(
                        lit,
                        "expected \"current_thread\" or \"multi_thread\"",
                    ));
                };
                if !matches!(flavor.value().as_str(), "current_thread" | "multi_thread") {
                    return Err(syn::Error::new_spanned(
                        flavor,
                        "unknown runtime flavor; expected \"current_thread\" or \"multi_thread\"",
                    ));
                }
                if args.flavor.replace(flavor.clone()).is_some() {
                    return Err(syn::Error::new_spanned(&pair.path, "duplicate `flavor`"));
                }
            } else if pair.path.is_ident("worker_threads") {
                let syn::Lit::Int(count) = lit else {
                    return Err(syn::Error::new_spanned(lit, "expected a worker count"));
                };
                if count.base10_parse::<usize>()? == 0 {
                    return Err(syn::Error::new_spanned(
                        count,
                        "a runtime needs at least one worker",
                    ));
                }
                if args.worker_threads.replace(count.clone()).is_some() {
                    return Err(syn::Error::new_spanned(
                        &pair.path,
                        "duplicate `worker_threads`",
                    ));
                }
            } else {
                return Err(syn::Error::new_spanned(
                    &pair.path,
                    "unknown argument; #[ruststream::app] takes `flavor` and `worker_threads`",
                ));
            }
        }
        if let (Some(flavor), Some(count)) = (&args.flavor, &args.worker_threads)
            && flavor.value() == "current_thread"
        {
            return Err(syn::Error::new_spanned(
                count,
                "`worker_threads` sizes a multi-threaded runtime; a current-thread runtime runs \
                 on the thread that calls `main`",
            ));
        }
        Ok(args)
    }
}

impl AppRuntimeArgs {
    /// The `AppRuntime` value the arguments name.
    fn runtime(&self) -> TokenStream2 {
        let runtime = quote!(::ruststream::runtime::cli::AppRuntime);
        if self
            .flavor
            .as_ref()
            .is_some_and(|flavor| flavor.value() == "current_thread")
        {
            return quote!(#runtime::CurrentThread);
        }
        let Some(count) = &self.worker_threads else {
            return quote!(#runtime::MultiThread);
        };
        // The literal was checked to be non-zero, so the `None` arm is unreachable; MIN keeps
        // the lowering panic-free.
        quote! {
            #runtime::MultiThreadWorkers(match ::core::num::NonZeroUsize::new(#count) {
                ::core::option::Option::Some(count) => count,
                ::core::option::Option::None => ::core::num::NonZeroUsize::MIN,
            })
        }
    }
}

pub(crate) fn expand_app(runtime: &AppRuntimeArgs, func: &ItemFn) -> syn::Result<TokenStream> {
    if let Some(asyncness) = func.sig.asyncness {
        return Err(syn::Error::new_spanned(
            asyncness,
            "#[ruststream::app] requires a synchronous builder returning `impl App` or `RustStream`",
        ));
    }
    if !func.sig.inputs.is_empty() {
        return Err(syn::Error::new_spanned(
            &func.sig.inputs,
            "#[ruststream::app] builder must take no arguments",
        ));
    }
    let name = &func.sig.ident;
    let runtime = runtime.runtime();
    Ok(quote! {
        #func

        fn main() -> ::ruststream::__private::ExitCode {
            ::ruststream::runtime::cli::run_main_on(#runtime, #name)
        }
    }
    .into())
}
