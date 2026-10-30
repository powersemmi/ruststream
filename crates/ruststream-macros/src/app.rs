//! `#[ruststream::app]`: the entry point a service's app builder gets.

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{ItemFn, LitInt, LitStr, Meta, Token};

use crate::runtime::Runtime;

/// The runtime arguments of `#[ruststream::app]`, as `#[tokio::main]` spells them.
pub(crate) struct AppRuntimeArgs {
    flavor: Option<LitStr>,
    worker_threads: Option<LitInt>,
}

impl Parse for AppRuntimeArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        Self::parse_for(input, Runtime::SELECTED)
    }
}

impl AppRuntimeArgs {
    /// Parses the arguments the attribute takes on `runtime`.
    fn parse_for(input: ParseStream, runtime: Runtime) -> syn::Result<Self> {
        let mut args = Self {
            flavor: None,
            worker_threads: None,
        };
        let metas = Punctuated::<Meta, Token![,]>::parse_terminated(input)?;
        if runtime == Runtime::Embassy {
            return metas
                .first()
                .map_or(Ok(args), |meta| Err(embassy_argument(meta)));
        }
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
    expand_for(runtime, func, Runtime::SELECTED).map(Into::into)
}

/// Why the embassy runtime refuses an argument: the attribute takes none there.
fn embassy_argument(meta: &Meta) -> syn::Error {
    let path = meta.path();
    let tokio_knob = ["flavor", "worker_threads"]
        .into_iter()
        .find(|knob| path.is_ident(knob));
    let message = match tokio_knob {
        Some("flavor") => {
            "`flavor` chooses a tokio runtime; on the embassy runtime the app runs as one embassy \
             task, and #[ruststream::app] takes no arguments"
        }
        Some(_) => {
            "`worker_threads` sizes a tokio runtime; on the embassy runtime the app runs as one \
             embassy task, and #[ruststream::app] takes no arguments"
        }
        None => {
            "#[ruststream::app] takes no arguments on the embassy runtime: the `embassy` feature \
             selects it"
        }
    };
    syn::Error::new_spanned(path, message)
}

/// The expansion on `target`.
fn expand_for(
    runtime: &AppRuntimeArgs,
    func: &ItemFn,
    target: Runtime,
) -> syn::Result<TokenStream2> {
    if target == Runtime::Embassy {
        return expand_embassy(func);
    }
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
    })
}

/// The embassy form: the builder stays as written, and a module of the same name beside it holds
/// the app's control and the one embassy task that runs the app.
///
/// The builder takes what the board hands the app (its brokers, a radio) as arguments, which
/// become the task's arguments. A run that ends on an error leaves the health at `Failed` and
/// logs the error, so the device reads what went wrong from the control and the log.
fn expand_embassy(func: &ItemFn) -> syn::Result<TokenStream2> {
    if let Some(asyncness) = func.sig.asyncness {
        return Err(syn::Error::new_spanned(
            asyncness,
            "#[ruststream::app] requires a synchronous builder returning \
             `EmbeddedApp<impl Mounts>`",
        ));
    }
    if !func.sig.generics.params.is_empty() || func.sig.generics.where_clause.is_some() {
        return Err(syn::Error::new_spanned(
            &func.sig.generics,
            "the app's embassy task is one concrete task, so its builder takes no generic \
             parameters; name the board's broker types",
        ));
    }
    let mut params = Vec::with_capacity(func.sig.inputs.len());
    let mut names = Vec::with_capacity(func.sig.inputs.len());
    for (index, input) in func.sig.inputs.iter().enumerate() {
        let syn::FnArg::Typed(typed) = input else {
            return Err(syn::Error::new_spanned(
                input,
                "#[ruststream::app] goes on a free function",
            ));
        };
        // A plain name keeps its name on the task; any other pattern (`mut radio`, a tuple) is
        // the builder's business, and the task hands the value over under a name of its own.
        let name = match &*typed.pat {
            syn::Pat::Ident(ident)
                if ident.by_ref.is_none()
                    && ident.mutability.is_none()
                    && ident.subpat.is_none() =>
            {
                ident.ident.clone()
            }
            _ => syn::Ident::new(&format!("__rs_arg{index}"), proc_macro2::Span::call_site()),
        };
        let ty = &typed.ty;
        params.push(quote!(#name: #ty));
        names.push(name);
    }
    let name = &func.sig.ident;
    let item_vis = item_visibility(&func.vis);
    let mod_vis = &func.vis;
    let doc_mod = format!("The embassy task that runs the app [`{name}`] builds, and its control.");
    let doc_task = format!(
        "Runs the app [`{name}`](super::{name}) builds until [`CONTROL`] stops it: spawn it once, \
         from `main`. A run that ends on an error leaves the health at `Failed` and logs the \
         error."
    );
    Ok(quote! {
        #func

        #[doc = #doc_mod]
        #mod_vis mod #name {
            #[allow(unused_imports)]
            use super::*;

            /// The app's control: [`stop`](::ruststream::runtime::Control::stop) asks the app to
            /// shut down, and [`health`](::ruststream::runtime::Control::health) reads where it is.
            #item_vis static CONTROL: ::ruststream::runtime::Control =
                ::ruststream::runtime::Control::new();

            #[doc = #doc_task]
            #[::embassy_executor::task]
            #item_vis async fn task(#(#params),*) {
                if let ::core::result::Result::Err(error) =
                    super::#name(#(#names),*).run(&CONTROL).await
                {
                    ::ruststream::__private::tracing::error!(
                        target: "ruststream::embassy",
                        error = %error,
                        "the app stopped on an error"
                    );
                }
            }
        }
    })
}

/// The visibility of the items in the module beside the builder: as visible as the builder, so
/// whoever can call it can spawn its task.
fn item_visibility(vis: &syn::Visibility) -> TokenStream2 {
    match vis {
        syn::Visibility::Inherited => quote!(pub(super)),
        syn::Visibility::Restricted(restricted) if restricted.path.is_ident("super") => {
            quote!(pub(in super::super))
        }
        other => quote!(#other),
    }
}

#[cfg(test)]
mod tests {
    use syn::parse::Parser;

    use super::*;

    fn args(runtime: Runtime, tokens: TokenStream2) -> syn::Result<AppRuntimeArgs> {
        (|input: ParseStream| AppRuntimeArgs::parse_for(input, runtime)).parse2(tokens)
    }

    fn rejection<T>(result: syn::Result<T>) -> String {
        match result {
            Ok(_) => panic!("this attribute is rejected"),
            Err(err) => err.to_string(),
        }
    }

    fn no_args(runtime: Runtime) -> AppRuntimeArgs {
        args(runtime, TokenStream2::new()).expect("no arguments parse")
    }

    #[test]
    fn the_tokio_flavor_is_refused_on_embassy() {
        let refused = rejection(args(Runtime::Embassy, quote!(flavor = "current_thread")));

        assert_eq!(
            refused,
            "`flavor` chooses a tokio runtime; on the embassy runtime the app runs as one \
             embassy task, and #[ruststream::app] takes no arguments"
        );
    }

    #[test]
    fn the_tokio_worker_count_is_refused_on_embassy() {
        let refused = rejection(args(Runtime::Embassy, quote!(worker_threads = 2)));

        assert_eq!(
            refused,
            "`worker_threads` sizes a tokio runtime; on the embassy runtime the app runs as one \
             embassy task, and #[ruststream::app] takes no arguments"
        );
    }

    #[test]
    fn any_other_argument_is_refused_on_embassy() {
        let refused = rejection(args(Runtime::Embassy, quote!(runtime = embassy)));

        assert_eq!(
            refused,
            "#[ruststream::app] takes no arguments on the embassy runtime: the `embassy` \
             feature selects it"
        );
    }

    #[test]
    fn an_embassy_builder_gets_a_task_and_a_control_beside_it() {
        let func: ItemFn = syn::parse_quote! {
            fn app(bus: Bus, mut radio: Radio) -> EmbeddedApp<impl Mounts> {
                EmbeddedApp::new().with_broker(bus, |b| b)
            }
        };

        let expanded = expand_for(&no_args(Runtime::Embassy), &func, Runtime::Embassy)
            .expect("an embassy builder takes arguments")
            .to_string();

        assert!(expanded.contains("mod app {"), "{expanded}");
        assert!(
            expanded.contains(
                "pub (super) static CONTROL : :: ruststream :: runtime :: Control = :: \
                 ruststream :: runtime :: Control :: new () ;"
            ),
            "{expanded}"
        );
        assert!(
            expanded.contains(
                "# [:: embassy_executor :: task] pub (super) async fn task (bus : Bus , \
                 __rs_arg1 : Radio)"
            ),
            "{expanded}"
        );
        assert!(
            expanded.contains("super :: app (bus , __rs_arg1) . run (& CONTROL) . await"),
            "{expanded}"
        );
        assert!(!expanded.contains("fn main"), "{expanded}");
    }

    #[test]
    fn a_public_builder_gets_a_public_task() {
        let func: ItemFn = syn::parse_quote! {
            pub fn app() -> EmbeddedApp<impl Mounts> {
                EmbeddedApp::new()
            }
        };

        let expanded = expand_for(&no_args(Runtime::Embassy), &func, Runtime::Embassy)
            .expect("a public builder expands")
            .to_string();

        assert!(expanded.contains("pub mod app {"), "{expanded}");
        assert!(expanded.contains("pub async fn task ()"), "{expanded}");
    }

    #[test]
    fn a_generic_builder_is_refused_on_embassy() {
        let func: ItemFn = syn::parse_quote! {
            fn app<B: LocalBroker>(bus: B) -> EmbeddedApp<impl Mounts> {
                EmbeddedApp::new().with_broker(bus, |b| b)
            }
        };

        let refused = rejection(expand_for(
            &no_args(Runtime::Embassy),
            &func,
            Runtime::Embassy,
        ));

        assert_eq!(
            refused,
            "the app's embassy task is one concrete task, so its builder takes no generic \
             parameters; name the board's broker types"
        );
    }

    #[test]
    fn a_tokio_builder_still_takes_no_arguments() {
        let func: ItemFn = syn::parse_quote! {
            fn app(bus: Bus) -> impl App {
                RustStream::new(AppInfo::new("svc", "0.1.0"))
            }
        };

        let refused = rejection(expand_for(&no_args(Runtime::Tokio), &func, Runtime::Tokio));

        assert_eq!(refused, "#[ruststream::app] builder must take no arguments");
    }
}
