//! Procedural macros for [RustStream](https://github.com/powersemmi/ruststream).
//!
//! Re-exported from the `ruststream` crate under the `macros` feature; depend on that rather than
//! on this crate directly.

#![forbid(unsafe_code)]

mod expand;
mod from_ref;
mod parse;

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{DeriveInput, ItemFn, parse_macro_input};

use parse::{SubscriberArgs, doc_description};

/// Turns an `async fn` handler into a mountable subscriber definition.
///
/// ```ignore
/// /// Processes incoming orders.
/// #[subscriber("orders")]
/// async fn handle(order: &Order) -> HandlerResult { HandlerResult::Ack }
/// // later: broker_scope.include(handle);
///
/// // reply form: the return value is encoded and published to "responses" through the
/// // TypedPublisher (broker + reply codec) passed at wiring time.
/// #[subscriber("requests", publish("responses"))]
/// async fn reply(req: &Request) -> Response { /* ... */ }
/// // later: broker_scope.include_publishing(reply, typed_publisher);
///
/// // reply form with explicit ack control: `Ok` publishes the reply, `Err` skips it and the
/// // dispatcher acts on the returned HandlerResult.
/// #[subscriber("requests", publish("responses"))]
/// async fn confirm(req: &Request) -> Result<Response, HandlerResult> { /* ... */ }
///
/// // batch form: the handler takes the whole decoded batch as a slice; the source's
/// // subscriber must implement BatchSubscriber. Mounted with include_batch.
/// #[subscriber(batch("orders"))]
/// async fn bill(orders: &[Order]) -> HandlerResult { /* settles the whole batch */ }
///
/// // raw form: no codec, no serde - the handler receives the payload bytes as-is. The
/// // message parameter must be `&[u8]`; a serde-typed parameter under `raw` is an error.
/// #[subscriber("frames", raw)]
/// async fn on_frame(frame: &[u8]) -> HandlerResult { /* parse it yourself */ }
///
/// // raw reply form: the returned bytes are published as-is to "frames-out" through the bare
/// // publisher attached at the include site (b.include(mirror).publisher(policy), or the
/// // broker's default publish policy without the call) - no codec on either side. Returning
/// // Result<Vec<u8>, HandlerResult> gives the same explicit ack control as the typed form.
/// #[subscriber("frames", raw, publish_raw("frames-out"))]
/// async fn mirror(frame: &[u8]) -> Vec<u8> { frame.to_vec() }
///
/// // publish_raw also composes with a typed input (the gateway shape): the input decodes with
/// // the scope codec, the returned bytes still go out unencoded.
/// #[subscriber("orders", publish_raw("orders-wire"))]
/// async fn encode(order: &Order) -> Vec<u8> { /* your wire format */ }
/// ```
///
/// Without `publish(..)` the handler returns any `Into<Settle>` (a `Settle`, a `HandlerResult`,
/// `()`, or `Result<_, E>`). Attach a post-settle continuation with `HandlerResult::ack().and_after`
/// (any outcome works), which runs after the message is settled. With `publish(..)` it returns the
/// reply value to publish, or `Result<Reply, HandlerResult>` to control acknowledgement:
/// `Err(result)` publishes nothing and returns `result` to the dispatcher. The `Result` form is
/// detected syntactically, so spell it out in the signature (a type alias is treated as a plain
/// reply type). `publish_raw(..)` is the byte reply clause: the handler returns `Vec<u8>` (any
/// owned `AsRef<[u8]>` type) and the bytes are published unencoded through the bare publisher
/// paired at the include site; a failed reply publish nacks the delivery with requeue, exactly
/// like the typed reply form. It composes with both a `raw` and a typed input; `raw` with the
/// encoded `publish(..)` is rejected (a raw handler's reply is bytes).
///
/// Wrapping the source in `batch(..)` switches the definition to a `BatchDef`: the handler takes
/// `&[T]` and runs once per batch pulled from the broker's `BatchSubscriber` (use the `Buffered`
/// adapter for brokers without native batching). It returns any `IntoBatchResult` - one outcome
/// for the whole batch (`HandlerResult`, `()`, `Result<_, E>`), or a per-element vector
/// (`Vec<Settle>`, or `Vec<HandlerResult>`) to settle element `i` of the slice with outcome `i`,
/// each element carrying its own optional `and_after` continuation. The source type is recovered
/// from the constructor path, so a generic source spells its parameters:
/// `batch(Buffered::<Name>::new(Name::new("orders")))`.
///
/// Combining `batch(..)` with `publish(..)` produces a `BatchPublishingDef` (mounted with
/// `include_batch_publishing`): the handler returns `Vec<Reply>` (or
/// `Result<Vec<Reply>, HandlerResult>` for explicit ack control, all-or-nothing - selective
/// outcomes do not compose with a transaction), every reply is published to the reply name, and
/// the whole batch is acked after. Hand the mount a `TypedPublisher` for independent reply
/// publishes, or `.transactional()` for one transaction per batch.
///
/// A `workers(n)` clause processes up to `n` deliveries (or batches) of this subscriber
/// concurrently, each in its own task; global processing order is lost by design, and
/// back-pressure holds at `n` in-flight deliveries. `workers(n, by_key)` switches to `n`
/// sequential lanes keyed by the message's partition key, preserving per-key ordering
/// (single-message forms only). The default is the sequential loop.
///
/// A `start_at(<position>)` clause opens the subscription at that position instead of the
/// broker's default, seeking before the first delivery ("start from the latest on deploy",
/// "replay the whole log"). The position is the broker's own type, named by its constructor
/// (`MemoryPosition::start()`, a Kafka-style `latest()`); an argument-less `start_at()` uses
/// the `Default` of the broker's position type, whose meaning the broker documents (the
/// in-memory broker's default is the start of the log). The source's subscriber must
/// implement the `Seekable` capability, so the clause does not compile against a broker
/// without a replayable log. The position is forced on every startup; conditional defaults
/// stay on the broker's own subscription descriptor.
///
/// ```ignore
/// // Opens at the start of the log: entries published before the service started are
/// // replayed into the fresh subscription.
/// #[subscriber("audit", start_at(MemoryPosition::start()))]
/// async fn record(entry: &Entry) -> HandlerResult { /* ... */ }
/// ```
///
/// In both forms the handler may declare an optional second parameter, the per-delivery
/// `&mut Context`, to read app state or publish manually. Any further parameter is an extractor: its
/// type must implement
/// [`FromContext`](../ruststream/runtime/trait.FromContext.html), and the generated handler resolves
/// it from the delivery context (in declaration order) before the body runs, so dependencies arrive
/// as arguments. A failed extraction settles the delivery by the rejection's `HandlerResult` without
/// running the body.
///
/// ```ignore
/// // `State<Db>` is resolved from the application state before the body runs.
/// #[subscriber("orders")]
/// async fn handle(order: &Order, State(db): State<Db>) -> HandlerResult { /* ... */ }
/// ```
#[proc_macro_attribute]
pub fn subscriber(attr: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as SubscriberArgs);
    let func = parse_macro_input!(item as ItemFn);
    expand::subscriber(&args, &func).unwrap_or_else(|err| err.to_compile_error().into())
}

/// Generates a `main` entry point for a `RustStream` service.
///
/// Place it on a synchronous, argument-free function that builds and returns an application -
/// `impl App` (the recommended form, hiding the composed type parameters) or a concrete
/// `RustStream<_>`. The expansion keeps the function and adds a `main` that hands it to
/// `ruststream::runtime::cli::run_main`, producing a binary that understands the `run` and
/// `asyncapi gen` commands with no hand-written runtime boilerplate.
///
/// ```ignore
/// #[ruststream::app]
/// fn app() -> impl App {
///     RustStream::new(AppInfo::new("svc", "0.1.0")).register_broker(MemoryBroker::new())
/// }
/// ```
#[proc_macro_attribute]
pub fn app(attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);
    expand_app(&attr.into(), &func).unwrap_or_else(|err| err.to_compile_error().into())
}

fn expand_app(attr: &TokenStream2, func: &ItemFn) -> syn::Result<TokenStream> {
    if !attr.is_empty() {
        return Err(syn::Error::new_spanned(
            attr,
            "#[ruststream::app] takes no arguments",
        ));
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
    Ok(quote! {
        #func

        fn main() -> ::std::process::ExitCode {
            ::ruststream::runtime::cli::run_main(#name)
        }
    }
    .into())
}

/// Derives [`Message`](../ruststream/trait.Message.html) metadata: the type name and its doc
/// comment.
///
/// ```ignore
/// /// An order placed by a customer.
/// #[derive(Message)]
/// struct Order { id: u32 }
/// // Order::NAME == "Order", Order::DESCRIPTION == Some("An order placed by a customer.")
/// ```
#[proc_macro_derive(Message)]
pub fn derive_message(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let name = &input.ident;
    let name_str = name.to_string();
    let description = doc_description(&input.attrs);
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    quote! {
        impl #impl_generics ::ruststream::Message for #name #ty_generics #where_clause {
            const NAME: &'static str = #name_str;
            const DESCRIPTION: ::core::option::Option<&'static str> = #description;
        }
    }
    .into()
}

/// Derives [`FromRef`](../ruststream/runtime/trait.FromRef.html) for each field of an
/// application-state struct, so `#[subscriber]` handlers can inject any field with
/// `State<FieldType>` without a hand-written impl.
///
/// Each field gets a `FromRef` impl that clones it out of the state. Because the generated impl
/// carries no generic parameter, it is legal even for fields whose type comes from another crate (a
/// broker publisher, a client pool). A field that another field's type already claims, or that
/// should not be injectable, opts out with `#[from_ref(skip)]`; two fields may not share a type
/// (injection by type would be ambiguous).
///
/// ```ignore
/// #[derive(FromRef)]
/// struct AppState {
///     orders: OrderService, // handlers can now take `State<OrderService>`
///     #[from_ref(skip)]
///     config: Config,
/// }
/// ```
#[proc_macro_derive(FromRef, attributes(from_ref))]
pub fn derive_from_ref(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    from_ref::expand(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
