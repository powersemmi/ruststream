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
use quote::{ToTokens, quote};
use syn::parse::{Parse, ParseStream};
use syn::punctuated::Punctuated;
use syn::{Attribute, DeriveInput, Expr, ItemFn, Meta, Token, Type, parse_macro_input};

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
/// Clause values need not be literals: `workers(..)` takes any `usize` expression (a constant,
/// a static, a function call - an integer literal keeps the compile-time zero rejection, a
/// runtime value of zero panics at registration), `publish(..)` / `publish_raw(..)` take a
/// `&'static str` expression, `on_failure(..)` keys accept a `FailurePolicy` expression next
/// to the keyword vocabulary, and a `#[publishes(..)]` channel takes a const-evaluable
/// `&'static str` (it initializes an associated const, so a `static` cannot feed it).
///
/// An `Out(out): Out<P>` parameter injects a live publisher, paired at startup from the
/// source attached at the include site (`b.include(f).publisher(..)`); its optional third
/// position declares the message set the handler publishes (`Out<impl Publisher, Marker,
/// (A, B)>` - a tuple, a single type, or a `#[derive(OutMessages)]` set enum), enabling the
/// dictionary-driven typed publish path; a `Seek(seeker): Seek<K>` parameter injects the
/// subscription's own seeker, minted right after the subscription opens (the source's
/// subscriber must implement the `Seekable` capability). They combine freely in one handler:
/// with each other, with `raw`, with `batch(..)`, and with every reply form (`publish(..)`,
/// `publish_raw(..)`, and the batch publishing form). An `Out` parameter's attachment is
/// required at the include site: `.publisher(..)` on the plain and batch forms, `.out(..)` on
/// the reply forms (where `.publisher(..)` stays the reply's own attachment).
///
/// A `start_at(<position>)` clause opens the subscription at that position instead of the
/// broker's default, seeking before the first delivery ("start from the latest on deploy",
/// "replay the whole log"). The position is the broker's own type, named by its constructor
/// (`MemoryPosition::start()`, a Kafka-style `latest()`); the source's subscriber must
/// implement the `Seekable` capability, so the clause does not compile against a broker
/// without a replayable log. The position is forced on every startup; without the clause the
/// subscription simply opens at the broker's default, and conditional defaults stay on the
/// broker's own subscription descriptor.
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

/// Derives [`Message`](../ruststream/trait.Message.html) metadata: the type name, its doc
/// comment, and its header contract.
///
/// The optional `#[message(headers(Meta))]` attribute declares the type's typed header
/// contract (see `MessageHeaders` in the core crate): `Meta` is serialized into the header map
/// on the typed publish path and lifted into the message's `AsyncAPI` headers schema. Without
/// the attribute the type declares no typed headers.
///
/// The derive also makes the type a one-element declared message set (see `OutMessages` in the
/// core crate), so `Out<impl Publisher, Marker, TheType>` declares it directly. A type meant
/// as a set of several messages derives `OutMessages` instead - the two derives conflict on
/// purpose (one type is a message or a set, not both).
///
/// ```ignore
/// /// An order placed by a customer.
/// #[derive(Message)]
/// struct Order { id: u32 }
/// // Order::NAME == "Order", Order::DESCRIPTION == Some("An order placed by a customer.")
///
/// #[derive(Message, Serialize)]
/// #[message(headers(ChunkMeta))]
/// struct ChunkDone { output_key: String }
/// ```
#[proc_macro_derive(Message, attributes(message))]
pub fn derive_message(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let name = &input.ident;
    let name_str = name.to_string();
    let description = doc_description(&input.attrs);
    let contract = match message_headers_contract(&input.attrs) {
        Ok(Some(headers)) => quote!(::ruststream::WithHeaders<#headers>),
        Ok(None) => quote!(::ruststream::NoHeaders),
        Err(err) => return err.into_compile_error().into(),
    };
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    // The OutMessages impl adds its own `__RsM` TYPE parameter; pushing it onto a clone of the
    // generics (after any lifetimes) and re-splitting keeps the emitted parameter order legal
    // for lifetime-generic message types.
    let mut set_generics = input.generics.clone();
    set_generics
        .params
        .push(syn::parse_quote!(__RsM: ::ruststream::runtime::OutSlot));
    set_generics
        .make_where_clause()
        .predicates
        .push(syn::parse_quote!(#name #ty_generics: ::ruststream::runtime::OutMessage<__RsM>));
    let (set_impl_generics, _, set_where_clause) = set_generics.split_for_impl();

    quote! {
        impl #impl_generics ::ruststream::Message for #name #ty_generics #where_clause {
            const NAME: &'static str = #name_str;
            const DESCRIPTION: ::core::option::Option<&'static str> = #description;
        }

        impl #impl_generics ::ruststream::MessageHeaders for #name #ty_generics #where_clause {
            type Contract = #contract;
        }

        // The type as a one-element declared message set: `Out<.., Marker, TheType>`.
        impl #impl_generics
            ::ruststream::runtime::ContainsMessage<#name #ty_generics, ::ruststream::runtime::SlotPos<0>>
            for #name #ty_generics #where_clause
        {
        }

        impl #set_impl_generics ::ruststream::runtime::OutMessages<__RsM> for #name #ty_generics
        #set_where_clause
        {
            fn outgoing()
                -> ::std::vec::Vec<::ruststream::runtime::OutgoingMessageMetadata>
            {
                ::std::vec![
                    ::ruststream::runtime::OutgoingMessageMetadata::new(
                        <#name #ty_generics as ::ruststream::runtime::OutMessage<__RsM>>::CHANNEL,
                        ::core::any::type_name::<#name #ty_generics>(),
                    )
                    .with_message_name({
                        #[allow(unused_imports)]
                        use ::ruststream::__private::NoMessageProbe as _;
                        ::ruststream::__private::Probe::<#name #ty_generics>::new().message_name()
                    })
                    .with_message_description({
                        #[allow(unused_imports)]
                        use ::ruststream::__private::NoMessageProbe as _;
                        ::ruststream::__private::Probe::<#name #ty_generics>::new()
                            .message_description()
                    })
                    .with_payload_schema({
                        #[allow(unused_imports)]
                        use ::ruststream::__private::NoSchemaProbe as _;
                        ::ruststream::__private::Probe::<#name #ty_generics>::new().schema_json()
                    })
                    .with_headers_schema({
                        #[allow(unused_imports)]
                        use ::ruststream::__private::NoHeadersSchemaProbe as _;
                        ::ruststream::__private::Probe::<#name #ty_generics>::new()
                            .headers_schema_json()
                    }),
                ]
            }
        }
    }
    .into()
}

/// Parses the optional `#[message(headers(Type))]` helper attribute of `#[derive(Message)]`.
fn message_headers_contract(attrs: &[Attribute]) -> syn::Result<Option<Type>> {
    let mut found: Option<Type> = None;
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("message")) {
        let meta: Meta = attr.parse_args()?;
        let Meta::List(list) = &meta else {
            return Err(syn::Error::new_spanned(
                meta,
                "expected `headers(<Type>)` inside #[message(..)]",
            ));
        };
        if !list.path.is_ident("headers") {
            return Err(syn::Error::new_spanned(
                &list.path,
                "expected `headers(<Type>)` inside #[message(..)]",
            ));
        }
        let headers: Type = list.parse_args()?;
        if found.replace(headers).is_some() {
            return Err(syn::Error::new_spanned(
                attr,
                "duplicate `headers(..)`: a message declares one header contract",
            ));
        }
    }
    Ok(found)
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

/// Derives [`OutSlot`](../ruststream/runtime/trait.OutSlot.html) for a unit struct, making it a
/// slot marker for `Out<impl Publisher, Marker>` handler parameters. The marker's
/// [`NAME`](../ruststream/runtime/trait.OutSlot.html#associatedconstant.NAME) is the struct's
/// identifier, used by startup errors and test assertions.
///
/// The optional `#[publishes(Type = "channel", ..)]` attribute declares the slot's publish
/// dictionary: each pair maps one message type to the channel it publishes to, enabling the
/// typed publish path (`out.publish_typed(&value)`) and the `AsyncAPI` `send` operations.
/// Several types may share a channel; listing one type twice is an error (its destination
/// would be ambiguous). A marker without the attribute keeps the byte-level path only.
///
/// ```ignore
/// #[derive(OutSlot)]
/// struct Encoded;
///
/// #[derive(OutSlot)]
/// #[publishes(ChunkDone = "chunks.done", Progress = "chunks.progress")]
/// struct Events;
///
/// #[subscriber("chunks", raw)]
/// async fn transcode(chunk: &[u8], Out(out): Out<impl Publisher, Encoded>) -> HandlerResult {
///     /* ... */
/// }
/// // include site: b.include(transcode).out(Encoded, KafkaPublish::default());
/// ```
#[proc_macro_derive(OutSlot, attributes(publishes))]
pub fn derive_out_slot(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let syn::Data::Struct(data) = &input.data else {
        return syn::Error::new_spanned(&input.ident, "OutSlot is derived on a unit struct")
            .into_compile_error()
            .into();
    };
    if !matches!(data.fields, syn::Fields::Unit) {
        return syn::Error::new_spanned(
            &data.fields,
            "an OutSlot marker is a unit struct: it identifies the slot and carries no data",
        )
        .into_compile_error()
        .into();
    }
    if !input.generics.params.is_empty() {
        return syn::Error::new_spanned(
            &input.generics,
            "an OutSlot marker takes no generic parameters",
        )
        .into_compile_error()
        .into();
    }
    let dictionary = match publishes_dictionary(&input.attrs) {
        Ok(dictionary) => dictionary,
        Err(err) => return err.into_compile_error().into(),
    };
    let name = &input.ident;
    let name_str = name.to_string();

    let outgoing = if dictionary.is_empty() {
        quote!()
    } else {
        let entries = dictionary.iter().map(|entry| {
            let ty = &entry.ty;
            let channel = &entry.channel;
            quote! {
                ::ruststream::runtime::OutgoingMessageMetadata::new(
                    #channel,
                    ::core::any::type_name::<#ty>(),
                )
                .with_message_name({
                    #[allow(unused_imports)]
                    use ::ruststream::__private::NoMessageProbe as _;
                    ::ruststream::__private::Probe::<#ty>::new().message_name()
                })
                .with_message_description({
                    #[allow(unused_imports)]
                    use ::ruststream::__private::NoMessageProbe as _;
                    ::ruststream::__private::Probe::<#ty>::new().message_description()
                })
                .with_payload_schema({
                    #[allow(unused_imports)]
                    use ::ruststream::__private::NoSchemaProbe as _;
                    ::ruststream::__private::Probe::<#ty>::new().schema_json()
                })
                .with_headers_schema({
                    #[allow(unused_imports)]
                    use ::ruststream::__private::NoHeadersSchemaProbe as _;
                    ::ruststream::__private::Probe::<#ty>::new().headers_schema_json()
                })
            }
        });
        quote! {
            fn outgoing() -> ::std::vec::Vec<::ruststream::runtime::OutgoingMessageMetadata> {
                ::std::vec![#(#entries),*]
            }
        }
    };
    let channels = dictionary.iter().map(|entry| {
        let ty = &entry.ty;
        let channel = &entry.channel;
        quote! {
            impl ::ruststream::runtime::OutMessage<#name> for #ty {
                const CHANNEL: &'static str = #channel;
            }
        }
    });

    quote! {
        impl ::ruststream::runtime::OutSlot for #name {
            const NAME: &'static str = #name_str;

            #outgoing
        }

        #(#channels)*
    }
    .into()
}

/// Derives a declared message set (`OutMessages` in the core crate) from an enum whose
/// variants each wrap one message model. The enum is a type-level declaration: it is never
/// constructed, its variants' inner types are the set.
///
/// Naming the enum as the third `Out` argument (`Out<impl Publisher, Marker, SendSet>`)
/// declares that the handler publishes exactly those models: `publish_typed` compiles for each
/// of them (routed by the marker's `#[publishes(..)]` dictionary), and the generated `AsyncAPI`
/// document lists them as the handler's `send` operations.
///
/// Do not combine with `#[derive(Message)]` on the same enum: a type is a message or a set,
/// and the two derives deliberately produce conflicting impls.
///
/// ```ignore
/// #[derive(OutMessages)]
/// enum ConvertSends {
///     Progress(Progress),
///     Done(ChunkDone),
/// }
///
/// #[subscriber("chunks.raw")]
/// async fn convert(
///     chunk: &Chunk,
///     Out(out): Out<impl Publisher, Events, ConvertSends>,
/// ) -> HandlerResult { /* out.publish_typed(&Progress { .. }) */ }
/// ```
#[proc_macro_derive(OutMessages)]
pub fn derive_out_messages(item: TokenStream) -> TokenStream {
    let input = parse_macro_input!(item as DeriveInput);
    let syn::Data::Enum(data) = &input.data else {
        return syn::Error::new_spanned(
            &input.ident,
            "OutMessages is derived on an enum whose variants each wrap one message model",
        )
        .into_compile_error()
        .into();
    };
    if !input.generics.params.is_empty() {
        return syn::Error::new_spanned(
            &input.generics,
            "an OutMessages set takes no generic parameters",
        )
        .into_compile_error()
        .into();
    }
    let models = match out_messages_models(data) {
        Ok(models) => models,
        Err(err) => return err.into_compile_error().into(),
    };
    let name = &input.ident;
    let memberships = models.iter().enumerate().map(|(index, model)| {
        quote! {
            impl ::ruststream::runtime::ContainsMessage<
                #model,
                ::ruststream::runtime::SlotPos<#index>,
            > for #name
            {
            }
        }
    });
    let entries = models.iter().map(|model| {
        quote! {
            ::ruststream::runtime::OutgoingMessageMetadata::new(
                <#model as ::ruststream::runtime::OutMessage<__RsM>>::CHANNEL,
                ::core::any::type_name::<#model>(),
            )
            .with_message_name({
                #[allow(unused_imports)]
                use ::ruststream::__private::NoMessageProbe as _;
                ::ruststream::__private::Probe::<#model>::new().message_name()
            })
            .with_message_description({
                #[allow(unused_imports)]
                use ::ruststream::__private::NoMessageProbe as _;
                ::ruststream::__private::Probe::<#model>::new().message_description()
            })
            .with_payload_schema({
                #[allow(unused_imports)]
                use ::ruststream::__private::NoSchemaProbe as _;
                ::ruststream::__private::Probe::<#model>::new().schema_json()
            })
            .with_headers_schema({
                #[allow(unused_imports)]
                use ::ruststream::__private::NoHeadersSchemaProbe as _;
                ::ruststream::__private::Probe::<#model>::new().headers_schema_json()
            })
        }
    });
    quote! {
        #(#memberships)*

        impl<__RsM: ::ruststream::runtime::OutSlot> ::ruststream::runtime::OutMessages<__RsM>
            for #name
        where
            #(#models: ::ruststream::runtime::OutMessage<__RsM>,)*
        {
            fn outgoing()
                -> ::std::vec::Vec<::ruststream::runtime::OutgoingMessageMetadata>
            {
                ::std::vec![#(#entries),*]
            }
        }
    }
    .into()
}

/// The set enum's models, one per variant: each variant wraps exactly one type, and a
/// duplicate model is rejected (its `publish_typed` index inference would be ambiguous).
fn out_messages_models(data: &syn::DataEnum) -> syn::Result<Vec<&Type>> {
    let mut models: Vec<&Type> = Vec::new();
    for variant in &data.variants {
        let syn::Fields::Unnamed(fields) = &variant.fields else {
            return Err(syn::Error::new_spanned(
                variant,
                "an OutMessages variant wraps exactly one message model: `Variant(Model)`",
            ));
        };
        if fields.unnamed.len() != 1 {
            return Err(syn::Error::new_spanned(
                fields,
                "an OutMessages variant wraps exactly one message model: `Variant(Model)`",
            ));
        }
        let model = &fields.unnamed[0].ty;
        let rendered = model.to_token_stream().to_string();
        if models
            .iter()
            .any(|existing| existing.to_token_stream().to_string() == rendered)
        {
            return Err(syn::Error::new_spanned(
                model,
                "this model is already in the set: a duplicate would make publish_typed's \
                 index inference ambiguous",
            ));
        }
        models.push(model);
    }
    Ok(models)
}

/// One `Type = "channel"` pair of the `#[publishes(..)]` attribute.
struct PublishesEntry {
    ty: Type,
    /// The channel value: a string literal, or a const-evaluable `&'static str` expression (an
    /// associated const initializer cannot read a `static`).
    channel: Expr,
}

impl Parse for PublishesEntry {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let ty: Type = input.parse()?;
        input.parse::<Token![=]>()?;
        let channel: Expr = input.parse()?;
        Ok(Self { ty, channel })
    }
}

/// Parses every `#[publishes(Type = "channel", ..)]` attribute into one dictionary, rejecting a
/// type listed twice (its destination would be ambiguous at the publish site).
fn publishes_dictionary(attrs: &[Attribute]) -> syn::Result<Vec<PublishesEntry>> {
    let mut dictionary: Vec<PublishesEntry> = Vec::new();
    for attr in attrs
        .iter()
        .filter(|attr| attr.path().is_ident("publishes"))
    {
        let entries =
            attr.parse_args_with(Punctuated::<PublishesEntry, Token![,]>::parse_terminated)?;
        for entry in entries {
            let name = entry.ty.to_token_stream().to_string();
            if dictionary
                .iter()
                .any(|existing| existing.ty.to_token_stream().to_string() == name)
            {
                return Err(syn::Error::new_spanned(
                    &entry.ty,
                    "this type is already in the slot's publish dictionary: one type maps to \
                     one channel per slot (use a second slot for a second destination)",
                ));
            }
            dictionary.push(entry);
        }
    }
    Ok(dictionary)
}
