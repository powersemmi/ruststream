//! The lane derives: `Deserialized` (a self-deserializing input) and `Serialized` (a
//! self-serialized outgoing value - a reply, or a typed publish).
//!
//! Both are sugar over short public-trait impls (see the core traits' rustdoc for the
//! hand-written form). Without `#[wire(..)]` they cover the obvious shape - a newtype or
//! single-field struct over the bytes. With it they cover a type that serializes itself: the
//! attribute names the format's own encode and decode functions, and the expansion calls them.
//! Naming the functions rather than depending on the format's crate is what keeps a binary
//! protocol out of the core: Protobuf, Cap'n Proto and a hand-rolled frame all arrive the same
//! way, and none of them is a cargo feature here.

use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{ToTokens, quote};
use syn::{
    Attribute, Data, DeriveInput, Error, Expr, ExprPath, Fields, GenericParam, Lifetime, Path,
    Token, Type, punctuated::Punctuated,
};

/// The one field of a lane-derive struct: its accessor tokens (`0` or the name) and its type.
fn single_field(input: &DeriveInput, derive: &str) -> syn::Result<(TokenStream2, Type)> {
    let Data::Struct(data) = &input.data else {
        return Err(Error::new_spanned(
            &input.ident,
            format!("{derive} is derived on a struct with exactly one field"),
        ));
    };
    let mut fields = match &data.fields {
        Fields::Named(named) => named.named.iter(),
        Fields::Unnamed(unnamed) => unnamed.unnamed.iter(),
        Fields::Unit => {
            return Err(Error::new_spanned(
                &input.ident,
                format!("{derive} is derived on a struct with exactly one field, not a unit one"),
            ));
        }
    };
    let field = fields.next().ok_or_else(|| {
        Error::new_spanned(
            &input.ident,
            format!("{derive} is derived on a struct with exactly one field"),
        )
    })?;
    if let Some(extra) = fields.next() {
        return Err(Error::new_spanned(
            extra,
            format!(
                "{derive} is derived on a struct with exactly one field; a type that serializes \
                 itself declares `#[wire(..)]`, and a larger byte shape implements the trait by \
                 hand (see its rustdoc)"
            ),
        ));
    }
    let accessor = field
        .ident
        .as_ref()
        .map_or_else(|| quote!(0), ToTokens::to_token_stream);
    Ok((accessor, field.ty.clone()))
}

/// True when `ty` is syntactically a `&[u8]` reference (any lifetime).
fn is_byte_slice_ref(ty: &Type) -> bool {
    let Type::Reference(reference) = ty else {
        return false;
    };
    if reference.mutability.is_some() {
        return false;
    }
    let Type::Slice(slice) = &*reference.elem else {
        return false;
    };
    matches!(&*slice.elem, Type::Path(path) if path.qself.is_none() && path.path.is_ident("u8"))
}

/// The `#[wire(..)]` declaration: the functions a type that serializes itself is written in
/// terms of. Both halves are optional here because each derive reads only its own.
#[derive(Default)]
struct WireFormat {
    encode: Option<Path>,
    decode: Option<Path>,
}

/// The paths `#[wire(prost)]` stands for.
fn prost_paths() -> (Path, Path) {
    (
        syn::parse_quote!(::prost::Message::encode),
        syn::parse_quote!(::prost::Message::decode),
    )
}

/// Reads every `#[wire(..)]` on the item into one declaration; `None` when the type declares
/// none and so is the plain byte shape.
fn wire_format(attrs: &[Attribute]) -> syn::Result<Option<WireFormat>> {
    let mut wire: Option<WireFormat> = None;
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("wire")) {
        let format = wire.get_or_insert_with(WireFormat::default);
        let args = attr.parse_args_with(Punctuated::<Expr, Token![,]>::parse_terminated)?;
        if args.is_empty() {
            return Err(Error::new_spanned(
                attr,
                "`#[wire(..)]` names the format's functions: `#[wire(encode = <path>, decode = \
                 <path>)]`, or the `#[wire(prost)]` shorthand for both",
            ));
        }
        for arg in args {
            match arg {
                // The shorthand: the one format common enough that every service would otherwise
                // write the same two paths.
                Expr::Path(ExprPath { path, .. }) if path.is_ident("prost") => {
                    let (encode, decode) = prost_paths();
                    set_once(&mut format.encode, encode, &path, "encode")?;
                    set_once(&mut format.decode, decode, &path, "decode")?;
                }
                Expr::Assign(assign) => {
                    let Expr::Path(ExprPath { path: key, .. }) = &*assign.left else {
                        return Err(Error::new_spanned(
                            &assign.left,
                            "`#[wire(..)]` takes `encode = <path>` and `decode = <path>`",
                        ));
                    };
                    let Expr::Path(ExprPath { path: value, .. }) = &*assign.right else {
                        return Err(Error::new_spanned(
                            &assign.right,
                            "a `#[wire(..)]` entry names a function path, with no arguments: \
                             `encode = prost::Message::encode`",
                        ));
                    };
                    if key.is_ident("encode") {
                        set_once(&mut format.encode, value.clone(), key, "encode")?;
                    } else if key.is_ident("decode") {
                        set_once(&mut format.decode, value.clone(), key, "decode")?;
                    } else {
                        return Err(Error::new_spanned(
                            key,
                            "unknown `#[wire(..)]` entry; it takes `encode = <path>`, `decode = \
                             <path>`, and the `prost` shorthand",
                        ));
                    }
                }
                other => {
                    return Err(Error::new_spanned(
                        other,
                        "`#[wire(..)]` names the format's functions: `#[wire(encode = <path>, \
                         decode = <path>)]`, or the `#[wire(prost)]` shorthand for both",
                    ));
                }
            }
        }
    }
    Ok(wire)
}

/// Fills one half of the declaration, rejecting a second one for the same half.
fn set_once(slot: &mut Option<Path>, value: Path, spanned: &Path, half: &str) -> syn::Result<()> {
    if slot.replace(value).is_some() {
        return Err(Error::new_spanned(
            spanned,
            format!("`{half}` is declared twice; a type has one wire format"),
        ));
    }
    Ok(())
}

/// The lifetime a derive may carry: the payload's, or none at all for an owned type.
fn payload_lifetime(input: &DeriveInput, derive: &str) -> syn::Result<Option<()>> {
    let mut lifetimes = input
        .generics
        .params
        .iter()
        .filter(|param| matches!(param, GenericParam::Lifetime(_)));
    let first = lifetimes.next();
    if lifetimes.next().is_some() {
        return Err(Error::new_spanned(
            &input.generics,
            format!("{derive} carries at most one lifetime parameter (the payload's)"),
        ));
    }
    if input
        .generics
        .params
        .iter()
        .any(|param| !matches!(param, GenericParam::Lifetime(_)))
    {
        return Err(Error::new_spanned(
            &input.generics,
            format!(
                "{derive} takes no type or const parameters; implement the traits by hand for a generic shape"
            ),
        ));
    }
    Ok(first.map(|_| ()))
}

pub(crate) fn derive_deserialized(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let name = &input.ident;
    // A distinct name so the GAT's lifetime can never shadow the struct's own.
    let out = Lifetime::new("'__rs_out", Span::call_site());
    let Some(format) = wire_format(&input.attrs)? else {
        let (accessor, field_ty) = single_field(input, "Deserialized")?;
        if !is_byte_slice_ref(&field_ty) {
            return Err(Error::new_spanned(
                &field_ty,
                "#[derive(Deserialized)] covers a struct over the payload view `&'a [u8]`; a \
                 type that deserializes itself declares `#[wire(decode = <path>)]`, and any \
                 other construction implements `Deserialized` (and the `Input` spelling) by hand \
                 - see the trait's rustdoc",
            ));
        }
        if payload_lifetime(input, "#[derive(Deserialized)]")?.is_none() {
            return Err(Error::new_spanned(
                &input.generics,
                "#[derive(Deserialized)] over `&'a [u8]` expects exactly one lifetime parameter \
                 (the payload's)",
            ));
        }
        return Ok(quote! {
            impl ::ruststream::runtime::Deserialized for #name<'_> {
                type Output<#out> = #name<#out>;
                type Error = ::core::convert::Infallible;

                fn from_payload(
                    payload: &[u8],
                ) -> ::core::result::Result<#name<'_>, Self::Error> {
                    ::core::result::Result::Ok(#name { #accessor: payload })
                }
            }

            impl ::ruststream::runtime::Input for #name<'_> {
                type Axis = ::ruststream::runtime::SoloDeserialized<#name<'static>>;
            }
        });
    };

    let Some(decode) = format.decode else {
        return Err(Error::new_spanned(
            &input.ident,
            "this type's `#[wire(..)]` names no `decode`; #[derive(Deserialized)] needs \
             `decode = <path>` (or the `#[wire(prost)]` shorthand)",
        ));
    };
    // The borrowing form is the same expansion with the lifetime threaded through, so a
    // zero-copy reader (flatbuffers, capnp) rides the attribute as readily as an owned message.
    let borrowing = payload_lifetime(input, "#[derive(Deserialized)]")?.is_some();
    let (self_ty, output, constructed, axis) = if borrowing {
        (
            quote!(#name<'_>),
            quote!(#name<#out>),
            quote!(#name<'_>),
            quote!(#name<'static>),
        )
    } else {
        (quote!(#name), quote!(Self), quote!(Self), quote!(#name))
    };
    Ok(quote! {
        impl ::ruststream::runtime::Deserialized for #self_ty {
            type Output<#out> = #output;
            // Erased: the derive cannot name the format's own error, and the construction
            // failure is reported, never matched on.
            type Error = ::std::boxed::Box<
                dyn ::std::error::Error + ::core::marker::Send + ::core::marker::Sync,
            >;

            fn from_payload(
                payload: &[u8],
            ) -> ::core::result::Result<#constructed, Self::Error> {
                let __rs_value: #constructed =
                    ::ruststream::runtime::DecodeOutcome::finish(#decode(payload))?;
                ::core::result::Result::Ok(__rs_value)
            }
        }

        impl ::ruststream::runtime::Input for #self_ty {
            type Axis = ::ruststream::runtime::SoloDeserialized<#axis>;
        }
    })
}

pub(crate) fn derive_serialized(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let name = &input.ident;
    if !input.generics.params.is_empty() {
        return Err(Error::new_spanned(
            &input.generics,
            "#[derive(Serialized)] covers a type with no generic parameters; implement \
             `Serialized` (and its wire spellings) by hand for a generic shape - see the trait's \
             rustdoc",
        ));
    }
    // The spellings that route the type onto the serialized wire, identical for both forms: the
    // bytes differ, where they may be used does not.
    let wires = quote! {
        impl ::ruststream::runtime::MessageWire for #name {
            type Wire = ::ruststream::runtime::SerializedWire;
        }

        impl ::ruststream::runtime::ReplyShape for #name {
            type Body = Self;
            type Headers = ();
            type Wire = ::ruststream::runtime::SerializedReply;
        }
    };
    let Some(format) = wire_format(&input.attrs)? else {
        let (accessor, _field_ty) = single_field(input, "Serialized")?;
        return Ok(quote! {
            impl ::ruststream::runtime::Serialized for #name {
                type Error = ::core::convert::Infallible;

                fn wire_bytes<'__rs_wire>(
                    &'__rs_wire self,
                    _buf: &'__rs_wire mut ::ruststream::BytesMut,
                ) -> ::core::result::Result<&'__rs_wire [u8], Self::Error> {
                    ::core::result::Result::Ok(&self.#accessor)
                }
            }

            #wires
        });
    };

    let Some(encode) = format.encode else {
        return Err(Error::new_spanned(
            &input.ident,
            "this type's `#[wire(..)]` names no `encode`; #[derive(Serialized)] needs \
             `encode = <path>` (or the `#[wire(prost)]` shorthand)",
        ));
    };
    Ok(quote! {
        impl ::ruststream::runtime::Serialized for #name {
            type Error = ::ruststream::runtime::SerializePayloadError;

            fn wire_bytes<'__rs_wire>(
                &'__rs_wire self,
                buf: &'__rs_wire mut ::ruststream::BytesMut,
            ) -> ::core::result::Result<&'__rs_wire [u8], Self::Error> {
                // Straight into the publish path's buffer: the value is encoded once, and
                // nothing intermediate is allocated.
                ::ruststream::runtime::EncodeOutcome::finish(#encode(self, &mut *buf))?;
                ::core::result::Result::Ok(&buf[..])
            }
        }

        #wires
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The rejected shapes are the reason this module has unit tests at all: a compile-fail
    /// snapshot pins the wording of one of them at a time, on one toolchain, opt-in - and the
    /// arms themselves are ordinary functions.
    fn rejection(result: syn::Result<TokenStream2>) -> String {
        result.expect_err("this shape is rejected").to_string()
    }

    /// The same for the attribute itself. Spelled as a match rather than `expect_err` because
    /// `syn` provides `Debug` for a parsed path only under `extra-traits`, which is off here.
    fn wire_rejection(input: &DeriveInput) -> String {
        match wire_format(&input.attrs) {
            Ok(_) => panic!("this declaration is rejected"),
            Err(err) => err.to_string(),
        }
    }

    #[test]
    fn a_type_with_no_attribute_is_the_byte_shape() {
        let input: DeriveInput = syn::parse_quote!(
            struct Export(Vec<u8>);
        );

        assert!(
            wire_format(&input.attrs)
                .expect("no attribute is not a failure")
                .is_none()
        );
        assert!(derive_serialized(&input).is_ok());
    }

    #[test]
    fn the_prost_shorthand_fills_both_halves() {
        let input: DeriveInput = syn::parse_quote!(
            #[wire(prost)]
            struct Order {
                id: u64,
            }
        );
        let format = wire_format(&input.attrs)
            .expect("the shorthand parses")
            .expect("the shorthand is a declaration");

        assert!(format.encode.is_some());
        assert!(format.decode.is_some());
        assert!(derive_serialized(&input).is_ok());
        assert!(derive_deserialized(&input).is_ok());
    }

    #[test]
    fn the_two_halves_may_be_named_one_at_a_time() {
        let input: DeriveInput = syn::parse_quote!(
            #[wire(encode = write_order)]
            #[wire(decode = read_order)]
            struct Order {
                id: u64,
            }
        );

        assert!(derive_serialized(&input).is_ok());
        assert!(derive_deserialized(&input).is_ok());
    }

    #[test]
    fn a_borrowing_view_may_declare_its_own_reader() {
        let input: DeriveInput = syn::parse_quote!(
            #[wire(decode = read_frame)]
            struct Frame<'a> {
                root: &'a str,
            }
        );

        assert!(derive_deserialized(&input).is_ok());
    }

    #[test]
    fn an_empty_attribute_names_what_it_takes() {
        let input: DeriveInput = syn::parse_quote!(
            #[wire()]
            struct Order {
                id: u64,
            }
        );

        assert!(wire_rejection(&input).contains("names the format's functions"));
    }

    #[test]
    fn an_unknown_entry_is_rejected() {
        let input: DeriveInput = syn::parse_quote!(
            #[wire(frame = write_order)]
            struct Order {
                id: u64,
            }
        );

        assert!(wire_rejection(&input).contains("unknown `#[wire(..)]` entry"));
    }

    #[test]
    fn an_entry_that_is_not_an_assignment_is_rejected() {
        let input: DeriveInput = syn::parse_quote!(
            #[wire(protobuf)]
            struct Order {
                id: u64,
            }
        );

        assert!(wire_rejection(&input).contains("names the format's functions"));
    }

    #[test]
    fn an_entry_whose_key_is_not_a_name_is_rejected() {
        let input: DeriveInput = syn::parse_quote!(
            #[wire(1 = write_order)]
            struct Order {
                id: u64,
            }
        );

        assert!(wire_rejection(&input).contains("`encode = <path>` and `decode = <path>`"));
    }

    #[test]
    fn an_entry_whose_value_is_not_a_path_is_rejected() {
        let input: DeriveInput = syn::parse_quote!(
            #[wire(encode = write_order(1))]
            struct Order {
                id: u64,
            }
        );

        assert!(wire_rejection(&input).contains("names a function path"));
    }

    #[test]
    fn a_half_declared_twice_is_rejected() {
        let input: DeriveInput = syn::parse_quote!(
            #[wire(encode = write_order, encode = write_order_v2)]
            struct Order {
                id: u64,
            }
        );

        assert!(wire_rejection(&input).contains("declared twice"));
    }

    #[test]
    fn the_shorthand_does_not_stack_on_an_explicit_half() {
        let input: DeriveInput = syn::parse_quote!(
            #[wire(encode = write_order, prost)]
            struct Order {
                id: u64,
            }
        );

        assert!(wire_rejection(&input).contains("declared twice"));
    }

    #[test]
    fn each_derive_demands_its_own_half() {
        let outgoing: DeriveInput = syn::parse_quote!(
            #[wire(encode = write_order)]
            struct Order {
                id: u64,
            }
        );
        let incoming: DeriveInput = syn::parse_quote!(
            #[wire(decode = read_order)]
            struct Order {
                id: u64,
            }
        );

        assert!(rejection(derive_deserialized(&outgoing)).contains("names no `decode`"));
        assert!(rejection(derive_serialized(&incoming)).contains("names no `encode`"));
    }

    #[test]
    fn the_byte_shape_is_one_field_of_the_right_type() {
        let unit: DeriveInput = syn::parse_quote!(
            struct Export;
        );
        let two: DeriveInput = syn::parse_quote!(
            struct Export(Vec<u8>, u32);
        );
        let enumeration: DeriveInput = syn::parse_quote!(
            enum Export {
                Bytes(Vec<u8>),
            }
        );
        let decoded: DeriveInput = syn::parse_quote!(
            struct Order {
                id: u64,
            }
        );
        let mutable: DeriveInput = syn::parse_quote!(
            struct Frame<'a>(&'a mut [u8]);
        );
        let not_bytes: DeriveInput = syn::parse_quote!(
            struct Frame<'a>(&'a [u16]);
        );

        assert!(rejection(derive_serialized(&unit)).contains("not a unit one"));
        assert!(rejection(derive_serialized(&two)).contains("exactly one field"));
        assert!(rejection(derive_serialized(&enumeration)).contains("exactly one field"));
        assert!(rejection(derive_deserialized(&decoded)).contains("payload view"));
        assert!(rejection(derive_deserialized(&mutable)).contains("payload view"));
        assert!(rejection(derive_deserialized(&not_bytes)).contains("payload view"));
    }

    #[test]
    fn the_generic_shapes_are_rejected_with_their_own_reason() {
        let outgoing: DeriveInput = syn::parse_quote!(
            struct Export<T>(T);
        );
        let owned_view: DeriveInput = syn::parse_quote!(
            struct Frame(&'static [u8]);
        );
        let two_lifetimes: DeriveInput = syn::parse_quote!(
            #[wire(decode = read_frame)]
            struct Frame<'a, 'b>(&'a [u8], &'b [u8]);
        );
        let type_parameter: DeriveInput = syn::parse_quote!(
            #[wire(decode = read_frame)]
            struct Frame<'a, T>(&'a [u8], T);
        );

        assert!(rejection(derive_serialized(&outgoing)).contains("no generic parameters"));
        assert!(rejection(derive_deserialized(&owned_view)).contains("exactly one lifetime"));
        assert!(rejection(derive_deserialized(&two_lifetimes)).contains("at most one lifetime"));
        assert!(rejection(derive_deserialized(&type_parameter)).contains("no type or const"));
    }
}
