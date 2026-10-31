//! Expansion of `#[derive(FromRef)]`: generates a `FromRef` impl for each field of an
//! application-state struct, so handlers can inject any field with `State<FieldType>` (no
//! hand-written impl). Because the generated `impl FromRef<State> for FieldType` carries no generic
//! parameter, it is legal even for fields whose type comes from another crate (a broker publisher, a
//! client pool), unlike a per-field `FromContext` impl.

use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::spanned::Spanned;
use syn::{Data, DeriveInput, Field, Fields};

pub(crate) fn expand(input: &DeriveInput) -> syn::Result<TokenStream2> {
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "#[derive(FromRef)] does not support generic state types",
        ));
    }
    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            input,
            "#[derive(FromRef)] supports only structs with named fields",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(syn::Error::new_spanned(
            &data.fields,
            "#[derive(FromRef)] supports only structs with named fields",
        ));
    };

    let name = &input.ident;
    let mut seen: Vec<String> = Vec::new();
    let mut impls = Vec::new();
    for field in &fields.named {
        if field_skipped(field)? {
            continue;
        }
        let ident = field.ident.as_ref().expect("named field");
        let ty = &field.ty;

        // Two fields of the same type would generate conflicting `FromRef` impls: injection by type
        // would be ambiguous, so reject it with a clear span instead.
        let key = quote!(#ty).to_string();
        if seen.contains(&key) {
            return Err(syn::Error::new(
                ty.span(),
                "two fields share a type, so injection by type is ambiguous; mark one \
                 `#[from_ref(skip)]`",
            ));
        }
        seen.push(key);

        impls.push(quote! {
            #[automatically_derived]
            impl ::ruststream::runtime::FromRef<#name> for #ty {
                fn from_ref(__rs_state: &#name) -> Self {
                    ::core::clone::Clone::clone(&__rs_state.#ident)
                }
            }
        });
    }

    if impls.is_empty() {
        return Err(syn::Error::new_spanned(
            input,
            "#[derive(FromRef)] generated no impls: every field is `#[from_ref(skip)]`",
        ));
    }
    Ok(quote!(#(#impls)*))
}

/// Whether a field opts out with `#[from_ref(skip)]` (a field that is not a dependency, or one whose
/// type another field already claims).
fn field_skipped(field: &Field) -> syn::Result<bool> {
    let mut skip = false;
    for attr in &field.attrs {
        if attr.path().is_ident("from_ref") {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("skip") {
                    skip = true;
                    Ok(())
                } else {
                    Err(meta.error("unknown `#[from_ref]` option; expected `skip`"))
                }
            })?;
        }
    }
    Ok(skip)
}

#[cfg(test)]
mod tests {
    use syn::parse_quote;

    use super::*;

    fn refusal(input: &DeriveInput) -> String {
        match expand(input) {
            Ok(_) => panic!("this state is refused"),
            Err(err) => err.to_string(),
        }
    }

    #[test]
    fn a_state_is_a_plain_struct_with_named_fields() {
        let generic: DeriveInput = parse_quote!(
            struct AppState<Db> {
                db: Db,
            }
        );
        let enumeration: DeriveInput = parse_quote!(
            enum AppState {
                Up,
            }
        );
        let tuple: DeriveInput = parse_quote!(
            struct AppState(Db);
        );

        assert_eq!(
            refusal(&generic),
            "#[derive(FromRef)] does not support generic state types"
        );
        assert_eq!(
            refusal(&enumeration),
            "#[derive(FromRef)] supports only structs with named fields"
        );
        assert_eq!(
            refusal(&tuple),
            "#[derive(FromRef)] supports only structs with named fields"
        );
    }

    #[test]
    fn each_injected_type_comes_from_one_field() {
        let shared: DeriveInput = parse_quote!(
            struct AppState {
                orders: Pool,
                invoices: Pool,
            }
        );
        let one_skipped: DeriveInput = parse_quote!(
            struct AppState {
                orders: Pool,
                #[from_ref(skip)]
                invoices: Pool,
            }
        );

        assert_eq!(
            refusal(&shared),
            "two fields share a type, so injection by type is ambiguous; mark one \
             `#[from_ref(skip)]`"
        );
        assert!(
            expand(&one_skipped).is_ok(),
            "a skipped field claims no type"
        );
    }

    #[test]
    fn a_state_injects_at_least_one_field_and_knows_only_skip() {
        let all_skipped: DeriveInput = parse_quote!(
            struct AppState {
                #[from_ref(skip)]
                orders: Pool,
            }
        );
        let unknown: DeriveInput = parse_quote!(
            struct AppState {
                #[from_ref(rename)]
                orders: Pool,
            }
        );

        assert_eq!(
            refusal(&all_skipped),
            "#[derive(FromRef)] generated no impls: every field is `#[from_ref(skip)]`"
        );
        assert_eq!(
            refusal(&unknown),
            "unknown `#[from_ref]` option; expected `skip`"
        );
    }
}
