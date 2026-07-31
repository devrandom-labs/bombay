//! `#[derive(Provide)]` — emits one `bombay::caps::Provide<FieldTy>` impl
//! per named field of a capability-set struct (ADR-0026 Addendum: the
//! open seam of the caps encoding). See card #278.
//!
//! Deliberately NOT named `CapSet`: this derive does not (and cannot)
//! generate `CapSet::build` — building needs policy knowledge only the
//! user has; a build-generating derive is card #243. Duplicate field
//! TYPES produce overlapping `Provide` impls and are rejected by
//! coherence (E0119) — the intended duplicate-capability guard.

use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::{Data, DeriveInput, Fields, Ident, Type, parse::Parse, parse::ParseStream};

/// A parsed `#[derive(Provide)]` input: the struct identifier and its
/// named fields.
#[derive(Debug)]
pub struct DeriveProvide {
    ident: Ident,
    fields: Vec<(Ident, Type)>,
}

impl Parse for DeriveProvide {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let derive: DeriveInput = input.parse()?;

        if let Some(param) = derive.generics.params.first() {
            return Err(syn::Error::new_spanned(
                param,
                "`#[derive(Provide)]` needs a concrete cap-set struct: a \
                 generic set cannot emit per-field `Provide` impls without \
                 repeating its generics on every impl",
            ));
        }
        let Data::Struct(data) = &derive.data else {
            return Err(syn::Error::new_spanned(
                &derive.ident,
                "`#[derive(Provide)]` supports structs with named fields \
                 only (a capability set is a named struct of capability \
                 fields)",
            ));
        };
        let Fields::Named(named) = &data.fields else {
            return Err(syn::Error::new_spanned(
                &derive.ident,
                "`#[derive(Provide)]` needs NAMED fields: each field is a \
                 capability; tuple/unit structs have nothing to provide",
            ));
        };
        let fields = named
            .named
            .iter()
            .filter_map(|f| f.ident.clone().map(|ident| (ident, f.ty.clone())))
            .collect::<Vec<_>>();
        if fields.is_empty() {
            return Err(syn::Error::new_spanned(
                &derive.ident,
                "`#[derive(Provide)]` on an empty struct is a no-op; a \
                 capability-less actor uses `type Caps = ()` instead",
            ));
        }
        Ok(Self {
            ident: derive.ident,
            fields,
        })
    }
}

impl ToTokens for DeriveProvide {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ident = &self.ident;
        for (field, ty) in &self.fields {
            tokens.extend(quote! {
                #[automatically_derived]
                impl ::bombay::caps::Provide<#ty> for #ident {
                    fn provide(&mut self) -> &mut #ty {
                        &mut self.#field
                    }
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DeriveProvide;

    #[test]
    fn generic_struct_is_rejected() {
        let err = syn::parse_str::<DeriveProvide>("struct S<T> { a: T }").unwrap_err();
        assert!(
            err.to_string().contains("concrete cap-set struct"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn enum_is_rejected() {
        let err = syn::parse_str::<DeriveProvide>("enum E { A }").unwrap_err();
        assert!(
            err.to_string().contains("structs with named fields"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn union_is_rejected() {
        let err = syn::parse_str::<DeriveProvide>("union U { a: u32 }").unwrap_err();
        assert!(
            err.to_string().contains("structs with named fields"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn tuple_struct_is_rejected() {
        let err = syn::parse_str::<DeriveProvide>("struct S(u32);").unwrap_err();
        assert!(
            err.to_string().contains("NAMED fields"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn unit_struct_is_rejected() {
        let err = syn::parse_str::<DeriveProvide>("struct S;").unwrap_err();
        assert!(
            err.to_string().contains("NAMED fields"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn empty_struct_is_rejected() {
        let err = syn::parse_str::<DeriveProvide>("struct S {}").unwrap_err();
        assert!(err.to_string().contains("no-op"), "unexpected error: {err}");
    }

    #[test]
    fn one_provide_impl_per_field_is_emitted() {
        use quote::ToTokens as _;
        let parsed =
            syn::parse_str::<DeriveProvide>("struct Caps { stash: Stashing, rate: Limiter }")
                .expect("valid cap-set struct");
        let out = parsed.to_token_stream().to_string();
        assert_eq!(
            out.matches("impl :: bombay :: caps :: Provide <").count(),
            2,
            "exactly one Provide impl per field: {out}"
        );
        assert!(out.contains("& mut self . stash"), "field access: {out}");
        assert!(out.contains("& mut self . rate"), "field access: {out}");
    }
}
