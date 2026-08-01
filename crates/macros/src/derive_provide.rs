//! `#[derive(Provide)]` — emits one `bombay::caps::Provide<FieldTy>` impl
//! per named field of a capability-set struct (ADR-0026 Addendum: the
//! open seam of the caps encoding). See card #278.
//!
//! Deliberately NOT named `CapSet`: this derive does not (and cannot)
//! generate `CapSet::build` — building needs policy knowledge only the
//! user has; a build-generating derive is card #243. Duplicate field
//! TYPES produce overlapping `Provide` impls and are rejected by
//! coherence (E0119) — the intended duplicate-capability guard.
//!
//! It ALSO emits the loop-participation half (ADR-0026 stage 2, card #279):
//! one `bombay::caps::Replay<Msg>` impl so the `Shell` can drain in-step
//! replay uniformly. A `Stashing<M>` field forwards to its own `Replay`; a
//! set with no stash field yields `None`. This is the forget-proof wiring
//! for the `Stashing` capability — a stash you cannot forget to service.

use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::{
    Data, DeriveInput, Fields, GenericArgument, Ident, PathArguments, Type, parse::Parse,
    parse::ParseStream,
};

/// If `ty` is written `…::Stashing<M>` (any leading path), returns `M`.
///
/// The one deliberate core-type coupling of this derive: it recognizes the
/// stash capability **structurally** so it can emit that field's loop
/// participation ([`Replay`](bombay::caps::Replay)) alongside its `Provide`.
/// Recognizing it structurally — rather than via a `#[stash]` attribute — is
/// what keeps replay forget-proof: you cannot hold a `Stashing<M>` field the
/// derive fails to service. An *alias* (`type S = Stashing<X>`) is NOT
/// recognized; write the type directly.
fn stash_message_ty(ty: &Type) -> Option<&Type> {
    let Type::Path(type_path) = ty else {
        return None;
    };
    let segment = type_path.path.segments.last()?;
    if segment.ident != "Stashing" {
        return None;
    }
    let PathArguments::AngleBracketed(args) = &segment.arguments else {
        return None;
    };
    args.args.iter().find_map(|arg| match arg {
        GenericArgument::Type(inner) => Some(inner),
        _ => None,
    })
}

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

        // The loop-participation half (ADR-0026 stage 2): every cap set is
        // `Replay<Msg>` so the `Shell` can drain in-step replay uniformly. A
        // `Stashing<M>` field forwards to its own `Replay` impl; a set with no
        // stash field yields `None` for any message type. Emitting exactly one
        // shape (never both) keeps the impls non-overlapping (no E0119).
        let stash_fields: Vec<(&Ident, &Type)> = self
            .fields
            .iter()
            .filter_map(|(field, ty)| stash_message_ty(ty).map(|m| (field, m)))
            .collect();
        if stash_fields.is_empty() {
            tokens.extend(quote! {
                #[automatically_derived]
                impl<__CapsReplayMsg> ::bombay::caps::Replay<__CapsReplayMsg> for #ident {
                    fn next_replay(&mut self) -> ::core::option::Option<__CapsReplayMsg> {
                        ::core::option::Option::None
                    }
                }
            });
        } else {
            for (field, msg) in stash_fields {
                tokens.extend(quote! {
                    #[automatically_derived]
                    impl ::bombay::caps::Replay<#msg> for #ident {
                        fn next_replay(&mut self) -> ::core::option::Option<#msg> {
                            ::bombay::caps::Replay::next_replay(&mut self.#field)
                        }
                    }
                });
            }
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
            syn::parse_str::<DeriveProvide>("struct Caps { stash: Stashing<M>, rate: Limiter }")
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

    #[test]
    fn a_stashing_field_emits_a_concrete_replay_forwarding_to_it() {
        use quote::ToTokens as _;
        let parsed = syn::parse_str::<DeriveProvide>("struct Caps { buf: Stashing<GateMsg> }")
            .expect("valid cap-set struct");
        let out = parsed.to_token_stream().to_string();
        assert_eq!(
            out.matches("impl :: bombay :: caps :: Replay < GateMsg > for Caps")
                .count(),
            1,
            "one concrete Replay<GateMsg>: {out}"
        );
        assert!(
            out.contains(":: bombay :: caps :: Replay :: next_replay (& mut self . buf)"),
            "forwards to the stash field's own Replay: {out}"
        );
        assert!(
            !out.contains("__CapsReplayMsg"),
            "a stash field means NO None-blanket (would overlap, E0119): {out}"
        );
    }

    #[test]
    fn no_stash_field_emits_the_none_blanket() {
        use quote::ToTokens as _;
        let parsed = syn::parse_str::<DeriveProvide>("struct Caps { rate: Limiter, tags: Tags }")
            .expect("valid cap-set struct");
        let out = parsed.to_token_stream().to_string();
        assert_eq!(
            out.matches(
                "impl < __CapsReplayMsg > :: bombay :: caps :: Replay < __CapsReplayMsg > for Caps"
            )
            .count(),
            1,
            "one None-blanket Replay for a stash-less set: {out}"
        );
        assert!(
            out.contains(":: core :: option :: Option :: None"),
            "the blanket yields None: {out}"
        );
    }

    #[test]
    fn a_mixed_set_emits_exactly_one_concrete_replay_and_no_blanket() {
        use quote::ToTokens as _;
        let parsed = syn::parse_str::<DeriveProvide>(
            "struct Caps { buf: Stashing<GateMsg>, rate: Limiter }",
        )
        .expect("valid cap-set struct");
        let out = parsed.to_token_stream().to_string();
        assert_eq!(
            out.matches("impl :: bombay :: caps :: Replay <").count(),
            1,
            "only the stash field participates in replay: {out}"
        );
        assert!(
            !out.contains("__CapsReplayMsg"),
            "mixed set with a stash: concrete impl only, no blanket: {out}"
        );
    }
}
