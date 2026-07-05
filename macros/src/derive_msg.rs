//! `#[derive(Msg)]` — implements the `Msg` marker trait and (from Task 3) emits
//! a compile-time slot-size tripwire. See card #114 and the design spec.

use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::{
    Attribute, Data, DeriveInput, Ident, LitInt,
    parse::{Parse, ParseStream},
};

/// A parsed `#[derive(Msg)]` input: the type's identifier and an optional
/// per-type slot budget from `#[msg(budget = N)]`.
#[derive(Debug)]
pub struct DeriveMsg {
    ident: Ident,
    budget: Option<usize>,
}

impl Parse for DeriveMsg {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let derive: DeriveInput = input.parse()?;

        // NB: the `compile_fail` doctest for generics can't regression-test this guard
        // (an un-guarded derive also fails to compile a generic, for a different reason);
        // `generic_type_is_rejected` below is the real guard test.
        if let Some(param) = derive.generics.params.first() {
            return Err(syn::Error::new_spanned(
                param,
                "`#[derive(Msg)]` needs a concrete type: the slot-size tripwire \
                 cannot size an unmonomorphized generic",
            ));
        }
        if let Data::Union(data) = &derive.data {
            return Err(syn::Error::new_spanned(
                data.union_token,
                "`#[derive(Msg)]` supports structs and enums, not unions",
            ));
        }

        let budget = parse_budget(&derive.attrs)?;
        Ok(Self {
            ident: derive.ident,
            budget,
        })
    }
}

impl ToTokens for DeriveMsg {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ident = &self.ident;
        let budget_const = self
            .budget
            .map(|n| quote! { const SLOT_BUDGET: usize = #n; });
        let over_budget = format!(
            "`{ident}` exceeds its Msg::SLOT_BUDGET — box the largest variant \
             (as Signal boxes LinkDied), or raise it with #[msg(budget = N)]"
        );
        tokens.extend(quote! {
            #[automatically_derived]
            impl ::bombay_core::message::Msg for #ident {
                #budget_const
            }

            const _: () = ::core::assert!(
                ::core::mem::size_of::<#ident>()
                    <= <#ident as ::bombay_core::message::Msg>::SLOT_BUDGET,
                #over_budget
            );
        });
    }
}

/// Extracts `budget = N` from `#[msg(...)]` attributes, if present. Errors on a
/// non-integer value, a bare `budget`, any key other than `budget`, or a
/// duplicate `budget`.
fn parse_budget(attrs: &[Attribute]) -> syn::Result<Option<usize>> {
    let mut budget = None;
    for attr in attrs.iter().filter(|attr| attr.path().is_ident("msg")) {
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("budget") {
                if budget.is_some() {
                    return Err(meta.error("duplicate `budget`; specify it once"));
                }
                // `-1` is rejected because it tokenizes as `-` + LitInt (so LitInt::parse fails),
                // and an out-of-range literal fails base10_parse::<usize>() — both surface as
                // clean syn::Errors, not an explicit sign/range check here.
                budget = Some(meta.value()?.parse::<LitInt>()?.base10_parse()?);
                Ok(())
            } else {
                Err(meta.error("unknown `msg` key; the only key is `budget`"))
            }
        })?;
    }
    Ok(budget)
}

#[cfg(test)]
mod tests {
    use super::DeriveMsg;
    use super::parse_budget;
    use syn::{Attribute, parse_quote};

    fn attrs(attr: Attribute) -> Vec<Attribute> {
        vec![attr]
    }

    #[test]
    fn generic_type_is_rejected() {
        let err = syn::parse_str::<DeriveMsg>("enum Generic<T> { A(T) }").unwrap_err();
        assert!(
            err.to_string().contains("concrete type"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn union_type_is_rejected() {
        let err = syn::parse_str::<DeriveMsg>("union U { a: u32, b: u64 }").unwrap_err();
        assert!(
            err.to_string().contains("not unions"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn budget_attribute_yields_its_value() {
        let parsed = parse_budget(&attrs(parse_quote!(#[msg(budget = 8192)]))).unwrap();
        assert_eq!(parsed, Some(8192));
    }

    #[test]
    fn absent_attribute_yields_none() {
        let parsed = parse_budget(&attrs(parse_quote!(#[derive(Clone)]))).unwrap();
        assert_eq!(parsed, None);
    }

    #[test]
    fn non_integer_budget_is_an_error() {
        assert!(parse_budget(&attrs(parse_quote!(#[msg(budget = "x")]))).is_err());
    }

    #[test]
    fn bare_budget_without_value_is_an_error() {
        assert!(parse_budget(&attrs(parse_quote!(#[msg(budget)]))).is_err());
    }

    #[test]
    fn unknown_msg_key_is_an_error() {
        let err = parse_budget(&attrs(parse_quote!(#[msg(limit = 8)]))).unwrap_err();
        assert!(
            err.to_string().contains("unknown"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn duplicate_budget_is_an_error() {
        // repeated key within one #[msg(...)]
        let err = parse_budget(&attrs(parse_quote!(#[msg(budget = 1, budget = 2)]))).unwrap_err();
        assert!(
            err.to_string().contains("duplicate"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn duplicate_budget_across_attrs_is_an_error() {
        let a: Attribute = parse_quote!(#[msg(budget = 1)]);
        let b: Attribute = parse_quote!(#[msg(budget = 2)]);
        let err = parse_budget(&[a, b]).unwrap_err();
        assert!(
            err.to_string().contains("duplicate"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn negative_budget_is_an_error() {
        assert!(parse_budget(&attrs(parse_quote!(#[msg(budget = -1)]))).is_err());
    }

    #[test]
    fn overflowing_budget_is_an_error() {
        assert!(
            parse_budget(&attrs(
                parse_quote!(#[msg(budget = 999999999999999999999999999999)])
            ))
            .is_err()
        );
    }
}
