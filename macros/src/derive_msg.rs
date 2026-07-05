//! `#[derive(Msg)]` — implements the `Msg` marker trait and (from Task 3) emits
//! a compile-time slot-size tripwire. See card #114 and the design spec.

use proc_macro2::TokenStream;
use quote::{ToTokens, quote};
use syn::{
    DeriveInput, Ident,
    parse::{Parse, ParseStream},
};

/// A parsed `#[derive(Msg)]` input: the message type's identifier.
pub struct DeriveMsg {
    ident: Ident,
}

impl Parse for DeriveMsg {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let derive: DeriveInput = input.parse()?;
        Ok(Self {
            ident: derive.ident,
        })
    }
}

impl ToTokens for DeriveMsg {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let ident = &self.ident;
        let over_budget = format!(
            "`{ident}` exceeds its Msg::SLOT_BUDGET — box the largest variant \
             (as Signal boxes LinkDied), or raise it with #[msg(budget = N)]"
        );
        tokens.extend(quote! {
            #[automatically_derived]
            impl ::bombay_core::message::Msg for #ident {}

            const _: () = ::core::assert!(
                ::core::mem::size_of::<#ident>()
                    <= <#ident as ::bombay_core::message::Msg>::SLOT_BUDGET,
                #over_budget
            );
        });
    }
}
