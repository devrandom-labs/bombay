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
        tokens.extend(quote! {
            #[automatically_derived]
            impl ::bombay_core::message::Msg for #ident {}
        });
    }
}
